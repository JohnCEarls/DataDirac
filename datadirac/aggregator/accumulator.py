import random
import boto.sqs
import time
import boto
import json
import numpy as np
import tempfile
import hashlib
from collections import defaultdict
from boto.sqs.message import Message
from boto.s3.key import Key
import base64
import datetime
import resultset
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
import os.path
from datadirac.data import NetworkInfo
from masterdirac.models.aggregator import (  TruthGPUDiracModel,
        RunGPUDiracModel, DataForDisplay  )
import logging
import masterdirac.models.run as run_mdl
from mpi4py import MPI
import pandas
import re

from datadirac.aggregator.resultset import (  TruthException, FileCorruption, DirtyRunException)

class Accumulator(object):
    def __init__(self, run_model, holdout_file=None):
        #TODO: send network info with data
        self.logger = logging.getLogger(__name__)
        self._run_config = run_model
        self._run_id = run_model['run_id']
        self._sqs_data_to_agg = None
        self._s3_from_gpu = None
        self._data_queue = None
        self._s3_results_bucket_name = None
        self._results_bucket = None
        self._truth = None
        self._net_info = None
        self._pathways = None
        self._net_info = None
        self._redrive_queue = None
        self._prev_mess = None
        self._masks = None
        self._mask_ids = None
        self.acc_acc = {}
        self._joined_acc_acc = None
        self.logger.info("Aggregating on %s" % self.run_id)
        for mask_id in self.mask_ids:
            self.acc_acc[mask_id] = None
        self.acc_count = 0
        #DEBUGGING
        self._rec_q_name = None
        self._rec_q = None
        self._holdout_file = holdout_file

    @property
    def s3_from_gpu(self):
        """
        Name of the s3 bucket to pull data from
        """
        if self._s3_from_gpu is None:
            sfg = self.run_config['intercomm_settings']['s3_from_gpu_to_agg']
            self._s3_from_gpu = sfg
            self.logger.debug("s3_from_gpu: %s" % sfg )
        return self._s3_from_gpu

    @property
    def truth(self):
        ctr = 0
        while self._truth is None:
            try:
                self._truth = self._get_truth()
            except:
                self.logger.exception("No truth. waiting")
                time.sleep(2)
                ctr += 1
                if ctr > 5:
                    raise
        return self._truth

    @property
    def masks(self):
        """
        Tuple based representation of masks

        [(0,10), (1,11), (2,12) ... ]
        """
        if self._masks is None:
            masks = self.run_config['aggregator_settings']['masks']
            self._masks = self._parse_masks( masks )
            self.logger.debug("self.masks : %r " % self._masks )
        return self._masks

    def _parse_masks(self, masks):
        m = re.compile( resultset.MASK_PATTERN_PARSE )
        return m.findall( masks )

    @property
    def mask_ids(self):
        """
        String based representation of masks

        ['(0,10)', '(1,11)', '(2,12)' ... ]
        """
        if self._mask_ids is None:
            mask_ids = self.run_config['aggregator_settings']['masks']
            self._mask_ids = self._parse_mask_ids( mask_ids )
            self.logger.debug("self.masks : %r " % self._mask_ids )
        return self._mask_ids

    def _parse_mask_ids(self, mask_ids):
        m = re.compile( resultset.MASK_PATTERN_MATCH )
        return m.findall( mask_ids )

    @property
    def sqs_data_to_agg(self):
        """
        Name of the sqs queue containing run informations
        """
        if self._sqs_data_to_agg is None:
            sd2a =  self.run_config['intercomm_settings']['sqs_from_data_to_agg']
            self._sqs_data_to_agg = sd2a
            msg = "self. sqs_data_to_agg: %s" % self._sqs_data_to_agg
            self.logger.debug( msg )
            ##DEBUG
            self._rec_q_name = '%s-bak' % sd2a
        return self._sqs_data_to_agg

    @property
    def results_bucket_name(self):
        """
        Name of the bucket that contains the result(pvals) files
        """
        if self._s3_results_bucket_name is None:
            rb =  self.run_config['aggregator_settings']['results_bucket']
            self._s3_results_bucket_name = rb
            msg = "self.results_bucket_name: %s" % rb
            self.logger.debug( msg )
        return self._s3_results_bucket_name

    def _set_redrive_policy(self):
        """
        Set the redrive policy on the data queue to 3
        """
        dq = self.data_queue
        rq = self.redrive_queue
        
        if not dq.get_attributes('RedrivePolicy'):
            policy = {  "maxReceiveCount" : 3,
                    "deadLetterTargetArn" : rq.arn }
            policy = json.dumps( policy )
            self.logger.debug("redrive policy set: %s" % policy )
            dq.set_attribute('RedrivePolicy', policy)

    @property
    def data_queue(self):
        """
        SQS queue object that contains the data specifications
        """
        ctr = 0
        while not self._data_queue:
            try:
                conn = boto.sqs.connect_to_region('us-east-1')
                self._data_queue = conn.get_queue(self.sqs_data_to_agg)
                self._rec_q = conn.create_queue(self._rec_q_name)
                self.logger.debug("Connected data_queue")
            except:
                self.logger.exception("Attempt to get data queue failed")
                time.sleep(2)
                ctr += 1
                if ctr > 10:
                    raise
                else:
                    self.logger.warning("retrying")
        return self._data_queue

    @property
    def redrive_queue(self):
        ctr = 0
        while not self._redrive_queue:
            try:
                conn = boto.sqs.connect_to_region('us-east-1')
                rqn = '%s-rdq' % self.sqs_data_to_agg
                self._redrive_queue = conn.create_queue( rqn )
                self.logger.debug("Connected Redrive Queue")
            except:
                self.logger.exception("Attempt to get redrive queue failed")
                time.sleep(2)
                ctr += 1
                if ctr > 10:
                    raise
                else:
                    self.logger.warning("retrying")
        return self._redrive_queue
     
    def get_result_set(self):
        while self.data_queue.count() > 0:
            m = self.data_queue.read(30)
            if m:
                #put message away for future consumption
                inst = json.loads( m.get_body() )
                s3r = resultset.S3ResultSet( inst, self.s3_from_gpu )
                self.logger.debug("Got ResultSet")
                self._prev_mess = m
                return s3r
            else:
                self._data_queue = None
        return None

    def success(self):
        if self._prev_mess:
            try:
                #DEBUG
                self._rec_q.write(Message(body=self._prev_mess.get_body()))
                self.data_queue.delete_message( self._prev_mess )
                self.logger.debug("Deleting message")
                self._prev_mess = None
            except:
                message = "Error attempting to delete message"
                self.logger.exception(message)

    def handle_result_set(self, rs):
        self._handle_permutation( rs )

    def _get_truth( self):
        truth = {}
        for item in TruthGPUDiracModel.query(self.run_id):
            try:
                if item.strain_id in self.mask_ids:
                    truth[item.strain_id] = self._load_np( item.accuracy_file )
            except:
                pass
        return truth

    def _load_np( self, truth_s3_file ):
        k = Key(self.results_bucket)
        k.key = truth_s3_file
        with tempfile.SpooledTemporaryFile() as temp:
            k.get_contents_to_file(temp)
            temp.seek(0)
            accuracy = np.load(temp)
        self.logger.debug( "Loaded %s" % truth_s3_file )
        return accuracy

    def _handle_permutation(self, rs):
        for mid, m in zip(self.mask_ids, self.masks):
            self.logger.debug("Handling perm for %s" % mid)
            masked_rs = resultset.Masked( rs, m, self._holdout_file )
            accuracy = masked_rs.accuracy
            if self.acc_acc[mid] is None:
                self.acc_acc[mid] =  np.zeros_like(accuracy, dtype=int)
            self.acc_acc[mid] += (self.truth[mid] <= accuracy)
        self.acc_count += 1

    def _handle_truth( self, rs ):
        with TruthGPUDiracModel.batch_write() as tgdModel:
            base_key = '%s/truth-accuracy/%s'
            if self._truth is None:
                self._truth = {}#hacky, make setter
            for mid, m in zip(self.mask_ids, self.masks):
                masked_rs = resultset.Masked( rs, m, self._holdout_file )
                acc = self._truth[mid] = masked_rs.accuracy
                r_key = self._save_result_to_s3( base_key, acc )
                rf =  base64.b64encode( json.dumps( rs.get_result_files() ) )
                ts = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
                tgdModel.save(TruthGPUDiracModel( masked_rs.run_id, mid,
                        accuracy_file = r_key,
                        result_files = rf,
                        bucket = self.results_bucket_name,
                        timestamp = ts
                        ))
                self.logger.debug("Writing %s" % (r_key) )
        return self.truth

    def _save_result_to_s3( self, base_key, accuracy ):
        """
        Takes a base_key (templated string that accepts 2 strings, 
        the run_id and the file name
        accuracy, which an a numpy array containing the accuracy of this
        masked resultset for all networks
        returns the key name
        """
        key_name = None
        with tempfile.SpooledTemporaryFile() as temp:
            np.save(temp, accuracy)
            temp.seek(0)
            k = Key(self.results_bucket)
            m = hashlib.md5()
            m.update(accuracy)
            key_name = base_key % (self.run_id, m.hexdigest())
            k.key = key_name 
            
            k.set_contents_from_file( temp )
            self.logger.debug("Wrote %s" % key_name)
        return key_name

    @property
    def results_bucket( self ):
        attempts = 0
        while not self._results_bucket:
            attempts += 1
            try:
                conn = boto.connect_s3()
                b = conn.get_bucket(self.results_bucket_name)
                self._results_bucket = b
                self.logger.debug("Connected to %s" % self.results_bucket_name)
            except:
                if attempts > 5:
                    raise
                msg = "Could not connect to %s. Trying again. "
                msg = msg % self.results_bucket_name
                self.logger.exception( msg )
                time.sleep(2 + (random.random() * attempts))
        return self._results_bucket

    @property
    def networks(self):
        if self._pathways is None:
            ni = NetworkInfo( *self.net_info )
            self._pathways = ni.get_pathways()
            self.logger.debug("Got Networks")
        return self._pathways

    @property
    def net_info(self):
        if self._net_info is None:
            nt = self.run_config['network_config']['network_table']
            ns = self.run_config['network_config']['network_source']
            self._net_info = (nt, ns)
        return self._net_info

    @property
    def run_config(self):
        if self._run_config is None:
            self._run_config = run_mdl.get_ANRun( self.run_id )
            self.logger.debug("Run Config set %r" % self._run_config )
        return self._run_config

    @property
    def run_id(self):
        return self._run_id

    def join(self, comm):
        for mask_id in self.mask_ids:
            total = np.zeros_like(self.acc_acc[mask_id])
            comm.Reduce( [self.acc_acc[mask_id], MPI.INT], [total, MPI.INT] )
            if comm.rank == 0:
                self.acc_acc[mask_id] = total
        total_count = comm.reduce( self.acc_count )
        self.acc_count = total_count

    @property
    def joined_acc_acc( self ):
        """
        Returns a dataframe containing counts
        """
        if self._joined_acc_acc is None:
            index = self.networks
            column_names = self.mask_ids 
            base = np.zeros( ( len(index), len(column_names)), dtype=int)
            df = pandas.DataFrame( base, columns=column_names, index=index )
            for mask_id in self.mask_ids:
                df[mask_id] = self.acc_acc[mask_id]
            self._joined_acc_acc = df
        self.logger.debug("Joined accuracy accumulators")
        return self._joined_acc_acc

    @property
    def joined_pvals( self ):
        return self.joined_acc_acc / float(self.acc_count)

    def save_results( self ):
        results = {} 
        ts = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H.%M.%S')
        pval_key = '%s/results/%s/%s' % (self.run_id, ts, 'joined-pvals.csv')
        self._save_dataframe_to_s3( pval_key, self.joined_pvals.to_csv )
        results['pvalues'] = {
                                'bucket': self.results_bucket_name,
                                'file' : pval_key
                             }
        acc_key = '%s/results/%s/%s' % (self.run_id,ts, 'joined-counts.csv')
        self._save_dataframe_to_s3( acc_key, self.joined_acc_acc.to_csv )
        results['counts'] = {
                                'bucket': self.results_bucket_name,
                                'file' : pval_key
                            }
        results['num_observations'] = self.acc_count
        r = run_mdl.ANRunResults( self.run_id )
        r.results = results
        r.save()
        self.logger.info( "Results saved %r" % results )

    def _save_dataframe_to_s3( self, key_name, save_func ):
        """
        Takes a base_key (templated string that accepts 2 strings, 
        the run_id and the file name
        accuracy, which an a numpy array containing the accuracy of this
        masked resultset for all networks
        returns the key name
        """
        with tempfile.SpooledTemporaryFile() as temp:
            save_func(temp)
            temp.seek(0)
            k = Key(self.results_bucket)
            k.key = key_name 
            k.set_contents_from_file( temp )
            self.logger.debug("Saved %s" % key_name )
        return key_name

class Truthiness(Accumulator):
    def __init__(self, run_mdl, holdout_file):
        super(Truthiness, self).__init__(run_mdl, holdout_file)
        d2a =  run_mdl['intercomm_settings']['sqs_from_data_to_agg_truth']
        self._sqs_data_to_agg = d2a

    def handle_result_set(self, rs):
        if not rs.shuffle:
            self._handle_truth( rs )
            return True
        else:
            return False

