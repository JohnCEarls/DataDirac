
import boto.sqs
import time
import boto
import json
import numpy as np
import tempfile
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
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

import pandas
import re

class TruthException(Exception):
    pass

class FileCorruption(Exception):
    pass

class DirtyRunException(Exception):
    pass

class Accumulator:
    def __init__(self, run_model):
        #TODO: send network info with data
        self.logger = logging.getLogger(__name__)
        self.logger.info("Aggregating on %s" % run_id)
        self._run_config = run_model
        self._run_id = run_model['run_id']
        self.acc_acc = {}
        for mask_id in self.masks:
            self.acc_acc[mask_id] = None 
        self.acc_count = {}
        for mask_id in masks:
            self.acc_count[mask_id] = defaultdict(int)
        self._sqs_data_to_agg = None
        self._s3_from_gpu = None
        self._data_queue = None
        self._result_bucket = None
        self._s3_results_bucket = None
        self._truth = None
        self._net_info = None
        self._pathways = None
        self._net_info = None

    def s3_from_gpu(self):
        if self._s3_from_gpu is None:
            self._s3_from_gpu = self.run_config['intercomm_settings']['s3_from_gpu_to_agg']
        return self._s3_from_gpu

    @property
    def truth(self):
        if self._truth is None:
            self._truth = self._get_truth()
        return self._truth

    @property
    def masks(self):
        if self._masks is None:
            self._masks = self._parse_masks( self.run_config['aggregator_settings']['masks'] )
        return self._masks

    def _parse_masks(self, masks):
        m = re.compile( resultset.MASK_PATTERN_PARSE )
        return m.findall( masks )

    @property
    def sqs_data_to_agg(self):
        if self._sqs_data_to_agg is None:
            self._sqs_data_to_agg = self.run_config['intercomm_settings']['sqs_from_data_to_agg']
        return self._sqs_data_to_agg

    @property
    def results_bucket(self):
        if self._s3_results_bucket is None:
            self._s3_results_bucket = self.run_config['aggregator_settings']['results_bucket']
        return self._s3_results_bucket

    @property
    def data_queue(self):
        ctr = 0 
        while not self._data_queue:
            try:
                conn = boto.sqs.connect_to_region('us-east-1')
                self._data_queue = conn.get_queue(self.sqs_data_to_agg)
            except:
                print "authhandler exception"
                time.sleep(2)
                ctr += 1
                if ctr > 10:
                    raise
        return self._data_queue

    def get_result_set(self, delete=True):
        while self.data_queue.count() > 0:
            m = self.data_queue.read(30)
            if m:
                #put message away for future consumption
                inst = json.loads( m.get_body() )
                if delete:
                    self.data_queue.delete_message(m)
                return S3ResultSet( instructions=inst, 
                                    s3_from_gpu = self.s3_from_gpu )
            else:
                self._data_queue = None
        return None

    def handle_result_set(self, rs):
        self._handle_permutation( rs )

    def _get_truth( self):
        truth = {}
        for item in TruthGPUDiracModel.query(self.run_id):
            truth[item.strain_id] = self._load_np(self.s3_results, item.accuracy_file )
        return truth

    def _load_np( self, bucket,  s3_file):
        conn = boto.connect_s3()
        b = conn.get_bucket( bucket )
        k = Key(b)
        k.key = s3_file
        with tempfile.SpooledTemporaryFile() as temp:
            k.get_contents_to_file(temp)
            temp.seek(0)
            accuracy = np.load(temp)
        return accuracy

    def _handle_permutation(self, rs):
        for mask_id in self._masks:
            masked_rs = Masked( rs, mask_id )
            accuracy = masked_rs.accuracy()
            if self.acc_acc[mask_id] is None:
                self.acc_acc[mask_id] =  np.zeros_like(accuracy, dtype=int)
            self.acc_acc[mask_id] += (self.truth[mask_id] <= accuracy)
        self.acc_count += 1

    def _handle_truth( self, rs ):
        with TruthGPUDiracModel.batch_write() as tgdModel:
            base_key = 'truth-accuracy/%s/%s' 
            for mask_id in self._masks:
                masked_rs = Masked( rs, mask_id )
                accuracy = masked_rs.accuracy()
                self.truth[mask_id] = accuracy
                with tempfile.SpooledTemporaryFile() as temp:
                    np.save(temp, accuracy)
                    temp.seek(0)
                    conn = boto.connect_s3()
                    bucket = conn.create_bucket( results_bucket )
                    k = Key(bucket)
                    m = hashlib.md5()
                    m.update(accuracy)
                    md5 =  m.hexdigest()
                    k.key = base_key % (rs.run_id, md5)
                    k.set_contents_from_file( temp )
                rf =  base64.b64encode( json.dumps( rs.get_result_files() ) )
                tgdModel.save( masked_rs.run_id, mask_id,
                        accuracy_file = md5,
                        result_files = rf,
                        bucket = self.s3_results,
                        timestamp = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
                        )
        return self.truth

    def save_acc(self, path, prefix='acc'):
        for k,mat in self.acc_acc.iteritems():
            my_path = os.path.join(path, '-'.join([prefix, k]) + '.npy')
            np.save(my_path, mat)

    @property
    def networks(self):
        if self._pathways is None:
            ni = NetworkInfo( *self.net_info )
            self._pathways = ni.get_pathways()
        return self._pathways


    @property
    def net_info(self):
        if self._net_info is None: 
            self._net_info = ( self.run_config['network_config']['network_table'], 
                self.run_config['network_config']['network_source'])
        return self._net_info

    @property
    def run_config(self):
        if self._run_config is None:
            self._run_config = run_mdl.get_ANRun( self.run_id )
        return self._run_config

    @property
    def run_id(self):
        return self._run_id

    def generate_csv(self, result, column_names, index,  filename):
        df = pandas.DataFrame(result, columns = column_names, index=index)
        df.to_csv( filename )

    def write_csv(self, bucket, file_path):
        conn = boto.connect_s3()
        csv_bucket = conn.create_bucket(bucket)
        _, fname = os.path.split(file_path)
        k = Key(csv_bucket)
        k.key = fname
        k.set_contents_from_filename( file_path )
        print "%s written to s3://%s/%s" % (file_path, csv_bucket, fname)

class Truthiness(Accumulator):
    def __init__(self, run_mdl):
        super(Truthiness, self).__init__(run_mdl)
        self._sqs_data_to_agg = run_mdl['intercomm_settings']['sqs_from_data_to_agg_truth']

    def handle_result_set(self, rs):
        if not rs.shuffle:
            self._handle_truth( rs )
            self.data_queue.delete_message(self.prev_msg)
            return True
        else:
            return False

