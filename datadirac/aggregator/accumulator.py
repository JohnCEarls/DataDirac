
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
        self.logger.info("Aggregating on %s" % run_model['run_id'])
        ic = run_model['intercomm_settings']
        sqs_recycling_to_agg = '%s-bak' % ic['sqs_from_data_to_agg']
        sqs_data_to_agg = ic['sqs_from_data_to_agg']
        self.sqs_data_to_agg = sqs_data_to_agg
        sqs_recycling_to_agg = '%s-bak' % sqs_data_to_agg
        self.sqs_recycling_to_agg = sqs_recycling_to_agg
        s3_from_gpu = ic['s3_from_gpu_to_agg']
        self.s3_from_gpu = s3_from_gpu
        self.s3_results =run_model['aggregator_settings']['results_bucket']
        self._data_queue = None
        self._recycling_queue = None
        self._result_bucket = None
        run_truth_table = run_model['run_settings']['run_truth_table']
        self.run_truth_table =run_truth_table
        self.truth = self._get_truth()
        #accuracy accumulator
        self._masks = run_model['aggregator_settings']['masks']
        #accuracy accumulator
        self.acc_acc = {}
        for mask_id in masks:
            self.acc_acc[mask_id] = {}
        self.acc_count = {}
        for mask_id in masks:
            self.acc_count[mask_id] = defaultdict(int)
        self.prev_msg = None
        self._run_id = run_model
        self._run_config = None
        self._net_info = None
        self._pathways = None
        nc = run_config['network_config']
        self._net_info = (nc['network_table'], nc['network_source'])

    def recycling_swap(self):
        """
        Swap the base and the recycling queue
        """
        self.sqs_recycling_to_agg, self.sqs_data_to_agg = (self.sqs_data_to_agg,
                self.sqs_recycling_to_agg)

    @property
    def recycling_queue( self ):
        if not self._recycling_queue:
            conn = boto.sqs.connect_to_region('us-east-1')
            self._recycling_queue = conn.create_queue(self.sqs_recycling_to_agg)
        return self._recycling_queue

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

    def get_result_set(self):
        while self.data_queue.count() > 0:
            m = self.data_queue.read(30)
            if m:
                #put message away for future consumption
                self.recycling_queue.write( Message(body=m.get_body()) )
                inst = json.loads( m.get_body() )
                self.prev_msg = m
                return S3ResultSet( instructions=inst, self.s3_from_gpu )
            else:
                self._data_queue = None
        return None

    def handle_result_set(self, rs):
        try:
            self._handle_permutation( rs, self.self.s3_from_gpu )
        except TruthException as te:
            self.logger.error("No truth for %s %s" % (rs.run_id, rs.strain))
        except FileCorruption as fc:
            pass
            #print "Corrupt file"
        self.data_queue.delete_message(self.prev_msg)

    def _get_truth( self):
        truth = {}
        tt = self.truth_table.query( run_id__eq=self.run_id )
        for t in tt:
            truth[t['strain_id']] = self._load_np(self.s3_results, t['accuracy_file'] )
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
        for mask_id in self._masks:
            masked_rs = Masked( rs, mask_id )
            accuracy = masked_rs.accuracy()
            self.truth[mask_id] = accuracy
            with tempfile.SpooledTemporaryFile() as temp:
                np.save(temp, accuracy)
                temp.seek(0)
                conn = boto.connect_s3(  )
                bucket = conn.create_bucket( self.s3_results )
                k = Key(bucket)
                m = hashlib.md5()
                m.update(accuracy)
                md5 =  m.hexdigest()
                k.key = md5
                k.set_contents_from_file( temp )
            item = Item( self.truth_table, {'run_id':self.run_id,
                    'strain_id': mask_id} )
            item['accuracy_file'] = md5
            item['result_files'] =  base64.b64encode( json.dumps( 
                rs.get_result_files() ) )
            item['bucket'] = self.s3_results
            item['timestamp'] = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
            item.save()
        return self.truth

    @property
    def truth_table(self):
        conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
        table = Table( self.run_truth_table, connection = conn )
        return table

    def save_acc(self, path, prefix='acc'):
        for k,mat in self.acc_acc.iteritems():
            my_path = os.path.join(path, '-'.join([prefix, k]) + '.npy')
            np.save(my_path, mat)

    @property
    def networks(self):
        if self._pathways is None:
            print self.net_info
            ni = NetworkInfo( *self.net_info )
            self._pathways = ni.get_pathways()
        return self._pathways

    def _get_config(self):
        net_table = Table('run_gpudirac_hd')
        r_spec = net_table.query(run_id__eq=self._run_id)
        config = None
        last_time = datetime.datetime(1975,2,18,0,0,0)
        for s in r_spec:
            time_stamp = s['timestamp']
            curr = datetime.datetime.strptime(time_stamp, '%Y.%m.%d-%H:%M:%S') 
            if curr > last_time:
                config =  json.loads(base64.b64decode(s['config']))
        return config

    @property
    def net_info(self):
        if self._net_info is None: 
            self._net_info = ( self.run_config['network_config']['network_table'], 
                self.run_config['network_config']['network_source'])
        return self._net_info

    @property
    def run_config(self):
        if self._run_config is None:
            self._run_config = self._get_config()
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

    def get_mask_labels(self):
        if self.mask_id == 'all':
            return ['All times']
        elif self.mask_id == 'lt12':
            return ['less than 12 weeks']
        elif self.mask_id == 'gte12':
            return ['greater than 12 weeks']
        else:
            return [self.mask_id]

class Truthiness(Accumulator):
    def __init__(self, run_mdl, masks):
        super(Truthiness, self).__init__(run_mdl, masks)
        self.sqs_data_to_agg = run_mdl['intercomm_settings']['sqs_from_data_to_agg_truth']
        self.sqs_recycling_to_agg = '%s-bak' % self.self.sqs_data_to_agg

    def handle_result_set(self, rs):
        if not rs.shuffle:
            self._handle_truth( rs )
            self.data_queue.delete_message(self.prev_msg)
            return True
        else:
            return False

