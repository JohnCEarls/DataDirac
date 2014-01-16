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

class TruthException(Exception):
    pass

class ResultSet(object):
    def __init__(self, instructions, s3_from_gpu, by_network, mask_id):
        self.s3_from_gpu= s3_from_gpu
        self._result_bucket = None
        self._alleles = None
        #print instructions
        self.run_id = instructions['run_id']
        self.file_id = instructions['file_id']
        self.result_files = instructions['result_files']
        self.sample_allele = instructions['sample_allele']
        self.sample_names = instructions['sample_names']
        self.shuffle = instructions['shuffle']
        self.strain = instructions['strain']
        self.num_networks = instructions['num_networks']
        self.by_network = by_network
        self.mask_id = mask_id
        self._data = None
        self._classified = None
        self._truth = None

    @property
    def nsamp(self):
        return len(self.sample_names)

    @property
    def nnets(self):
        return self.num_networks

    @property
    def result_bucket(self):
        while not self._result_bucket:
            try:
                conn = boto.connect_s3()
                self._result_bucket = conn.get_bucket(self.s3_from_gpu)
            except:
                print "could not connect to %s " % self.s3_from_gpu
                print "Try again"
                time.sleep(5)
        return self._result_bucket

    @property
    def alleles(self):
        if not self._alleles:
            self._alleles = self.result_files.keys()
            self._alleles.sort()
        return self._alleles

    def _get_data(self, allele):
        with tempfile.SpooledTemporaryFile() as temp:
            key = self.result_bucket.get_key( self.result_files[allele] )
            key.get_contents_to_file( temp )
            temp.seek(0)
            buffered_matrix = np.load( temp )
        return buffered_matrix[:self.nnets, :self.nsamp]

    @property
    def data(self):
        if not self._data:
            stacked = []
            for allele in self.alleles:
                stacked.append(self._get_data(allele))
            self._data = np.array(stacked)
        return self._data

    @property
    def classified(self):
        if not self._classified:
            self._classified = np.argmax( self.data, axis=0 )
        return self._classified

    @property
    def truth(self):
        if not self._truth:
            classes = []
            for a in self.alleles:
                classes.append(set([sn for _, sn in self.sample_allele[a]]))
            def clsfy(classes, s):
                for i,_set in enumerate(classes):
                    if s in _set:
                        return i
            self._truth = np.array( [clsfy(classes, sname) 
                                        for sname in self.sample_names] )
        return self._truth

    def get_run_id(self):
        return self.run_id

    def get_strain(self):
        return self.strain

    def get_result_files(self):
        return self.result_files

    def get_mask(self):
        mask_id = self.mask_id
        if mask_id == 'all':
            mask = np.array(range(self.nsamp))
        return mask


    @property
    def spec_string( self):
        if self.by_network:
            n = '1'
        else:
            n = '0'
        spec = '%s-%s-%s' % (self.get_strain(), n, self.mask_id)
        return spec

    def accuracy(self):
        by_network, mask_id = ( self.by_network, self.mask_id )
        mask = self.get_mask() 
        truth_mat = np.tile(self.truth, (self.nnets, 1))
        compare_mat = (truth_mat == self.classified)
        if by_network:
            accuracy = compare_mat[:,mask].sum(axis=1)/float(len(mask))
        else:
            accuracy = compare_mat[:,mask].sum()/float(len(mask) * self.nnets)
        return accuracy

class Aggregator:
    def __init__(self, sqs_data_to_agg, sqs_recycling_to_agg, s3_from_gpu, 
            s3_results, run_truth_table, by_network, mask_id):
        #TODO: send network info with data
        self.sqs_data_to_agg = sqs_data_to_agg
        self.sqs_recycling_to_agg = sqs_recycling_to_agg
        self.s3_from_gpu = s3_from_gpu
        self.s3_results = s3_results
        self._data_queue = None
        self._recycling_queue = None
        self._result_bucket = None
        self.run_truth_table = run_truth_table
        #accuracy accumulator
        self.acc_acc = {}
        self.acc_count = defaultdict(int)
        self.by_network = by_network
        self.mask_id = mask_id
        self.prev_msg = None
        self.truth = {} 

    @property
    def recycling_queue( self ):
        if not self._recycling_queue:
            conn = boto.sqs.connect_to_region('us-east-1')
            self._recycling_queue = conn.create_queue(self.sqs_recycling_to_agg)
        return self._recycling_queue

    @property
    def data_queue(self):
        if not self._data_queue:
            conn = boto.sqs.connect_to_region('us-east-1')
            self._data_queue = conn.get_queue(self.sqs_data_to_agg)
        return self._data_queue

    def get_result_set(self):
        m = self.data_queue.read(50)
        if m:
            #put message away for future consumption
            self.recycling_queue.write( Message(body=m.get_body()) )
            inst = json.loads( m.get_body() )
            self.prev_msg = m
            return ResultSet(inst, self.s3_from_gpu, self.by_network, self.mask_id)
        else:
            return None

    def handle_result_set(self, rs):
        if rs.shuffle:
            try:
                self._handle_permutation( rs )
            except TruthException as te:
                print "No truth for this"
                print rs.get_run_id()
                print rs.spec_string
        else:
            self._handle_truth( rs )
        self.data_queue.delete_message(self.prev_msg)

    def get_truth(self, rs):
        if rs.spec_string not in self.truth:
            self.truth = self._get_truth(rs)
        return self.truth[rs.spec_string]

    def _get_truth( self, rs):
        truth = {}
        tt = self.truth_table.query( run_id__eq=rs.get_run_id() )
        for t in tt:
            t_spec_str = t['strain_id'].split('-')
            r_spec_str = rs.spec_string.split('-')
            if r_spec_str[1] == t_spec_str[1] and r_spec_str[2] == t_spec_str[2]:
                truth[t['strain_id']] = self._load_np(self.s3_results, t['accuracy_file'] )
        if rs.spec_string not in truth:
            raise TruthException("Well, fuck")
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
        accuracy = rs.accuracy()
        if rs.spec_string not in self.acc_acc:
            self.acc_acc[rs.spec_string] = np.zeros_like(accuracy, dtype=int)
        truth = self.get_truth(rs)
        self.acc_acc[rs.spec_string] += (truth <= accuracy)
        self.acc_count[rs.spec_string] += 1

    def _handle_truth( self, rs ):
        accuracy = rs.accuracy()
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
        run_id = rs.get_run_id()
        try:
            item = Item( self.truth_table, {'run_id':run_id,
                    'strain_id': rs.spec_string} )
            item['accuracy_file'] = md5
            item['result_files'] =  base64.b64encode( json.dumps( 
                rs.get_result_files() ) )
            item['bucket'] = self.s3_results
            item['timestamp'] = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
            item.save()
        except ConditionalCheckFailedException as ccfe:
            print "*"*20
            print ccfe
            print  {'run_id':run_id,'strain_id': rs.spec_string}
            print rs.get_result_files()

    @property
    def truth_table(self):
        conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
        table = Table( self.run_truth_table, connection = conn )
        return table

    def save_acc(self, path, prefix='acc'):
        for k,mat in self.acc_acc.iteritems():
            my_path = os.path.join(path, '-'.join([prefix, k]) + '.npy')
            np.save(my_path, mat)

def recycle( sqs_recycling_to_agg, sqs_data_to_agg ):
    conn = boto.connect_sqs()
    rec = conn.get_queue( sqs_recycling_to_agg)
    d2a = conn.get_queue(sqs_data_to_agg)
    while rec.count() > 0:
        m = rec.read(60)
        if m:
            d2a.write(m)
            rec.delete_message(m)
        print "reduce, recycle, reuse"


if __name__ == "__main__":
    sqs_data_to_agg = 'from-data-to-agg' 
    sqs_recycling_to_agg = 'recycling'
    s3_from_gpu = 'ndp-from-gpu-to-agg'
    s3_results = 'ndp-gpudirac-results'
    run_truth_table = 'truth_gpudirac_hd'
    by_network = True
    mask_id = 'all'
    if True:
        a = Aggregator( sqs_data_to_agg, sqs_recycling_to_agg, s3_from_gpu, 
                s3_results, run_truth_table, by_network, mask_id)
        rs =a.get_result_set()
        ctr = 0
        while rs:
            ctr += 1
            a.handle_result_set(rs) 
            rs =a.get_result_set()
            if ctr%100 == 0:
                acc_pre = "acc-k-11-%i" %ctr
                a.save_acc( '/scratch/sgeadmin', acc_pre )
                print "saving %s" %acc_pre 
        a.save_acc( '/scratch/sgeadmin')
        
        print "No more results"
    if False:
        recycle(sqs_recycling_to_agg, sqs_data_to_agg)
