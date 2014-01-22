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
import pandas

class TruthException(Exception):
    pass

class FileCorruption(Exception):
    pass

class DirtyRunException(Exception):
    pass

class ResultSet(object):
    def __init__(self, instructions, s3_from_gpu, by_network, mask_id ):
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
        self._pathways = None
        self._mask = None

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
        complete = False
        count = 0
        while not complete:
            try:
                with tempfile.SpooledTemporaryFile() as temp:
                    key = self.result_bucket.get_key( self.result_files[allele] )
                    key.get_contents_to_file( temp )
                    temp.seek(0)
                    buffered_matrix = np.load( temp )
                complete = True
            except Exception as e:
                 #print e
                 #print "error on get[%r], trying again" % self.result_files[allele] 
                 count += 1
                 if count > 1:
                     raise FileCorruption('Error on File')
                 pass
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
        if self._mask is None:
            if mask_id == 'all':
                self._mask = np.array(range(self.nsamp))
            elif mask_id == 'lt12':
                self._mask = self._select_range( 0.0, 12.0)
            elif mask_id == 'gte12':
                self._mask = self._select_range( 12.0, 120.0)
        return self._mask

    def _select_range(self, start, end):
        samp = set([])
        for _, sl in self.sample_allele.iteritems():
            samp |= set([ sample_name for age, sample_name in sl if start <= age < end ])
        return np.array([i for i,s in enumerate(self.sample_names) if s in samp])

    def set_mask(self, mask):
        self._mask = mask

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
        self._run_id = None
        self._run_config = None
        self._net_info = None
        self._pathways = None
        self._mask = None

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
        if self._run_id is None:
            self._run_id =  rs.get_run_id()
        if self._run_id != rs.get_run_id():
            print "Inconsistent run_ids, agg[%s], res[%s]", (self._run_id, rs.get_run_id())
            raise DirtyRunException("Inconsistent run_ids, agg[%s], res[%s]", 
                    (self._run_id, rs.get_run_id()))
        if rs.shuffle:
            try:
                self._handle_permutation( rs )
            except TruthException as te:
                print "No truth for this"
                print rs.get_run_id()
                print rs.spec_string
            except FileCorruption as fc:
                pass
                #print "Corrupt file"
        else:
            print "Found Truth"
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
        if self._mask is None:
            self._mask = rs.get_mask()
        rs.set_mask(self._mask)
        accuracy = rs.accuracy()
        if rs.spec_string not in self.acc_acc:
            self.acc_acc[rs.spec_string] = np.zeros_like(accuracy, dtype=int)
        truth = self.get_truth(rs)
        self.acc_acc[rs.spec_string] += (truth <= accuracy)
        self.acc_count[rs.spec_string] += 1

    def _handle_truth( self, rs ):
        if self._mask is None:
            self._mask = rs.get_mask()
        rs.set_mask(self._mask)
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
            return ['less than 12 weeks']

class Truthiness(Aggregator):
    def handle_result_set(self, rs):
        if not rs.shuffle:
            self._handle_truth( rs )
            self.data_queue.delete_message(self.prev_msg)
            return True
        else:
            return False

def recycle( sqs_recycling_to_agg, sqs_data_to_agg ):
    print "reduce, recycle, reuse"
    conn = boto.connect_sqs()
    rec = conn.get_queue( sqs_recycling_to_agg)
    d2a = conn.get_queue(sqs_data_to_agg)
    max_count = 500000
    start = rec.count()
    while rec.count() > 0:
        m = rec.read(60)
        if m:
            d2a.write(m)
            rec.delete_message(m)
        max_count -= 1
        if max_count < 0:
            return
        if rec.count() < start - 100:
            print "%i messages enqueued." % rec.count()
            start=rec.count()

if __name__ == "__main__":
    from  mpi4py import MPI
    #sqs_data_to_agg = 'from-data-to-agg-go' 
    sqs_data_to_agg = 'recycling-go' 
    sqs_truth_to_agg = 'from-data-to-agg-go-truth'
    #sqs_recycling_to_agg = 'recycling-go'
    sqs_recycling_to_agg = 'from-data-to-agg-go'
    s3_from_gpu = 'ndp-from-gpu-to-agg'
    s3_results = 'ndp-gpudirac-results'
    run_truth_table = 'truth_gpudirac_hd'
    s3_csvs = 'ndp-hdproject-csvs'
    by_network = True
    mask_id ='all'

    comm = MPI.COMM_WORLD
    truth_only = False 
    rec = None
    if comm.rank == 0:
        sqs = boto.connect_sqs()
        d2a = sqs.create_queue( sqs_data_to_agg )
        print "Num data %i" % d2a.count()
        if d2a.count() > 0:
            rec = False 
        else:
            rec = True
    rec = comm.bcast(rec)
    if truth_only:
        print "I want the truth!!!"
        a = Truthiness( sqs_truth_to_agg, sqs_truth_to_agg, s3_from_gpu, 
                s3_results, run_truth_table, by_network, mask_id)
        rs =a.get_result_set()
        ctr = 0
        if rs:
            while not a.handle_result_set(rs):
                print "not the truth", ctr
                rs =a.get_result_set()
                if rs is None:
                    break 
                ctr += 1
    elif not rec:
        print "Aggregating"
        a = Aggregator( sqs_data_to_agg, sqs_recycling_to_agg, s3_from_gpu, 
                s3_results, run_truth_table, by_network, mask_id)
        rs =a.get_result_set()
        ctr = 0
        while rs:
            ctr += 1
            a.handle_result_set(rs) 
            rs =a.get_result_set()
        acc_pre = "acc-k-11-%i-%i" %(ctr, comm.rank)
        a.save_acc( '/scratch/sgeadmin', acc_pre)
        strains = a.acc_acc.keys()
        strains.sort()
        strains = comm.bcast(strains)
        zero = None
        for mat in a.acc_acc.itervalues():
            zero = np.zeros_like(mat, dtype = np.int)
        for k in strains:
            if k in a.acc_acc:
                curr = a.acc_acc[k]
            else:
                curr = zero
            total = np.zeros_like(curr)
            comm.Reduce([curr, MPI.INT],[total, MPI.INT])
            if comm.rank == 0:
                a.acc_acc[k] = total
            total_count = 0
            print "acc", a.acc_count[k]
            total_count = comm.reduce(a.acc_count[k])
            if comm.rank == 0:
                print total_count
                divisor = float(total_count)
                pv_table = a.acc_acc[k]/divisor
                file_loc = '/scratch/sgeadmin/pvals-%s-%s-%s.csv' % (
                    a.run_config['run_settings']['k'], a.run_id, mask_id) 
                a.generate_csv( pv_table, column_names = a.get_mask_labels(), 
                    index=a.networks,  filename=file_loc)
                a.write_csv(s3_csvs, file_loc)


        if comm.rank==0:
            a.save_acc( '/scratch/sgeadmin', 'acc-k-11-combined-total' )
        print "No more results"
    if rec:
        print "Recycling"
        recycle(sqs_recycling_to_agg, sqs_data_to_agg)
