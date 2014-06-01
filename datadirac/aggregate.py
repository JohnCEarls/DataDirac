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

import pandas
import re

class TruthException(Exception):
    pass

class FileCorruption(Exception):
    pass

class DirtyRunException(Exception):
    pass

class Aggregator:
    def deprecated___init__(self, sqs_data_to_agg, sqs_recycling_to_agg, s3_from_gpu, 
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
    def deprecated_recycling_queue( self ):
        if not self._recycling_queue:
            conn = boto.sqs.connect_to_region('us-east-1')
            self._recycling_queue = conn.create_queue(self.sqs_recycling_to_agg)
        return self._recycling_queue

    @property
    def deprecated_data_queue(self):
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
                    raise Exception("Fuck")
        return self._data_queue

    def deprecated_get_result_set(self):
        while self.data_queue.count() > 0:
            m = self.data_queue.read(30)
            if m:
                #put message away for future consumption
                self.recycling_queue.write( Message(body=m.get_body()) )
                inst = json.loads( m.get_body() )
                self.prev_msg = m
                return ResultSet(inst, self.s3_from_gpu, self.by_network, self.mask_id)
            else:
                self._data_queue = None
        return None

    def deprecated_handle_result_set(self, rs):
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

    def deprecated_get_truth(self, rs):
        if rs.spec_string not in self.truth:
            self.truth = self._get_truth(rs)
        return self.truth[rs.spec_string]

    def deprecated__get_truth( self, rs):
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

    def deprecated__load_np( self, bucket,  s3_file):
        conn = boto.connect_s3()
        b = conn.get_bucket( bucket )

        k = Key(b)
        k.key = s3_file
        with tempfile.SpooledTemporaryFile() as temp:
            k.get_contents_to_file(temp)
            temp.seek(0)
            accuracy = np.load(temp)
        return accuracy

    def deprecated__handle_permutation(self, rs):
        if self._mask is None:
            self._mask = rs.get_mask()
        rs.set_mask(self._mask)
        accuracy = rs.accuracy()
        if rs.spec_string not in self.acc_acc:
            self.acc_acc[rs.spec_string] = np.zeros_like(accuracy, dtype=int)
        truth = self.get_truth(rs)
        self.acc_acc[rs.spec_string] += (truth <= accuracy)
        self.acc_count[rs.spec_string] += 1

    def deprecated__handle_truth( self, rs ):
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
            if rs is not None:
                print  {'run_id':run_id,'strain_id': rs.spec_string}
                print rs.get_result_files()

    @property
    def deprecated_truth_table(self):
        conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
        table = Table( self.run_truth_table, connection = conn )
        return table

    def deprecated_save_acc(self, path, prefix='acc'):
        for k,mat in self.acc_acc.iteritems():
            my_path = os.path.join(path, '-'.join([prefix, k]) + '.npy')
            np.save(my_path, mat)

    @property
    def deprecated_networks(self):
        if self._pathways is None:
            print self.net_info
            ni = NetworkInfo( *self.net_info )
            self._pathways = ni.get_pathways()
        return self._pathways

    def deprecated__get_config(self):
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
    def deprecated_net_info(self):
        if self._net_info is None: 
            self._net_info = ( self.run_config['network_config']['network_table'], 
                self.run_config['network_config']['network_source'])
        return self._net_info

    @property
    def deprecated_run_config(self):
        if self._run_config is None:
            self._run_config = self._get_config()
        return self._run_config

    @property
    def deprecated_run_id(self):
        return self._run_id

    def deprecated_generate_csv(self, result, column_names, index,  filename):
        df = pandas.DataFrame(result, columns = column_names, index=index)
        df.to_csv( filename )

    def deprecated_write_csv(self, bucket, file_path):
        conn = boto.connect_s3()
        csv_bucket = conn.create_bucket(bucket)
        _, fname = os.path.split(file_path)
        k = Key(csv_bucket)
        k.key = fname
        k.set_contents_from_filename( file_path )
        print "%s written to s3://%s/%s" % (file_path, csv_bucket, fname)

    def deprecated_get_mask_labels(self):
        if self.mask_id == 'all':
            return ['All times']
        elif self.mask_id == 'lt12':
            return ['less than 12 weeks']
        elif self.mask_id == 'gte12':
            return ['greater than 12 weeks']
        else:
            return [self.mask_id]

class Truthiness(Aggregator):
    def deprecated_handle_result_set(self, rs):
        if not rs.shuffle:
            self._handle_truth( rs )
            self.data_queue.delete_message(self.prev_msg)
            return True
        else:
            return False

def deprecated_recycle( sqs_recycling_to_agg, sqs_data_to_agg ):
    print "reduce, recycle, reuse"
    conn = boto.connect_sqs()
    rec = conn.get_queue( sqs_recycling_to_agg)
    d2a = conn.get_queue(sqs_data_to_agg)
    max_count = 500000
    start = rec.count()
    while rec.count() > 0:
        m = rec.read(30)
        if m:
            d2a.write(m)
            rec.delete_message(m)
        max_count -= 1
        if max_count < 0:
            return
        if rec.count() < start - 100:
            print "%i messages enqueued." % rec.count()
            start=rec.count()

def deprecated_run_once(comm, mask_id, sqs_data_to_agg,  sqs_truth_to_agg, sqs_recycling_to_agg, s3_from_gpu, s3_results, run_truth_table, s3_csvs ):
    by_network = True
  
    rec = None
    if comm.rank == 0:
        sqs = boto.connect_sqs()
        d2a = sqs.create_queue( sqs_data_to_agg )
        d2a_bak =  sqs.create_queue( sqs_recycling_to_agg )
        print "Num data %i in %s" %  (d2a.count(), sqs_data_to_agg)
        print "Num data %i in %s" %  (d2a_bak.count(), sqs_recycling_to_agg)
        if d2a.count() > d2a_bak.count():
            rec = False 
        else:
            assert d2a_bak.count() > 0, "both queues empty"
            rec = True
    rec = comm.bcast(rec)
    if rec:
        sqs_data_to_agg, sqs_recycling_to_agg = sqs_recycling_to_agg, sqs_data_to_agg
    if comm.rank == 0:
        print "I want the truth!!!"
        a = Truthiness( sqs_truth_to_agg, sqs_truth_to_agg, s3_from_gpu, 
                s3_results, run_truth_table, by_network, mask_id)
        rs =a.get_result_set()
        if rs:
            while not a.handle_result_set(rs):
                print "not the truth", ctr
                rs =a.get_result_set()
                if rs is None:
                    break 
    comm.Barrier()
    #print "Aggregating", mask_id, sqs_data_to_agg,  sqs_truth_to_agg, sqs_recycling_to_agg, s3_from_gpu, s3_results, run_truth_table, s3_csvs
    a = Aggregator( sqs_data_to_agg, sqs_recycling_to_agg, s3_from_gpu, 
            s3_results, run_truth_table, by_network, mask_id)
    rs =a.get_result_set()
    
    if comm.rank == 0:
        rid = rs.get_run_id()
        st = rs.spec_string
    ctr = 0
    while rs:
        ctr += 1
        a.handle_result_set(rs) 
        rs =a.get_result_set()
    comm.Barrier()
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
            print "total obs. %i" % total_count
            divisor = float(total_count)
            pv_table = a.acc_acc[k]/divisor
            file_loc = '/scratch/sgeadmin/pvals-%s-%s-%s.csv' % (
                a.run_config['run_settings']['k'], a.run_id, mask_id) 
            a.generate_csv( pv_table, column_names = a.get_mask_labels(), 
                index=a.networks,  filename=file_loc)
            a.write_csv(s3_csvs, file_loc)
            try:
                res = TruthGPUDiracModel.query(rid, strain_id__eq=st)
                for r in res:
                    r.pval_file = os.path.split(file_loc)[1]
                    r.mask = a.get_mask_labels()
                    r.save()
            except Exception as e:
                print "Unable to store in dynamo"
                print "%r" % e
    if comm.rank==0:
        a.save_acc( '/scratch/sgeadmin', 'acc-k-11-combined-total' )
    comm.Barrier()

def deprecated_join_run( run_id, csv_bucket, mask_id, strains, alleles, description, 
        column_label, row_label, network_desc ):
    rs =  TruthGPUDiracModel.query( run_id )
    res_list = []
    for result in rs:
        temp = {}
        temp['csv'] = result.pval_file
        temp['range'] = list(result.mask)[0]
        m = re.match(r'\[(\d+),(\d+)\)', temp['range'])
        temp['start'] = int(m.group(1))
        if temp['range'] in mask_id:
            res_list.append( temp )
    res_list.sort(key=lambda x:x['start'])
    s3 = boto.connect_s3()
    b = s3.create_bucket( csv_bucket )
    pv_list = []
    for res in res_list:
        k = b.get_key( res['csv'] )
        csv = k.get_contents_as_string()
        first = True
        temp = {}
        for line in csv.split('\n'):
            if first:
                first = False
            else:
                if line.strip():
                    network, pval = line.split(',')
                    temp[network] = pval
        pv_list.append(temp)
    master = defaultdict(list)
    for col in  pv_list:
        for k,v in col.iteritems():
            master[k].append(v)
    table = [['networks'] + [r['range'] for r in res_list]]
    for k,v in master.iteritems():
        table.append([k] + v)
    my_table = ''
    for row in table:
        my_table += '\t'.join(row) + '\n'
    ts = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H.%M')
    tsv_name = "%s-joined-%s.tsv" % (run_id,ts)
    with open(tsv_name, 'w') as joined:
        joined.write( my_table )
    k = Key(b)
    k.key = tsv_name
    k.set_contents_from_filename(tsv_name)
    if not DataForDisplay.exists():
        DataForDisplay.create_table( wait=True, read_capacity_units=2, write_capacity_units=1)
    dfd_item = DataForDisplay( run_id, ts )
    #    strains, allele,description, column_label, row_label ):
    dfd_item.strains = strains
    dfd_item.alleles = alleles
    dfd_item.description = description
    dfd_item.data_bucket = csv_bucket
    dfd_item.column_label = column_label
    dfd_item.row_label = row_label
    dfd_item.data_file = tsv_name
    dfd_item.network = network_desc
    dfd_item.save()


if __name__ == "__main__":
    run_id = 'b6-canonical-1'
    #these should be moved to the run_model
    s3_results = 'an-jocelynn-results'
    s3_csvs = 'an-%s-csvs' % run_id
    #should be in the config model
    #Not sure how to set these
    #could get from data
    #or be set when configuring run
    strains=['B6']
    alleles=['WT','Q111']

    import masterdirac.models.run as run_mdl
    run_model = run_mdl.get_ANRun(run_id)
    ic = run_model['intercomm_settings']
    sqs_data_to_agg = ic['sqs_from_data_to_agg']
    sqs_truth_to_agg = ic['sqs_from_data_to_agg_truth']
    sqs_recycling_to_agg = '%s-bak' % ic['sqs_from_data_to_agg']
    s3_from_gpu = ic['s3_from_gpu_to_agg']
    run_truth_table = run_model['run_settings']['run_truth_table']

    description = run_model['description']
    network_desc = run_model['network_config']['network_source']
    column_label='time range'
    row_label='pathways'

    mask_id = ["[%i,%i)" % (i, i+5) for i in range(4,16)]
    mask_id += ["[4,20)", "[4,12)", "[12,20)"]
    from  mpi4py import MPI
    comm = MPI.COMM_WORLD
    for mask in mask_id[:]:
        print "Mask: ", mask
        run_once(comm, mask,  sqs_data_to_agg,  sqs_truth_to_agg, 
                sqs_recycling_to_agg, s3_from_gpu, s3_results, 
                run_truth_table, s3_csvs )

    comm.Barrier()
    if comm.rank == 0:
        join_run( run_id, s3_csvs, mask_id, strains, alleles,
            description, column_label, row_label, network_desc )
    comm.Barrier()
