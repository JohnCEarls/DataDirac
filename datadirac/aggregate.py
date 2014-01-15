import boto.sqs
import boto
import json
import numpy as np
import tempfile
from boto.dynamodb2.table import Table

class ResultSet(object):
    def __init__(self, instructions, s3_from_gpu):
        self.s3_from_gpu= s3_from_gpu
        self._result_bucket = None
        self._alleles = None
        self.run_id = instructions['run_id']
        self.file_id = instructions['file_id']
        self.result_files = instructions['result_files']
        self.sample_allele = instructions['sample_allele']
        self.sample_names = instructions['sample_names']
        self.shuffle = instructions['shuffle']
        self.strain = instructions['strain']
        #HACK, add networks to instructions
        self.num_networks = instructions['num_networks']
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
        if not self._result_bucket:
            conn = boto.connect_s3()
            self._result_bucket = conn.get_bucket(self.s3_from_gpu)
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

    def get_strain_id(self):
        return self.strain_id

    def get_result_files(self):
        return self.result_files

    def accuracy(self,by_network=True,  mask=None):
        if mask is None:
            mask = np.array(range(self.nsamp))
        truth_mat = np.tile(self.truth, (self.nnets, 1))
        compare_mat = (truth_mat == self.classified)
        if by_network:
            accuracy = compare_mat[:,mask].sum(axis=1)/float(len(mask))
        else:
            accuracy = compare_mat[:,mask].sum()/float(len(mask) * self.nnets)
        return accuracy

class Aggregator:
    def __init__(self, sqs_data_to_agg, sqs_recycling_to_agg, s3_from_gpu, 
            s3_results, run_truth_table):
        #TODO: send network info with data
        self.sqs_data_to_agg = sqs_data_to_agg
        self.sqs_recycling_to_agg = sqs_agg_to_agg
        self.s3_from_gpu = s3_from_gpu
        self.s3_results = s3_results
        self._data_queue = None
        self._recycling_queue = None
        self._result_bucket = None
        self.num_nets = num_nets
        self.run_truth_table = run_truth_table
        #accuracy accumulator
        self.acc_acc = {}
        self.acc_count = default_dict(int)

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
            self._data_queue = conn.create_queue(self.sqs_data_to_agg)
        return self._data_queue

    def get_result_set(self):
        m = self.data_queue.get_messages()
        self.recycling_queue.write( Message(body=m[0].get_body()) )
        inst = json.loads( m[0].get_body() )
        self.data_queue.delete_message(m[0])
        return ResultSet(inst, self.s3_from_gpu)

    def handle_result_set(self, rs):
        if rs.shuffle:
            self._handle_permutation( rs )
        else:
            self._handle_truth( rs )

    def get_truth(self, rs):
        if rs.get_strain_id() not in self.truth:
            self.truth = self._get_truth(rs)
        return self.truth[rs.get_strain_id()]

    def _get_truth( self, rs):
        truth = {}
        tt = self.truth_table.query( run_id__eq=rs.get_run_id() )
        for t in tt:
            truth[t['strain']] = self._load_np(t['bucket'], t['accuracy_file'] )
        if rs.get_strain_id() not in truth:
            raise Exception("Well, fuck")
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
        accuracy = rs.get_accuracy()
        if rs.get_strain_id() not in self.acc_acc:
            self.acc_acc[rs.get_strain_id()] = np.zeros_like(accuracy, dtype=int)
        truth = self.get_truth(rs)
        self.acc_acc[rs.get_strain_id()] += (truth <= accuracy)
        self.acc_count[rs.get_strain_id()] += 1

    def _handle_truth( self, rs ):
        accuracy = rs.accuracy()
        with tempfile.SpooledTemporaryFile() as temp:
            accuracy.save(temp)
            temp.seek(0)
            conn = boto.connect_s3(  )
            bucket = conn.create_bucket( self.s3_results )
            k = Key(bucket)
            m = hashlib.md5()
            m.update(accuracy)
            md5 =  m.hexdigest()
            k.key = md5
            k.set_contents_from_file( temp, md5=md5 )
        run_id = rs.get_run_id()
        strain_id = rs.get_strain_id()
        item = Item( self.truth_table, {'run_id':run_id,
                'strain_id':strain_id} )
        item['accuracy_file'] = md5
        item['result_files'] =  base64.b64encode( json.dumps( 
            rs.get_result_files() ) )
        item['bucket'] = self._result_bucket
        item['timestamp'] = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
        item.save()

    @property
    def truth_table(self):
        conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
        table = Table( self.run_truth_table, connection = conn )
        return table

if __name__ == "__main__":
    a = Aggregator('from-data-to-agg', 'ndp-from-gpu-to-agg', num_nets=217)
    rs =a.get_result_set()
    print rs.accuracy()
