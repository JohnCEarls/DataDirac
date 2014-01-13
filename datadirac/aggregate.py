import boto.sqs
import boto
import json
import numpy as np
import tempfile

class ResultSet(object):
    def __init__(self, instructions, s3_from_gpu):
        self.s3_from_gpu= s3_from_gpu
        self._result_bucket = None
        self._alleles = None
        self.file_id = instructions['file_id']
        self.result_files = instructions['result_files']
        self.sample_allele = instructions['sample_allele']
        self.sample_names = instructions['sample_names']
        self.shuffle = intstructions['shuffle']
        self.strain = instructions['strain']
        #HACK, add networks to instructions
        self.networks = [str(i) for i in range(217)]
        self._data = None
        self._classified = None
        self._truth = None

    @property
    def nsamp(self):
        return len(self.sample_names)

    @property
    def nnets(self):
        return len(self.networks)

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

    def accuracy(self,by_network=True  mask=None):
        if mask is None:
            mask = np.array(range(self.nsamp))
        truth_mat = np.tile(self.truth, (self.nnets, 1))
        compare_mat = (truth == classified)
        if by_network:
            accuracy = compare[:,mask].sum(axis=1)/float(self.len(mask))
        else:
            accuracy = compare[:,mask].sum()/float(len(mask) * self.nnets)
        return accuracy


class Aggregator:
    def __init__(self, sqs_data_to_agg, s3_from_gpu, num_nets=217):
        self.sqs_data_to_agg = sqs_data_to_agg
        self.s3_from_gpu = s3_from_gpu
        self._data_queue = None
        self._result_bucket = None
        self.num_nets = num_nets

    @property
    def data_queue(self):
        if not self._data_queue:
            conn = boto.sqs.connect_to_region('us-east-1')
            self._data_queue = conn.create_queue(self.sqs_data_to_agg)
        return self._data_queue


    def get_result_set(self):
        m = self.data_queue.get_messages()
        inst = json.loads( m[0].get_body() )
        self.data_queue.delete_message(m[0])
        return ResultSet(inst, self.s3_from_gpu)

