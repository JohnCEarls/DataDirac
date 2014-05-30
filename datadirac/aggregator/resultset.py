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
import masterdirac.models.run as run_mdl

import pandas
import re

class TruthException(Exception):
    pass

class FileCorruption(Exception):
    pass

class DirtyRunException(Exception):
    pass

class InvalidMask(Exception):
    #the given mask doesnt parse
    pass

MASK_PATTERN_MATCH = r'([\[\(]\d+,\d+[\)\]])'
MASK_PATTERN_PARSE = r'[\[\(](\d+),(\d+)[\)\]])'


class ResultSet(object):
    """
    Abstract BaseClass
    For representing and manipulating a result
    """
    def __init__(self, instructions ):
        self._result_bucket = None
        self._alleles = None
        #print instructions
        self._instructions = instructions
        self.run_id = instructions['run_id']
        self._file_id = instructions['file_id']
        self.result_files = instructions['result_files']
        self.sample_allele = instructions['sample_allele']
        self.sample_names = instructions['sample_names']
        self.shuffle = instructions['shuffle']
        self.strain = instructions['strain']
        self.num_networks = instructions['num_networks']
        self._data = None
        self._classified = None
        self._truth = None

    @property
    def nsamp(self):
        return len(self.sample_names)

    @property
    def file_id(self):
        return self._file_id

    @property
    def nnets(self):
        return self.num_networks

    @property
    def alleles(self):
        if not self._alleles:
            self._alleles = self.result_files.keys()
            self._alleles.sort()
        return self._alleles

    @property
    def data(self):
        if self._data is None:
            stacked = []
            for allele in self.alleles:
                stacked.append(self._get_data(allele))
            self._data = np.array(stacked)
        return self._data

    @property
    def classified(self):
        if self._classified is None:
            self._classified = np.argmax( self.data, axis=0 )
        return self._classified

    @property
    def truth(self):
        if self._truth is None:
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

    def archive_package(self):
        return (self.file_id, self._instructions, self.data)

class S3ResultSet(ResultSet):
    def __init__(self, instructions, s3_from_gpu ):
        super(S3ResultSet,self).__init__(instructions)
        self.s3_from_gpu= s3_from_gpu

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


    def _get_data(self, allele ):
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
                 print e
                 #print "error on get[%r], trying again" % self.result_files[allele] 
                 count += 1
                 if count > 1:
                     raise FileCorruption('Error on File')
                 pass
        return buffered_matrix[:self.nnets, :self.nsamp]

class LocalResultSet(ResultSet):
    def __init__(self, instructions, data_obj ):
        super(LocalResultSet, self).__init__( instructions )
        self._data = data_obj

        self.local_path = local_path

class Masked(object):
    def __init__(self, result_set, mask_id):
        self._mask_id = mask_id
        self._result_set = result_set
        self._mask = None

    @property
    def mask(self):
        if self._mask is None:
            self._mask = self.get_mask( self.mask_id )
        return self._mask

    @property
    def mask_id(self):
        return self._mask_id

    def get_mask(self, mask_id):
        m = re.match(MASK_PATTERN_PARSE, mask_id)
        if m:
            lower = float(m.group(1)) - .0001
            upper = float(m.group(2)) + .0001
            assert lower < upper, "Bad mask [%s]" % mask_id
            return self._select_range( lower, upper)
        else:    
            raise InvalidMask("%s does not parse correctly" % mask_id)
       

    def _select_range(self, start, end):
        rs = self._result_set
        samp = set([])
        for _, sl in rs.sample_allele.iteritems():
            samp |= set([ sample_name for age, sample_name in sl if start <= age < end ])
        return np.array([i for i,s in enumerate(rs.sample_names) if s in samp])

    def accuracy(self):
        rs = self._result_set
        mask = self.mask 
        truth_mat = np.tile(rs.truth, (rs.nnets, 1))
        compare_mat = (truth_mat == rs.classified)
        accuracy = compare_mat[:,mask].sum(axis=1)/float(len(mask))
        return accuracy

class ResultSetArchive(object):
    def __init__( self,run_id, num_result_sets=100, truth=False):
        self._run_id = run_id
        self._num = num_result_sets
        self._rs_ctr = 0    # a single archive count
        self._arch_ctr = 0  # the total count for this resultset archive
        self._instructions = {}
        self._data = {}
        self._sent = {}
        self._file_name = hashlib.md5()
        self._arch_name = hashlib.md5()
        self._truth = truth

    @property
    def run_id(self):
        return self._run_id

    def add_result_set( self, result_set):
        (file_id, inst, data) = result_set.archive_package()
        self._instructions[file_id] = inst
        self._data[file_id] = data
        self._file_name.update( file_id )
        self._rs_ctr += 1
        self._arch_ctr += 1
        if self._rs_ctr >= self._num:
            self.write()

    def write(self):
        self._write_instructions()
        self._write_data()
        self._sent[self.file_hash] = self._instructions.keys()
        self._arch_name.update( self.file_hash )
        self._instructions = {}
        self._data = {}
        self._rs_ctr = 0
        self._file_name = hashlib.md5()

    @property
    def file_hash(self):
        return self._file_name.hexdigest()

    @property
    def archive_hash(self):
        return self._arch_name.hexdigest()

    @property
    def sent(self):
        return self._sent

class S3ResultSetArchive(ResultSetArchive):
    def __init__(self,run_id, bucket_name, path=None, num_result_sets=100 ):
        super(S3ResultSetArchive,self).__init__(run_id, num_result_sets)
        self._bucket_name = bucket_name
        self._bucket = None
        self._path = path

    def _write_data(self):
        with tempfile.SpooledTemporaryFile() as temp:
            json.dump( self._instructions, temp)
            temp.seek(0)
            key = Key(self.bucket)
            if self._path:
                key.key = '%s/%s.json' % ( self._path, self.file_hash)
            else:
                key.key = '%s.json' % self.file_hash
            key.set_contents_from_file( temp )

    def _write_instructions(self):
         with tempfile.SpooledTemporaryFile() as temp:
            np.savez(temp, **self._data)
            temp.seek(0)
            key = Key(self.bucket)
            if self._path:
                key.key = '%s/%s.npz' % ( self._path, self.file_hash)
            else:
                key.key = '%s.npz' % self.file_hash
            key.set_contents_from_file( temp )

    @property
    def bucket(self):
        while not self._bucket:
            try:
                conn = boto.connect_s3()
                self._bucket = conn.create_bucket(self._bucket_name)
            except:
                print "could not connect to %s " % self.s3_from_gpu
                print "Try again"
                time.sleep(5)
        return self._bucket

    def close_archive(self):
        if self._rs_ctr > 0:
            self.write()
        with tempfile.SpooledTemporaryFile() as temp:
            json.dump( self._sent, temp)
            temp.seek(0)
            key = Key(self.bucket)
            if self._path:
                key.key = '%s/%s.manifest.json' % ( self._path, self.archive_hash)
            else:
                key.key = '%s.manifest.json' % self.archive_hash
            key.set_contents_from_file( temp )
        run_mdl.insert_ANRunArchive( self.run_id, self.archive_hash, self._arch_ctr,
                bucket = self._bucket, 
                archive_manifest = '%s.manifest.json' % self.archive_hash,
                path = self._path, truth = self._truth)

if __name__ == "__main__":
        sqs = boto.connect_sqs()
        d2a = sqs.create_queue( 'from-data-to-agg-b6-canonical-q92-bak' )
        archive = S3ResultSetArchive('this-is-a-test-run-id', 'an-scratch-bucket', 
                path="S3ResultSetArchiveTest3", num_result_sets=9 )
        ctr = 0
        for i in range(2):
            messages = d2a.get_messages(10)
            for message in messages:
                ctr += 1
                instructions = json.loads( message.get_body() )
                rs = S3ResultSet(instructions, 'an-from-gpu-to-agg-b6-canonical-q92')
                """
                print "rs.nsamp"
                print rs.nsamp
                print "rs.file_id"
                print rs.file_id
                print "rs.nnets"
                print rs.nnets
                print "rs.alleles"
                print rs.alleles
                print "rs.data"
                print rs.data
                print "rs.classified"
                print rs.classified
                print "rs.truth"
                print rs.truth
                print "rs.get_run_id()"
                print rs.get_run_id()
                print "rs.get_strain()"
                print rs.get_strain()
                print "rs.get_result_files()"
                print rs.get_result_files()
                print "rs.archive_package()"
                print rs.archive_package()
                for m in ["[0,100)", "[10,20)", "[13,17)", "[0,100)"]:
                    mrs = Masked( rs, m)
                    print "Mask id"
                    print mrs.mask_id
                    print "mrs.mask"
                    print mrs.mask
                    print "Masked accuracy"
                    print mrs.accuracy()
                """
                archive.add_result_set( rs )
                print ctr 
                print archive.sent
        archive.close_archive()
