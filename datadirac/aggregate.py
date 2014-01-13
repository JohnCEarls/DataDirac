import boto.sqs
import boto
import json
import numpy as np
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

    @property
    def result_bucket(self):
        if not self._result_bucket:
            conn = boto.connect_s3()
            self._result_bucket = conn.get_bucket(self.s3_from_gpu)
        return self._result_bucket

    def get_spec(self):
        m = self.data_queue.get_messages()
        inst = json.loads( m[0].get_body() )
        self.data_queue.delete_message(m[0])
        return inst

    def get_nsamp(self, inst):
        return len(inst['sample_names'])
    
    def 

