import boto
from boto.sqs.message import Message
import json

def clean( q1, q2):
    a = q1.read()
    while a:
        m = a.get_body()
        temp = json.loads( m )
        q2.write(Message(body=m))
        q1.delete_message(a)
        a = q1.read()

if __name__  == "__main__":
    from  mpi4py import MPI
    comm = MPI.COMM_WORLD
    sqs = boto.connect_sqs()
    frm = sqs.get_queue('from-data-to-agg-s129-reactome-bak')
    to = sqs.get_queue('from-data-to-agg-s129-reactome')
    clean(frm, to)

