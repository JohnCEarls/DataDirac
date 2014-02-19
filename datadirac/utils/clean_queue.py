import boto
from boto.sqs.message import Message
import json

def clean( q1, q2):
    myset = set()
    a = q1.read()
    
    while a:
        m = a.get_body()
        temp = json.loads( m )
        if temp['file_id'] not in myset:
            q2.write(Message(body=m))
            myset.add( temp['file_id'] )
        q1.delete_message(a)
        a = q1.read()

if __name__  == "__main__":
    sqs = boto.connect_sqs()
    frm = sqs.get_queue('from-data-to-agg-react')
    to = sqs.get_queue('from-data-to-agg-react-bak')
    clean(frm, to)
    frm.clear()
