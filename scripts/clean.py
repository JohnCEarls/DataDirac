import boto
import boto.sqs

def empty_bucket( bucket_name ):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket( bucket_name )
    for k in bucket.list():
        k.delete()

def delete_queue( sqs_name ):
    conn = boto.sqs.connect_to_region( 'us-east-1' )
    q = conn.get_queue( sqs_name )
    q.clear()
    q.delete()

if __name__ == "__main__":
    empty_bucket( 'gpudirac_to_gpu' )
    delete_queue( 'gpudirac_data_agg' )
    delete_queue( 'gpudirac_gpu_source' )
