import accumulator
import resultset
import masterdirac.models.run as run_mdl
import json
import logging
import collections
import boto
from boto.exception import  S3ResponseError
from boto.s3.lifecycle import Lifecycle,Expiration

DEL_LIFECYCLE_PATTERN = '%s-lc-delete-all'
#the pattern for marking a bucket as deletable
#a lifecycle rule deleting all objects is created under this
#name % bucket, and if the bucket is empty, it can be safely deleted

class AggManager(object):
    def __init__( self, comm, run_id=None, holdout_file=None ):
        self._logger = None
        self.comm = comm
        self.logger.debug("__init__")
        self.run_id = run_id
        self._run_model = self._get_run_model()
        self._errors = collections.defaultdict( int )
        self._archiver = None
        self._holdout_file = holdout_file


    def handle_truth( self, delete_message = True ):
        """
        Master generates the results for comparison to the permuted
        dirac results
        """
        complete = False
        if self.is_master():
            self.logger.info("Generating Truth")
            t = accumulator.Truthiness( self.run_model, self._holdout_file)
            t._set_redrive_policy()
            complete = False
            ctr = 0
            while not complete and ctr < 3:
                rs = t.get_result_set()
                ctr += 1
                if not rs:
                    self.logger.debug("empty result set")
                    continue
                else:
                    complete = t.handle_result_set( rs )
                    if complete:
                        at = self.archiver.add_result_set(rs)
                        self.close_archiver()
                    if delete_message:
                        print "Should not get here on debug"
                        #t.success()
            if not complete:
                try:
                    t._get_truth()
                    complete = True
                    #the truth exists in database
                except:
                    pass
        complete = self.comm.bcast( complete )
        if not complete:
            message = "Unable to get truth resultset for %s"
            message = message % self.run_id
            raise resultset.TruthException( message )

    @property
    def archiver(self):
        if self._archiver is None:
            ab = self.run_model['aggregator_settings']['archive_bucket']
            self._archiver = resultset.S3ResultSetArchive( self.run_id,
                                        bucket_name = ab, path=self.run_id,
                                        num_result_sets = 100)
        return self._archiver

    def close_archiver(self):
        self.archiver.close_archive()
        self._archiver = None


    def run(self, delete_message=True):
        a = accumulator.Accumulator( self.run_model, self._holdout_file )
        if self.is_master(): 
            a._set_redrive_policy( )
        ctr = 0
        rs = a.get_result_set()
        while rs:
            try:
                a.handle_result_set(rs)
                at = self.archiver.add_result_set(rs)
                if delete_message:
                    a.success()
            except accumulator.FileCorruption as fc:
                self.logger.exception("File corruption error")
                self._errors['filecorruption'] += 1
            except:
                message = "Error occured while handling resultset"
                self.logger.exception(message)
            rs = a.get_result_set()
            ctr += 1
            if a.acc_count % 1000 == 0:
                self.archiver.close_archive()
                self._archiver = None
        self.logger.debug("Worker returned")
        self.comm.barrier()
        self.logger.debug("All resultsets consumed")
        a.join( self.comm )
        if self.is_master():
            a.save_results()
        message = "Completed Aggregation[%s] " % self.run_id
        self.logger.info(message)

    @property
    def run_model(self):
        if self._run_model is None:
            self._run_model = self.get_run_model()
        return self._run_model

    def _get_run_model(self):
        """
        Returns the run model

        Note: trying to limit conns to run db, so
        there is a little jiggery pokery with MPI
        to minimize those communications
        """
        packed = None
        if self.is_master():
            run_model = self.get_run_model()
            packed = self._pack_run_model(run_model)
        packed = self.comm.bcast( packed )
        return self._unpack_run_model( packed )

    def get_run_model(self):
        self.logger.info("Getting run[%s] info" % self.run_id )
        return run_mdl.get_ANRun( self.run_id )

    def _pack_run_model(self, run_model):
        return json.dumps(self._clean_run_model(run_model))

    def _unpack_run_model(self, packed_run_model):
        return json.loads(packed_run_model)

    def _clean_run_model(self, resp ):
        """
        Converts variables to jsonable format
        """
        if type(resp) is dict:
            for key, value in resp.iteritems():
                try:
                    #if datetime convert to string
                    resp[key] = value.isoformat()
                except AttributeError as ae:
                    pass
            return resp
        elif type(resp) is list:
            return [self._clean_response( item ) for item in resp]
        else:
            return resp

    def is_master(self):
        return self.comm.Get_rank() == 0

    def clean_up(self):
        self.archiver.close_archive()
        if self.is_master():
            ic = self.run_model['intercomm_settings']
            for k,v in ic.iteritems():
                try:
                    if k[:3] == 'sqs':
                        self._cleanup_sqs(v)
                    if k[:2] == 's3':
                        self._cleanup_s3(v)
                except:
                    self.logger.exception("error cleaning up %s:%s" % (k,v))

    def _cleanup_sqs(self, queue):
        """
        Deletes queue
        """
        conn = boto.connect_sqs()
        success = conn.delete_queue( queue )
        if not success:
            self.logger.warning("Could not delete %s" % v )

    def _cleanup_s3(self, bucket_name):
        """
        Adds lifecycle rule (DEL_LIFECYCLE_PATTERN % bucket_name)
        to bucket_name that marks all objects in this
        bucket as expiring(delete) in 1 day
        """
        conn = boto.connect_s3()
        b = conn.get_bucket( bucket_name )
        del_all_pattern = DEL_LIFECYCLE_PATTERN 
        msg =  "Setting deletion lifecycle rule for %s" 
        msg = msg % bucket_name 
        self.logger.info(msg)
        lf = Lifecycle()
        lf.add_rule( id=del_all_pattern % b.name,
                expiration=Expiration(days=1),
                prefix='', status='Enabled',
                transition=None  )
        b.configure_lifecycle(lf)

    @property
    def logger(self):
        if self._logger is None:
            ln = "%s-%i" % (__name__, self.comm.rank)
            self._logger = logging.getLogger( ln )
        return self._logger

def run_hack( run_id ):
    mask_id = ["[%i,%i)" % (i, i+5) for i in range(4,16)]
    mask_id += ["[4,20)", "[4,12)", "[12,20)"]
    str_join = ','.join(mask_id)
    aggregator_settings = {
            'masks': str_join,
            'results_bucket': 'an-hdproject-csvs',
            'archive_bucket': 'an-hdproject-data-archive'
            }
    run_mdl.update_ANRun( run_id, aggregator_settings=aggregator_settings )

if  __name__ == "__main__":
    from mpi4py import MPI
    import sys
    try:
        comm = MPI.COMM_WORLD
        run_id = 'all-q111-reactome'
        holdout_file = '/home/sgeadmin/holdout.txt'
        print "Running as holdout"
        print run_id
        if comm.rank == 0:
            run_hack( run_id )
        comm.barrier()
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        b = logging.getLogger('botocore')
        b.setLevel(logging.WARNING)
        b = logging.getLogger('boto')
        b.setLevel(logging.WARNING)
        p = logging.getLogger('pynamodb')
        p.setLevel(logging.WARNING)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)
        am = AggManager( comm, run_id, holdout_file )
        am.handle_truth(delete_message=False)
        am.run()
        am.clean_up()
    except:
        am.archiver.close_archive()
        raise



