import accumulator
import resultset
import masterdirac.models.run as run_mdl
import json
import logging
import collections
DEBUG = True

class AggManager(object):
    def __init__( self, comm, run_id=None ):
        self._logger = None
        self.comm = comm
        self.logger.debug("__init__")
        self.run_id = run_id
        self._run_model = self._get_run_model()
        self._errors = collections.defaultdict( int )
        self._archiver = None

    def handle_truth( self, delete_message = True ):
        """
        Master generates the results for comparison to the permuted
        dirac results
        """
        complete = False
        if self.is_master():
            self.logger.info("Generating Truth")
            t = accumulator.Truthiness( self.run_model )
            if not DEBUG:
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
                        self.archiver.add_result_set(rs)
                    if delete_message:
                        t.success()
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
                                        bucket = ab,
                                        num_result_sets = 10)
        return self._archiver

    def run(self):
        a = accumulator.Accumulator( self.run_model )
        self._set_redrive_policy( a.data_queue, a.redrive_queue )
        rs = a.get_result_set()
        while rs:
            try:
                a.handle_result_set(rs)
            except accumulator.FileCorruption as fc:
                self.logger.exception("File corruption error")
                self._errors['filecorruption'] += 1
            except:
                message = "Error occured while handling resultset"
                self.logger.exception(message)
            rs = a.get_result_set()
        message = "Completed Accumulation[%s] " % self.run_id
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

    @property
    def logger(self):
        if self._logger is None:
            ln = "%s-%i" % (__name__, self.comm.rank)
            self._logger = logging.getLogger( ln )
        return self._logger

def run_hack( run_id ):
    aggregator_settings = {
            'masks':'[0,20],[10,15], [5,12]',
            'results_bucket': 'an-hdproject-csvs',
            'archive_bucket': 'an-hdproject-data-archive'
            }
    run_mdl.update_ANRun( run_id, aggregator_settings=aggregator_settings )

if  __name__ == "__main__":
    from mpi4py import MPI
    import sys
    comm = MPI.COMM_WORLD
    run_id = 'test-agg-009'
    print run_id
    if comm.rank == 0:
        run_hack( run_id )
    comm.barrier()
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
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
    am = AggManager( comm, run_id )
    am.handle_truth( delete_message = False)
    print "%i, [%r]" % ( am.comm.rank, am.run_model )

