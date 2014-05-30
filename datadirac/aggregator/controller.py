import accumulator
import resultset
from masterdirac.models.run import run_mdl
import json

class AggManager(object):
    def __init__( self, comm, run_id=None ):
        self.comm = comm
        self.run_id = run_id
        self._run_model = self._get_run_model()

    def _get_run_model(self):
        """
        Returns the run model

        Note: trying to limit conns to run db, so
        there is a little jiggery pokery with MPI
        """
        packed = None
        if self.is_master():
            run_model = run_mdl.get_ANRun( self.run_id )
            packed = self._pack_run_model(run_model)
        packed = self.comm.bcast( packed )
        return self._unpack_run_model( packed )

    def get_run_model(self):
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
