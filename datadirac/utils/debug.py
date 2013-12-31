import time
import os
import os.path
import logging
import logging.handlers
import random
import static
from mpi4py import MPI

class MPILogFile(MPI.File):
    def write(self, buff, status=None):
        """
        Wrap {MPI.File.}Write_shared() 
        """
        self.Write_shared( buff, status)

class MPIFileHandler(logging.FileHandler):
    def __init__(self, filename, comm=MPI.COMM_WORLD , encoding=None, delay=0,
        mode=MPI.MODE_WRONLY|MPI.MODE_CREATE|MPI.MODE_APPEND):
        """
        Creates a logging.FileHandler for an MPI.File

        filename - path and name for logging file, path must exist
        comm - communicator object for logging
        encoding - note this is not used
        delay(bool) - means don't create file stream right away
        mode - mode that the file is opened in
        """
        encoding = None
        self.baseFilename = os.path.abspath(filename)
        self.mode = mode
        self.encoding = encoding
        self.comm = comm
        if delay:
            #We don't open the stream, but we still need to call the
            #Handler constructor to set level, formatter, lock etc.
            logging.Handler.__init__(self)
            self.stream = None
        else:
           logging.StreamHandler.__init__(self, self._open())

    def _open(self):
        stream = MPILogFile.Open( self.comm, self.baseFilename, self.mode )
        stream.Set_atomicity(True)
        return stream

    def close(self):
        if self.stream:
            self.stream.Sync()
            self.stream.Close()
            self.stream = None

def initMPILogger( log_file, level=logging.WARNING, 
                    boto_level=logging.ERROR ):
    """
    Configures rootlogger to use MPIFileHandler for logging
    log_file(string) - path and filename for shared log file
      NOTE: Path is automagically created by this method
    level = logging level
    """
    full_path = os.path.abspath( log_file )
    tries = 0
    path, f_name = os.path.split(full_path)
    while not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError:
            #many injuns
            #diff procs may be in the process of making this dir
            time.sleep(random.random() + 1)
            tries += 1
            if tries > 10:
                raise
    logger = logging.getLogger("")
    mh = MPIFileHandler(full_path)
    formatter = logging.Formatter(('%(asctime)s - %(name)s - %(levelname)s' 
                                    ' - %(message)s'))
    mh.setFormatter(formatter)
    mh.setLevel( level )
    logger.addHandler(mh)
    boto_logger = logging.getLogger("boto")
    boto_logger.setLevel( boto_level )

if __name__ == "__main__":
        initLogging()
        logging.error("test")
        logger = logging.getLogger("test1-new")
        logger.error("test")

