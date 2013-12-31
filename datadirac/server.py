import logging
from mpi4py import MPI
from utils import debug
import worker

def init_logging( logging_file, level, boto_level, std_out_level):
    debug.initMPILogger( logging_file, level=level, boto_level=boto_level)
    if std_out_level:
        root = logging.getLogger()
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(std_out_level)
        formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)

def run(data_log_dir, working_dir,  init_q ):
    nf = worker.NodeFactory(MPI.COMM_WORLD,data_log_dir,working_dir, init_q)
    thisNode = nf.getNode()
    thisNode.get_data()
    while thisNode.run():
        thisNode.logger.info("Completed one run")
    world_comm.Barrier()

def main():
    comm = MPI.COMM_WORLD
    import argparse
    import ConfigParser
    import os, os.path
    name = "InitNode[%i]" % comm.rank
    if comm.rank == 0:
        #only master reads config
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', '--config', help='Configfile name', required=True)
        args = parser.parse_args()
        defaults = {'boto_level':'ERROR',
                    'std_out_level':None}
                    'level':'ERROR'}
        config = ConfigParser.ConfigParser(defaults=defaults )
        config.read(args.config)
        #get logging(master)
        boto_level = config.get('logging', 'boto_level')
        std_out_level = config.get('logging', 'std_out_level')
        level = config.get('logging', 'level')
        logging_file = config.get('logging', 'logging_file')
        #bcast logging
        comm.bcast(boto_level)
        comm.bcast(std_out_level)
        comm.bcast(level)
        comm.bcast(logging_file)
    else:
        #get logging(worker)
        boto_level = comm.bcast()
        std_out_level = comm.bcast()
        level = comm.bcast()
        logging_file = comm.bcast()
        
    init_logging( logging_file, level, boto_level, std_out_level )
    logger = logging.getLogger(name)
    logger.info( "Logging initialized" )

    if comm.rank == 0:
        data_log_dir = config.get('cluster_init', 'data_log_dir')
        working_dir =  config.get('cluster_init', 'working_dir')
        init_q = config.get('cluster_init', 'init_q')
    else:
        data_log_dir, working_dir,  init_q = (None, None, None)
    run( data_log_dir, working_dir, init_q )
    logger.info("Exitting...")


