import logging
from mpi4py import MPI
from utils import debug
import worker
import sys

def push_log():
    import time
    import os, os.path
    import boto.utils
    import boto
    from boto.s3.key import Key
    #only master reads config
    import masterdirac.models.systemdefaults as sys_def
    config = sys_def.get_system_defaults( 'logging', 'Data Cluster')
    log_file = config['logging_file']
    inst_id = boto.utils.get_instance_metadata()['instance-id']
    ctime = time.strftime('%Y-%m-%d-%T', time.gmtime())
    lf_name =config['log_s3_name_format'] % (inst_id,ctime)
    conn = boto.connect_s3()
    bkt = conn.create_bucket( config['log_bucket'] )
    k = Key(bkt)
    k.key = lf_name
    k.set_metadata('project', 'HD')
    k.storage_class = 'REDUCED_REDUNDANCY'
    k.set_contents_from_filename( log_file )

def init_logging( logging_file, level, boto_level, std_out_level):
    debug.initMPILogger( logging_file, level=level, boto_level=boto_level)
    #set root logger to the lowest level
    lowest = min([l for l in  [level, boto_level, std_out_level] if l])
    logging.getLogger().setLevel(lowest)
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
    MPI.COMM_WORLD.Barrier()
    thisNode.logger.info("Exiting run")

LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL,
          'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

def main():
    comm = MPI.COMM_WORLD
    import os, os.path
    name = "InitNode[%i]" % comm.rank
    if comm.rank == 0:
        #only master reads config
        defaults = {'boto_level':'ERROR',
                    'std_out_level':None,
                    'level':'ERROR'}
        #get logging(master)
        import masterdirac.models.systemdefaults as sys_def
        config = sys_def.get_system_defaults( 'logging', 'Data Cluster')
        boto_level = config['boto_level']
        std_out_level = config['std_out_level']
        level = config['level']
        logging_file = config[ 'logging_file']
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
    level = LEVELS[level] if level else None
    std_out_level = LEVELS[std_out_level] if std_out_level else None
    boto_level = LEVELS[boto_level] if boto_level else None 

    init_logging( logging_file, level, boto_level, std_out_level )
    logger = logging.getLogger(name)
    logger.info( "Logging initialized" )

    if comm.rank == 0:
        ci_cfg = sys_def.get_system_defaults( 'cluster_init' , 'Data Cluster')
        data_log_dir = ci_cfg[ 'data_log_dir' ]
        working_dir =  ci_cfg[ 'working_dir' ]
        init_q = ci_cfg[ 'init_queue' ]
    else:
        data_log_dir, working_dir,  init_q = (None, None, None)
    run( data_log_dir, working_dir, init_q )
    logger.info("Exitting...")


