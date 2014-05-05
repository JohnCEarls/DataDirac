#from datadirac
from utils import debug
import data
import gpudata
import dirac
#sys funcs
import time
import random
import logging
import socket
import sys
import os
import os.path as op
import hashlib
import copy
import json
import bisect
import itertools
import string
import base64
#scipy stack
import pandas
import numpy as np
import scipy.misc
#aws
import boto
import boto.sqs
from boto.s3.key import Key
from boto.exception import S3ResponseError
from boto.sqs.message import Message
#mpi
from mpi4py import MPI

#tags
WORKER_READY = 123
WORKER_EXIT = 321
WORKER_JOB = 333
#original source for this is in tcDirac/tcdirac/workers
#on the subcluster branch
class NodeFactory:
    def __init__(self, world_comm, data_log_dir, working_dir, init_q):
        self.logger = logging.getLogger('NodeFactory_[%i]' % world_comm.rank)
        self.logger.info("NodeFactory init")
        if world_comm.rank > 0:
            self.thisNode = DataNode(world_comm, data_log_dir, working_dir, init_q)
        else:
            self.thisNode = MasterDataNode(world_comm,
                                            data_log_dir, working_dir, init_q)

    def getNode(self):
        return self.thisNode

class DataDescription(object):
    def __init__( self, dataNode_pkg, file_id, strain, shuffle):
        """
        Saves the data description to a file
        """
        (sample_names, alleles, sample_allele) = dataNode_pkg
        desc_dict = {}
        desc_dict['file_id'] = file_id
        desc_dict['sample_names'] = list(sample_names)
        desc_dict['alleles'] = alleles
        desc_dict['sample_allele'] = sample_allele
        for k,v in sample_allele.iteritems():
            desc_dict['sample_allele'][k] = list(v)
        desc_dict['data_matrices'] = {}
        self.desc_dict = desc_dict

    def add_matrices(self, allele_file_id, f_types, fpaths):
        afi = self.desc_dict['data_matrices'][allele_file_id] = {}
        for ft, fp in zip(f_types, fpaths):
            _ ,f_tail = op.split(fp)
            afi[ft] = f_tail

    def package( self ):
        return json.dumps( self.desc_dict )

class SQSMessage(object):
    def package(self):
        return json.dumps(self._msg)

class GPUSQSMessage(SQSMessage):
    def __init__(self, file_id):
        self._msg = {}
        self._msg['file_id'] = file_id
        self._msg['f_names'] = {}

    def add_matrices(self, f_types, fpaths):
        for ft, fp in zip(f_types, fpaths):
            _ ,f_tail = op.split(fp)
            self._msg['f_names'][ft] = f_tail

class DataSQSMessage(SQSMessage):
    def __init__(self,run_id, file_id, strain, shuffle, num_nets ):
        self._msg = {}
        self._msg['run_id'] = run_id
        self._msg['file_id'] = file_id
        self._msg['strain'] = strain
        self._msg['shuffle'] = shuffle
        self._msg['result_files'] = {}
        self._msg['sample_names'] = {}
        self._msg['sample_allele'] = {} 
        self._msg['num_networks'] = num_nets

    def add_result(self, allele, allele_file_id):
        self._msg['result_files'][allele] = \
                                    self._result_file_name( allele_file_id )

    def add_sample_names( self, sample_names ):
        self._msg['sample_names'] = sample_names.tolist()

    def add_sample_allele( self, sample_allele ):
        self._msg['sample_allele'] = sample_allele

    def _result_file_name( self, allele_file_id):
        #see gpudirac.subprocesses.packer:138
        return "%s_rms" % allele_file_id

class MPINode:
    """
    Abstract base class for an mpi process
    """
    def __init__(self, world_comm ):
        self.name = 'MPINode_%i' % world_comm.rank
        self.logger = logging.getLogger(self.name)
        self.world_comm = world_comm
        self.nb = np.empty((1,), dtype=int)#nullbuffer
        self.log_init()

    def log_init(self):
        self.logger.info( "world comm rank:\t %i" % self.world_comm.rank )

    def make_dirs(self, dirs, force=False):
        e_ctr = 0
        for d in dirs:
            while not op.exists(d):
                try:
                    self.logger.info("Creating [%s]"%d)
                    os.makedirs(d)
                except OSError:
                    if e_ctr < 10:
                        self.logger.exception("****RECOVERABLE: Trying again***")
                        time.sleep(random.random()*e_ctr)
                    else:
                        e_ctr += 1
                        raise

    def get_data(self):
        s2f = self._s3_to_fname
        self._master_init()
        self._cluster_init()
        self.make_dirs([self.working_dir, self.data_log_dir])
        self._get_data_files()
        sd = data.SourceData()
        mi = None
        self.logger.info('init SourceData')
        sd.load_dataframe( op.join( self.working_dir, s2f(self.data_file)) )
        self.logger.info('init MetaInfo')
        mi = data.MetaInfo(op.join(self.working_dir, s2f(self.meta_file) ))
        self._handle_net_info(sd)
        self._sd = sd
        self._mi = mi
        self.logger.debug("Received SourceData and MetaInfo")
        self._pathways = self._get_pathways()

    def _s3_to_fname( self, s3_file ):
        return op.split( s3_file )[-1]

    def _get_data_files(self):
        """
        Retrieves metadata and parsed dataframe files
            (generated by utilities/hddata_process.py) from S3
        """
        s2f = self._s3_to_fname
        while not op.exists(op.join( self.working_dir, s2f(self.meta_file))):
            try:
                conn = boto.connect_s3()
                b = conn.get_bucket(self.ds_bucket)
                k = Key(b)
                k.key = self.meta_file
                k.get_contents_to_filename(
                        op.join( self.working_dir, s2f(self.meta_file)))
            except:
                time.sleep(random.random())

        while not op.exists(op.join( self.working_dir, s2f(self.data_file))):
            conn = boto.connect_s3()
            try:
                b = conn.get_bucket(self.ds_bucket)
                k = Key(b)
                k.key = self.data_file
                k.get_contents_to_filename( op.join( self.working_dir,
                                                            s2f(self.data_file)) )
            except S3ResponseError:
                self.logger.exception( 'Master has not generated files' )
                raise
            except OSError:
                time.sleep(random.random())

class DataNode(MPINode):
    def __init__(self, world_comm, data_log_dir, working_dir, init_q):
        MPINode.__init__(self, world_comm )
        self.name = 'DataNode_%i' % world_comm.rank
        self.logger = logging.getLogger(self.name)
        self._nominal_alleles = {}
        self._sd = None #data.SourceData object
        self._mi = None #data.MetaInfo
        self._results = {}
        self._srt_cache = {}
        self._sample_x_allele = {}
        self._pathways = None
        self.to_gpu_sqs = None
        self.to_data_sqs = None
        self.data_log_dir = data_log_dir
        self.working_dir = working_dir
        self.init_q = init_q

    def _master_init(self):
        """
        Data Master recvs initial instructions from Master server
        """
        self.world_comm.barrier()

    def _cluster_init(self):
        """
        Worker nodes receive initial settings from Master
        """
        self.logger.info("Cluster init starting")
        self.data_sqs_queue = self.world_comm.bcast()
        self.logger.debug("data_sqs_queue: %s" % self.data_sqs_queue )
        self.data_sqs_queue_truth = self.world_comm.bcast()
        self.logger.debug("data_sqs_queue_truth: %s" % self.data_sqs_queue_truth )
        self.gpu_sqs_queue = self.world_comm.bcast()
        self.logger.debug("gpu_sqs_queue: %s" % self.gpu_sqs_queue )
        self.working_dir = self.world_comm.bcast() 
        self.working_dir = op.join( self.working_dir, self.name )
        self.logger.debug("working_dir: %s" % self.working_dir)
        self.working_bucket = self.world_comm.bcast()
        self.logger.debug("working_bucket: %s" % self.working_bucket)
        self.ds_bucket = self.world_comm.bcast()
        self.logger.debug("ds_bucket: %s" % self.ds_bucket)
        self.data_file = self.world_comm.bcast()
        self.logger.debug("data_file: %s" % self.data_file )
        self.meta_file = self.world_comm.bcast()
        self.logger.debug("meta_file: %s" % self.meta_file)
        self.data_log_dir = self.world_comm.bcast()
        self.logger.debug("data_log_dir: %s" % self.data_log_dir)
        self.sample_block_size = self.world_comm.bcast()
        self.pairs_block_size = self.world_comm.bcast()
        self.nets_block_size = self.world_comm.bcast()
        self.logger.debug("(%i, %i, %i)" % (
                           self.sample_block_size, 
                           self.pairs_block_size, 
                           self.nets_block_size))
        self.gpu_mem_max = self.world_comm.bcast( ) 
        self.logger.info("Cluster init complete")

    def  _handle_net_info( self, sd ):
        self.logger.debug("recving net info")
        sd.set_net_info(self.world_comm.bcast(None, root=0))

    def run(self):
        """
        Main logic, recieves from data master a strain, 
            whether to shuffle samples or not, and a
            number of times to generate data.

            Then generates the described matrices, uploads them to 
            s3, then sends the data descriptions to sqs for both the
            data aggregation nodes and the gpu nodes.
            It keeps a detailed log of what it has generated to a local file.
        """
        self.world_comm.send( self.world_comm.rank, tag=WORKER_READY)
        run_id, shuffle, strain, num_runs, k = self.world_comm.recv( 
                                            source=0, tag=WORKER_JOB )
        self.k = k
        if num_runs > 0:
            run_log, gpu_sqs_msgs, data_sqs_msgs = ([],[],[])
            num_nets = len(self._pathways)
            while num_runs > 0:
                data_pkg, gpu_pkg = self._package_data(strain, shuffle)
                file_id = self._create_file_id()
                desc = DataDescription( data_pkg, 
                        file_id, strain, shuffle)
                dsm = DataSQSMessage( run_id, file_id, strain, shuffle, num_nets)
                sample_names, alleles, sample_allele = data_pkg
                for i,allele in enumerate(alleles):
                    a_gpu_pkg = self._buffer_data( gpu_pkg, i )
                    allele_fid = self._allele_file_id( file_id, allele)
                    gsm = GPUSQSMessage( allele_fid )
                    dsm.add_result( allele, allele_fid)
                    dsm.add_sample_names( sample_names )
                    dsm.add_sample_allele( sample_allele )
                    f_types, f_paths = self._save_data( a_gpu_pkg, allele_fid )
                    self._transmit_data( allele_fid, f_types, f_paths )
                    desc.add_matrices( allele_fid, f_types, f_paths)
                    gsm.add_matrices( f_types, f_paths)
                    gpu_sqs_msgs.append( gsm.package() ) 
                data_sqs_msgs.append( dsm.package() )
                run_log.append( desc.package() )
                num_runs -= 1
            self._save_log( '\n'.join( run_log ) + '\n' )
            self._transmit_sqs( gpu_sqs_msgs, self.gpu_sqs_queue )
            if shuffle:
                self._transmit_sqs( data_sqs_msgs, self.data_sqs_queue )
            else:
                self._transmit_sqs( data_sqs_msgs, self.data_sqs_queue_truth )
            return True
        else:
            self.world_comm.send(self.world_comm.rank, tag=WORKER_EXIT)
            return False

    def _create_file_id(self):
        """
        Generates a file id, this marks the dirac package, 
            i.e. all of the matrices for comparison share this id
        """
        return ''.join(random.choice(string.ascii_lowercase) 
                for x in range(7))

    def _allele_file_id(self, file_id, allele):
        """
        This id marks a subunit of comparison, 
            i.e. we compare the results for each allele
                 with a common file_id
        """
        return "%s-%s" % (file_id, allele)

    def _save_log(self, log):
        """
        This log refers to a complete specification of the generated data
        """
        m = hashlib.md5()
        m.update( log )
        with open(op.join(self.data_log_dir, m.hexdigest()), 'w') as mylog:
            mylog.write( log )

    def _transmit_sqs( self, sqs_msgs, sqs_queue ):
        """
        Given a list of json encoded strings,
            passes batches of messages to SQS
        """
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        my_queue = conn.get_queue(sqs_queue)
        max_msgs = 10
        msg_batch = []
        for i, msg in enumerate(sqs_msgs):
            if len(msg_batch) == max_msgs:
                my_queue.write_batch( msg_batch )
                msg_batch = []
            msg_batch.append( (i,base64.b64encode(msg) ,0) )
        if len(msg_batch):
            my_queue.write_batch( msg_batch )

    def _transmit_data(self,file_id, f_types, f_paths):
        mess = {}
        mess['file_id'] = file_id
        mess['f_names'] = dict(((ft,f) for ft, f in zip(f_types, 
                                        [op.split(f)[1] for f in f_paths])))
        conn = boto.connect_s3() 
        bucket = conn.get_bucket(self.working_bucket)
        self.logger.debug("Sending data for file_id[%s]"%file_id)
        for f in f_paths:
            k = Key(bucket)
            k.key = op.split(f)[1]
            if not k.exists():
                k.set_contents_from_filename(f)
            self.logger.debug("Sending from [%s] to [s3://%s/%s]" % \
                    (f, self.working_bucket, k.key) )

    def _buffer_data(self, data_pkg, allele_index):
        """
        Copies data_pkg to buffered data copy
        Each allele has a different sample map, so sample map changes

        #TODO
        #OPTIMIZATION - only create other 3 data buffers when necessary
        """
        (samp_maps, net_map, gene_map, expression_matrix) = data_pkg
        samp = gpudata.SampleMap( samp_maps[allele_index])
        samp.createBuffer( self.sample_block_size, buff_dtype=np.int32 )
        exp = gpudata.Expression( expression_matrix )
        exp.createBuffer( self.sample_block_size, buff_dtype=np.float32 )
        gene = gpudata.GeneMap( gene_map )
        gene.createBuffer( self.pairs_block_size, buff_dtype=np.int32 )
        net = gpudata.NetworkMap( net_map )
        net.createBuffer( self.nets_block_size, buff_dtype=np.int32 )
        return ( samp.buffer_data, net.buffer_data,
                                        gene.buffer_data, exp.buffer_data )

    def _save_data(self, data_pkg, file_id):
        """
        Writes data_pkg files to disk and returns the file names
        in (samp_file, net_file, gene_file, exp_file)
        """
        (samp_maps, net_map, gene_map, expression_matrix) = data_pkg
        data_map = ['sm','nm','gm','em']
        file_paths = [self._save_matrix(matrix) for matrix in data_pkg]
        return (data_map, file_paths)

    def _save_matrix( self, matrix ):
        """
        Given a numpy matrix, creates a file_name using md5 hash,
        and if the file does not exist in the working dir, writes
        the matrix to a file in the working dir with the created
        file_name.
        Returns the absolute path to the file(string)
        """
        f_name = self.get_file_name( matrix )
        f_path = op.join(self.working_dir, f_name )
        count = 0
        while not op.exists(f_path):
            try:
                with open(f_path, 'wb') as df:
                    np.save(df, matrix )
            except:
                count += 1
                time.sleep(.1 * random.random())
                if count > 10:
                    raise
        return f_path

    def get_file_name(self, nd_array):
        """
        Generates the md5 of nd_array and returns
        the hexdigest
        """
        m = hashlib.md5()
        m.update(nd_array)
        return m.hexdigest()

    def k_nearest(self,compare_list,samp_name, samp_age, k):
        """
        Given compare_list, which contains tuples in sorted order
            of (sample_age, sample_name).
        returns k sample names that are closest in age to samp_age
        """
        compare_list = [(a,n) for a,n in compare_list if (a,n) != (samp_age,samp_name)]
        while k > len(compare_list):
            self.logger.warning("k is too large [%i], adjusting to [%i]"%(k,k-1))
            k -= 1
        off = k/2
        i = bisect.bisect_left(compare_list,(samp_age,samp_name) )
        l = i - off
        u = i + off
        if l < 0:
            u = u - l
            l = 0
        if u >= len(compare_list):
            l = l - (u - (len(compare_list) - 1))
            u = len(compare_list) - 1
        samp_compare = [s for a,s in compare_list[l:u+1]]
        return samp_compare

    def get_alleles(self, strain):
        """
        Returns  ['strain1_allele1', 'strain1_allele2', ...],

        These are the elements we are comparing.
        NOTE: these are the "Nominal" alleles.
        """
        if strain not in self._nominal_alleles:
            self._nominal_alleles[strain] = self._mi.get_nominal_alleles(strain)
        return self._nominal_alleles[strain]

    def get_strains(self):
        """
        Returns ['strain1', 'strain2', ... ]
        """
        return self._mi.get_strains()
 
    def _get_pathways(self):
        """
        Returns dict containing 
            {
            'pathway0': ['g1','g2','g3', ... 'gn],
            'pathway1': ['g1','g2','g3', ... 'gn],
            'pathway2': ['g1','g2','g3', ... 'gn],
            'pathway3': ['g1','g2','g3', ... 'gn],
            'pathway4': ['g1','g2','g3', ... 'gn],
            }
        """
        pws = None
        pws = self._sd.get_pathways()
        return pws 

    def _get_samples_by_strain(self, strain):
        """
        Returns a list of all sample ids belonging to strain
        """
        return self._mi.get_sample_ids(strain)

    def _partition_samples_by_allele(self, cstrain, shuffle=False):
        """
        Get a dictionary of lists of sample names and ages 
            partitioned by allele in increasing
            age order.
        Given alleles(list of strings), mi(metadataInfo object), 
            cstrain (string: current strain)
        returns dict[allele] -> list:[(age_1,samp_name_1), ... ,
            (age_n,samp_name_n)] sorted by age
        """
        mi = self._mi
        samples = {}
        if not shuffle:
            for allele in self.get_alleles(cstrain):
                samples[allele] = [(mi.get_age(sample),sample) 
                        for sample in mi.get_sample_ids(cstrain,allele)]
                samples[allele].sort()
        else:
            #test
            old = copy.deepcopy(self._sample_x_allele)
            all_samples = mi.get_sample_ids(cstrain)[:]
            random.shuffle(all_samples)
            for allele in self.get_alleles(cstrain):
                n = len(mi.get_sample_ids(cstrain,allele))
                samples[allele] = [(mi.get_age(s),s) for s in all_samples[:n]]
                samples[allele].sort()
                all_samples = all_samples[n:]
        self._sample_x_allele[cstrain] = samples 

    def _get_samples_by_allele(self, cstrain, allele):
        """
        Given a strain and allele, return list of all samples
        of that type
        """
        return self._sample_x_allele[cstrain][allele]

    def _package_data(self, strain, shuffle=False):
        """
        Given a strain and shuffling instructions return
            dataNode_pkg - describes generated data
            gpuNode_pkg - contains the necessary matrices 
                          for a gpudirac run
        """
        self._partition_samples_by_allele( strain, shuffle)
        sample_names = self._get_samples_by_strain(strain)
        alleles = self.get_alleles(strain)
        sample_allele = {}
        exp = self._sd.get_expression(sample_names).copy()
        sample_names = exp.columns
        sm = dict( ((sm,i) for i, sm in enumerate(exp.columns)) )
        gm = dict( ((gn,i) for i, gn in enumerate(exp.index)) )
        samp_maps = []
        for allele in alleles:
            a_samp_map = np.empty((len(sample_names),self.k),dtype=np.int32)
            sample_allele[allele] = self._get_samples_by_allele(strain,allele)
            for samp_name in sample_names:
                samp_age = self._mi.get_age(samp_name)
                neighbors = self.k_nearest(sample_allele[allele], 
                                                 samp_name, samp_age, self.k) 
                samp_i = sm[samp_name]
                for n_i, n_samp_id in enumerate(neighbors):
                    a_samp_map[samp_i, n_i] = sm[n_samp_id]
            samp_maps.append( a_samp_map )
        gene_index = []
        pw_offset = [0]
        for pw in self._pathways:
            genes = self._sd.get_genes(pw)
            for g1,g2 in itertools.combinations(genes,2):
                gene_index += [gm[g1],gm[g2]]
            pw_offset.append(pw_offset[-1] + 
                    scipy.misc.comb(len(genes),2,exact=1) )
        net_map = np.array(pw_offset, dtype=np.int32)
        gene_map = np.array(gene_index, dtype=np.int32)
        expression_matrix = exp.values.astype(np.float32)
        dataNode_pkg = (sample_names, alleles, sample_allele)
        gpuNode_pkg = (samp_maps, net_map, gene_map, expression_matrix) 
        return (dataNode_pkg, gpuNode_pkg)

class MasterDataNode(DataNode):
    """
    The mpi master for a cluster of data generation nodes.
    Takes a communicator object.
    """
    def __init__(self, world_comm, data_log_dir, working_dir, init_q):
        DataNode.__init__(self, world_comm, data_log_dir,
                working_dir, init_q)
        self.name = "MasterDataNode"
        self.logger = logging.getLogger(self.name)
        self.logger.debug("Initializing")
        self.data_log_dir = data_log_dir
        self.working_dir = working_dir
        self.init_q_name = init_q

    def _master_init(self):
        """
        Retrieves settings from master server
        """
        self.logger.debug("Generating command queues")
        self._generate_command_queues()
        md = boto.utils.get_instance_metadata()
        self._availabilityzone = md['placement']['availability-zone']
        self._region = self._availabilityzone[:-1]
        init_msg = {'message-type':'data-gen-init',
                    'name':self.name,
                    'instance-id': md['instance-id'],
                    'command': self.command_q_name,
                    'response': self.response_q_name,
                    'zone': self._availabilityzone,
                    'num-nodes': self.world_comm.size
                    }
        m = Message( body=json.dumps( init_msg ) )
        conn = boto.sqs.connect_to_region( 'us-east-1' )

        init_q = None
        ctr = 0
        while init_q is None and ctr < 6:
            init_q = conn.get_queue( self.init_q_name  )
            self.logger.debug("Init Q: %r" % init_q)
            self.logger.debug("Init msg: %s" % json.dumps( init_msg ) )
            time.sleep(1+ctr**2)
            ctr += 1
        if init_q is None:
            self.logger.error("Unable to connect to init q")
            raise Exception("Unable to connect to init q")
        init_q.write( m )
        command_q = conn.create_queue( self.command_q_name )
        command = None
        while command is None:
            command = command_q.read( wait_time_seconds=20)
            if command is None:
                self.logger.warning("No message in command queue.")
        init_msg = command.get_body()
        command_q.delete_message( command )
        self.logger.info("Init Message %s" % init_msg)
        parsed = json.loads(init_msg)
        self._handle_command( parsed )

    def _set_settings( self, settings ):
        self.data_sqs_queue = settings['data_sqs_queue']
        self.gpu_sqs_queue = settings['gpu_sqs_queue']
        self.data_sqs_queue_truth = settings['data_sqs_queue_truth']
        conn = boto.sqs.connect_to_region('us-east-1')
        conn.create_queue( self.data_sqs_queue )
        conn.create_queue( self.data_sqs_queue_truth )
        conn.create_queue( self.gpu_sqs_queue )
        self.working_bucket = settings['working_bucket']
        conn = boto.connect_s3()
        conn.create_bucket( self.working_bucket )
        self.ds_bucket = settings['ds_bucket']
        self.data_file = settings['data_file']
        self.meta_file = settings['meta_file']
        self.network_table = settings['network_table']
        self.network_source = settings['network_source']
        self.sample_block_size = settings['sample_block_size']
        self.pairs_block_size = settings['pairs_block_size']
        self.nets_block_size = settings['nets_block_size']
        self.gpu_mem_max = settings['gpu_mem_max']
        self.logger.info("Settings inititialized")
        self.world_comm.barrier()


    def _generate_command_queues(self):
        md = boto.utils.get_instance_metadata()
        self.command_q_name = "%s-%s-command" % (self.name, md['instance-id'])
        self.response_q_name = "%s-%s-response" % (self.name, md['instance-id'])
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        command_q = conn.create_queue( self.command_q_name )
        command_q = None
        while command_q is None:
            #make sure command queue is accessible
            command_q = conn.get_queue(  self.command_q_name )
            time.sleep(1)
        self.logger.info("Command queue[%s] created" % self.command_q_name)
        response_q = conn.create_queue( self.response_q_name )
        response_q = None
        while response_q is None:
            #make sure response queue is accessible
            response_q = conn.get_queue(  self.response_q_name )
            time.sleep(1)
        self.logger.info("Response queue[%s] created" % self.response_q_name)

    def _cluster_init(self):
        """
        Distributes settings
        """
        self.logger.info("Cluster init starting")
        self.data_sqs_queue = self.world_comm.bcast(self.data_sqs_queue)
        self.logger.debug("data_sqs_queue: %s" % self.data_sqs_queue )
        self.data_sqs_queue_truth = self.world_comm.bcast(
                                                self.data_sqs_queue_truth)
        self.gpu_sqs_queue = self.world_comm.bcast(self.gpu_sqs_queue )
        self.logger.debug("gpu_sqs_queue: %s" % self.gpu_sqs_queue )
        self.working_dir = self.world_comm.bcast(self.working_dir ) 
        self.logger.debug("working_dir: %s" % self.working_dir)
        self.working_bucket = self.world_comm.bcast(self.working_bucket)
        self.logger.debug("working_bucket: %s" % self.working_bucket)
        self.ds_bucket = self.world_comm.bcast(self.ds_bucket)
        self.logger.debug("ds_bucket: %s" % self.ds_bucket)
        self.data_file = self.world_comm.bcast(self.data_file )
        self.logger.debug("data_file: %s" % self.data_file )
        self.meta_file = self.world_comm.bcast(self.meta_file)
        self.logger.debug("meta_file: %s" % self.meta_file)
        self.data_log_dir = self.world_comm.bcast(self.data_log_dir)
        self.logger.debug("data_log_dir: %s" % self.data_log_dir)
        self.sample_block_size = self.world_comm.bcast(self.sample_block_size)
        self.pairs_block_size = self.world_comm.bcast(self.pairs_block_size)
        self.nets_block_size = self.world_comm.bcast(self.nets_block_size)
        self.logger.debug("(%i, %i, %i)" % (
                           self.sample_block_size, 
                           self.pairs_block_size, 
                           self.nets_block_size))
        self.gpu_mem_max = self.world_comm.bcast( self.gpu_mem_max ) 
        self.logger.info("Cluster init complete")

    def _handle_net_info(self, sd):
        """
        Gets network information from dynamodb and broadcasts it to the
        worker nodes
        """
        s2f = self._s3_to_fname
        sd.load_dataframe(op.join( self.working_dir, s2f(self.data_file)))
        sd.load_net_info(table_name=self.network_table,
                            source_id=self.network_source)
        self.logger.debug("sending net info")
        self.world_comm.bcast(sd.net_info)


    def run(self):
        
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        terminate = False
        while not terminate:

            command_q = conn.get_queue( self.command_q_name )
            command = None
            while command is None:
                command = command_q.read( wait_time_seconds=20)
                if command is None:
                    self.logger.warning("No message in command queue.")
            msg = json.loads(command.get_body())
            command_q.delete_message( command )
            terminate = self._handle_command( msg )
            self.logger.debug("Finished command %s" % msg['message-type'])

        exit_count = 0
        while exit_count < (self.world_comm.size - 1):
            worker_ready_id = self.world_comm.recv(source=MPI.ANY_SOURCE, 
                                                    tag=WORKER_READY )
            self.logger.info("Terminate to worker[%i]" % worker_ready_id)
            message = (None, True, None, -1, 0)
            self.world_comm.send(dest=worker_ready_id, obj=message, tag=WORKER_JOB)
            self.world_comm.recv(source=worker_ready_id, tag=WORKER_EXIT)
            exit_count += 1
        self.logger.info( "Exiting run" )

    def _handle_command( self, command ):
        self.logger.debug("Recvd command: %s " %\
            json.dumps(command) )
        if command['message-type'] == 'init-settings':
            self._set_settings( command )
            return False
        if command['message-type'] == 'run-instructions':
            self._run(command['run-id'], command['strain'], command['num-runs'], 
                    command['shuffle'], command['k'])
            return False
        if command['message-type'] == 'termination-notice':
            self.logger.warning("Recvd Terminate")
            return True

    def _run(self,run_id, strain, total_runs, shuffle, k):     
        self.total_runs = total_runs
        self.k = k
        quit = False
        status = MPI.Status()
        partial_run_size = self.partial_run_size 
        self.logger.debug("Each run has a size of [%i]" % partial_run_size)
        start_time = time.time()
        while total_runs > 0:
            self.logger.debug("Getting ready worker")
            worker_ready_id = self.world_comm.recv(source=MPI.ANY_SOURCE, 
                                                    tag=WORKER_READY )
            partial_runs = min(partial_run_size, total_runs)
            self.logger.debug("Sending work to [%i] num_runs[%i]" % \
                    (worker_ready_id, partial_runs ))
            message = (run_id, shuffle, strain, partial_runs, k)
            self.world_comm.send(dest=worker_ready_id, obj=message, tag=WORKER_JOB)
            total_runs -= partial_runs
            self.logger.debug("Runs remaining[%i]" % total_runs)
        self.logger.info("Waiting for a worker to finish")
        self.world_comm.Probe( source=MPI.ANY_SOURCE, tag=WORKER_READY )
        elapsed_time = time.time() - start_time
        self._send_run_complete( strain, self.total_runs, shuffle, k, elapsed_time )
        return False

    def _send_run_complete(self, strain, num_runs, shuffle, k, elapsed_time):
        message = {'message-type': 'run-complete',
                    'strain': strain,
                    'num-runs': num_runs,
                    'shuffle': shuffle,
                    'k': k,
                    'elapsed_time': elapsed_time}
        js_mess = json.dumps( message )
        self.logger.debug("Sending run complete message")
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        rq = conn.get_queue( self.response_q_name )
        self.logger.debug( "Sending %s:" % js_mess)
        rq.write( Message( body=js_mess ) )

    @property
    def partial_run_size(self):
        return (self.total_runs/(3*(self.world_comm.size -1))) + 1

if __name__ == "__main__":
    import time
    world_comm = MPI.COMM_WORLD
    level = logging.DEBUG
    host_file = '/home/sgeadmin/hdproject/tcDirac/tcdirac/mpi.hosts'
    isgpu = False
    logfile = "/scratch/sgeadmin/log_mpi_r%i.txt"%world_comm.Get_rank()
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=level, format=log_format)
    debug.initMPILogger("/scratch/sgeadmin/logging/worker.log", level = level)

    root = logging.getLogger()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    bl = logging.getLogger('boto')
    bl.setLevel(logging.ERROR)

    nf = NodeFactory( world_comm )
    thisNode = nf.getNode()
    thisNode.get_data()
    while thisNode.run():
        thisNode.logger.info("Completed one run")
    world_comm.Barrier()
