import hashlib
import scipy.misc
from mpi4py import MPI
import data
import dirac
import logging
import socket
import os.path as op
import os
import boto
import bisect
import itertools
import pandas
import numpy as np
import time
from profiler import MPIProfiler
import random
from boto.s3.key import Key
import boto
import copy
from boto.exception import S3ResponseError

#original source for this is in tcDirac/tcdirac/workers
#on the subcluster branch
class NodeFactory:
    def __init__(self, world_comm):
        self.logger = logging.getLogger('NodeFactory_[%i]' % world_comm.rank)
        self.logger.info("NodeFactory init")
        if world_comm.rank > 0:
            self.thisNode = DataNode(world_comm)
        else:
            self.thisNode = MasterDataNode(world_comm)

    def getNode(self):
        return self.thisNode

class MPINode:
    def __init__(self, world_comm ):
        self.name = 'MPINode_[%i]' % world_comm.rank
        self.logger = logging.getLogger(self.name)
        self.world_comm = world_comm
        self.nb = np.empty((1,), dtype=int)#nullbuffer
        self.log_init()


    def log_init(self):
        self.logger.info( "world comm rank:\t %i" % self.world_comm.rank )

    def makeDirs(self, dirs, force=False):
        e_ctr = 0
        for d in dirs:
            while not os.path.exists(d):
                try:
                    self.logger.info("Creating [%s]"%d)
                    os.makedirs(d)
                except OSError:
                    if e_ctr < 10:
                        self.logger.exception("Trying again")
                        time.sleep(random.random()*e_ctr)
                    else:
                        e_ctr += 1
                        raise

    def getData(self, working_dir, working_bucket, ds_bucket, k):
        pass

   
class DataNode(MPINode):
    def __init__(self, world_comm ):
        MPINode.__init__(self, world_comm )
        self.name = 'DataNode_[%i]' % world_comm.rank
        self.logger = logging.getLogger(self.name)
        self._nominal_alleles = {}
        self._sd = None #data.SourceData object
        self._mi = None #data.MetaInfo
        self._results = {}
        self._srt_cache = {}
        self._sample_x_allele = {}
        self._pathways = None

    def getData(self, working_dir, working_bucket, ds_bucket,k):
        self.working_dir = working_dir
        self.working_bucket = working_bucket
        self.ds_bucket = ds_bucket
        self.k = k
        self.logger.debug("Getting sourcedata and metainfo")
        self._sd = self.world_comm.bcast(None)
        self._mi = self.world_comm.bcast(None)
        self.logger.debug("Received SourceData and MetaInfo")
        self._pathways = self._getPathways()

    def run(self):
        quit = False
        status_buffer = np.empty((1,),dtype=int)
        while not quit:
            #create data
            strains = self.getStrains()
            self.world_comm.Recv( status_buffer, source=0)
            if status_buffer[0] < 0:
                quit = True
            else:
                if status_buffer[0] < len(strains):
                    shuffle=False
                else:
                    shuffle=True
                    status_buffer[0] = status_buffer[0]/2
                mystrain = strains[status_buffer[0]]
                dataNode_pkg, gpuNode_pkg = self.packageData( mystrain, shuffle )

                (sample_names, alleles, sample_allele) = dataNode_pkg 
                (samp_maps, net_map, gene_map, expression_matrix) = gpuNode_pkg 
                self.save_data(self, gpuNode_pkg)

                self.world_comm.Recv( status_buffer, source=0)
                mygpu = status_buffer[0]
                if mygpu > 0:
                    #send to gpu
                    self.sendData(  gpuNode_pkg, mygpu )
                    #receive from gpu
                    #results = self.world_comm.recv( source=mygpu )
                else:
                    quit = True
                #partition

    def sendData( self, gpuNode_pkg, gpurank):
        (samp_maps, net_map, gene_map, expression_matrix) = gpuNode_pkg
        self.world_comm.send( samp_maps, dest=gpurank )
        self.world_comm.send(  net_map, dest=gpurank )
        self.world_comm.send( gene_map, dest=gpurank )
        self.world_comm.send( expression_matrix, dest=gpurank)

    def save_data(self, data_pkg):
         (samp_maps, net_map, gene_map, expression_matrix) = data_pkg
         rn = str(random.randint(10000, 99999))
         with open(os.path.join(self.working_dir, '_'.join(['sm',rn]))) as df:
            np.save(df, samp_maps)
         with open(os.path.join(self.working_dir, '_'.join(['nm',rn]))) as df:
            np.save(df, net_maps)
         with open(os.path.join(self.working_dir, '_'.join(['gm',rn]))) as df:
            np.save(df, gene_map)
         with open(os.path.join(self.working_dir, '_'.join(['em',rn]))) as df:
            np.save(df, expression_matrix)


    def kNearest(self,compare_list,samp_name, samp_age, k):
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
    

    def getAlleles(self, cstrain):
        if cstrain not in self._nominal_alleles:
            self._nominal_alleles[cstrain] = self._mi.getNominalAlleles(cstrain)
        return self._nominal_alleles[cstrain]

    def getStrains(self):
        return self._mi.getStrains()
 
    def _getPathways(self):
        pws = None
        pws = self._sd.getPathways()
        return pws 

    def _getSamplesByStrain(self, strain):
        """
        Returns a list of all sample ids belonging to strain
        """
        return self._mi.getSampleIDs(strain)

    def initRMSDFrame(self,cstrain):
        alleles = self.getAlleles(cstrain)
        samples = self._getSamplesByStrain(cstrain)
        indexes = ["%s_%s" % (pw,allele) for pw,allele in  itertools.product(self._pathways, alleles)]
        return pandas.DataFrame(np.empty(( len(indexes), len(samples)), dtype=float), index=indexes, columns=samples)


    def _partitionSamplesByAllele(self, cstrain, shuffle=False):
        """
        Get a dictionary of lists of sample names and ages partitioned by allele in increasing
            age order.
        Given alleles(list of strings), mi(metadataInfo object), cstrain (string: current strain)
        returns dict[allele] -> list:[(age_1,samp_name_1), ... ,(age_n,samp_name_n)] sorted by age
        """
        mi = self._mi

        samples = {}
        if not shuffle:
            for allele in self.getAlleles(cstrain):
                samples[allele] = [(mi.getAge(sample),sample)for sample in mi.getSampleIDs(cstrain,allele)]
                samples[allele].sort()
        else:
            #test
            old = copy.deepcopy(self._sample_x_allele)
            all_samples = mi.getSampleIDs(cstrain)[:]
            random.shuffle(all_samples)
            for allele in self.getAlleles(cstrain):
                n = len(mi.getSampleIDs(cstrain,allele))
                samples[allele] = [(mi.getAge(s),s) for s in all_samples[:n]]
                samples[allele].sort()
                all_samples = all_samples[n:]
        self._sample_x_allele[cstrain] = samples 


    def initStrain(self, cstrain, mypws, shuffle=False):
        self._cstrain = cstrain
        self._partitionSamplesByAllele(cstrain, shuffle)
        self._results[cstrain] = self.initRMSDFrame( mypws, cstrain )

    def getSamplesByAllele(self, cstrain, allele):
        return self._sample_x_allele[cstrain][allele]

    def genSRTs(self, cstrain, pw):
        #self.p.start("genSRTs")
        if (cstrain, pw) not in self._srt_cache:
          
            srts = None 
            sd = self._sd
            mi = self._mi
            samples = []
            for allele in self.getAlleles(cstrain):
                samples += [s for a,s in self._sample_x_allele[cstrain][allele]]
            expFrame = sd.getExpression( pw, samples)
            srts = dirac.getSRT( expFrame )
            self._srt_cache[(cstrain,pw)] = srts

        return self._srt_cache[(cstrain, pw)]

    def getRMS(self, rt, srt):
        return dirac.getRMS( srt, rt )

    def setRMS(self, rms, index, samp):
        self._results[self._cstrain][samp][index] = rms

    def saveRMS(self,prefix='rms'):
        for strain, table in self._results.iteritems():
            ofile_name = '%s.%s.%i.pandas.pkl' % (prefix,strain,self.world_comm.rank)
            table.to_pickle(op.join(self._working_dir,ofile_name))

    def classify( self, comm=None ):
        
        if comm is None:
            comm = self.world_comm
        class_dict = {}
        for strain in self.getStrains():
            mypws = self.getMyPathways(comm)
            res = self._results[strain]
            class_dict[strain] = pandas.DataFrame(np.empty(( len(mypws), len(res.columns)), dtype=int) , index=mypws, columns = res.columns )
            for pw in mypws:
                alleles = self.getAlleles(strain)
                for b_allele in alleles:
                    samps = [s for a,s in self.getSamplesByAllele(strain, b_allele)]

                    b_rows = ["%s_%s" %(pw,allele) for allele in alleles if allele != b_allele]
                    
                    for samp in samps:
                        class_dict[strain][samp][pw] = 1
                        for row in b_rows:
                            if res.loc[row,samp] >= res.loc["%s_%s" % (pw, b_allele), samp]:
                                class_dict[strain][samp][pw] = 0
            class_dict[strain] = class_dict[strain].sum(1)
        self._classification_res = class_dict
        return class_dict

    

    def packageData(self, strain, shuffle=False):
        """
        return exp for a strain
        return 
        """
    
        self._partitionSamplesByAllele( strain, shuffle)

        sample_names = self._getSamplesByStrain(strain)
        alleles = self.getAlleles(strain)
        sample_allele = {}
        self.k

        exp = self._sd.getExpression(sample_names).copy()
        sample_names = exp.columns
        sm = dict( ((sm,i) for i, sm in enumerate(exp.columns)) )
        gm = dict( ((gn,i) for i, gn in enumerate(exp.index)) )
        samp_maps = []
        for allele in alleles:
            a_samp_map = np.empty((len(sample_names),self.k),dtype=np.int32)
            sample_allele[allele] = self.getSamplesByAllele(strain,allele)
            for samp_name in sample_names:
                samp_age = self._mi.getAge(samp_name)
                neighbors = self.kNearest(sample_allele[allele], samp_name, samp_age, self.k) 
                samp_i = sm[samp_name]
                for n_i, n_samp_id in enumerate(neighbors):
                    a_samp_map[samp_i, n_i] = sm[n_samp_id]
            samp_maps.append( a_samp_map )


        gene_index = []
        pw_offset = [0]

        for pw in self._pathways:
            genes = self._sd.getGenes(pw)
            for g1,g2 in itertools.combinations(genes,2):
                gene_index += [gm[g1],gm[g2]]
            pw_offset.append(pw_offset[-1] + scipy.misc.comb(len(genes),2,exact=1) )
        net_map = np.array(pw_offset, dtype=np.int32)
        gene_map = np.array(gene_index, dtype=np.int32)
        expression_matrix = exp.values.astype(np.float32)
        dataNode_pkg = (sample_names, alleles, sample_allele)
        gpuNode_pkg = (samp_maps, net_map, gene_map, expression_matrix) 
        return (dataNode_pkg, gpuNode_pkg)

class MasterDataNode(DataNode):
    def __init__(self, world_comm ):
        DataNode.__init__(self, world_comm )   
        self._status = np.zeros((world_comm.size,), dtype=int)

    def getData(self, working_dir, working_bucket, ds_bucket,k):
        self.working_dir = working_dir
        self.working_bucket = working_bucket
        self.ds_bucket = ds_bucket

        self.makeDirs([self.working_dir])
        self._getDataFiles()
        sd = data.SourceData()
        mi = None
        self.logger.info('init SourceData')
        sd.load_dataframe()
        sd.load_net_info()
        self.logger.info('init MetaInfo')
        mi = data.MetaInfo(op.join(self.working_dir,'metadata.txt'))
        self.logger.info("Broadcasting SourceData and MetaInfo")
        sd = self.type_comm.bcast(sd)
        mi = self.type_comm.bcast(mi)
        self.logger.info("Received SourceData and MetaInfo")

        self._sd = sd
        self._mi = mi
        self._pathways = self._getPathways()
        self.k = k

    def _getDataFiles(self):
        """
        Retrieves metadata and parsed dataframe files
            (generated by utilities/hddata_process.py) from S3
        """
        working_dir = self.working_dir
        data_source_bucket = self.ds_bucket
   
        if not op.exists(op.join( working_dir,'metadata.txt')):
            conn = boto.connect_s3()
            b = conn.get_bucket(data_source_bucket)
            k = Key(b)
            k.key = 'metadata.txt'
            k.get_contents_to_filename(op.join( working_dir,'metadata.txt'))

        if not op.exists(op.join( working_dir, 'trimmed_dataframe.pandas')):
            conn = boto.connect_s3()
            try:
                b = conn.get_bucket(self.working_bucket)
                k = Key(b)
                k.key ='trimmed_dataframe.pandas'
                k.get_contents_to_filename(op.join( working_dir,'trimmed_dataframe.pandas'))
            except S3ResponseError:
                print "Have you run ~/hdproject/utilities/hddata_process.py lately"
                raise

    def run(self):
        quit = False
        strain_counter = self._firstBurst()
        status_buffer = np.zeros((1,), dtype=int)
        status = MPI.Status()
        while not quit:
            self.world_comm.Recv( status_buffer, source=MPI.ANY_SOURCE, status=status )
            if status_buffer[0] == 0:
                self.world_comm.Isend(np.array([strain_counter%len(self.strains)],dtype=int), dest=status.source)
                strain_counter += 1
            elif status_buffer[0] == 1:
                self.world_comm.Send( np.array([2], dtype=int), dest=0 )
                self.world_comm.Recv( status_buffer, source=0)

    def _firstBurst(self):
        strains = self.getStrains()
        self._strains = strains
        strain_counter = 0
        for i in range(1, self.world_comm.size):
            self.world_comm.Isend(np.array([strain_counter%len(strains)],dtype=int), dest=i)
            self._status[i] = 1 #preparing data
            strain_counter += 1
        return strain_counter 
        
class GPUNode(MPINode):
    def __init__(self, world_comm, host_comm, type_comm, master_comm ):
        pass
        """
        MPINode.__init__(self, world_comm, host_comm, type_comm, master_comm )   

        self.logger.debug("In GPUNode init")
        self.logger.debug("Cuda Initialized")
        self.ctx = None

    def _startCTX(self, dev_id):
        dev = cuda.Device(dev_id)
        self.ctx = dev.make_context()

    def _endCTX(self):
        self.ctx.pop()
        self.ctx = None

    def getStatus(self):
        self.logger.debug("getStatus")
        self.logger.debug("exitted getStatus")

    def run(self):
        status_buffer = np.zeros((1,), dtype=int)
        while not quit:
            
            self.host_comm.Recv(status_buffer)
            if status_buffer < 0:
                quit = True
            else:
                source, samp_maps, net_map, gene_map, expression_matrix = self.recvData()
                self.host_comm.Send(np.array([1],dtype=int))
                self.host_comm.Recv(status_buffer)
                if status_buffer[0] < 0:
                    quit = True
                else:
                    mygpu = status_buffer[0]
                    result = self.runProcess( samp_maps, net_map, gene_map, expression_matrix, gpu=mygpu )
                    self.world_comm.send(result, dest=source)
                    self.host_comm.Isend(np.array([0],dtype=int))



    def runProcess( self,samp_maps, net_map, gene_map, expression_matrix, gpu ):
        self._startCTX(self, gpu)
        #here goes gpu
        result = None
        print "GPU vroom, vroom"
        self._endCTX()
        self.host_comm.Isend(np.array([2],dtype=int))
        return result
        

    def recvData(self):
        status = MPI.Status()
        samp_maps = self.world_comm.recv( source=MPI.ANY_SOURCE, status=status )
        net_map = self.world_comm.recv( source=status.source )
        gene_map = self.world_comm.recv(  source=status.source )
        expression_matrix = self.world_comm.recv(  source=status.source )
        return status.source, samp_maps, net_map, gene_map, expression_matrix"""

if __name__ == "__main__":
    import time
    worker_settings = {
                'working_dir':'/scratch/sgeadmin/hddata/', 
                'working_bucket':'hd_working_0', 
                'ds_bucket':'hd_source_data', 
                'k':5
                }
    world_comm = MPI.COMM_WORLD
    level = logging.DEBUG
    host_file = '/home/sgeadmin/hdproject/tcDirac/tcdirac/mpi.hosts'
    isgpu = False
    logfile = "/scratch/sgeadmin/log_mpi_r%i.txt"%world_comm.Get_rank()
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=level, format=log_format)

    nf = NodeFactory( world_comm )
    thisNode = nf.getNode()
    thisNode.getData(**worker_settings)
    thisNode.run()
    world_comm.Barrier()
    
