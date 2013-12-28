import pandas.io.parsers
import boto
from boto.s3.key import Key
import os
import os.path
import logging
import pandas.io.parsers
from pandas.io import parsers
import numpy as np
import cPickle as pickle
from pandas import DataFrame
from boto.dynamodb2.table import Table
from boto.exception import S3ResponseError
from boto.s3.connection import Location

join = os.path.join

def getFromS3( source_bucket, data, meta_data, annotation_data, syn_file, agilent_file, data_dir='/scratch/sgeadmin/hddata/', force_write=False ):
    """
    Retrieves data, meta_data and annotation_data from s3 bucket and writes them to the data_dir
    If force_write is False, it only downloads the files if the file does not already exist.
    If force_write is True, it downloads and overwrites the current file.
    returns the local location of data, meta_data, annotation_data
    """
    logging.info("Running: getFromS3('%s','%s','%s','%s','%s',%s)"%( source_bucket, data, meta_data, annotation_data, data_dir, str(force_write) ))
    #check that data dir exists, if not create it
    if not os.path.exists(data_dir):
        logging.info( "Creating directory [%s]" % data_dir )
        os.makedirs(data_dir)

    conn = boto.connect_s3()
    b = conn.get_bucket(source_bucket)
    file_list = [data,meta_data,annotation_data, syn_file, agilent_file]
    local_file_list = []
    for f in file_list:
        fname = os.path.split(f)[-1]
        local_path = os.path.join(data_dir, fname)
        exists = os.path.exists(local_path)
        local_file_list.append(local_path)
        if exists:
            logging.warning("%s exists"%local_path)
        if force_write or not exists:
            if force_write:
                logging.info('force_writ on')
        try: 
            logging.info( "Transferring s3://%s/%s to %s" % (source_bucket,fname, local_path ))
            k = Key(b)
            k.key = fname
            k.get_contents_to_filename(local_path)
            logging.info("Transfer complete")
        except S3ResponseError as sre:
            logging.error("bucket:[%s] file:[%s] upload." % (source_bucket,fname))
            logging.error(str(sre))
            raise(sre)
    logging.info("getFromS3 Complete")
    return local_file_list

class HDDataGen:
    """
    Generates a stripped down datatable (pandas data frame) for
    GPUDirac 

    working_bucket - str, name of s3 bucket in us-east-1 where the dataframe
                    will be written
    """
    def __init__(self, working_dir ):
        self.working_dir = working_dir

    def _get_data( self, data_file, annotations_file):
        data_orig = parsers.read_table( join(self.working_dir,data_file) )
        annot = self._get_annotations( annotations_file)
        #set data index
        data_orig.index = annot['ProbeName']
        return self._drop_controls( data_orig, annotations_file )
        

    def generate_dataframe(self, data_file, annotations_file, 
            agilent_file, synonym_file, network_table, source_id ):
        data_orig = self._get_data( data_file, annotations_file )
        probe_map = self._get_probe_mapping( agilent_file )
        network_genes = self._get_network_genes( network_table, source_id )
        syn_map = self._get_synonyms( probe_map, network_genes, synonym_file )
        ng2pm = self._get_probe_to_gene_map( probe_map, syn_map )
        new_df = DataFrame(np.zeros( (len(ng2pm), len(data_orig.columns))),
            index=ng2pm.keys(), columns=data_orig.columns )
        #map each network gene to the median of synonymous probes
        test = True
        for k, probes in ng2pm.iteritems():
            new_df.ix[k] = data_orig.ix[probes].median()
        return new_df




    def _get_probe_mapping(self, agilent_file):
        """
        Given an agilent file that maps probe ids to gene symbols
        return dataframe with ProbeID and GeneSymbol columns
        """
        agl = parsers.read_table( join( self.working_dir, agilent_file ) )
        agl.set_index('ProbeID')
        agl2 = agl[agl['GeneSymbol'].notnull()]
        agl2 =  agl2.set_index('ProbeID')
        return agl2

    def _get_synonyms(self, probe_mapping, network_genes, synonym_file):
        #genes in the data file
        base_gene_symbols = set(probe_mapping['GeneSymbol'])
        #genes in the network file
        network_genes = set(network_genes)
        #syn map is a mapping between genes in the base gene symbols set
        #to genes in the network genes set
        def parse_line( line ):
            line_list = []
            parsed = line.split()
            base = parsed[2]
            for part in line.split():
                for part_i in part.split('|'):
                    if len(part_i) > 2:
                        line_list.append(part_i)
            return base, line_list
        syn_map = {}
        with open(join( self.working_dir, synonym_file), 'r') as syn:
            for line in syn:
                base_gene, parsed = parse_line( line )
                for poss_gene in parsed:
                    if poss_gene in network_genes and \
                        base_gene in base_gene_symbols:
                        if base_gene not in syn_map:
                            syn_map[base_gene] = set()
                        syn_map[base_gene].add(poss_gene)
        return syn_map

    def _get_probe_to_gene_map(self, probe_mapping, syn_map ):
        net_gene_to_probes = {}
        for probe_gene, net_genes in syn_map.iteritems():
            for net_gene in net_genes:
                probes = probe_mapping[probe_mapping['GeneSymbol'] == probe_gene ].index
                net_gene_to_probes[net_gene] = probes.tolist()
        return net_gene_to_probes



    def _get_network_genes( self, network_table, source_key, region='us-east-1'):
        """
        Given a dynamodb table and a source_key (network set name)
        return a set of all gene symbols in the network set
        """
        conn = boto.dynamodb.connect_to_region( region )
        table = conn.get_table( network_table )
        network_genes = set()
        for item in table.query( source_key ):
            gene_list = item['gene_ids']
            gene_list = gene_list[6:]
            gene_list = gene_list.split('~:~')
            for gene in gene_list:
                network_genes.add(gene)
        return network_genes


    def _get_annotations(self, annotations_file):
        return parsers.read_table( join( self.working_dir, annotations_file ) )

    def _drop_controls( self,data, annotation_file):
        annot = self._get_annotations( annotation_file )
        control_probes = annot['ProbeName'][annot['ControlType'] != 0]
        data = data.drop(control_probes)
        return data
       

    def write_to_s3( self, bucket_name, dataframe,  dataframe_name, 
            location=Location.DEFAULT):
        """
        Writes the dataframe(cleaned and aggregated source data) and the 
        metadata file to the given S3 bucket
        """
        conn = boto.connect_s3()
        bucket = conn.create_bucket(bucket_name, location=location)
        dataframe.save('temp.tmp')
        for fname in [dataframe_name]:
            k = Key(bucket)
            k.key = dataframe_name
            k.storage_class = 'REDUCED_REDUNDANCY'
            k.set_contents_from_filename('temp.tmp')
        os.remove('temp.tmp')

def mapNewData(working_bucket, data, meta_data, anno_data,syn_file,agilent_file,network_table):
    """
    Given local file locations for source data, meta data, annotations data,
        synonyms file and the agilent (probe->gene) file,
    Creates a new dataframe, containing only gene information for genes
        present in the network table, indexed by gene name, columns are sample ids
    Returns dataframe pickle file location and dataframe
    """
    anno = pandas.io.parsers.read_table(anno_data)
    data = pandas.io.parsers.read_table(data)
    metadata = pandas.io.parsers.read_table(meta_data)
    agl = pandas.io.parsers.read_table(agilent_file)
    
    #get rid of control probes

    data.index = anno['ProbeName']
    control_probe_names = anno['ProbeName'][anno['ControlType'] != 0]
    data = data.drop(control_probe_names)

    agl.set_index('ProbeID')
    agl2 = agl[agl['GeneSymbol'].notnull()]
    agl2 = agl2.set_index('ProbeID')

    #map probes to genes from network

    a = agl2['GeneSymbol'].tolist()
    b = set(a)
    table = Table(network_table)
    temp_nets = table.scan()
    network_genes = []
    i = 0
    for net in temp_nets:
        network_genes += net['gene_ids'][6:].split('~:~')
    network_genes_set = set(network_genes)


    mm = {}
    added = []
    with open(syn_file,'r') as synonyms:
        for line in synonyms:
            parsed = line.split()
            try:
                temp = []
                for p in parsed[:5]:
                    tt = p.split('|')
                    for t in tt:
                        if len(t) > 2 and t in network_genes_set and parsed[2] in b:
                            added.append(t)
                            temp.append(t)
                if len(temp) > 0:
                    if parsed[2] not in mm:
                      mm[parsed[2]] = []
                    for t in temp:
                        if t not in mm[parsed[2]]:
                            mm[parsed[2]].append(t)
                
            except IndexError:
                pass
    ng2p = {}
    probes = []
    with open(agilent_file, 'r') as gl:
        for line in gl:
            parsed = line.split()
            try:
                if parsed[2] in mm: #mouse gene is mapped to network gene
                    for ng in mm[parsed[2]]:
                        if ng not in ng2p:
                            ng2p[ng] = []
                        if parsed[0] not in ng2p[ng]:
                            ng2p[ng].append(parsed[0])
                            probes.append(parsed[0])          
            except IndexError:
                pass
    #create newly trimmed and annotated data frame
    #save pickle locally

    df = DataFrame(np.zeros((len(ng2p), len(data.columns))), index=ng2p.keys(), columns=data.columns)
    for k,v in ng2p.iteritems():
        df.ix[k] = data.ix[v].median()
    saved = os.path.join(os.path.split(agilent_file)[0],'trimmed_dataframe.pandas')
    df.save(saved)
    
    #send pickled dataframe to working bucket
    conn = boto.connect_s3()
    b = conn.get_bucket(working_bucket)
    k=Key(b)
    k.key = 'trimmed_dataframe.pandas'
    k.storage_class = 'REDUCED_REDUNDANCY'
    k.set_contents_from_filename(saved)

    k.key = 'metadata.txt'
    k.storage_class = 'REDUCED_REDUNDANCY'
    k.set_contents_from_filename(meta_data)

    return saved,df 

if __name__ == "__main__":
    logging.basicConfig(filename='/scratch/sgeadmin/hddata_process.log', level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info("Starting... hddata_process.py")

    """
    source_bucket = 'hd_source_data'
    d = 'norm.mean.proc.txt'
    md = 'metadata.txt'
    ad = 'annodata.txt'
    agilent_file = 'HDLux_agilent_gene_list.txt'
    syn_file = 'Mus_homo.gene_info'
    network_table = 'net_info_table'
    data_dir='/scratch/sgeadmin/hddata/'

    data, meta_data, anno_data,syn_file,agilent_file = getFromS3( source_bucket,d,md,ad, syn_file, agilent_file, data_dir)
    working_bucket = 'hd_working_0'
    #df_path, df = mapNewData(working_bucket, data, meta_data, anno_data,syn_file,agilent_file, network_table)"""

    #TODO rework this
    #data_file, meta_data, anno_data,syn_file,agilent_file = getFromS3( source_bucket,d,md,ad, syn_file, agilent_file, data_dir)
    source_bucket = 'hd_source_data'
    working_dir = '/scratch/sgeadmin/test'                                      
    data_file = 'norm.mean.proc.txt'                                            
    annotations_file = 'annodata.txt'                                           
    agilent_file = 'HDLux_agilent_gene_list.txt'                                
    synonym_file = 'Mus_homo.gene_info'                                         
    network_table = 'net_info_table'                                            
    source_id = 'c2.cp.biocarta.v4.0.symbols.gmt'  
    bucket_name = 'hd_working_0'

    #generate dataframe
    hddg = HDDataGen( working_dir )                              
    df = hddg.generate_dataframe( data_file, annotations_file, agilent_file,    
         synonym_file, network_table, source_id )
    dataframe_name = 'trimmed_dataframe.pandas'
    hddg.write_to_s3( bucket_name, df,  dataframe_name)
    #copy metadata
    conn = boto.connect_s3()
    bucket = conn.create_bucket(source_bucket, location=Location.DEFAULT)
    k = Key(bucket)
    k.key = 'metadata.txt'
    k.copy(bucket_name, k.name) 

    logging.info("Ending... hddata_process.py")
