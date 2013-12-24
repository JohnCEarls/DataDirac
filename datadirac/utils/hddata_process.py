import pandas.io.parsers
import boto
from boto.s3.key import Key
import os
import os.path
import logging
import pandas.io.parsers
import numpy as np
import cPickle as pickle
from pandas import DataFrame
from boto.dynamodb2.table import Table
from boto.exception import S3ResponseError
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
            #print parsed
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
    df_path, df = mapNewData(working_bucket, data, meta_data, anno_data,syn_file,agilent_file, network_table)
    print df 
    print df_path
    logging.info("Ending... hddata_process.py")
