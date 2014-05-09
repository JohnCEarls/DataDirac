import boto
import pandas
import json
from pandas import DataFrame
import pandas.io.parsers
import os
import os.path
import numpy as np
import scipy.misc
import sys
sys.path.append('/home/sgeadmin/DataDirac')
from datadirac.data import NetworkInfo
#postcommit test
join = os.path.join
def get_log_files( log_dir ):
    log_files = []
    for f_name in os.listdir(log_dir):
        log_files.append(join(log_dir, f_name))
    return log_files

def get_source_data( source_dataframe, source_meta):
    source_df = pandas.read_pickle( source_dataframe )
    meta_info = pandas.io.parsers.read_table( source_meta )
    return source_df, meta_info

def find_working( files ):
    base = '/scratch/sgeadmin/working/DataNode_%i'
    for i in range(4):
        dn_dir = base % i
        there = True
        for k, v in files.iteritems():
            if not os.path.exists( join( dn_dir, v )):
                there = False
        if there:
            return dn_dir

def em_to_df( em_file,  source_dataframe, sample_names):
    em = np.load(em_file)
    nsamples = len(sample_names)
    ngenes = em.shape[0]
    shrunk = np.zeros((ngenes, nsamples))
    shrunk[:,:] = em[:ngenes, :nsamples]
    em_df = DataFrame( shrunk, columns = sample_names, 
                        index=source_dataframe.index)
    return em_df

def test_em( log_files, source_dataframe, source_meta ):
    print "Testing expression matrix"
    for log in log_files:
        with open(log, 'r') as l:
            for run_json in l:
                run = json.loads( run_json )
                for allele in run['alleles']:
                    source_allele = "%s-%s" % (run['file_id'], allele)
                    working_dir = find_working( 
                            run['data_matrices'][source_allele] )
                    em_file = join( working_dir, 
                            run['data_matrices'][source_allele]['em'] )
                    em_df = em_to_df( em_file, source_dataframe,
                                      run['sample_names'])
                    for sm in run['sample_names']:
                        for gene in source_dataframe.index:
                            assert (np.abs(em_df[sm][gene] - 
                                source_dataframe[sm][gene]) < .01), (
                                "Error in expression matrix[%s]" % 
                                 (source_allele))
    print "Expression Matrix test ... PASSED"

def test_sm( log_files, source_meta ):
    print "Testing Sample Map"
    for log in log_files:
        with open(log, 'r') as l:
            for run_json in l:
                run = json.loads( run_json )
                all_ages = []
                s_to_allele = {}
                boundaries = {}
                for a, c in  run['sample_allele'].iteritems():
                    all_ages += c
                    boundaries[a] = (c[0][0], c[-1][0])
                    for age, s in c:
                        s_to_allele[s] = a
                ages_d = dict([(s,a) for a,s in all_ages])
                for allele in run['alleles']:
                    source_allele = "%s-%s" % ( run['file_id'], allele )
                    working_dir = find_working( 
                            run['data_matrices'][source_allele] )
                    sm_file = join( working_dir, 
                                run['data_matrices'][ source_allele ]['sm'])
                    sm = np.load(sm_file)

                    for i in range(sm.shape[0]):
                        if sum(sm[i,:]) > 0:
                            curr = run['sample_names'][i]
                            comp_samp = [run['sample_names'][x] for x in sm[i,:]]
                            ages = [age for age,sample_name in 
                                        run['sample_allele'][allele]
                                        if sample_name in comp_samp ] 
                            lb = min(ages ) 
                            ub = max(ages)
                            if ages_d[curr] >= boundaries[allele][1]:
                               ub = max(ub, ages_d[curr] )
                            elif ages_d[curr] <= boundaries[allele][0]:
                               lb = min(lb, ages_d[curr])
                            assert lb <= ages_d[curr] <= ub, (
                                    ("ages[%s] out of bound for %s(%s)[%f]"
                                    " curr allele[%s] ") % (str(ages),
                                    curr, s_to_allele[curr], ages_d[curr],
                                    allele))
    print "Sample Map test ... PASSED"

def test_gm(log_files, source_dataframe):
    print "Testing gene map and network map"
    ni = NetworkInfo( "net_info_table","c2.cp.biocarta.v4.0.symbols.gmt")
    genes = []
    for p in ni.get_pathways():
        genes.append(set(ni.get_genes(p)))
    for log in log_files:
        with open(log, 'r') as l:
            for run_json in l:
                run = json.loads( run_json )
                for allele in run['alleles']:
                    source_allele = "%s-%s" % ( run['file_id'], allele )
                    working_dir = find_working( 
                            run['data_matrices'][source_allele] )
                    gm_file = join( working_dir, 
                                run['data_matrices'][ source_allele ]['gm'])
                    nm_file = join( working_dir, 
                                run['data_matrices'][ source_allele ]['nm'])
                    gm = np.load(gm_file)
                    nm = np.load(nm_file)
                    for i in range(1, len(nm)):
                        start = nm[i-1]
                        end = nm[i]
                        if start < end:
                            base_set = genes[i-1]
                            my_net = gm[2*start:2*end]
                            my_net_genes = source_dataframe.index[my_net]
                            gene_set = set(my_net_genes)
                            assert gene_set.issubset(base_set), "Genes do not match network genes"

                            assert (end - start) == scipy.misc.comb(len(gene_set),2, exact=1), "Wrong number of genes"
    print "Gene map and network map tests ... PASSED"

if __name__ == "__main__":
    """
    This should be run after a worker.py run
    """
    source_dataframe_f = "/scratch/sgeadmin/working/trimmed_dataframe.pandas"
    source_meta_f = "/scratch/sgeadmin/working/metadata.txt"
    
    source_dataframe, source_meta = get_source_data( source_dataframe_f, 
                                                            source_meta_f)
    log_files = get_log_files( '/scratch/sgeadmin/data_log/' )

    test_em( log_files, source_dataframe, source_meta)
    test_sm( log_files, source_meta)
    test_gm( log_files, source_dataframe )
