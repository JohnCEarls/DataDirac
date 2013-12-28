import sys
sys.path.append('/home/sgeadmin/DataDirac')
from datadirac import data
from datadirac.utils import hddata_process
import boto
import os
import os.path
import random

def test_metadata( meta_file):
    header = []
    meta_base = []
    with open(meta_file, 'r') as meta:
        for line in meta:
            a = line.strip().split('\t')
            if header:
                meta_base.append( dict( ((k,v) for k,v in zip( header, a))) )
            else:
                header = a
    meta_base
    metainfo_obj = data.MetaInfo( meta_file )
    #test strains
    strains = metainfo_obj.get_strains()
    for x in meta_base:
        assert x['strain'] in strains, "%s not in mi strains" % x['strain']
    for strain in strains:
        assert strain in (x['strain'] for x in meta_base), (
                "%s in MetaInfo but not in file" % strain)
    #test alleles
    for strain in strains:
       alleles = metainfo_obj.get_nominal_alleles( strain=strain )
       for x in meta_base:
           if x['strain'] == strain:
               assert x['allele_nominal'] in alleles, (
                "Strain %s and Allele %s not in metainfo" % (x['strain'], 
                    x['allele_nominal']) )

    #test samples
    for strain in strains:
        samples = metainfo_obj.get_sample_ids( strain )
        s = [x['sample_id'] for x in meta_base if x['strain'] == strain]
        assert len(s) == len(samples), "Mismatched sizes in sample strains"
        for sample in samples:
            assert sample in s, "%s not in %s " %( sample, strain )
            
        alleles = metainfo_obj.get_nominal_alleles( strain=strain )
        for allele in alleles:
            s = [x['sample_id'] for x in meta_base if x['strain'] == strain and
                    x['allele_nominal'] == allele]
            samples = metainfo_obj.get_sample_ids( strain, allele)
            assert len(samples) == len(s), "Mismatched sizes in sample alleles"
            for sample in samples:
                assert sample in s, "%s not in %s " %( sample, strain )
    #test age
    for x in meta_base:
        assert metainfo_obj.get_age( x['sample_id'] ) == float(x['age']), (
                "age for %s does not match %d" % (x['sample_id'] , 
                    float(x['age'])))
    print "MetaInfo passed...."
"""
def test_sourcedata( local_meta, local_dataframe ):
    source_bucket = 'hd_source_data'
    d = 'norm.mean.proc.txt'
    md = 'metadata.txt'
    ad = 'annodata.txt'
    agilent_file = 'HDLux_agilent_gene_list.txt'
    syn_file = 'Mus_homo.gene_info'
    network_table = 'net_info_table'
    data_dir='/scratch/sgeadmin/test/'
    main_data, meta_data, anno_data,syn_file,agilent_file = hddata_process.getFromS3( 
            source_bucket,d,md,ad, syn_file, agilent_file, data_dir)
    print main_data
    print meta_data, anno_data, syn_file, agilent_file
    ctr = 0
    header = []
    data_dict = {}
    with open(main_data,'r') as df: 
        for line in df:
            if header:
                parsed =  line.strip().split('\t')
                assert len(header) == len(parsed), "Header does not match line"
                for k, v in zip( header, parsed ):
                    data_dict[k].append(float(v))
            else:
                header = line.strip().split('\t')
                for sn in header:
                    data_dict[sn] = []
    header = []
    annotations = []
    with open(anno_data, 'r') as anno:
        for line in anno:
            if header:
                parsed = line.strip().split('\t')
                annotations.append( (int(parsed[0]),parsed[1]) )
            else:
                header = line.strip().split('\t')
    for k,v in data_dict.iteritems():
        assert len(v) == len(annotations), "annotations does not match input data"

    probe_to_row_map = {}
    for i,a in enumerate(annotations):
        if a[1] not in probe_to_row_map:
            probe_to_row_map[a[1]] = []
        probe_to_row_map[a[1]].append(i)
    agilent_dict = {} 
    #probe id -> ['ProbeID', 'TargetID', 'GeneSymbol', 'GeneName', 
    #             'Accessions', 'Description']
    with open(agilent_file, 'r') as ag:
        ctr = 0
        header = []
        for line in ag:
            parsed = line.strip().split('\t')
            if not header:
                header = parsed
            else:
                agilent_dict[parsed[0]] = dict( ( 
                    ((k,v) for k,v in zip(header, parsed)) ) )
    syn_set_list = []
    with open(syn_file, 'r') as syn:
        ctr = 0
        for line in syn:
            syn_set = set()
            a = line.strip().split()
            for p in a[:5]:
                tt = p.split('|')
                for t in tt:
                    if len(t) > 2:
                        syn_set.add(t)
            syn_set_list.append(syn_set)
    probelist = []
    gene_set = []
    for probe_id, v in agilent_dict.iteritems():
        my_geneset = set()
        for myset in syn_set_list:
            if v['GeneSymbol'] in myset:
                my_geneset |= myset
        if my_geneset:
            probelist.append(probe_id)
            gene_set.append(my_geneset)
    print probelist[:5]
    print gene_set[:5]

    sd = data.SourceData()
    sd.load_dataframe( local_dataframe )
    gene_to_row = []
    for gene in sd.genes:
        gene_to_probeset.append([])
        for i, v in enumerate( gene_set ):
            if gene in v:
                if probe_list[i] in probe_to_row_map:
                    for row in probe_to_row_map:
                        gene_to_row[-1].append(row)

"""

def test__HDDataGen__get_network_genes( ):
    working_dir = '/scratch/sgeadmin/test'
    hddg = hddata_process.HDDataGen( working_dir )
    ng = hddg._get_network_genes( 'net_info_table', 'c2.cp.biocarta.v4.0.symbols.gmt')
    s3 = boto.connect_s3()
    b = s3.get_bucket('hd_source_data')
    k = b.get_key('c2.cp.biocarta.v4.0.symbols.gmt')
    with open('tmp.tmp', 'r+') as tmp:
        k.get_contents_to_file(tmp)
        tmp.seek(0)
        for line in tmp:
            for g in line.strip().split()[2:]:
                if g not in ng:
                    print line
                assert g in ng, "%s is missing" % g
    print 'test__HDDataGen__get_network_genes ... PASSED'

def test__HDDataGen_get_data():
    working_dir = '/scratch/sgeadmin/test'
    hddg = hddata_process.HDDataGen( working_dir )
    data = hddg._get_data( 'norm.mean.proc.txt', 'annodata.txt')

    agi = hddg._get_probe_mapping( 'HDLux_agilent_gene_list.txt' )
    anno = hddg._get_annotations( 'annodata.txt' )
    nd_set = set(data.index.tolist())
    for ind in data.index:
        message = "%s is in new data and has a ct of %i"
        assert anno['ControlType'][anno['ProbeName'] == ind] == 0,( message
                % (ind, anno['ControlType'][anno['ProbeName'] == ind]))
    print  "test__HDDataGen_get_data ... PASSED"

def test__HDDataGen_get_synonyms():
    working_dir = '/scratch/sgeadmin/test'
    hddg = hddata_process.HDDataGen( working_dir )
    probe_mapping = hddg._get_probe_mapping(  'HDLux_agilent_gene_list.txt' )
    network_genes = hddg._get_network_genes( 'net_info_table', 
            'c2.cp.biocarta.v4.0.symbols.gmt')
    hddg._get_synonyms( probe_mapping, network_genes, 'Mus_homo.gene_info')
    print "test__HDDataGen_get_synonyms ... Passed"

def test__HDDataGen_get_probe_to_gene_map():
    working_dir = '/scratch/sgeadmin/test'
    hddg = hddata_process.HDDataGen( working_dir )
    probe_mapping = hddg._get_probe_mapping(  'HDLux_agilent_gene_list.txt' )
    network_genes = hddg._get_network_genes( 'net_info_table', 
            'c2.cp.biocarta.v4.0.symbols.gmt')
    syn_map = hddg._get_synonyms( probe_mapping, network_genes, 'Mus_homo.gene_info')
    ng2pm = hddg._get_probe_to_gene_map( probe_mapping, syn_map )
    print "test__HDDataGen_get_probe_to_gene_map ... passes"

def test__HDData_generate_dataframe():
    working_dir = '/scratch/sgeadmin/test'
    hddg = hddata_process.HDDataGen( working_dir )
    data_file = 'norm.mean.proc.txt'
    annotations_file = 'annodata.txt'
    agilent_file = 'HDLux_agilent_gene_list.txt'
    synonym_file = 'Mus_homo.gene_info'
    network_table = 'net_info_table'
    source_id = 'c2.cp.biocarta.v4.0.symbols.gmt'

    data_orig = hddg._get_data( 'norm.mean.proc.txt', 'annodata.txt')
    df = hddg.generate_dataframe( data_file, annotations_file, agilent_file, 
            synonym_file, network_table, source_id )
    assert all(data_orig.columns == df.columns), "Columns are fubared"
    desc = df.describe() 
    for c in df.columns:
        if random.random() > .05:
            print desc[c]
    print "test__HDData_generate_dataframe ... Passed"

if __name__ == "__main__":
    if not os.path.exists('/scratch/sgeadmin/test'):
        os.makedirs('/scratch/sgeadmin/test')
    s3 = boto.connect_s3()
    b = s3.get_bucket('hd_working_0')
    k = b.get_key('metadata.txt')
    local_meta = '/scratch/sgeadmin/test/metadata.txt'
    k.get_contents_to_filename(local_meta)

    k = b.get_key('trimmed_dataframe.pandas')
    local_dataframe = '/scratch/sgeadmin/test/trimmed_dataframe.pandas'
    k.get_contents_to_filename( local_dataframe )
    test_metadata(local_meta)
    test__HDDataGen__get_network_genes()
    test__HDDataGen_get_data()
    test__HDDataGen_get_synonyms()
    test__HDDataGen_get_probe_to_gene_map()
    test__HDData_generate_dataframe()
