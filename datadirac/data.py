from boto.dynamodb2.table import Table
from pandas import DataFrame
import pandas
import logging
import os.path
from collections import defaultdict

class SourceData:
    """
    A repository for data

    Loads a pandas file and makes it available for slicing and dice
    """
    def __init__(self):
        self.logger = logging.getLogger('SourceData')
        self.logger.info("creating SourceData object")
        self.source_dataframe = None
        self.net_info = None
        self.genes = set()

    def load_dataframe( self, data_frame_source):
        """
        Given a filename of a pandas file containing expression
        data, load file.

        See datadirac.utils.hddata_process for generation information
        """
        self.logger.info(("Loading existing expression"
                        "dataframe[%s]") % data_frame_source)
        try:
            self.source_dataframe = pandas.read_pickle(data_frame_source)
            self.genes = set(self.source_dataframe.index)
        except Exception:
            self.logger.exception("Error loading [%s]" % data_frame_source)
            raise

    def load_net_info(self, table_name, source_id):
        """
        Loads network info from Dynamo
        table_name - name of table
        source_id - table_key (currently using name of source file)

        See datadirac.utils.gmt_process for details
        """
        self.net_info = NetworkInfo(table_name,source_id)
        self.init_genes()

    def set_net_info(self, net_info):
        """
        Sets net_info from NetworkInfo object
        """
        self.net_info = net_info
        self.init_genes()

    def get_expression(self, sample_ids):
        """
        Return all gene expression given list of sample_ids
        """
        df = self.source_dataframe
        return df.loc[:, sample_ids]

    def get_pathways(self):
        """
        Returns list of all pathways
        """
        return self.net_info.get_pathways()

    def get_genes(self, pathway_id):
        """
        Returns list of all genes for a given pathway_id

        Also removes genes from the pathway that are 
        not available in the dataset
        """
        ni = self.net_info
        genes = ni.get_genes(pathway_id)
        if not ni.is_clean(pathway_id):
            gset =  self.genes
            genes = [g for g in genes if g in gset]
            ni.update_genes( pathway_id, genes )
        return self.net_info.get_genes(pathway_id)

    def init_genes(self):
        """
        Filter and initialize pathways
        """
        for pw in self.get_pathways():
            self.get_genes(pw)

class MetaInfo:
    """
    Loads the metadata for this run.
    This will need to change and be generalized.
    """
    def __init__(self, meta_file):
        self.logger = logging.getLogger('MetaInfo')
        self.logger.info("Loading [%s]" % meta_file)
        self.metadata = pandas.io.parsers.read_table(meta_file)
        self.metadata.index = self.metadata['sample_id']
        for i in self.metadata.index:
            assert( self.metadata['sample_id'][i] == i)

    def get_sample_ids(self, strain, allele=None):
        """
        Returns list of sample ids for given strain [and allele]
        """
        md = self.metadata
        if allele is None:
            return md[md['strain'] == strain]['sample_id'].tolist()
        else:
            return md[(md['strain'] == strain) & (md['allele_nominal'] == allele)]['sample_id'].tolist()

    def get_strains(self):
        """
        Returns list of all strains
        """
        return self.metadata['strain'].unique().tolist()

    def get_nominal_alleles(self, strain=None):
        """
        Returns list of available nominal alleles [for given strain].
        """
        md = self.metadata
        if strain is None:
            return md['allele_nominal'].unique().tolist()
        else:
            return md[md['strain'] == strain]['allele_nominal'].unique().tolist()

    def get_age(self, sample_id):
        """
        Returns the age of the given sample_id
        """
        return self.metadata['age'][sample_id]


class NetworkInfo:
    """
    Stores network information
    """
    def __init__(self, table_name, source_id, pw_init=True):
        self.logger = logging.getLogger('NetworkInfo')
        self.table_name = table_name
        #self.table = Table(table_name)
        self.source_id = source_id
        self.gene_map = {}
        #clean refers to the genes being filtered to match the genes
        #available in the expression file
        self.gene_clean = defaultdict(bool)
        self.pathways = []
        if pw_init:
            self.get_pathways()

    def get_genes(self, pathway_id, cache=True):
        """
        Returns list of genes in pathway.

        Initially these genes are unfiltered(i.e. there may be 
        genes that do not show up in data set.)
        """
        if pathway_id not in self.gene_map:
            table = Table(self.table_name)
            source_id = self.source_id
            self.logger.info("Getting network info [%s.%s.%s]" % (table.table_name, source_id, pathway_id))
            nit_item = table.get_item(src_id=source_id, pw_id=pathway_id)
            self.gene_map[pathway_id] = nit_item['gene_ids'][6:].split('~:~')
            self.gene_clean[pathway_id] = False
        return self.gene_map[pathway_id]

    def get_pathways(self):
        """
        Returns list of pathways
        """
        if len(self.pathways) == 0:
            table = Table(self.table_name)
            pw_ids = table.query(src_id__eq=self.source_id, attributes=('pw_id','gene_ids'))
            for pw in pw_ids:
                self.gene_map[pw['pw_id']] =pw['gene_ids'][6:].split('~:~')
            #simple load balancing
            t = [(len(g), p) for p,g in self.gene_map.iteritems()]
            t.sort()
            self.pathways = [pw for _,pw in t]            
        return self.pathways

    def is_clean(self, pathway_id):
        """
        Returns true if the genes in the pathway are subset of dataset genes
        """
        return self.gene_clean[pathway_id]

    def update_genes(self, pathway_id, genes):
        """
        Updates gene list for pathway_id to genes(list)
        Primarily a helper function to allow filtering of 
        extraneous genes
        """
        self.gene_map[pathway_id] = genes
        self.gene_clean[pathway_id] = True

    def __getstate__(self):
        odict = self.__dict__.copy() # copy the dict since we change it
        del odict['logger']              # remove filehandle entry
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)   # update attributes
        self.logger = logging.getLogger('NetworkInfo')

if __name__ == "__main__":
    #getNetworkExpression( "c2.cp.biocarta.v4.0.symbols.gmt", "BIOCARTA_AKAPCENTROSOME_PATHWAY")
    local_data_dir = '/scratch/sgeadmin/hddata/'
    meta_file = os.path.join(local_data_dir, 'metadata.txt')
    data_file = os.path.join(local_data_dir,'/scratch/sgeadmin/hddata/trimmed_dataframe.pandas') 
    pathway_id = 'BIOCARTA_AKAPCENTROSOME_PATHWAY'
    pathway_id = 'BIOCARTA_MAPK_PATHWAY'
    sd = SourceData()
    sd.load_dataframe(data_file)
    sd.load_net_info(table_name="net_info_table",source_id="c2.cp.biocarta.v4.0.symbols.gmt")
    sd.init_genes()
    sd.get_pathways()
    #print [x for x in sd.get_pathways() if x == 'BIOCARTA_AKAPCENTROSOME_PATHWAY']
    #print sd.get_genes(pathway_id)
    mi = MetaInfo(meta_file)
    #print mi.metadata
    for sid in mi.get_sample_ids('FVB'):
        mi.get_age(sid)
    sd.get_expression(mi.get_sample_ids('FVB'))

