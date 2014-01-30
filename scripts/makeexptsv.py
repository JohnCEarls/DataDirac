from datadirac import data
import itertools
import pandas
from collections import defaultdict
import numpy as np
import os.path
import json
class TSVGen:
    def __init__(self, df, meta):
        self.df = df
        self.meta = meta
        self.getData()

    def getData( self ):
        
        sd = data.SourceData()
        sd.load_dataframe( self.df )
        sd.load_net_info('net_info_table', 'c5.all.v4.0.symbols.gmt')
        mi = data.MetaInfo( self.meta)
        self._sd = sd
        self._mi = mi

    def genBivariate( self, pathway ):
        genes = self._sd.get_genes( pathway )
        descriptions = []
        web_path = 'static/data/'
        for strain in self._mi.get_strains():
            alleles = self._mi.get_nominal_alleles( strain )
            for a1, a2 in itertools.combinations(alleles,2):
                sid1 = self._mi.get_sample_ids( strain, a1)
                ages1 = [self._mi.get_age( s ) for s in sid1] 
                sid2 = self._mi.get_sample_ids( strain, a2)
                ages2 =  [self._mi.get_age( s ) for s in sid2]
                sub1 = self._sd.get_expression( sid1 )
                pw_sub1 = sub1.loc[genes,:]
                pw_sub1T = pw_sub1.transpose()
                series_1 = {}
                for gene in genes:
                    a2s_map = defaultdict(list)
                    for a,sid in zip(ages1,sid1):
                       a2s_map[a].append(sid) 
                    new_series = pandas.Series(np.zeros(len(a2s_map)), index=a2s_map.keys()) 
                    for a,samps in a2s_map.iteritems():
                        new_series.ix[a] = pw_sub1T[gene].ix[ samps ].median()
                    new_series.name = "%s" % (a1,)
                    series_1[gene] = new_series

                sub2 = self._sd.get_expression( sid2 )
                pw_sub2 = sub2.loc[genes,:]
                pw_sub2T = pw_sub2.transpose()
                series_2 = {}

                for gene in genes:
                    a2s_map = defaultdict(list)
                    for a,sid in zip(ages2,sid2):
                       a2s_map[a].append(sid) 
                    new_series = pandas.Series(np.zeros(len(a2s_map)), index=a2s_map.keys()) 
                    for a,samps in a2s_map.iteritems():
                        new_series.ix[a] = pw_sub2T[gene].ix[ samps ].median()
                    new_series.name = "%s" % (a2,)
                    series_2[gene] = new_series
                for gene in genes:
                    a,b = series_1[gene].align(series_2[gene])
                    a = a.interpolate().bfill().ffill()
                    b = b.interpolate().bfill().ffill()
                    q = pandas.DataFrame(a)
                    q = q.join(b)
                    fname = "expression-%s-%s-%s.tsv" % (strain, gene,  '-V-'.join(q.columns))
                    q.to_csv(fname, sep='\t', index_label="age")
                    description = { 
                            'type': 'expression',
                            'filename' : os.path.join(web_path, fname),
                            'strain': strain,
                            'gene': gene,
                            'age' : 'age',
                            'base' : a1,
                            'baseLong': a1,
                            'comp' : a2,
                            'compLong':a2 }
                    descriptions.append(description)
        with open("%s_descriptions.json" % pathway, 'w') as pw:
            json.dump(descriptions, pw)



if __name__ == "__main__":
    base = "/home/earls3/secondary/tcdiracweb/tcdiracweb/static/data"

    t = TSVGen( base + "/exp_mat_b6_wt_q111.pandas", base + "/metadata_b6_wt_q111.txt")
    t.genBivariate('HISTONE_MODIFICATION')
    



