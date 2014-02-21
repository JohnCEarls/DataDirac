import nipy.algorithms.statistics.empirical_pvalue as pval
import pandas
from collections import defaultdict

def get_fdr_cutoffs( tsv_file, index='networks', alphas=[.05, .01] ):
    for a in alphas:
        if a < .01:
            raise Exception("Alphas only go to .01, easy to fix, but I have bigger fish to fry")
    b6 = pandas.read_csv( tsv_file , sep='\t')
    b6.set_index(index)
    cutoffs = defaultdict(dict)
    for alpha in alphas:
        for c in b6.columns:
            if c != index:
                cutoff = pval.fdr_threshold(b6[c].values, alpha=alpha)
                cutoffs[c]["{:.2f}".format(alpha)] = cutoff
    return cutoffs


from rpy2.robjects.packages import importr
import numpy as np
from rpy2.robjects import rinterface, r, IntVector, FloatVector, StrVector


def get_qval_table( tsv_file, index='networks'):
    b6 = pandas.read_csv( tsv_file , sep='\t')
    b6.set_index(index)
    b6_out = b6.copy()
    for c in b6.columns:
        if c != index:
            _, qvals = qvalues( b6[c].values )
            b6_out[c][:] = qvals
    return b6_out


def qvalues( pvals ):
    """
    See (https://gist.github.com/JohnCEarls/050543dd2d7a403a7dd3) for instructions to setup system.
    """
    qvalue_obj = importr("qvalue")
    base = importr('base')
    kw = {'pi0.method':'bootstrap'}

    res = qvalue_obj.qvalue(FloatVector(pvals), **kw)
    print "warnings",base.warnings()
    print "res", res
    try:
        _, pi0_r, qv_r, pv_r, lam_r = res 
    except ValueError:
        raise
    pi0 = np.array(pi0_r)[0]
    qv = np.array(qv_r)
    return pi0, qv

if __name__ == "__main__":
    import json
    #co = get_fdr_cutoffs( '../black_6_go_4-joined-2014.02.13.01:19:21.tsv')
    #print json.dumps(co)
    #converted = get_qval_table( '../black_6_go_4-joined-2014.02.13.01:19:21.tsv')
    #converted.to_csv('../black_6_go_4-joined-qvals-2014.02.13.01:19:21.tsv',
    #    sep='\t', index_label='networks')

    import numpy.random
    qvalues(np.random.randn(100))

 
