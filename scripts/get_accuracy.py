from datadirac.aggregator.accumulator import Truthiness
from datadirac.aggregator.controller import AggManager
import masterdirac.models.run as run_mdl
from datadirac.aggregator.accumulator import Accumulator
import pandas

#change this to current run id
run_id = 'b6-q50-nov28-3'
run_model = run_mdl.get_ANRun( run_id )
truth_obj = Truthiness( run_model )
truth = truth_obj._get_truth()
accum = Accumulator(run_model)
nets = accum.networks

df = pandas.DataFrame( truth )
df.index = nets

df.to_csv("%s.csv" % run_id)


