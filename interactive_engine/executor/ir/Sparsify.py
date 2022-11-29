from scipy.optimize import minimize
from scipy.optimize import LinearConstraint
from scipy.optimize import Bounds
import numpy as np
import os
import json

def uniform_min(x):
    return sum((1/x[:]-1)/r[:])

# edges labels order are the sorted order in csv-file order
env_dist=os.environ
sparse_rate=float(env_dist.get('SPARSE_RATE'))
sparse_file=str(env_dist.get('SPARSE_STATISTIC_PATH'))
export_path=str(env_dist.get('SPARSE_RATE_PATH'))
with open(sparse_file) as json_file:
    statistic=json.load(json_file)
relation=[]
var=[]
for keys, value in dict(statistic).items():
    relation.append(keys)
    var.append(int(value))
r=np.array(var) #scale-1 distribution
e = 1e-12 # 非常接近0的值
# basic bound of p
bounds = Bounds([0]*len(r),[1]*len(r))
linearr_constarint = LinearConstraint(r[:],[e],[sparse_rate*sum(r[:])])

x0=np.empty(len(r))
for i in range(len(r)):
    x0[i]=sparse_rate/10

# execute optimization
res = minimize(uniform_min, x0, method='trust-constr', constraints=[linearr_constarint], bounds=bounds)
# dump to file
result ={}
for i in range(len(res.x)):
    result[relation[i]]=res.x[i]
with open(export_path, "w") as outfile:
    json.dump(result, outfile)
print("done")