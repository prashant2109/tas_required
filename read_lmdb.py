import lmdb, sys
p, d = sys.argv[1].split('_')
path    = '/var/www/html/TASFundamentalsV2/tasfms/data/output/%s/%s/1_1/21/sdata/Norm_Grid_Info/%s/'%(p, d, sys.argv[2])
print path
env = lmdb.open(path)
t_d = {}
with env.begin() as txn:
    for k, v in txn.cursor():
        if k != 'triplet':continue
        print k
        v   = eval(v)
        for tv in v:
            print '\n============================='
            for k1, v1 in tv.items():
                print '\t',k1, v1
                if k1 == 'GV':  
                    t_d.setdefault(v1[0][0], {})[v1[0][1]]  = v1
rows    = t_d.keys()
rows.sort()
for r in rows:
    cols    = t_d[r].keys()
    cols.sort()
    print 
    for c in cols:
        print '\t',t_d[r][c]
