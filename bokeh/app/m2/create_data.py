# create hourly data for M2 : Lightsail version Mapi2
# run crontab
# 3 redis key df_M2_orgs_wk, _dfpie, _org_stack_wk
import redis
import pickle
import pandas as pd
from math import pi
from datetime import datetime as dt, timedelta
import sys
sys.path.append("/home/bitnami/sharepoint")
sys.path.append("..")
from Mapi2 import DBconx, Org

db_table = "M+_daily"  #"M+_daily_structure" # "M_daily1"
orgDict = Org.getOrg_Name_dict() # {ID : NAME, }
gm_orgs = tuple([k for k,v in orgDict.items() if v.startswith('GM')])
tuptup = DBconx('T').query(f"SELECT imei, checked_on, last_measure_at, count, org from `{db_table}` WHERE org IN {gm_orgs}")
df1 = pd.DataFrame([i for i in tuptup], columns=['imei', 'checked', 'last', 'count', 'org']) #.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})

df1.org = df1.org.map(orgDict) #.astype('category')
df1.set_index('last', drop=False, inplace=True)
df1.sort_index(inplace=True)

# summary data for pie, total devices by org
dev_org = df1.groupby(['org'])['imei'].nunique().to_frame().rename(columns={'imei':'Total'})
dm = df1.groupby(['org', pd.Grouper(level=0, freq='M')])['imei'].nunique().to_frame() # monthly data by org
# get month ends
d_end_month = (dt.now().replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)-timedelta(days=1)
d_end_month_1 = (dt.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)-timedelta(days=1))
#lst_mon = dm.loc[(slice(None), d_end_month_1),].droplevel('last').rename(columns={'imei': d_end_month_1.strftime('%b%y')})
lst_mon = dm.loc[(slice(None), d_end_month_1),].rename(columns={'imei': d_end_month_1.strftime('%b%y')})
mtd = dm.loc[(slice(None), d_end_month),].droplevel('last').rename(columns={'imei': d_end_month.strftime('%b%y')})
# shift xaxis 1 month
lst_mon['xaxis'] = [(str(x[0]), (x[1] - timedelta(days=60)).date().strftime('%W %y')) for x in lst_mon.index]
lst_mon['xaxis1'] = [(str(x[0]), (x[1] - timedelta(days=5)).date().strftime('%W %y')) for x in lst_mon.index]
dfpie = dev_org.join(lst_mon, how='inner')
dfpie = dfpie.join(mtd, how='inner') #pd.concat([lst_mon,dev_org], axis=1, join='inner')# active devices (last not NaT) by org by month
by=d_end_month_1.strftime('%b%y')
mtd_txt=d_end_month.strftime('%b%y')
piescale= 15
dfpie['ang'] = dfpie[by]/dfpie.Total * 2*pi
dfpie['rad'] = piescale/(dfpie.Total.sum()/dfpie.Total)
dfpie['Tlab'] = dfpie['Total'].astype(str)
dfpie['Llab'] = dfpie[by].astype(str)
dfpie['Rlab'] = dfpie[mtd_txt].astype(str)
dfpie['ang1'] = dfpie[mtd_txt]/dfpie.Total * 2*pi
dfpie['rad1'] = piescale/(dfpie.Total.sum()/dfpie.Total)

# weekly data (can be M)
orgs_wk = df1.groupby(['org', pd.Grouper(level=0, freq='W')])['imei'].nunique().to_frame()
#orgs_mn = df1.groupby(['org', pd.Grouper(level=0, freq='M')])['imei'].nunique().to_frame()
no_orgs = orgs_wk.index.unique('org').to_list()

#x = [(str(x[0]), x[1].date().strftime('%b%y')) for x in dev_org_wk.index]
orgs_wk['xaxis'] = [(str(x[0]), x[1].date().strftime('%W %y')) for x in orgs_wk.index]

# stacked by org == dm?
org_stack_wk = df1.groupby([pd.Grouper(level=0, freq='W'), 'org'])['imei'].nunique().to_frame().unstack(level=1, fill_value=0).T.reset_index(level=0, drop=True).T#.unstack(level=1, fill_value=0)
org_stack_wk = org_stack_wk[sorted(org_stack_wk.columns)]
org_stack_wk['xaxis'] = [x.date().strftime('%W %y') for x in org_stack_wk.index] #mon_org_stack._get_label_or_level_values('last').astype('M8[D]').astype('str')
org_stack_wk['TWk'] = org_stack_wk.sum(axis=1, numeric_only=True)

ts = orgs_wk._get_label_or_level_values('last').astype('<M8[s]') # obhects to pd-timestmp
orgs_wk['ts'] = ts # converted to numpy.dt64 (int64) in CDS

# save keys
r = redis.Redis()
r.set("df_M2_org_stack_wk", pickle.dumps(org_stack_wk))  #dev_org_mon = pickle.loads(r.get("df"))
r.set("df_M2_dfpie", pickle.dumps(dfpie))  #dev_org_mon = pickle.loads(r.get("df"))
r.set("df_M2_orgs_wk", pickle.dumps(orgs_wk))  #dev_org_mon = pickle.loads(r.get("df"))

# these not reqd
"""
org_cmap = factor_cmap('xaxis', palette=Spectral11, factors= no_orgs, end=1)
"""