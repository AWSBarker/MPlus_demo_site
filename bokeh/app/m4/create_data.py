# create dummy hourly data for m4 : Lightsail version Mapi2
# replace showas with id
# run crontab
import redis
import pickle
import pandas as pd
from datetime import timedelta
import sys
sys.path.append("/home/bitnami/sharepoint")
sys.path.append("..")
from Mapi2 import DBconx, Mapi

# paste below for main
orgIDdict = {k:v[-4:] for k,v in Mapi.getID_Org_id_dict().items()}
tuptup = DBconx('T').query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")# WHERE org IN (1,2,3,4,5,6)")
df1 = pd.DataFrame([i for i in tuptup], columns=['imei', 'checked', 'last', 'count', 'org']) #.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
#df1.org = df1.org.map(orgDict) #.astype('category')
df1.org = df1.org.map(orgIDdict)
# total devices by org
dev_org = df1.groupby(['org'])['imei'].nunique()
dev_org_mon = df1.groupby(['org', pd.Grouper(key='last', freq='M')])['imei'].nunique().to_frame()
# weekly data (can be M)
orgs_wk = df1.groupby(['org', pd.Grouper(key='last', freq='W')])['imei'].nunique().to_frame()
orgs_wk['xaxis'] =pd.to_datetime(orgs_wk._get_label_or_level_values('last')).date
orgs_wk['org'] = orgs_wk._get_label_or_level_values('org')
#ts = dev_org_mon._get_label_or_level_values('last')
dev_org_mon['month'] = pd.to_datetime(dev_org_mon._get_label_or_level_values('last')).date
dev_org_mon['datestr'] = pd.to_datetime(dev_org_mon['month']).dt.strftime('%Y%b')
dev_org_mon['xax'] = dev_org_mon.month - timedelta(days=15) # just for xaxis drawing in mid month
dev_org_mon['org'] = dev_org_mon._get_label_or_level_values('org')
dev_org_mon['uri'] = "https://demo.medisante-group.com/data/"
dev_org_mon['filename'] = "OrgReport_" + dev_org_mon['org'] + "_" + dev_org_mon['datestr'] +".xlsx"
dev_org_mon['dormant'] = dev_org_mon.org.map(dev_org) - dev_org_mon.imei

# paste above for main
r = redis.Redis()
r.set("df_M4_dev_org_mon", pickle.dumps(dev_org_mon))  #dev_org_mon = pickle.loads(r.get("df"))
r.set("df_M4_orgs_wk", pickle.dumps(orgs_wk))  #dev_org_mon = pickle.loads(r.get("df"))
