import redis
import sys
sys.path.append("..")
from sharepoint.Mapi2 import DBconx
import pandas as pd
import numpy as np
import time
import pickle
from datetime import timedelta

#r = redis.Redis(host="3.72.229.78", port=6379)
r = redis.Redis()

st= time.time()
tuptup = DBconx('T').query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")
df1 = pd.DataFrame([i for i in tuptup], columns=['imei', 'checked', 'last', 'count', 'org']).astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})

# total devices by org
dev_org = df1.groupby(['org'])['imei'].nunique()
dev_org_mon = df1.groupby(['org', pd.Grouper(key='last', freq='M')])['imei'].nunique().to_frame()

#ts = dev_org_mon._get_label_or_level_values('last')
dev_org_mon['month'] = pd.to_datetime(dev_org_mon._get_label_or_level_values('last')).date
dev_org_mon['xax'] = dev_org_mon.month - timedelta(days=15) # just for xaxis drawing in mid month
dev_org_mon['org'] = dev_org_mon._get_label_or_level_values('org')

print(f'sql pandas took {time.time() - st}s')

dev_org_mon.info()

r.set("key", pickle.dumps(dev_org_mon))
st= time.time()
df2 = pickle.loads(r.get("key"))
print(f'redis took {time.time() - st}s')

df2.info()
