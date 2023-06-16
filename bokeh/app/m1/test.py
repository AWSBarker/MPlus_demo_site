# compare pymsql vs mysql
from Mapi2 import Mapi, Org, DBconx, Dev
import pandas as pd
import numpy as np
import time

st = time.time()
tuptup = DBconx('T').query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")
df1 = pd.DataFrame([i for i in tuptup], columns=['imei', 'checked', 'last', 'count', 'org']).astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
df1.info()
print(time.time() - st)

st = time.time()
tuptup2 = DBconx('M').query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")
df2 = pd.DataFrame([i for i in tuptup2], columns=['imei', 'checked', 'last', 'count', 'org']).astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
#df2 = df2.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
df2.info()
print(time.time() - st)

print(df2.equals(df1))




