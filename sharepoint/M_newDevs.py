# try extract new devs, not used  per period per
# per week : during this week how many new devices were used i.e. never used before any week
#

import pandas as pd
from datetime import timedelta
import numpy as np
from Mapi2 import Mapi, Org, DBconx
import matplotlib.pyplot as plt

orgDict = Mapi.getOrg_ID_dict()
db_table = "M+_daily" # "M_daily1" "M+_daily_structure" #
tuptup = DBconx('M').query(f"SELECT imei, checked_on, last_measure_at, count, org from `{db_table}` WHERE org in (1,2,3,4,5,6,100)")
df1 = pd.DataFrame([i for i in tuptup]) #, columns=['imei', 'checked', 'last', 'count', 'org']) #.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
df1.columns = ['imei', 'checked', 'lastm', 'count', 'org']
df1.astype({'imei': np.int64, 'count' : int, 'org': np.int8})
df1.org = df1.org.map(Org.getOrg_Name_dict())
df1.set_index('checked', inplace=True)

gb1 = df1.groupby([pd.Grouper(level=0, freq='D')])['imei'].unique().to_frame()

gb1['cumIMEI'] = gb1['imei'].shift()
gb1.loc[gb1.index.min(), 'cumIMEI'] = gb1.loc[gb1.index.min()].imei
gb1['new1'] = gb1['imei'] #np.empty((1,), dtype=

#for d in gb1.index[1:]:
for r in gb1.itertuples():
    print(f'row {r}  ')
    #v = np.union1d(r.imei, r.cumIMEI)
    gb1.loc[r.Index].cumIMEI = np.union1d(r.imei, r.cumIMEI)
    gb1.loc[r.Index].new1 = np.setdiff1d(r.imei, r.cumIMEI)
    gb1.loc[r.Index, 'cumnew'] = r.cumIMEI.size

#gb1.loc[r.Index, 'Nnew1'] = r.cumIMEI.size - gb1.loc[d-timedelta(days=1)].cumIMEI.size
gb1['Nnew1'] = gb1['cumnew']-gb1['cumnew'].shift()
#gb1['Nnew1'] = 0
for d in gb1.index[1:]:
    gb1.loc[d, 'cumnew'] = gb1.loc[d].cumIMEI.size
    gb1.loc[d, 'Nnew1'] = gb1.loc[d].cumIMEI.size - gb1.loc[d-timedelta(days=1)].cumIMEI.size

gb1[1:-1].plot(y='Nnew1')
#plt.show()
gb1[1:-1].plot(y='cumnew')
plt.show()

gb1.info()
