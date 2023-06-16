# with C19daily2 and iso
# try local hourly pickle of data to/from /data folder
import MySQLdb
import MySQLdb.cursors
import numpy as np
import pandas as pd
pd.set_option('use_inf_as_na', False)
import time
import os

cwd = "/home/ab/bokeh/examples/app/c19"  # os.getcwd()
data_folder = f'{cwd}/data'

def db(sql):
    connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi',
                             password='7914920',  db='Health', cursorclass=MySQLdb.cursors.DictCursor)
    conn = connection.cursor()# setup the dropdowns dicts
    conn.execute(sql) #"SELECT * FROM countries")
    r = conn.fetchall() # tuple of dicts ({},{}..  ,)
    conn.close()
    connection.close()
    return r

start = time.time()

df_pays = pd.DataFrame(db("SELECT * FROM countries"))
ghes = pd.DataFrame(db("SELECT ghe, C FROM ghe"))
dfall = pd.DataFrame(db("SELECT * FROM C19daily2"))

print(f'db selected {time.time()-start}') # ~1s

dfall.set_index(['geoId', 'date'], drop=False, inplace=True)
dfall = dfall.sort_index(ascending=True)

# map subregions
am = df_pays.set_index('Code')[['SRegion', 'Name']]
dfall['sr'] = dfall['geoId'].map(am['SRegion']).astype('str')
dfall['sr'] = dfall.sr.replace('nan', 'Other')
dfall['Name'] = dfall['geoId'].map(am['Name'])
dfall['geoId'] = dfall['geoId'].astype('str')

gbl0 = dfall.groupby(level=0)
gb10_cases = gbl0['cases']
gb10_deaths = gbl0['deaths']

dfall['ccases'] = gb10_cases.cumsum()
dfall['dpc_cases'] = gb10_cases.pct_change(fill_method='pad', periods=7)
dfall['cdeaths'] = gb10_deaths.cumsum()
dfall['dpc_deaths'] = gb10_deaths.pct_change(fill_method='pad', periods=7)
dfall = dfall.replace([np.inf, -np.inf], np.nan)
dfall['dmax'] = gb10_deaths.rolling(7, min_periods=1).mean().reset_index(0,drop=True)
dfall['cmax'] = gb10_cases.rolling(7, min_periods=1).mean().reset_index(0,drop=True)
dfall['ccolor'] = ['bisque' if c >= 0 else 'lightgreen' for c in dfall.dpc_cases]
dfall['dcolor'] = ['bisque' if c >= 0 else 'lightgreen' for c in dfall.dpc_deaths]
# id in each group
dfall['id'] = gbl0.cumcount()

gball = dfall.groupby(level=0)
gbcm = gball.cmax
i = gbcm.transform('idxmax')
dfall['id'] = dfall.id - dfall.loc[i, 'id'].values

# normalise cases again max in each group
#dfall['dfm_cmax'] = dfall.groupby(level=0).cmax.transform('max') - dfall.cmax
dfall['dfm_cmax'] = dfall.cmax / gbcm.transform('max')

# post peak days and last data for labelset
gbtmp = gball.last().reset_index(drop=True)
gblast = gbtmp.set_index('sr') #.replace({np.nan: None})


dfall.to_pickle(f'{data_folder}/dfall.pkl')
df_pays.to_pickle(f'{data_folder}/df_pays.pkl')
gblast.to_pickle(f'{data_folder}/gblast.pkl')

print(f'done {time.time() - start}')  # ~ 11s
