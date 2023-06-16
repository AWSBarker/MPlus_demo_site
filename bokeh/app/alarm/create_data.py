# create hourly data for alarm : Lightsail version Mapi2
# run crontab
import redis
import pickle
import pandas as pd
import numpy as np
import datetime as dt
import sys
sys.path.append("/home/bitnami/sharepoint")
sys.path.append("..")
from Mapi2 import DBconx, Mapi

def initial_df():
    with DBconx('t') as d:
        tuptup = d.query(f"SELECT md.imei, md.last_measure_at, mo.showas from `M+_daily` md "
                         f"JOIN `M+_orgs` mo ON md.org = mo.id "
                         f"WHERE md.last_measure_at BETWEEN '{beg}' AND curdate()"
                         )
    df1 = pd.DataFrame([i for i in tuptup])
    df1.columns = ['imei', 'hours', 'id']
    df1 = df1.astype({'imei': np.int64, 'id': str}) # no effect 'last' : '<M8[s]'})
    df1.set_index('hours', drop=False, inplace=True)
    return  df1.pivot_table(columns=df1.index.hour, index=[df1.id, df1.index.date], values='imei',
                                     aggfunc=np.count_nonzero, fill_value=0)

beg = dt.date(2022,1,1)
r = redis.Redis()
r.set("initial_df", pickle.dumps(initial_df()))  #df0 == pickle.loads(r.get("initial_df"))
