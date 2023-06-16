# -mysql on Mapi2
# check timeit on sql join vs pd.map
import pandas as pd
from datetime import datetime as dt, timedelta
import sys, time
sys.path.append("..")
from Mapi2 import Mapi, Org, DBconx

orgDict = Mapi.getOrg_Name_dict()

map_times =[]
join_times = []

def dict_map():
    sql = f"SELECT id, showas FROM `M+_orgs` WHERE org_id IS NOT NULL"
    with DBconx() as dbc:
        return dbc.query(sql)

def maper():
    st = time.time()
    tuptup = DBconx('T').query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")# WHERE org IN (1,2,3,4,5,6)")
    df1 = pd.DataFrame([i for i in tuptup], columns=['imei', 'checked', 'last', 'count', 'org']) #.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
    #df1.org = df1.org.map(orgDict) #.astype('category')
    df1.org = df1.org.map(Mapi.getOrgs_showas_dict())
    print(f" maper completed in {time.time() - st}s")
    map_times.append(time.time() - st)
    return df1

def joiner():
    st = time.time()
    with DBconx('t') as d:
        q = d.query(f"SELECT md.imei, md.checked_on, md.last_measure_at, md.count, mo.showas from `M+_daily` md "
                         f"JOIN `M+_orgs` mo ON md.org = mo.id")
    df2 = pd.DataFrame([i for i in q], columns=['imei', 'checked', 'last', 'count', 'org'])
    print(f" joiner completed in {time.time() - st}s")
    join_times.append(time.time() - st)

    return df2

dm = dict_map()

for i in range(3):
    maper()
    joiner()
for i in range(3):
    joiner()
    maper()

print(f" map mean {pd.Series(map_times).mean()}, join mean {pd.Series(join_times).mean()}  ")
