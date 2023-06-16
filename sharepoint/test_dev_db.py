
import sys
sys.path.append("..")
from datetime import datetime as dt
import time
from sharepoint.Mapi2 import DBconx
beg ='2022.12.30'

st = time.time()
with DBconx('t') as d:
    tuptup = d.query(f"SELECT imei, checked_on, last_measure_at, count from `M+_daily` "
                     f"WHERE last_measure_at BETWEEN '{beg}' AND curdate()")

LoT = list(tuptup)
print(f'finished {time.time() - st}')
print(LoT[1])

