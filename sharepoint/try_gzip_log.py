# test gzip logs
import gzip
import sys, os
sys.path.append("..")
from datetime import datetime as dt
from sharepoint.Mapi2 import DBconx
beg ='2021.11.01'
with DBconx('t') as d:
    tuptup = d.query(f"SELECT imei, checked_on, last_measure_at, count from `M+_daily` "
                     f"WHERE last_measure_at BETWEEN '{beg}' AND curdate()")

class TestLog:

    data_folder = 'data'
    def logit_gz(self, LoT=[]):
        # not always M+daily, also CTgov
        self.LoT = LoT
        l = f"{self.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log.gz"
        mode = 'at' if os.path.exists(l) else 'wt'
        with gzip.open(l, mode) as f:
            f.write(f" {dt.now().strftime('%X')} data saved to DB\n")
            if len(LoT) > 0:
                f.writelines(f"{ln}\n" for ln in self.LoT)
    def addlog_gz(self, t):  # add to above log
        self.t = t
        l = f"{self.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log.gz"
        mode = 'at' if os.path.exists(l) else 'wt'
        with gzip.open(l, mode) as f:
            f.write(self.t)

    def logit(self, LoT=[]):
        # not always M+daily, also CTgov
        self.LoT = LoT
        l = f"{self.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log"
        mode = 'a' if os.path.exists(l) else 'w'
        with open(l, mode) as f:
            f.write(f" {dt.now().strftime('%X')} data saved to DB\n")
            if len(LoT) > 0:
                f.writelines(f"{ln}\n" for ln in self.LoT)
    def addlog(self, t):  # add to above log
        self.t = t
        l = f"{self.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log"
        mode = 'a' if os.path.exists(l) else 'w'
        with open(l, mode) as f:
                f.write(self.t)

LoT = list(tuptup)
print(LoT[1])
tl = TestLog()
tl.logit_gz(LoT)
tl.logit(LoT)
tl.addlog_gz('LoT ends')
tl.addlog('LoT ends')
