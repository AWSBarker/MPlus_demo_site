from io import BytesIO
from zipfile import ZipFile
import pandas as pd
import datetime as dt
import urllib3, shutil
from memory_profiler import profile
import time
from Study_Studies import Enviro
import gc
import tempfile
import os

@profile
def url_shutil():
    tday = 2 # couple day back not worth it
    daysback = [0,1,2,3,4,5,6,7]

    pm = urllib3.PoolManager()

    while tday in daysback:
        yesterday3 = (dt.datetime.now()-dt.timedelta(days=tday)).strftime('%Y%m%d')
        aurl = f"https://aact.ctti-clinicaltrials.org/static/exported_files/daily/{yesterday3}_pipe-delimited-export.zip"

        try:
            r = pm.request('HEAD', aurl)
            if r.status == 200:
                tday = -1
                r.close()
                with pm.request('GET', aurl, preload_content=False) as res:
                    tf = 'a.zip'
                    with open(tf, 'wb') as outfile:
                        shutil.copyfileobj(res, outfile)
                with ZipFile('a.zip') as z:
                    for file in filelist:
                        z.extract(file)
                        print(f"Extracted  {file}")
        except:
            print(f'trying again less {tday} days')
            tday += 1

@profile
def read_csv1():
    cols = ['nct_id', 'brief_title', 'official_title', 'overall_status', 'phase',  'enrollment', 'last_update_posted_date']
    studies = pd.read_csv(source+'/'+filelist[0], sep='|', usecols=cols, parse_dates=['last_update_posted_date'], dayfirst=True)
    studies.rename(columns= {'last_update_posted_date' : 'updated'}, inplace=True)
    filter400d = dt.datetime.now()-dt.timedelta(days=401)
    phase34r = studies[studies.phase.str.contains('3|4|Not Applicable', case=False, na=True, regex=True)
                       & (studies.overall_status.str.contains('Recruiting', case=False, na=False, regex=False))
                       & (studies.updated >= filter400d)]
    del studies
    gc.collect()

    cols = ['id', 'nct_id', 'outcome_type' , 'measure' , 'time_frame', 'description']
    outcome_m = pd.read_csv(source+'/'+filelist[2], sep='|', usecols=cols) # first 5 cols reqd
    outP34r = pd.merge(phase34r, outcome_m, on='nct_id', how='left')
    del outcome_m
    gc.collect()

    cols = ['id', 'nct_id', 'description']
    desc = pd.read_csv(source+'/'+filelist[1], sep='|', usecols=cols)
    contacts = pd.read_csv(source+'/'+filelist[3], sep='|')
    del desc, contacts
    gc.collect()
    return phase34r


if __name__ == "__main__":
    enviro = Enviro()
    tab = enviro.table
    source = enviro.data_folder
    filelist = ("studies.txt", "detailed_descriptions.txt", "design_outcomes.txt", "central_contacts.txt")

    st = time.time()
    url_shutil()
    print(f'finished URL download unzip in {time.time() - st}')

    st = time.time()
    df = read_csv1()
    print(df.info())
    print(f'finished CSV read in {time.time() - st}')

"""
Using Url_shutil

Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
    33     22.8 MiB     22.8 MiB           1   @profile
    34                                         def url_shutil():
    35     22.8 MiB      0.0 MiB           1       tday = 2 # couple day back not worth it
    36     22.8 MiB      0.0 MiB           1       daysback = [0,1,2,3,4,5]
    37                                         
    38     25.4 MiB      0.0 MiB           2       while tday in daysback:
    39     22.8 MiB      0.0 MiB           1           yesterday3 = (dt.datetime.now()-dt.timedelta(days=tday)).strftime('%Y%m%d')
    40     22.8 MiB      0.0 MiB           1           aurl = f"https://aact.ctti-clinicaltrials.org/static/exported_files/daily/{yesterday3}_pipe-delimited-export.zip"
    41     22.8 MiB      0.0 MiB           1           c = urllib3.PoolManager(num_pools=2)
    42     22.8 MiB      0.0 MiB           1           downfile = '1.zip'
    43     22.8 MiB      0.0 MiB           1           try:
    44     25.1 MiB      2.3 MiB           1               with c.request('GET', aurl, preload_content=False) as res:
    45     25.1 MiB      0.0 MiB           1                   tday = -1 if res.status == 200 else tday
    46     25.1 MiB      0.0 MiB           1                   with open(downfile, 'wb') as outfile:
    47     25.4 MiB      0.3 MiB           1                       shutil.copyfileobj(res, outfile)
    48                                                 except HTTPError as e:
    49                                                     print(f"some HTTP error {e}")
    50                                                 except:
    51                                                     tday += 1
    52                                         
    53     25.4 MiB      0.0 MiB           1       with ZipFile(downfile) as z:
    54     25.7 MiB      0.0 MiB           5               for file in file_list:
    55     25.7 MiB      0.3 MiB           4                   z.extract(file)
    56     25.7 MiB      0.0 MiB           4                   print(f"Extracted  {file}")


finished in 236.68709683418274



Using BytesIO
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
    11     22.6 MiB     22.6 MiB           1   @profile
    12                                         def downloadzip():
    13                                             # static https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/20220218_clinical_trials.zip
    14     22.6 MiB      0.0 MiB           1       tday = 2 # couple day back not worth it
    15     22.6 MiB      0.0 MiB           1       daysback = [0,1,2,3,4,5]
    16                                         
    17     24.3 MiB      0.0 MiB           2       while tday in daysback:
    18     22.6 MiB      0.0 MiB           1           yesterday3 = (dt.datetime.now()-dt.timedelta(days=tday)).strftime('%Y%m%d')
    19     22.6 MiB      0.0 MiB           1           aurl = f"https://aact.ctti-clinicaltrials.org/static/exported_files/daily/{yesterday3}_pipe-delimited-export.zip"
    20     22.6 MiB      0.0 MiB           1           try:
    21     24.3 MiB      1.6 MiB           1               if urlopen(aurl).code == 200:
    22     24.3 MiB      0.0 MiB           1                   tday = -1
    23                                                 except HTTPError as e:
    24                                                     print(f"some HTTP error {e}")
    25                                                 except:
    26                                                     tday += 1
    27                                         
    28     24.3 MiB      0.0 MiB           1       with urlopen(aurl) as ziparch:
    29   1425.7 MiB   1401.5 MiB           1           with ZipFile(BytesIO(ziparch.read())) as zfile:
    30   1426.0 MiB  -1400.3 MiB           5               for f in file_list: #[filename1, filename2, filename3, filename4]:
    31   1426.0 MiB      0.3 MiB           4                   zfile.extract(f)


finished in 227.53095078468323

"""
