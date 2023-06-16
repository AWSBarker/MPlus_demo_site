# TRY CLASS, test timing, inde url download module
# 27 Sept 22 - URL changed to popup, B4 require to get URL
# lightsail version [VESA] - tested 24.6.22 Health_test
# converted tables to show NULL instead None
# db.table should come from Study_Studies.Env OR DBconx
# ensure emailed_studies_date.txt is clean 'NCT9999999' and weekly_studies_date.log is logger
# crontab run combined CTgov_3 and Study_Studies
import pandas as pd
import numpy as np
import urllib3
import shutil
from zipfile import ZipFile
import os
import datetime as dt
from Study_Studies import Study, Studies, Enviro
import time
from os.path import exists
import gc
import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt

class Aact():
    """
    - create url, files list, environment (data folders, db)
    - identify aact daily url for downloading, unzipping into txt files to server
    - open logger
    - confirm URL and date of file, compare with existing
    - download and unzip if all OK
    """
    headers = {"User-Agent": "Mozilla/5.0 (Linux; U; Android 4.2.2; he-il; NEO-X5-116A Build/JDQ39) AppleWebKit/534.30 ("
                             "KHTML, like Gecko) Version/4.0 Safari/534.30"}
    url = "https://aact.ctti-clinicaltrials.org/download"
    filename2 = "detailed_descriptions.txt"
    filename1 = "studies.txt"
    filename3 = "design_outcomes.txt"
    filename4 = "central_contacts.txt"
    newfiles = (filename1, filename2, filename3, filename4)
    enviro = Enviro()
    tab = enviro.table
    source = enviro.data_folder #"/media/Data2/ownCloud/PycharmProjects/sharepoint/data/"

    def __init__(self):
        Study.logger("started V3 : aact GTgov weekly")
#    def geturl_check_age(self): # scrpae new URL lastest files, if exist and newer then download
        response = requests.get(self.url) #"https://aact.ctti-clinicaltrials.org/pipe_files", headers=headers)
        if response.status_code == 200:
            webpage = response.content
            soup = BeautifulSoup(webpage, "html.parser")
            # first dropdown list element is latest zip file
            dropbox = soup.find_all(class_='form-select')[2].contents[3]
            self.aurl = dropbox.attrs['value']
            filename = dropbox.text.strip()
            self.aurl_date = dt.strptime(filename[:8], '%Y%m%d').date()
        else:
            self.aurl_date, self.aurl = (None, None)
        fpath = f"{self.source}/{self.filename1}"
        if exists(fpath):
            self.f_age = dt.fromtimestamp(os.path.getmtime(fpath)).date()
        else: # if not exsiting
            self.f_age = dt(2022,1,1).date() # old to force download

    def download_unzip(self): # need : aurl, url_age
        st = time.time()
        Study.logger(f'starting URL download {st}')
        pm = urllib3.PoolManager()
        try:
            r = pm.request('HEAD', self.aurl)
            if r.status == 200:
                r.close()
                with pm.request('GET', self.aurl, preload_content=False) as res:
                    tf = os.path.join(self.source, 'a.zip')
                    with open(tf, 'wb') as outfile:
                        Study.logger(f"Downloading Copying {tf} start at {time.time() - st}s")
                        shutil.copyfileobj(res, outfile)
                        Study.logger(f"Downloading Copying {tf} end at {time.time() - st}s")
                with ZipFile(tf) as z:
                    for file in self.newfiles:
                        Study.logger(f'starting extraction {file}')
                        z.extract(file, self.source)
                        Study.logger(f"Extracted {file} at {time.time() - st}s")
                Study.logger(f'finished all download, unzip in {time.time() - st}s')
                return True
            else:
                return False
        except:
            Study.logger(f'error in download URL {self.aurl}, with date {self.aurl_date}')
            return False


class Filter_Studies_Send(Aact):

    def __init__(self):
        cols = ['nct_id', 'brief_title', 'official_title', 'overall_status', 'phase',  'enrollment', 'last_update_posted_date']
        studies = pd.read_csv(self.source+'/'+ self.filename1, sep='|', usecols=cols, parse_dates=['last_update_posted_date'], dayfirst=True)
        studies.rename(columns= {'last_update_posted_date' : 'updated'}, inplace=True)
        filter400d = dt.datetime.now()-dt.timedelta(days=401)
        phase34r = studies[studies.phase.str.contains('3|4|Not Applicable', case=False, na=True, regex=True)
                           & (studies.overall_status.str.contains('Recruiting', case=False, na=False, regex=False))
                           & (studies.updated >= filter400d)]
        del studies
        gc.collect()

        # get desc, outcome_m based on nct_id from phase34r
        cols = ['id', 'nct_id', 'outcome_type' , 'measure' , 'time_frame', 'description']
        outcome_m = pd.read_csv(self.source+'/'+self.filename3, sep='|', usecols=cols) # first 5 cols reqd
        # id|nct_id|outcome_type|measure|time_frame|population|description
        #outcome_m.drop(columns = ['id'], inplace = True)

        outP34r = pd.merge(phase34r, outcome_m, on='nct_id', how='left')
        outP34r.insert(len(outP34r.columns),'Drank',0)

        cols = ['id', 'nct_id', 'description']
        desc = pd.read_csv(self.source+'/'+self.filename2, sep='|', usecols=cols)
        '''id|nct_id|description'''
        descP34r = pd.merge(phase34r, desc, on='nct_id', how='left')
        del desc

        descP34r.insert(len(descP34r.columns),'Drank',0)

        # ranking keywords in outcome_m and description
        kw_outcome = [i['kw'] for i in Studies().kw('outcome')]
        kw_desc = [i['kw'] for i in Studies().kw('desc')]

        # added 'other' outcomes to ranking
        for kw in kw_outcome:
            outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
                        (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
                        (outP34r.outcome_type.str.contains('primary', na=False)), 'Drank'] += 20

            outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
                        (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
                        (outP34r.outcome_type.str.contains('secondary', na=False)), 'Drank'] += 10
        # capture 'other' outcomes
            outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
                        (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
                        (outP34r.outcome_type.str.contains('other', na=False)), 'Drank'] += 10

            descP34r.loc[descP34r.official_title.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=1
            descP34r.loc[descP34r.description.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=1

        print(f" kw_outcomes {outP34r.Drank.value_counts()}, {descP34r.Drank.value_counts()}")

        for kw in kw_desc:
            outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
                        (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
                        (outP34r.outcome_type.str.contains('primary', na=False)), 'Drank'] += 20

            outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
                        (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
                        (outP34r.outcome_type.str.contains('secondary', na=False)), 'Drank'] += 10
        # capture 'other' outcomes
            outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
                        (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
                        (outP34r.outcome_type.str.contains('other', na=False)), 'Drank'] += 10

            #outP34r.loc[outP34r.description.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=100
            descP34r.loc[(descP34r.official_title.str.contains(kw, case=False, na=False, regex=False)) |
                         (descP34r.description.str.contains(kw, case=False, na=False, regex=False)), 'Drank'] +=100

        print(f" kw_descriptions {outP34r.Drank.value_counts()}, {descP34r.Drank.value_counts()}")

        # get unique nct_id and sum Drank
        UoutP34r = outP34r[outP34r.Drank > 1].groupby(['nct_id']).sum().reset_index()
        UdescP34r = descP34r[descP34r.Drank > 1].groupby(['nct_id']).sum().reset_index()

        # filter and sort
        dr = pd.merge(UoutP34r, UdescP34r, left_on='nct_id', right_on='nct_id', how='inner', suffixes=('_out', '_desc'))
        dr['Drank'] = dr.loc[(dr.Drank_out >= 30), 'Drank_out'] + dr.loc[(dr.Drank_desc >= 2), 'Drank_desc']
        dr = dr.loc[dr.Drank.notna()]

        sorted_Drank = dr[dr.Drank >= 32].sort_values('Drank', ascending=False)[['nct_id','Drank', 'Drank_out', 'Drank_desc']].values.tolist()

        db_desc = dr.join(descP34r.set_index('nct_id'), on='nct_id', lsuffix='_final')
        db_desc.drop(['id', 'Drank','enrollment_out', 'enrollment_desc'], axis = 1, inplace=True)
        db_desc = db_desc.replace({np.nan: None})
        db_desc['url'] = 'https://clinicaltrials.gov/ct2/show/' + db_desc['nct_id'].astype(str)
        #db_desc['updated'] = db_desc['updated'].astype(str)
        db_desc['updated'] = pd.to_datetime(db_desc['updated'], format = '%Y%m%d').dt.date
        #db_desc['updated'] = db_desc['updated'].dt.date

        # get new from latest.difference(original)
        latest = db_desc.nct_id.to_list()
        # whats new and use as email targets
        original = set(Studies().nct_df().nct_id.values)
        newones = set(latest).difference(original) # removed NCT02365974, NCT04182464
        # create LoT for log
        mask = db_desc.nct_id.isin(newones)
        Studies().log_newones(db_desc[mask].to_records().tolist())

        # NewStudies.log created. but note E = Existing below needs editing for manual re-runs else newonesGT doesn't work
        sql= f"INSERT INTO {self.tab} " \
             f"(nct_id, id_out, Drank_out, id_desc, Drank_desc, Drank_final, updated, brief_title, official_title, overall_status, phase, enrollment, description, url) " \
             f"VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE " \
             f"nct_id = VALUES(nct_id),id_out = VALUES(id_out),Drank_out = VALUES(Drank_out)," \
             f"id_desc = VALUES(id_desc),Drank_desc = VALUES(Drank_desc),Drank_final = VALUES(Drank_final),updated = VALUES(updated),brief_title = VALUES(brief_title)," \
             f"official_title = VALUES(official_title),overall_status = VALUES(overall_status),phase = VALUES(phase)," \
             f"enrollment = VALUES(enrollment),description = VALUES(description),url = VALUES(url), new = 'E'"
        # if duplicate, Existing record new = E otherwise default is N = New. emailAB = 0,

        Studies().allocate(db_desc.to_records(index=False, column_dtypes={'id_out' : int, 'Drank_desc': int, 'Drank_final': int, 'Drank_out': int, 'enrollment': int}).tolist(), sql)
        #df2db(db_desc.to_records(index=False, column_dtypes={'Drank_desc' : int, 'Drank_final' : int, 'Drank_out' : int}).tolist(), sql)

        # update contact db
        contacts = pd.read_csv(self.source+'/'+self.filename4, sep='|')
        contacts = contacts.replace({np.nan: None})
        # connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi', password='7914920', db='Health')
        # with connection as con:
        #     studies = read_sql("SELECT nct_id FROM CTgov1", con)
        studies = Studies().nct_df()
        cs = studies.merge(contacts, how='inner', on='nct_id')
        del contacts

        # will update changes and add new
        sql= """INSERT INTO CTgovContacts (nct_id, id, contact_type, name, phone, email)    
        VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
        nct_id = VALUES(nct_id),
        id = VALUES(id),
        contact_type = VALUES(contact_type),
        name = VALUES(name),
        phone = VALUES(phone),
        email = VALUES(email)
        """
        # reduce fields
        Studies().allocate(cs.iloc[:,0:6].to_records(index=False).tolist(), sql)
        #df2db(cs.to_records(index=False).tolist(), sql)
        # update outcomes db
        o = studies.merge(outcome_m, how='left', on='nct_id')
        o = o.replace({np.nan: None})
        o.id.fillna(0, inplace=True) # remove None id
        del outcome_m
        sql= """INSERT INTO CTgovOutcomes (nct_id, id, outcome_type, measure, time_frame, description)    
        VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE 
        nct_id = VALUES(nct_id),
        id = VALUES(id),
        outcome_type = VALUES(outcome_type),
        measure = VALUES(measure),
        time_frame = VALUES(time_frame),
        description = VALUES(description)
        """
        Studies().allocate(o.to_records(index=False, column_dtypes={'id' : int}).tolist(), sql)
        Studies().table_backup()

    # 0. Setup ranking filter cutoffs
        existing_rankfrom = 150  # filter existing studies from 150 down
        rankGT = 145  # only include new studies GT 150
        getnew = Studies().new_rankedGT(rankGT)
        na150 = Studies().not_mailed_alloc_new(existing_rankfrom)
        cut_at = 5 * len(Study.maildict)

    # 1. If send any new studies send all immediately greater than rank X
        if len(getnew) > 0: # only if some new ones
            newna150 = getnew.nct_id.to_list()
            Studies().send_email(newna150, 'N')

    # 2. send only X each to mailing list of Existing studies until ranked > 150 (18 on Jan5 2022)
            # if > 3*len, send 18 from na150[-18:],  < 3*len send na150[len:], if 0 skip
        if len(na150) > 0:
            na150s = na150.sort_values(by= ['Drank_final']).nct_id.to_list()
            Study.logger(f'Mailing Exiting trials: {len(na150s)} found to send')
            if len(na150s) >= cut_at:
                Studies().send_email(na150s[-cut_at:], 'E')
            else :
                Studies().send_email(na150s, 'E')
        else:
            Study.logger(f'no Exiting trials Rank > {existing_rankfrom} found to send')

    #3 allocate (update new = E) to db only what has been sent using /data/emailed_studies_20111104.txt # update DB emailAB = 1, Alloc = st , new = E
    ### Alloc needs to be 99 NOT AB

        s = Studies()   # table inherited from Env (s.table='CTgov1')
        asql= f"INSERT INTO {s.table} (nct_id, Alloc) VALUES(%s,%s) ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id)," \
                     f"new = 'E', Alloc=VALUES(Alloc), emailAB = 1"
        # test allocate what has been sent from file emailed_studies_20111104.txt
        lot = []
        reverse_usid = {i['username'] : i['id'] for i in s.dbcon_query("SELECT id, username FROM accounts_user WHERE is_sales")}
        try:
            with open(f"{s.data_folder}/{s.dbc.d}_{s.table}_emailed_studies_{time.strftime('%Y%m%d')}.txt", 'r') as f:
                #(f"{self.nct_id}, {st}\n")
                for line in f:
                    pair = line.replace(' ','').replace('\n','').split(',') # [nct, ab]
                    lot.append((pair[0], reverse_usid.get(pair[1], 2)))
            Studies().allocate(lot, asql)
            Study.logger(f'completed writing to db new studies emailed {s.table} ')
        except:
            Study.logger('some error in writing emailed files')

if __name__ == "__main__":
    a = Aact()
#    url_age, aurl = a.geturl_check_age()  # assume all file same age (last download all or nothing)
#    f_age = a.getfilenames2() # checks files exist get age as dt

    if a.f_age < a.aurl_date: # only continue of exiting files are older than new ones
        allok = a.download_unzip()
        if allok:
            Filter_Studies_Send()
        else:
            Study.logger(f'aact.ctti-clinicaltrials.org URL failed, check it')
            exit()
    else:
        Study.logger(' Nothing new to work on ')
        exit()

"""

def download_unzip(self.source, aurl, url_age):
    pm = urllib3.PoolManager()
    try:
            r = pm.request('HEAD', aurl)
            if r.status == 200:
                r.close()
                with pm.request('GET', aurl, preload_content=False) as res:
                    tf = os.path.join(self.source,'a.zip')
                    with open(tf, 'wb') as outfile:
                        Study.logger(f"Downloading Copying {tf}")
                        shutil.copyfileobj(res, outfile)
                with ZipFile(tf) as z:
                    for file in newfiles:
                        z.extract(file, self.source)
                        Study.logger(f"Extracted  {file}")
                return True
            else:
                return False
    except:
        Study.logger(f'error in download URL {aurl}, with date {url_age}')
        return False

def url_shutil(self.source):
    tday = 2 # couple day back not worth it
    daysback = [0,1,2,3,4,5]
    pm = urllib3.PoolManager()

    while tday in daysback:
        yesterday3 = (dt.datetime.now()-dt.timedelta(days=tday)).strftime('%Y%m%d')
        aurl = f"https://aact.ctti-clinicaltrials.org/static/exported_files/daily/{yesterday3}_pipe-delimited-export.zip"

        try:
            r = pm.request('HEAD', aurl)
            if r.status == 200:
                r.close()
                with pm.request('GET', aurl, preload_content=False) as res:
                    tf = os.path.join(self.source,'a.zip')
                    with open(tf, 'wb') as outfile:
                        Study.logger(f"Downloading Copying {tf}")
                        shutil.copyfileobj(res, outfile)

                with ZipFile(tf) as z:
                    for file in newfiles:
                        z.extract(file, self.source)
                        Study.logger(f"Extracted  {file}")
                tday = -1
                return tday
        except:
            Study.logger(f'trying URL less {tday} days')
            tday += 1
    return tday



def downloadzip(self.source):
    # static https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/20220218_clinical_trials.zip
    tday = 2 # couple day back not worth it
    daysback = [0,1,2,3,4,5]

    while tday in daysback:
        yesterday3 = (dt.datetime.now()-dt.timedelta(days=tday)).strftime('%Y%m%d')
        aurl = f"https://aact.ctti-clinicaltrials.org/static/exported_files/daily/{yesterday3}_pipe-delimited-export.zip"
        try:
            if urlopen(aurl).code == 200:
                tday = -1
        except HTTPError as e:
            Study.logger(f"some HTTP error {e}")
            exit()
        except:
            tday += 1

    with urlopen(aurl) as ziparch:
        Study.logger('downloading ')
        with ZipFile(BytesIO(ziparch.read())) as zfile:
            for f in newfiles: #[filename1, filename2, filename3, filename4]:
                Study.logger(f'extracting {f}')
                zfile.extract(f, self.source)
                
                
def getfilenames(): # add to download list if 1. older than 1 days or 2. not existin
    newfiles =  []
    for f in os.scandir(self.source):
        if f.name in [filename1, filename2, filename3, filename4]: # filename5]:
            ts = os.path.getmtime(f)
            if (dt.datetime.now() - dt.datetime.fromtimestamp(ts)).days > 0:
                newfiles.append(f.name)
                Study.logger(f'getting file {f.name}')
    return newfiles
"""
