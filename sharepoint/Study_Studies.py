# lightsail version [VESA test]
# testing in Health_test from VESA 1. convert auth user to Health accounts user
# v8.6.22 added bluewin.ch mail server
# TODO given trial ID produce a HTML summary and drank-final
# TODO : Alloc is linked to Mtest.authuser 1 = AB
# create class for each trial for reporting
#from pandas.io.sql import read_sql
import pandas as pd
import numpy as np
import MySQLdb
import MySQLdb.cursors
import MySQLdb._exceptions
import re
from nltk.tokenize import sent_tokenize
import nltk
import time
from smtplib import SMTP, SMTPServerDisconnected, ssl, SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from Mapi2 import DBconx
from os.path import exists
import datetime as dt
from io import BytesIO
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from zipfile import ZipFile
import dask.dataframe as dd

class Enviro:
    """
    ! ALL emails = andrew.barker@medisante.group.com
    get environment production or Dev from Mapi
    """
    table = 'CTgov1'

    with DBconx() as dbc:
        maildict = {i['id'] : i['email'] for i in dbc.query("SELECT id, email FROM accounts_user WHERE is_sales")} #| \
        usermaildict = {i['username'] : 'andrew.barker@medisante.group.com' for i in dbc.query("SELECT username, email FROM accounts_user WHERE is_sales")}
        usid = {i['id'] : i['username'] for i in dbc.query("SELECT id, username, email FROM accounts_user WHERE is_sales")}
        data_folder = dbc.data_folder #"/media/Data2/ownCloud/PycharmProjects/sharepoint/data"
    nltk.data.path.append(data_folder)

    def __init__(self):
        print(self.dbc.condb)

    @staticmethod
    def dbcon_query(s):
        with DBconx() as dbc:
            return dbc.query(s)

    @staticmethod
    def dbcon_writemany(s, LoT):
        with DBconx() as dbc:
            dbc.writemany(s,LoT)

class Study(Enviro):
    '''
    maildict = is_sales
    for dummy test emails Health.accounts_user = 'barker@bluewin.ch'
    '''

    def __init__(self, nct_id):
        self.nct_id = nct_id
        tuple_dict = self.dbcon_query(f"SELECT * FROM {self.table} WHERE nct_id = '{self.nct_id}'")#[0]

        if len(tuple_dict) > 0:
            for k,v in tuple_dict[0].items():
                setattr(self, k, v)

            td = Enviro.dbcon_query(f"SELECT outcome_type, description FROM CTgovOutcomes WHERE nct_id = '{self.nct_id}'")

            #self.condb.execute(f"SELECT outcome_type, description FROM CTgovOutcomes WHERE nct_id = '{self.nct_id}'")
            #td = self.condb.fetchall() # tuple of dicts {outcome_type: , description : } --> {type : []}

            self.outcomes_other = set() # primary, secondy, other lists
            self.outcomes_primary = set()
            self.outcomes_secondary = set()
            for d in td:
                # {outcome_type: str, description : str}
                if d['outcome_type'] == 'primary' and d['description']:
                    self.outcomes_primary.add(d['description']) #if d['description'] else self.outcomes_primary.append('')
                elif d['outcome_type'] == 'secondary' and d['description']:
                    self.outcomes_secondary.add(d['description'])
                else :
                    self.outcomes_other.add(d['description']) if d['description'] else self.outcomes_other.add('')

            tc = Enviro.dbcon_query(f"SELECT contact_type, name, phone, email  FROM CTgovContacts WHERE nct_id = '{self.nct_id}'")
            #self.condb.execute(f"SELECT contact_type, name, phone, email  FROM CTgovContacts WHERE nct_id = '{self.nct_id}'")
            #tc = self.condb.fetchall() # tuple of dicts {name, phone, contact_type: , description : } --> {type : []}

            self.contacts = []
            for t in tc:
                self.contacts.append(tuple(str(v) for v in t.values())) # TODO None as str
            self.contacts = ['empty'] if not self.contacts else self.contacts
        else:
            self.description = f'{nct_id} not found'
            self.contacts = f'{nct_id} not found'

    def makeplain(self):
        # email text reqd nct_id, brief_title, body
        self.cont = ''
        self.head = str(self.official_title) + '\n' + str(self.url) + '\n' + str(self.description) + '\n'
        for t in self.contacts:
            self.cont = self.cont + '\n' + ' '.join(t)
        self.body = '\n1°\n'.join(self.outcomes_primary)+'\n2°\n'.join(self.outcomes_secondary)+'\n3°\n'.join(self.outcomes_other)

        return self.head, self.cont, self.body

    def last_update(self): # NOT USED. returns ({'updated': datetime.date(2022, 6, 2)},) dated.strftime . datetime of last update (NOT ts BUT updated)
        sql = f"SELECT updated FROM {self.table} WHERE nct_id = '{self.nct_id}'"
        return self.dbcon_query(sql) #f"SELECT ts FROM {self.table} ORDER BY ts DESC LIMIT 1")
        #return self.condb.fetchone()

    def makehtml(self, kws):
        # input tuple of tuple (nctID, [sentances,],) or single tuple (nctid, "sents") marked with bold kws
        # html report incl desc, outcomes3, kw bold
        def sent_convert2(str_s):
            # input string of sentences. clean, highlight, html, return html
            remov=r"(~|\r\n|\r|\n|\s+)"
            found_sents = set() # setfound sentences for each id
            sents = re.sub(remov, r" ", str_s) #sents = ' '.join(sents.split())
            sents = sent_tokenize(sents)        #sents = sents.split('.') # or '.'
            for sent in sents:  # single list of sentences
                foundkw = False
                if len(sent) >= 5:
                    for kw in kws:
                        if kw in sent:
                            sent = sent.replace(kw, f'<b>{kw}</b>')
                            foundkw = True
                    if foundkw:
                        sent = f"<li>{sent}</li>"
                        found_sents.add(sent) # unique set
                        print(sent)
            return ''.join(found_sents) if found_sents else ''

        desc = sent_convert2(self.description) if self.description else '' # str
        o1 = sent_convert2(','.join(self.outcomes_primary)) # list(str,)
        o2 = sent_convert2(','.join(self.outcomes_secondary)) # list(str,)
        o3 = sent_convert2(','.join(self.outcomes_other)) # list(str,)
        cont =''
        for c in set(self.contacts):
            cont = cont  + '<br>' + ' '.join(c) + '</br>'

        print(f'{self.data_folder}/{self.nct_id}.html')
        with open(f'{self.data_folder}/{self.nct_id}.html', 'w') as o:
                o.write('<html><body>')
                o.write(f'<h4>{self.nct_id} ({self.Drank_final}) : {self.brief_title}</h4>')
                o.write(f"<br>Last update {self.updated.strftime('%x')}</br>")
                o.write(f"<br>Size enrollment {self.enrollment}</br>")
                o.write(f"<br><a href={self.url} target='_blank'>{self.url}</a></br>")
                o.write('<br><u>Contacts</u></br>')
                o.write(f'{cont}')
                o.write('<p><u>Description keywords</u></p>')
                o.write(f'<p>{desc}</p>')
                o.write('<p><u>Primary Outcomes</u></p>')
                o.write("<p></p>")
                o.write(o1)
                o.write('<p><u> Secondary Outcomes </u></p>')
                o.write(o2)
                o.write('<p><u>  Other Outcomes </u></p>')
                o.write(o3)
                o.write('<hr></hr>')
                o.write(f"<p><sub>M+ Filters applied v2:</sub></p><p><sub> {','.join(kws)}</sub></p>")
                o.write('</body></html>')
        with open(f'{self.data_folder}/{self.nct_id}.html', 'r') as o:
            h = o.read()
        return h

    def send_html(self, r, s, a, st, ht, n): # rank, summary, head.body, contacts, sendto ('AB'), html, new
        h,b,c = a # head, body, contacts

        #self.ab = self.usid[st] # Alloc - accounts_user.username AB usid=2
        self.username = st # 'AB'
        self.email = self.usermaildict.get(st) # self.email is email

        msg = MIMEMultipart("alternative")
        msg['Subject'] = f"{self.table} : {self.nct_id} {n}, rank({r}) {s} ({self.username})"
        msg['From'] = 'barker@bluewin.ch'
        msg['To'] = self.email
        msg['Cc'] = 'andrew.barker@medisante-group.com'

        plain_1 = f'Dear sales \n\nHere is a nice lead, freshly delivered!\n{h} \n{b} \n{c}'
        plain_part = MIMEText(plain_1, 'plain')
        html_1 = ht
        html_part = MIMEText(html_1, 'html')
        msg.attach(html_part)
        msg.attach(plain_part)

        context = ssl.create_default_context()
        try:
            with SMTP_SSL("smtpauths.bluewin.ch", 465, context=context) as server:
                #server.login('AWSBarker', 'Ulusaba32')
                server.login('barker@bluewin.ch', 'Ulusaba@64')
                server.sendmail('barker@bluewin.ch', [self.email,'andrew.barker@medisante-group.com'], msg.as_string())

    # now done after using log
    #     # update DB emailAB = 1, Alloc = st , new = E
    #         asql= f"INSERT INTO {self.table} (nct_id, Alloc) VALUES(%s,%s) ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id)," \
    #              f"new = 'E', Alloc=VALUES(Alloc), emailAB = 1"
    #
    # #        asql= f"INSERT INTO {self.table} (nct_id, Alloc) VALUES(%s,%s) ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id), Alloc=VALUES(Alloc), new = 'E', emailAB = 1"""
    #         # should only allocate after confirming sent email, so use data_folder lists
    #         Studies().allocate([(self.nct_id,st)], asql)

###### CRITICAL ###### update emailed_file with self.st = AB not 2
            #####  BUT convert later from AB to 2 for db
            with open(f"{self.data_folder}/{self.dbc.d}_{self.table}_emailed_studies_{time.strftime('%Y%m%d')}.txt", 'a') as f:
                f.write(f"{self.nct_id}, {self.username}\n")
                self.logger(f"write emailed to file {self.nct_id}")

        except SMTPServerDisconnected as e:
            print(f'smpt error {e} with {self.nct_id}')
            self.logger(f"error smpt server {e}")

        except Exception as e :
            self.logger(f"write email send error {e} to emailed_studies file")

    def test_send(self, msg = 'testing', st = 'AB'): # test mail server
        self.username = st # 'AB'
        self.email = self.usermaildict.get(st)
        msg = MIMEMultipart("alternative")
        msg['Subject'] = "test"
        msg['From'] = 'andrew.barker@medisante-group.com'
        msg['To'] = self.email
        msg['Cc'] = 'andrew.barker@medisante-group.com'

        plain_1 = f'Dear {self.username},\n\nHere is a nice lead, freshly delivered!'
        plain_part = MIMEText(plain_1, 'plain')
        html_1 =""" <h1>TESTING</h1>"""
        html_part = MIMEText(html_1, 'html')
        msg.attach(html_part)
        msg.attach(plain_part)

        context = ssl.create_default_context()
        #context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        try:
#            with SMTP_SSL("smtpauths.bluewin.ch", 465, context=context) as server:
#                server.login('barker@bluewin.ch', 'Ulusaba@64')
#                server.sendmail('barker@bluewin.ch', [self.email,'andrew.barker@medisante-group.com'], msg.as_string())

            with SMTP("smtp.office365.com", 587) as server:
                server.starttls()
                server.login('andrew.barker@medisante-group.com', 'Ulusaba@128')
                server.sendmail('andrew.barker@medisante-group.com', [self.email,'andrew.barker@medisante-group.com'], msg.as_string())

        except SMTPServerDisconnected as e:
            print(f'smpt error {e} with {self.nct_id}')

        except Exception as e:
            print(f"write error to emailed_studies file : {e}")

    @staticmethod
    def logger(txt):
        print(f'logging : {txt}')
        with open(f"{Study.data_folder}/{Study.dbc.d}_{Study.table}_wk_studies_{time.strftime('%Y%m%d')}.log", 'a') as f:
            f.write(f"\n{time.strftime('%H:%M:%S')} - {txt}")

class Studies(Enviro):
    
    def __init__(self):
        #self.cur = DBconx().cur
        print('In Studies ')

    def return_df(self, sql): # return df from fetchall() as loft
        #self.cur.execute(sql)
        return pd.DataFrame(self.dbcon_query(sql))

    def return_tuptup(self, sql): # tuple of dicts
        #self.cur = self.condb.cursor(cursorclass=MySQLdb.cursors.Cursor)
        #self.cur.execute(sql)
        return self.dbcon_query(sql)

    def all(self): # get all records
        return self.return_df(f"SELECT * FROM {self.table}")

    def new(self): # only new = 1
        return self.return_df(f"SELECT * FROM {self.table} WHERE new = 'N' ORDER BY Drank_final DESC")

    def new_rankedGT(self, r): # new and GT rank
        return self.return_df(f"SELECT * FROM {self.table} WHERE Drank_final > {r} AND new = 'N' ORDER BY Drank_final DESC")

    def not_mailed_alloc_new(self, r): # not mailed not alloc not new
        return self.return_df(f"SELECT * FROM {self.table} WHERE new = 'E' AND (Alloc ='' OR Alloc is NULL) AND emailAB = 0 AND Drank_final > {r} ORDER BY Drank_final DESC")

    def not_alloc_GT(self, r): # not mailed not alloc not new
        return self.return_df(f"SELECT * FROM {self.table} WHERE (Alloc ='' OR Alloc is NULL) AND emailAB = 0 AND Drank_final > {r} ORDER BY Drank_final DESC")

    def buffer2go(self): # whats in the email buffer emailAB = 2
        return self.return_df(f"SELECT * FROM {self.table} WHERE emailAB = 2")

    def kw(self, k): # get keywords for filters
        return self.return_tuptup(f"SELECT kw, source FROM CTgovKW WHERE source = '{k}'")

    def nct_df(self): # just nct_id
        return self.return_df(f"SELECT nct_id FROM {self.table}")

    def allocate(self, ListOfTuples, sql): # write to db, can be [(),]
        self.dbcon_writemany(sql, ListOfTuples)
        """
        try:
            a = self.cur.executemany(sql, ListOfTuples)
            #print(f'data saved {a}')
            Study.logger(f'allocating updates N = {a} for {sql[:20]}')
        except MySQLdb._exceptions as e:
            Study.logger(f'some error {e} in writing emailed files')
        finally:
            print(f'closed  {self.cur}')
        
        
    def add1(self, tup, sql): # NOT USED single tuple [(),]
        print('connected')
        try:
            a = self.cur.execute(sql, tup)
            print(f'data saved {a}')
        except MySQLdb._exceptions as e:
             print(f'error {e}')
        finally:
            print(f'closed  {self.cur}')
        """

    def log_newones(self, newones): # newones is LoT
        dk = f"{self.data_folder}/{self.table}_NewStudies_{time.strftime('%Y%m%d')}.log"
        with open(dk, 'w') as f:
            f.writelines(f"{n}\n" for n in newones)
        Study.logger(f'{len(newones)} NewStudies written to log')

    def table_backup(self):
        bk = f"{self.data_folder}/{self.table}_bk_{time.strftime('%Y%m%d')}.pkl.bz2"
        d = self.all()
        d.to_pickle(bk)

    def table_unpickle(self, file):
            bk = f"{self.data_folder}/{self.table}_bk_{file}.pkl.bz2"
            d = pd.read_pickle(bk)
            return d

    def lastbackup(self):
        # get date (filename) of last pkl.bz2
        #import os
        ft = f"{self.data_folder}"
        list_of_files =  [x for x in os.listdir(ft) if x.endswith('.bz2')]
        list_of_paths = [os.path.join(ft, basename) for basename in list_of_files]
        latest_file = max(list_of_paths, key=os.path.getctime)
        return pd.read_pickle(latest_file)

    def send_email(self, LoT, ne): # LoT, 'N'
        #maildict = self.usermaildict #Study.maildict {1 : email} NO! double
        kw_outcome = [i['kw'] for i in self.kw('outcome')]
        kw_desc = [i['kw'] for i in self.kw('desc')]

        nk = tuple(self.usermaildict.keys())                 #[2,3,4,5,6   not 'AB', 'LC', 'RK', 'GL', 'GQ']
        usernames = tuple(np.resize(nk, len(LoT)))
        new_LoT = list(zip(tuple(LoT), usernames)) # ((nct_id, 2),)

        for n,send2id in new_LoT: #[0:3*len(maildict)]: # keep max size = 3 x maildict
            sd = Study(n)
            ht = sd.makehtml(kw_desc+ kw_outcome)
            sd.send_html(sd.Drank_final, sd.brief_title, sd.makeplain(), send2id, ht, ne)

class csvStudies(Enviro):
    """
    - manage csv files for trials not in CTgov1 db
    - lookup a trial
    - crontab csvStudies().downloadzip()
    """

    #data_folder = Studies().data_folder
    filename2 = "detailed_descriptions.txt" # /data2/downloads/CTgov/
    filename1 = "studies.txt"
    filename3 = "design_outcomes.txt"
    filename4 = "central_contacts.txt"
    kw_outcome = [i['kw'] for i in Studies().kw('outcome')]
    kw_desc = [i['kw'] for i in Studies().kw('desc')]

    def __init__(self):
        self.newfiles =[]

    def getfilenames2(self): # add to download list if 1. older than 1 days or 2. not existin
        #newfiles =  [] # list of file to download if 2 and 1
        for f in [self.filename1, self.filename2, self.filename3, self.filename4]:
            fpath = f"{self.data_folder}/{f}"
            if exists(fpath):
                fage = os.path.getmtime(fpath)
                if (dt.datetime.now() - dt.datetime.fromtimestamp(fage)).days > 0: #older than 1 day
                    self.newfiles.append(f)
            else:
                pass
                #self.newfiles.append(f)
        return self.newfiles

    def downloadzip(self):
        # static https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/20220218_clinical_trials.zip
        tday = 1
        daysback = [1,2,3,4]

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
                for f in self.newfiles: #[filename1, filename2, filename3, filename4]:
                    Study.logger(f'extracting {f}')
                    zfile.extract(f, self.data_folder)
                    Study.logger(f'completed {f}')

    def get_csv_df(self, cols, dcols, filename, parse_d=[], dfirst=True):
        tp = pd.read_csv(self.data_folder + '/' + filename, sep='|',low_memory=False, iterator=True, chunksize=50000, usecols=cols, dtype=dcols, parse_dates=parse_d, dayfirst=dfirst)
        df = pd.concat(tp, ignore_index=True)
        return df
#dask
    def get_csv_ddf(self, cols, filename, parse_d=[], dfirst=True):

        return dd.read_csv(self.data_folder + '/' + filename, sep='|', low_memory=False, usecols=cols, parse_dates=parse_d, dayfirst=dfirst, assume_missing=True)

    def mergeLR(self, l, r):
        # two DFs, merge left on nct_id into one, create empty Drank
        m = pd.merge(l, r, on='nct_id', how='left')
        m.insert(len(m.columns),'Drank',0)
        return m

    def d_mergeLR(self, l, r):
        # two dask DFs, merge left on nct_id into one, create empty Drank
        m = dd.merge(l, r, on='nct_id', how='left')
        m['Drank'] = 0
        return m

    def getDrank(self, df, kws):
        # shared ranking assignment between outcomes and desc
        for kw in kws:
            df.loc[(df.description.str.contains(kw, case=False, na=False, regex=False)) |
                (df.measure.str.contains(kw, case=False, na=False, regex=False)) &
                (df.outcome_type.str.contains('primary', na=False)), 'Drank'] += 20

            df.loc[(df.description.str.contains(kw, case=False, na=False, regex=False)) |
                (df.measure.str.contains(kw, case=False, na=False, regex=False)) &
                (df.outcome_type.str.contains('secondary', na=False)), 'Drank'] += 10
            # capture 'other' outcomes
            df.loc[(df.description.str.contains(kw, case=False, na=False, regex=False)) |
                (df.measure.str.contains(kw, case=False, na=False, regex=False)) &
                (df.outcome_type.str.contains('other', na=False)), 'Drank'] += 10
        return df

    def getDrank_outcomes(self, dfo_in, dfd_in): # kws = [i['kw'] for i in Studies().kw('outcome')]):
        # search DFs rows for kw in kws_outcomes, add rank to Drank
        df = self.getDrank(dfo_in, self.kw_outcome)
        
        for kw in self.kw_outcome:
            dfd_in.loc[dfd_in.official_title.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=1
            dfd_in.loc[dfd_in.description.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=1

        return df, dfd_in

    def getDrank_desc(self, dfo_in, dfd_in): #, kws = [i['kw'] for i in Studies().kw('desc')]):
        # search DFs rows for kw in kws_desc, add rank to Drank
        df = self.getDrank(dfo_in, self.kw_desc)

        for kw in self.kw_desc:
            dfd_in.loc[(dfd_in.official_title.str.contains(kw, case=False, na=False, regex=False)) |
                         (dfd_in.description.str.contains(kw, case=False, na=False, regex=False)), 'Drank'] +=100

        return df, dfd_in

    def send_html(self, nct, ht): # rank, summary, head.body, contacts, sendto, html, new
        msg = MIMEMultipart("alternative")
        msg['Subject'] = f"{nct} from csv "
        msg['From'] = 'barker@bluewin.ch'
        msg['To'] = 'barker@bluewin.ch'
        msg['Cc'] = 'andrew.barker@medisante-group.com'

        plain_1 = f'freshly delivered!\n'
        plain_part = MIMEText(plain_1, 'plain')
        html_1 = ht
        html_part = MIMEText(html_1, 'html')
        msg.attach(html_part)
        msg.attach(plain_part)

        context = ssl.create_default_context()
        try:
            with SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login('AWSBarker', 'Ulusaba32')
                server.sendmail('barker@bluewin.ch', ['barker@bluewin.ch', 'andrew.barker@medisante-group.com'], msg.as_string())

        except SMTPServerDisconnected as e:
            print(f'smpt error {e} with {self.nct_id}')
            self.logger(f"error smpt server {e}")

        except :
            self.logger("write error to emailed_studies file")
        
if __name__ == "__main__":
# test AB vs 2
    e = Enviro()
    s = Study('NCT05417490')   # table inherited from Env (s.table='CTgov1')
    s.test_send()


