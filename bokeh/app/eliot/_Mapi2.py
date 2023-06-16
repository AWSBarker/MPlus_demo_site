# Lightsail versions - ~/.bashrc has pathonpath
# test for run in 'srv02.hosttree.ch' using PyMySQL
# default orgdb is M+_orgs and M+_daily
# NOTE API calls are live status whereas sql are daily snapshots
# M+_daily can probably be reduced to weekly difference DB or
# TODO reference db_table with imei + count index for duplicates zero counts removal
# TODO remove all ref to GM - done
# run daily for all orgs in default
from datetime import datetime as dt, timedelta
import MySQLdb.cursors
import pymysql.cursors
import requests
import boto3
from botocore.config import Config
from pycognito.aws_srp import AWSSRP
import os
import pandas as pd

class LoginSession:
    """
    login to api M+ obtain token for GET / POST
    """
    def __init__(self,
                 headers = {'User_Agent' : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0", 'accept' : 'application/json'},
                 my_config = Config(region_name = 'eu-central-1',  signature_version = 'v4',   retries = {'max_attempts': 10, 'mode': 'standard'}),
                 pool_id = 'eu-central-1_P8l0OEy9K',  client_id = '7g6sangq93eruio247tlsp883n',
                 user = 'andrew.barker@medisante-group.com', passw = 'Ulusaba@128',
                 url = 'https://api-docs.medisante.net/login',
                 ):

        self.headers = headers
        self.my_config = my_config
        self.pool_id = pool_id
        self.client_id = client_id
        self.user = user
        self.passw = passw
        self.url = url
        self.client = boto3.client('cognito-idp', region_name='eu-central-1', config=self.my_config)
        try:
            self.aws = AWSSRP(username = self.user, password = self.passw, pool_id = self.pool_id, client_id = self.client_id, client = self.client)
        except self.client.exceptions.NotAuthorizedException as e:
            print(f"error: {e}")

        self.auth_init = self.client.initiate_auth(AuthFlow='USER_SRP_AUTH', AuthParameters=self.aws.get_auth_params(), ClientId=self.client_id)
        self.c_resp = self.aws.process_challenge(self.auth_init['ChallengeParameters'])
        self.response = self.client.respond_to_auth_challenge(ClientId=self.client_id, ChallengeName=self.auth_init['ChallengeName'], ChallengeResponses=self.c_resp)
        self.token = self.response['AuthenticationResult']['IdToken']
        self.headers['Authorization'] = 'Bearer '+self.token

class Mapi():
    """
    Create M+ api calls.
    - devices without any measures (no token) do not have 'lastMeasurementAt'
    - default Org list is M+_orgs
    """
    orgdb = 'M+_orgs'
    dailydb = 'M+_daily' #'M_daily1' #'M+_daily'

    def __init__(self):
        self.headers = LoginSession().headers

    @classmethod
    def test(cls):
        print(f'default db for orgs is {cls.orgdb} table for daily updateis {cls.dailydb}')

    @classmethod
    def getOrgs(cls):
        sql= f"SELECT org_id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return [d.get('org_id', None) for d in a]

    @classmethod
    def getOrg_ID_dict(cls): # { org_id : id, }
        sql= f"SELECT org_id, id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return {list(d.values())[0]: list(d.values())[1] for d in a}

    @classmethod
    def getOrg_ID_name_dict(cls): # { org_id : id, id : name, name : org_id}
        sql= f"SELECT org_id, id, name FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        d1 = {list(d.values())[0]: list(d.values())[1] for d in a}
        d2 = {list(d.values())[1]: list(d.values())[2] for d in a}
        d3 = {list(d.values())[2]: list(d.values())[0] for d in a}
        return {**d1, **d2, **d3}

    @classmethod
    def getOrg_Name_dict(cls): # { id : name, }
        sql= f"SELECT id, name FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return {list(d.values())[0]: list(d.values())[1] for d in a}

    @classmethod
    def getName_id_dict(cls): # { name : id, }
        sql= f"SELECT id, name FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return {list(d.values())[1]: list(d.values())[0] for d in a}

    def getjson_as_dict(self):
        try:
            return requests.get(self.url, headers = self.headers).json()
        except requests.exceptions.Timeout:
            return "timeout set up for a retry, or continue in a retry loop"
        except requests.exceptions.TooManyRedirects:
            return "Tell the user their URL was bad and try a different one"
        except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
            raise SystemExit(e)

class Org(Mapi):
    """
    Single Organisation actions default = M+_orgs defined by rep
    getdev_eq_0 returns all zero measure devices
    Self.url adjusted in each submodule call
    """

    def __init__(self, orgid = None): #'6e66e7c3-f425-4a56-84a4-cb425859b7fc'):
        super().__init__()
        #self.orgdb = orgdb
        if orgid is None:
            self.org_id = self.getOrgs()[0]
        else:
            self.org_id = orgid
        self.url = f"https://api.medisante.net/v1/orgs/{self.org_id}"
        dev = super().getjson_as_dict()
        for key in dev:
            setattr(self, key, dev[key])

    def test(self): # check db, connection
        self.org_id = '6e66e7c3-f425-4a56-84a4-cb425859b7fc'
        print(self.getalldevs())

    def getID(self): # from org_id get id from `{self.orgdb}`
        self.sql = f"SELECT id FROM `{self.orgdb}` WHERE org_id = '{self.org_id}'"
        with DBconx() as q:
            return q.query(self.sql)[0].get('id', None)

    def alldev_as_LoT(self, LoD): # a is list of dicts from getjson_as_dict. need LoT for DB incl owner == org_id == self.id
        """
            - devices without any measures (no token) do not have 'lastMeasurementAt'
            - Timestamp without tz 'Z' for SQL
        """
        self.LoD = LoD
        self.co = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        self.LoT = [(i['imei'], self.co, pd.to_datetime(i['lastMeasurementAt']).replace(tzinfo=None), i['measurementCount'], self.id) for i in self.LoD if i['measurementCount'] > 0]
        self.LoT.extend([(i['imei'],self.co, None,i['measurementCount'],self.id) for i in self.LoD if i['measurementCount'] == 0])
        return self.LoT

    def dailyupdate(self):#, db_table='M+_daily'):
        # update db with all devices in org
        self.id = self.getID()
        self.asql= f"INSERT INTO `{self.dailydb}` (imei, checked_on, last_measure_at, count,org) VALUES(%s,%s,%s,%s,%s)" \
                   f"ON DUPLICATE KEY UPDATE " \
                   f"imei = VALUES(imei), checked_on = VALUES(checked_on), last_measure_at = VALUES(last_measure_at), count = VALUES(count), org=VALUES(org)"
        with DBconx() as w:
            self.b=self.alldev_as_LoT(self.getalldevs())
            w.writemany(self.asql,self.b)

    def getalldevs(self):
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}"
        return self.getjson_as_dict()

    def getdev_eq_x(self, x = 0): # all devs in org with measures at 0 NOT = getaldevs
        self.x = x
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementEquals={self.x}"
        return self.getjson_as_dict()
#BUG
    def getdev_gt_eq_1(self): # all devs in org with measures >=  NOT WORKING
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementGreaterThan=1&measurementEquals=1"
        return self.getjson_as_dict()

    def getdev_gt_1(self): # all devs in org with measures > 1
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementGreaterThan=1"
        return self.getjson_as_dict()

    def getdev_gt_x(self, x=0): # all devs in org with measures > 1
        self.x = x
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementGreaterThan={self.x}"
        return self.getjson_as_dict()

    def getdev_lt_1(self): # all devs in org with measures < 1. Doesn't matter this month or not if total is 0
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementLessThan=1"
        return self.getjson_as_dict()

    def getdev_gt_1_since(self, s = dt(2021,10,1,0).date()): # all devs in org with measures > 0 since >= datetime end of current month
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementGreaterThan=1"
        self.st = s
        self.a = self.getjson_as_dict()
        self.ListOfTuples = [(i['imei'],i['lastMeasurementAt'],i['measurementCount'])
                for i in self.a if dt.strptime(i['lastMeasurementAt'], format('%Y-%m-%dT%H:%M:%SZ')).date() >= self.st]
        return self.ListOfTuples

    def getdev_gt_0_this_mon(self, s = dt.today().date().replace(day=1)): # all devs in org with measures > 0 during current month
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementGreaterThan=0"
        self.st = s
        self.a = self.getjson_as_dict()
        self.ListOfTuples = [(i['imei'],i['lastMeasurementAt'],i['measurementCount'])
                for i in self.a if dt.strptime(i['lastMeasurementAt'], format('%Y-%m-%dT%H:%M:%SZ')).date() >= self.st]
        return self.ListOfTuples
# not working after month end
    def getdev_gt_0_in_mon(self, s = dt.today().date().replace(day=1)): # all devs in org with measures > 0 during current month
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}&measurementGreaterThan=0"
        self.st = s # first month
        self.end = (self.st + timedelta(days=32)).replace(day=1) # start 1st of next month!
        self.a = self.getjson_as_dict()
        self.ListOfTuples = [(i['imei'],i['lastMeasurementAt'],i['measurementCount'])
                for i in self.a if dt.strptime(i['lastMeasurementAt'], format('%Y-%m-%dT%H:%M:%SZ')).date() >= self.st and
                             dt.strptime(i['lastMeasurementAt'], format('%Y-%m-%dT%H:%M:%SZ')).date() < self.end]
        return self.ListOfTuples

class Dev(Mapi):
    """
    Single Device actions
    TODO : chenge DB table to M+_dev_own
    """

    def __init__(self, imei = '354033090698260'):
        super().__init__()
        self.imei = imei
        self.url  = f"https://api.medisante.net/v1/devices/{self.imei}"
        dev = super().getjson_as_dict()
        for key in dev:
            print(f'key is {key}')
            setattr(self, key, dev[key])

    @staticmethod
    def update_owner(LoT):
        # update owner in GM_dev_own LoT [(imei, own), ]
        asql= f"INSERT INTO M+_dev_own (imei, owner) VALUES(%s,%s) " \
                   f"ON DUPLICATE KEY UPDATE imei=VALUES(imei), owner=VALUES(owner)"
        with DBconx() as d:
            d.writemany(asql, LoT)

    @staticmethod
    def getdev_owner():
        asql = f"SELECT * FROM `M+_dev_own`"
        with DBconx() as d:
            a = d.query(asql)
        return {list(d.values())[0]: list(d.values())[1] for d in a}

    @staticmethod
    def getIMEIfromSNlist(dlistfile='/home/ab/ownCloud/Medisanté/Stark2/ticket398IMEI'):
        with open(dlistfile , 'r') as f:
            dlist =f.readlines()
        for d in dlist:
            do = Dev(int(d.strip()))
            with open(dlistfile, 'a') as f:
                f.write(do.serialNumber)
                f.write(',')
                f.write(do.imei)
                f.write('\n')
        return f'list written to {dlistfile}'


class DBconx():
    """
    Connect to db with sql and use "with DBconx as dbc:+
    crontab from medisan1 seems to have cwd = /home/medisan1
    pymssql required to avoid TypeError: Only valid with DatetimeIndex, TimedeltaIndex or PeriodIndex, but got an instance of 'Float64Index'
    BUT MySQLdb not available on P2.7 M+ CPanel server
    """
    loc_dict = {'VESA-ubuntu' : ("/mnt/bitnami/home/bitnami/sharepoint/data", "3.72.229.78", 3306, "bitnami", "bitnami", "Health"), #("/media/Data2/ownCloud/PycharmProjects/sharepoint/data", '192.168.1.173', 3306, "pi", "7914920", "Health"),
                'awsb.ddns.net' : ("/home/ab/ownCloud/PycharmProjects/sharepoint/data", '192.168.1.173', 3306, "pi", "7914920", "Health"),
                'ip-172-26-13-81' : ("/home/bitnami/sharepoint/data", "127.0.0.1", 3306, "bitnami", "bitnami", "Health"),
                'other' : ("/home/Data2/ownCloud/PycharmProjects/sharepoint/data", 'awsbarker.ddns.net', 3306, "pi", "7914920", "Health"),
                }
    hostpc = os.uname()[1]
    data_folder, h, p, u, pw, d = loc_dict.get(hostpc,
    ("/home/bitnami/sharepoint/data", "3.72.229.78", 3306, "bitnami", "bitnami", "Health")) #"/media/Data2/ownCloud/PycharmProjects/sharepoint/data"

    def __init__(self, MorP='M'): # M is dicts, else T tuples - for datetime64?!
        print(self.hostpc, self.h)
        self.MorP = MorP
        try :
            if self.MorP == 'M':
                self.condb = MySQLdb.connect(host=DBconx.h, port=DBconx.p, user=DBconx.u, password=DBconx.pw, db=DBconx.d, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor) # DictCursor, SSCursor
            else: # called from m1,2,3 no dic
                #self.condb = pymysql.connect(host=DBconx.h, port=DBconx.p, user=DBconx.u, password=DBconx.pw, db=DBconx.d, charset='utf8') #, cursorclass=pymysql.cursors.DictCursor)
                self.condb = MySQLdb.connect(host=DBconx.h, port=DBconx.p, user=DBconx.u, password=DBconx.pw, db=DBconx.d, charset='utf8') #, cursorclass=pymysql.cursors.DictCursor)
            self.condb.autocommit(True)
            #self.cur = self.condb.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            self.cur = self.condb.cursor()
        except :
            print('error')

    def query(self, sql):
        self.sql = sql
        try:
            self.cur.execute(self.sql)
            return self.cur.fetchall()
        except:
            self.addlog(' * some db error *')

    def writemany(self, asql, LoT):
        self.ListOfTuples = LoT
        self.asql= asql
        try:
            self.a = self.cur.executemany(self.asql, self.ListOfTuples)
            self.addlog(f' * write executed on {str(self.a)} *')
#            self.logit(self.ListOfTuples)
        except pymysql.ProgrammingError as e:
            self.addlog(f' * some prog error : {e}')
        except pymysql.OperationalError as e:
            self.addlog(f' * some op error : {e}')
        except pymysql.InternalError as e:
            self.addlog(f' * some int error : {e}')
        except pymysql.DatabaseError as e:
            self.addlog(f' * some db error : {e}')
        except pymysql.MySQLError as e:
            self.addlog(f' * some mysql error : {e}')
        except pymysql.DataError as e:
            self.addlog(f' * some data error : {e}')
        except MySQLdb.OperationalError as e:
            self.addlog(f' * some mysqldb error : {e}')
#        MySQLdb._exceptions.OperationalError: (1366, "Incorrect integer value: '-' for column `Health`.`M+_daily`.`imei` at row 4")
        finally:
            self.logit(self.ListOfTuples)

    def execute(self, asql):
            self.asql= asql
            try:
                self.a = self.cur.execute(self.asql)
                self.addlog(f' * executed on {self.a} *')
            except:
                self.addlog(' * some db error *')

    def backup(self, table):
        self.table = table
        self.bk = f"{DBconx.data_folder}/{self.table}_bk_{dt.now().strftime('%Y%m%d')}.pkl.bz2"
        self.df = pd.DataFrame(self.query(f"SELECT * FROM `{self.table}`")).set_index('id')
        self.df.to_pickle(self.bk)

    def logit(self, LoT = []):
        #cwd = os.getcwd()
        self.LoT = LoT
        l = f"{DBconx.data_folder}/M+_daily_{dt.now().strftime('%Y%m%d')}.log"
        if os.path.exists(l):
            with open(l, 'a') as f:
                f.write(f" {dt.now().strftime('%X')} data saved to DB {DBconx.data_folder} by {DBconx.h}\n")
                if len(LoT) > 0:
                    f.writelines(f"{ln}\n" for ln in self.LoT)

    def addlog(self, t): # add to above log
        self.t = t
        l = f"{DBconx.data_folder}/M+_daily_{dt.now().strftime('%Y%m%d')}.log"
        if os.path.exists(l):
            with open(l, 'a') as f:
                f.write(self.t)
        else:
            with open(l, 'w') as f:
                f.write(self.t)

    def __enter__(self):
        print(f'DBconx open from {os.uname()[1]}')
        self.logit()
        return self

    def __exit__(self, type, value,traceback):
        print('DBconx closed')
        self.addlog(f" ++++  db connec closed {dt.now().strftime('%X')}\n")
        self.cur.close()
        self.condb.close()

if __name__ == "__main__":

    orgs = Org.getOrgs()
    for o in orgs:
       Org(o).dailyupdate()

    '''
    d = Dev()
    o = Org().test()
    t = Mapi().test()
    
    orgdict = Mapi().getOrg_Name_dict()

    devdets = Dev().getjson_as_dict()
    devown = Dev().getdev_owner() # default owner
    print(devown)
    exit()

    orgs = Org.getOrgs()
    for o in orgs:
       Org(o).dailyupdate()

   def report_xl(self, mon):

        self.cols = ['imei', 'lastMeasurementAt', 'measurementCount']
        self.filename = f"{DBconx.data_folder}/Mhub_Status{dt.now().strftime('%Y%m%d')}.xlsx"
        self.sheetname = f"{self.name}_{dt.now().strftime('%d%b%y')}"
        self.df0 =pd.DataFrame(self.getdev_lt_1(), columns=self.cols)
        self.df0.set_index('imei', inplace=True)
        self.df = pd.DataFrame(self.getdev_gt_0_this_mon(), columns=self.cols)
        self.df.set_index('imei', inplace=True)
        self.dfa = pd.DataFrame(self.getalldevs(), columns=self.cols)
        self.df1 = pd.concat([self.df, self.df0])
        if os.path.exists(self.filename):
            with pd.ExcelWriter(self.filename, mode = 'a', if_sheet_exists='replace') as w:
                self.df1.to_excel(w,sheet_name=self.sheetname)
        else:
            with pd.ExcelWriter(self.filename, mode = 'w') as w:
                self.df1.to_excel(w,sheet_name=self.sheetname)
        return (self.filename, self.name, len(self.dfa), len(self.df0), len(self.df))

'''
