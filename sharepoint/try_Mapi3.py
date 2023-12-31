# try token for session
# Lightsail versions - Health & ~/.bashrc has pathonpath
# added for m1/m2/m3    @classmethod def getOrgs_showas_dict(cls): #{id:showas}
# default orgdb is M+_orgs and M+_daily
# NOTE API calls are live status whereas sql are daily snapshots
# M+_daily can probably be reduced to weekly difference DB or
# run daily for all orgs in default
from datetime import datetime as dt, timedelta
import MySQLdb.cursors
import requests
import boto3
from botocore.config import Config
from pycognito.aws_srp import AWSSRP
import os
import pandas as pd
from decouple import AutoConfig, Csv

class LoginSession:
    """
    login to api M+ obtain token for GET / POST.
    .ENV FILE REQUIRED in '/home/bitnami/sharepoint'
    """
    if os.uname()[1] =='VESA-ubuntu':
        config = AutoConfig(search_path = '/home/ab/sharepoint')
        print(f'config search {os.uname()[1]} ')
    elif os.uname()[1] =='ThinkPad-E470':
        config = AutoConfig(search_path = '/home/andrew/ownCloud/PycharmProjects/sharepoint')
        print(f'config search {os.uname()[1]} ')
    else:
        config = AutoConfig(search_path = '/home/bitnami/sharepoint')
        print(f'config search bitnamo')

    def __init__(self,
                 headers = {'User_Agent' : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0", 'accept' : 'application/json'},
                 my_config = Config(region_name = 'eu-central-1',  signature_version = 'v4',   retries = {'max_attempts': 10, 'mode': 'standard'}),
                 pool_id = config('POOL_ID'),  client_id = config('CLIENT_ID'),
                 user = config('AUSER'), passw = config('PASSWD'),
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

class Mapi(LoginSession):
    """
    Create M+ api calls.
    - devices without any measures (no token) do not have 'lastMeasurementAt'
    - default Org list is M+_orgs
    """
    orgdb = 'M+_orgs'
    dailydb = 'M+_daily' #'M_daily1' #'M+_daily'

#    def __init__(self):
#        super().__init__()
        #self.headers = LoginSession().headers
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

    @classmethod
    def test(cls):
        print(f'default db for orgs is {cls.orgdb} table for daily updateis {cls.dailydb}')
        return f'default db for orgs is {cls.orgdb} table for daily updateis {cls.dailydb}'

    @classmethod
    def getOrgs(cls):
        sql= f"SELECT org_id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return [d.get('org_id', None) for d in a]

    @classmethod
    def getOrgs_showas_dict(cls): #{id:showas}
        sql= f"SELECT id, showas FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return {list(d.values())[0]: list(d.values())[1] for d in a}

    @classmethod
    def getOrg_ID_dict(cls): # { org_id : id, }
        sql= f"SELECT org_id, id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return {list(d.values())[0]: list(d.values())[1] for d in a}

    @classmethod
    def getID_Org_id_dict(cls): # { id : org_id , }
        sql= f"SELECT id, org_id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
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


class Org(Mapi):
    """
    Single Organisation actions default = M+_orgs defined by rep
    getdev_eq_0 returns all zero measure devices
    Self.url adjusted in each submodule call
    """
    # def __init__(self,  orgid = None):
    #     super().__init__()

    def get_org_details(self, orgid = None): #'6e66e7c3-f425-4a56-84a4-cb425859b7fc'):
        if orgid is None:
            self.org_id = self.getOrgs()[0]  #'6e66e7c3-f425-4a56-84a4-cb425859b7fc'  !!
        else:
            self.org_id = orgid
        self.url = f"https://api.medisante.net/v1/orgs/{self.org_id}"
        org = self.getjson_as_dict()
        for key in org:
            setattr(self, key, org[key])
        return org

    def test(self): # check db, connection
        #self.org_id = '6e66e7c3-f425-4a56-84a4-cb425859b7fc'
        adev = self.getalldevs()[0]
        print(adev)
        return adev

    def get_org_name(self):
        return self.name

    def _get_org_details(self):
        return self.__dict__

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

    def get_org_fleet_IMEI(self):
        # list of unique imei in org
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}"
        return [int(i['imei']) for i in self.getjson_as_dict()]

    def getalldev_details(self):
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
    Use self.headers
    Single Device actions
    """

    def get_device_details(self, imei = '354033090698260'):
        self.imei = imei
        self.url  = f"https://api.medisante.net/v1/devices/{self.imei}"
        dev = self.getjson_as_dict()
        for key in dev:
            setattr(self, key, dev[key])
        return dev

    def _get_device_details(self):
        d = self.__dict__
        d.pop('headers', 'None')
        return d

    @staticmethod
    def update_owner(LoT):
        # update owner on LoT [(imei, 5), ]
        asql= f"INSERT INTO `M+_dev_own` (imei, owner) VALUES(%s,%s) " \
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
    Production (bitnami, bitnami/Health)  |  Development (VESA, bitnami/Health_test) selection
    Mysql connector only (pymsql not needed, slowe) to give tuples or dicts
    Use in 'with' statement to get entry/exit
    """
    loc_dict = {'VESA-ubuntu' : LoginSession.config("VESA", cast=Csv(post_process=tuple)), #/mnt/bitnami/home/bitnami/sharepoint/data", "3.72.229.78", 3306, "bitnami", "bitnami", "Health"), #("/media/Data2/ownCloud/PycharmProjects/sharepoint/data", '192.168.1.173', 3306, "pi", "7914920", "Health"),
                'ip-172-26-13-81' : LoginSession.config("BITNAMI", cast=Csv(post_process=tuple)),
                }
    hostpc = os.uname()[1]
    data_folder, h, p, u, pw, d = loc_dict.get(hostpc, LoginSession.config("DEFAULT", cast=Csv(post_process=tuple)))

    def __init__(self, MorP='M'): # M is dicts, else T tuples - for datetime64?!
        print(self.hostpc, self.h)
        self.MorP = MorP
        try :
            if self.MorP == 'M':
                self.condb = MySQLdb.connect(host=DBconx.h, port=int(DBconx.p), user=DBconx.u, password=DBconx.pw, db=DBconx.d, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor) # DictCursor, SSCursor
            else: # called from m1,2,3 no dic
                #self.condb = pymysql.connect(host=DBconx.h, port=DBconx.p, user=DBconx.u, password=DBconx.pw, db=DBconx.d, charset='utf8') #, cursorclass=pymysql.cursors.DictCursor)
                self.condb = MySQLdb.connect(host=DBconx.h, port=int(DBconx.p), user=DBconx.u, password=DBconx.pw, db=DBconx.d, charset='utf8') #, cursorclass=pymysql.cursors.DictCursor)
            self.condb.autocommit(True)
            #self.cur = self.condb.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            self.cur = self.condb.cursor()
        except Exception as e:
            print(f'Some DB connect error, {e}')

    def query(self, sql):
        self.sql = sql
        try:
            self.cur.execute(self.sql)
            return self.cur.fetchall()
        except:
            self.addlog(' * some db error on query *')

    def writemany(self, asql, LoT):
        self.ListOfTuples = LoT
        self.asql= asql
        try:
            self.a = self.cur.executemany(self.asql, self.ListOfTuples)
            print(f' executemany done on {len(self.ListOfTuples)} records')
            self.addlog(f' * writemany executed on {str(self.a)} records *')
            self.logit(self.ListOfTuples)
            return self.a
        except MySQLdb.ProgrammingError as e:
            self.addlog(f' * some prog error : {e}')
        except MySQLdb.OperationalError as e:
            self.addlog(f' * some op error : {e}')
        except MySQLdb.InternalError as e:
            self.addlog(f' * some int error : {e}')
        except MySQLdb.DatabaseError as e:
            self.addlog(f' * some db error : {e}')
        except MySQLdb.MySQLError as e:
            self.addlog(f' * some mysql error : {e}')
        except MySQLdb.DataError as e:
            self.addlog(f' * some data error : {e}')
        except MySQLdb.OperationalError as e:
            self.addlog(f' * some mysqldb error : {e}')
#        MySQLdb._exceptions.OperationalError: (1366, "Incorrect integer value: '-' for column `Health`.`M+_daily`.`imei` at row 4")
        finally:
            self.addlog(" ### end of write many ###")

    def execute(self, asql):
            self.asql= asql
            try:
                self.a = self.cur.execute(self.asql)
                self.addlog(f' * {self.hostpc} {self.h} executed on {self.a} *')
            except:
                self.addlog(' * some db error *')

    def backup(self, table):
        self.table = table
        self.bk = f"{DBconx.data_folder}/{self.table}_bk_{dt.now().strftime('%Y%m%d')}.pkl.bz2"
        self.df = pd.DataFrame(self.query(f"SELECT * FROM `{self.table}`")) #.set_index('id')
        self.df.to_pickle(self.bk)

    def logit(self, LoT = []):
        # not always M+daily, also CTgov
        self.LoT = LoT
        l = f"{DBconx.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log"
        if os.path.exists(l):
            with open(l, 'a') as f:
                f.write(f" {dt.now().strftime('%X')} data saved to DB {self.d} from {self.hostpc} {self.h}\n")
                if len(LoT) > 0:
                    f.writelines(f"{ln}\n" for ln in self.LoT)

    def addlog(self, t): # add to above log
        self.t = t
        l = f"{DBconx.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log"
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
        self.addlog(f" ++++  DBconx closed {dt.now().strftime('%X')}\n")
        self.cur.close()
        self.condb.close()

if __name__ == "__main__":
# 1 aws session
    import time

    o1 = Org()
    odict = o1.getOrg_ID_name_dict()
    org = odict['GM_POL']
    o1_dets = o1.get_org_details()
    o1_devs = o1.get_org_fleet_IMEI()
    o1_devs1 = o1.getalldev_details()

    start = time.time()
    #with m1.headers as sh:
    aurl ='https://api.medisante.net/v1/devices/354033090698260'
    burl ='https://api.medisante.net/v1/devices/359804081343156'
    j0 = requests.get(aurl, headers=m1.headers).json()
    j1 = requests.get(burl, headers=m1.headers).json()
    print(f'time = {time.time()-start}')

#    o1 = Org('6e66e7c3-f425-4a56-84a4-cb425859b7fc')
    start = time.time()
    d = Dev()
    d1 = d.get_device_details()
    d2 = d.get_device_details(359804081343156)
    print(f'time = {time.time()-start}')

    s1 = Dev()
    s2 = Dev(359804081343156)
    d = Dev().get_device_details()

    print(len(gmp_devs))
