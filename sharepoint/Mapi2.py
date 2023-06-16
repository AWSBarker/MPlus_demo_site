# Lightsail versions (sp) - Health & ~/.bashrc has pathonpath
# added accounts_user lookup in Mapi
# added for m1/m2/m3    @classmethod def getOrgs_showas_dict(cls): #{id:showas}
# default orgdb is M+_orgs and M+_daily
# NOTE API calls are live status whereas sql are daily snapshots
# M+_daily can probably be reduced to weekly difference DB or
# run daily for all orgs in default
from datetime import datetime as dt, timedelta
import MySQLdb.cursors
from MySQLdb import _mysql #fastest
from MySQLdb.constants import FIELD_TYPE
import requests
import boto3
from botocore.config import Config
from pycognito.aws_srp import AWSSRP
import os
import pandas as pd
from decouple import AutoConfig, Csv
import gzip

class LoginSession:
    """
    login to api M+ obtain token for GET / POST.
    .ENV FILE REQUIRED in '/home/bitnami/sharepoint'
        if os.uname()[1] =='VESA-ubuntu':
        config = AutoConfig(search_path = '/home/ab/sharepoint')
        print(f'config search {os.uname()[1]} ')
    # elif os.uname()[1] =='ThinkPad-E470':
    #     config = AutoConfig(search_path = '/home/andrew/ownCloud/PycharmProjects/sharepoint')
    #     print(f'config search {os.uname()[1]} ')
    else:
        config = AutoConfig(search_path = '/home/bitnami/sharepoint')
        print(f'config search bitnamo')

    """
    lookup_config = {'VESA-ubuntu' : '/media/Data2/ownCloud/PycharmProjects/sharepoint',
                     'ThinkPad-E470' : '/home/andrew/ownCloud/PycharmProjects/sharepoint',
                     'awsbarker.ddns.net': '/home/pi/ownCloud/PycharmProjects/sharepoint',
                     'awsb.ddns.net': '/home/ab/ownCloud/PycharmProjects/sharepoint',
                     'mx21': '/home/ab/ownCloud/PycharmProjects/sharepoint',
                     }
    config = AutoConfig(search_path = lookup_config.get(os.uname()[1], '/home/bitnami/sharepoint'))

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
        return f'default db for orgs is {cls.orgdb} table for daily updateis {cls.dailydb}'

    @classmethod
    def getOrgs(cls):
        sql= f"SELECT org_id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql) # tuple of dicts
        return [d.get('org_id', None) for d in a]

    @classmethod
    def getOrgsids(cls):
        sql= f"SELECT id FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return [d.get('id', None) for d in a]

    @classmethod
    def getGMOrgs(cls):
        sql= f"SELECT org_id FROM `{cls.orgdb}` WHERE name LIKE 'GM_%' AND org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return [d.get('org_id', None) for d in a]

    @classmethod
    def getGMOrgids(cls):
        sql= f"SELECT id FROM `{cls.orgdb}` WHERE name LIKE 'GM_%' AND org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        return [d.get('id', None) for d in a]

    @classmethod
    def getOrgs_showas_dict(cls, inv = False): #{id:showas}
        sql= f"SELECT id, showas FROM `{cls.orgdb}` WHERE org_id IS NOT NULL"
        with DBconx() as dbc:
            a = dbc.query(sql)
        if inv:
            return {list(d.values())[1]: list(d.values())[0] for d in a}
        else:
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
            self.org_id = self.getOrgs()[0]  #'6e66e7c3-f425-4a56-84a4-cb425859b7fc'  !!
        else:
            self.org_id = orgid
        self.url = f"https://api.medisante.net/v1/orgs/{self.org_id}"
        dev = super().getjson_as_dict()
        for key in dev:
            setattr(self, key, dev[key])

    #@staticmethod
    def get_all_orgs(self): # get all orgs /orgs
        self.url = f"https://api.medisante.net/v1/orgs"
        return {d['id']:d['name'] for d in super().getjson_as_dict()}

    def get_org_name(self):
        return self.name

    def get_org_details(self):
        return self.__dict__

    def getID(self): # from org_id get id from `{self.orgdb}`
        self.sql = f"SELECT id FROM `{self.orgdb}` WHERE org_id = '{self.org_id}'"
        with DBconx() as q:
            return q.query(self.sql)[0].get('id', None)

    def alldev_as_LoT(self, LoD): # a is list of dicts from getjson_as_dict. need LoT for DB incl owner == org_id == self.id
        """
            - devices without any measures (no token) do not have 'lastMeasurementAt'
            - GW BT devices have no IMEI, need to filter (could capture mac as int by int(d['mac'].translate({58: None}), 16))
            - Timestamp without tz 'Z' for SQL
        """
        self.LoD = LoD
        self.co = dt.now().strftime('%Y-%m-%d %H:%M:%S')

# don't capture BT devices that have imei = '-'
        self.LoT = [(i['imei'], self.co, pd.to_datetime(i['lastMeasurementAt']).replace(tzinfo=None), i['measurementCount'], self.id) for i in self.LoD if (i['measurementCount'] > 0) & (i['imei'] !='-')]
        self.LoT.extend([(i['imei'],self.co, None,i['measurementCount'],self.id) for i in self.LoD if (i['measurementCount'] == 0) & (i['imei'] !='-')])
#
#        self.LoT = [(i['imei'], self.co, pd.to_datetime(i['lastMeasurementAt']).replace(tzinfo=None), i['measurementCount'], self.id) for i in self.LoD if i['measurementCount'] > 0]
#        self.LoT.extend([(i['imei'],self.co, None,i['measurementCount'],self.id) for i in self.LoD if i['measurementCount'] == 0])

        return self.LoT

    def dailyupdate(self):#, db_table='M+_daily'):
        # update db with all devices in org - every 15m crontab
        self.id = self.getID()
        self.asql= f"INSERT INTO `{self.dailydb}` (imei, checked_on, last_measure_at, count,org) VALUES(%s,%s,%s,%s,%s)" \
                   f"ON DUPLICATE KEY UPDATE " \
                   f"imei = VALUES(imei), checked_on = VALUES(checked_on), last_measure_at = VALUES(last_measure_at), count = VALUES(count), org=VALUES(org)"
        with DBconx() as w:
            self.b=self.alldev_as_LoT(self.getalldevs())
            w.writemany(self.asql,self.b)

    def get_org_fleet(self):
        # list of unique imei in org, some GW has IMEI '-'
        self.url = f"https://api.medisante.net/v1/orgs/devices?orgs={self.org_id}"
        return [int(i['imei']) for i in self.getjson_as_dict() if i['imei'] != '-' ] # some GW has IMEI '-'

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
    Update_owner - TODO compare and confirm
    """

    def __init__(self, imei = '354033090698260'):
        super().__init__()
        self.imei = imei
        self.url  = f"https://api.medisante.net/v1/devices/{self.imei}"
        dev = super().getjson_as_dict()
        for key in dev:
            setattr(self, key, dev[key])

    def get_device_details(self):
        d = self.__dict__
        d.pop('headers', 'None')
        return d

    @staticmethod
    def update_owner(LoT): # [(imei, 2),]
        # delete IMEI that have moved outside tracked orgs
        LoI = tuple(i[0] for i in LoT)
        ownedby = LoT[0][1]

        # update owner on LoT [(imei, 5), ]
        asql= f"INSERT INTO `M+_dev_own` (imei, owner) VALUES(%s,%s) " \
                   f"ON DUPLICATE KEY UPDATE imei=VALUES(imei), owner=VALUES(owner)"

        # delete stale owners
        dsql =f"DELETE FROM `M+_dev_own` WHERE imei NOT IN {LoI} AND owner = {ownedby}"

        with DBconx() as d:
            d.writemany(asql, LoT)
            d.execute(dsql)

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
    - try _mysql as 'U'
    !!! Use in 'with' statement to get open/close entry/exit !!!
    Production (bitnami, bitnami/Health)  |  Development (VESA, bitnami/Health_test) selection
    Mysql connector only (pymsql not needed, slowe) to give tuples or dicts
    """
    loc_dict = {'VESA-ubuntu' : LoginSession.config("VESA", cast=Csv(post_process=tuple)), #/mnt/bitnami/home/bitnami/sharepoint/data", "3.72.229.78", 3306, "bitnami", "bitnami", "Health"), #("/media/Data2/ownCloud/PycharmProjects/sharepoint/data", '192.168.1.173', 3306, "pi", "7914920", "Health"),
                'ip-172-26-13-81' : LoginSession.config("BITNAMI", cast=Csv(post_process=tuple)),
                'ip-172-26-14-9' : LoginSession.config("DEFAULT", cast=Csv(post_process=tuple)),
                'ThinkPad-E470': LoginSession.config("THINKPAD", cast=Csv(post_process=tuple)),
                'awsbarker.ddns.net': LoginSession.config("AWSB", cast=Csv(post_process=tuple)),
                'awsb.ddns.net': LoginSession.config("AWSB", cast=Csv(post_process=tuple)),
                }
    hostpc = os.uname()[1]
    data_folder, h, p, u, pw, d = loc_dict.get(hostpc, LoginSession.config("DEFAULT", cast=Csv(post_process=tuple)))

    def __init__(self, MorP='M'): # M is dicts, else T tuples - for datetime64?!
        print(f'Mapi2 DBconx oC : {self.hostpc}, {self.h}')
        self.MorP = MorP
        try :
            if self.MorP == 'M':
                self.condb = MySQLdb.connect(host=self.h, port=int(self.p), user=self.u, password=self.pw, db=self.d, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor) # DictCursor, SSCursor
            elif self.MorP == 'U':
                # mei, checked_on, last_measure_at, count, org
                import numpy as np
                conv = {FIELD_TYPE.LONG: np.uint64, FIELD_TYPE.DATETIME: object, FIELD_TYPE.DATETIME: str, FIELD_TYPE.LONG: int, FIELD_TYPE.SHORT: int }
                self.con = _mysql.connect(host=self.h, port=int(self.p), user=self.u, passwd=self.pw, db=self.d, conv=conv)#, charset='utf-8') #, cursorclass=MySQLdb.cursors.DictCursor) # DictCursor, SSCursor
            else: # called from m1,2,3 no dic
                self.condb = MySQLdb.connect(host=self.h, port=int(self.p), user=self.u, password=self.pw, db=self.d, charset='utf8') #, cursorclass=pymysql.cursors.DictCursor)
            self.condb.autocommit(True)
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

    def uquery(self, sql): # for 'U' _msql
        self.con.query(sql)
        return self.con.store_result().fetch_row(maxrows=0)

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
        self.bk = f"{self.data_folder}/{self.table}_bk_{dt.now().strftime('%Y%m%d')}.pkl.bz2"
        self.df = pd.DataFrame(self.query(f"SELECT * FROM `{self.table}`")) #.set_index('id')
        self.df.to_pickle(self.bk)

    def logit(self, LoT = []):
        # not always M+daily, also CTgov
        self.LoT = LoT
        l = f"{self.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log.gz"
        mode = 'at' if os.path.exists(l) else 'wt'
        try:
            with gzip.open(l, mode) as f:
                f.write(f" {dt.now().strftime('%X')} data saved to DB\n")
                if len(LoT) > 0:
                    f.writelines(f"{ln}\n" for ln in self.LoT)
        except:
            pass

    def addlog(self, t): # add to above log
        self.t = t
        l = f"{self.data_folder}/DB_{dt.now().strftime('%Y%m%d')}.log.gz"
        mode = 'at' if os.path.exists(l) else 'wt'
        try:
            with gzip.open(l, mode, errors=None) as f:
                f.write(self.t)
        except:
            pass

    def __enter__(self):
        self.logit()
        return self

    def __exit__(self, type, value,traceback):
        self.addlog(f" ++++  DBconx closed {dt.now().strftime('%X')}\n")
        self.cur.close()
        self.condb.close()

if __name__ == "__main__":

# Eumaco '872b62b0-9527-4e1b-9822-bdeb48c5ac27'
    eu = Org('872b62b0-9527-4e1b-9822-bdeb48c5ac27').getalldevs()
    euLoT = Org('872b62b0-9527-4e1b-9822-bdeb48c5ac27').alldev_as_LoT(eu)
    for d in eu:
        try :
            if d['gatewayID']:
                print(d['mac'], int(d['mac'].translate({58: None}), 16))
        except:
            pass


    oi = Org.getOrgs()
    for i in oi:  # need OrgID for Org() getOrg_ID_name_dict
        l = Org(i).get_org_fleet()
        d = Org(i).getalldevs()

    import time
    st = time.time()
    u = DBconx('U').uquery("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")
    print(f"U took {time.time() - st}s")
    st = time.time()

    c = DBconx().query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")
    print(f"normal took {time.time() - st}s")


    '''
    s2 = Dev(358173054439511)
    o2 = Org.getName_id_dict()
    sa2 = Org.getOrgs_showas_dict(True)
    s2 = Dev(354033090707343)
    d = Dev().get_device_details()

   s1 = Dev()
    s2 = Dev(359804081343156)
    d = Dev().get_device_details()
    odict = Org.getOrg_ID_name_dict()
    gmp_org = odict['GM_POL']
    gmp_devs = Org(gmp_org).getalldevs()
    print(len(gmp_devs))

    db_table = "M+_daily"
    orgDict = Org.getOrg_Name_dict() # {ID : NAME, }
    gm_orgs = tuple([k for k,v in orgDict.items() if v.startswith('GM')])
    tuptup = DBconx('T').query(f"SELECT imei, checked_on, last_measure_at, count, org from `{db_table}` WHERE org IN {gm_orgs}")

    ot = Org().test()
    orgs = Org.getOrgs()
    for o in orgs:
       Org(o).dailyupdate()

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
