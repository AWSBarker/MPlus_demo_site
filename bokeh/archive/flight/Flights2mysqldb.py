# replace cs with icao24 so that plane lookup is better
# store flight data to db
# run from cron 1x
import MySQLdb
import time
from opensky_api import OpenSkyApi
from pyproj import Transformer, CRS

class Update(object):
    """ # epsg.io WGS 84 (46.515229, 6.559460) - EPSG 3857 (730195.75, 5863302.96)    """
    d = 0.01# size of map 0.01 about 1° square
    def __init__(self, loc):
        self.loc=loc
        self.lat,self.lon = locations[self.loc]
        self.states = api.get_states(time_secs=0,bbox=(self.get_bbox())).states
        self.ss=[] # callsigns
        if len(self.states) == 0:
            logit('nothing found')
            print('nothing found')
        else:
            for s in self.states:
                if not s.on_ground:
                    self.east,self.north = self.lalo_en(s.latitude,s.longitude)
                    self.ss.append((s.time_position,s.callsign, s.icao24, self.east, self.north, s.baro_altitude))
                    #print('{} {},{}'.format(self.east, self.north, s.baro_altitude))

    def speed(self, v): # m/s to Km/hr 3600/1000
        return v * 3.6

    def get_yrange(self): # north   # x (mlon) tuple min,max = extent of the map blokeh plot
        east,north = self.lalo_en(self.lat, self.lon)
        return (north - north*Update.d , north + north*Update.d)

    def get_xrange(self): # east   # y (lat) tuple min,max = extent of the map blokeh plot
        east,north = self.lalo_en(self.lat, self.lon)
        return (east - east*Update.d , east + east*Update.d)

    def get_bbox(self):        # tuple (min latitude, max latitude, min longitude, max longitude)
        return (self.lat - 100*Update.d, self.lat + 100*Update.d, self.lon - 100*Update.d , self.lon + 100*Update.d)

    def lalo_en(self, lat,lon):
        east, north = trans.transform(lat,lon) # Lat, Lon -> X East, Y North
        return east,north

def logit(l):
    with open('flight2db.log', 'a') as f:
        t =time.ctime()
        f.write(f'{t} : wrote {l} flights'+'\n')

trans = Transformer.from_crs(4326, 3857)
api = OpenSkyApi(username='awsbarker', password=7914920)
locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.23631, 6.108055)}
location = 'HOM' # hom E761959.65058179 N5901752.6812215 gvs E690503.669441 N 5828877.71618
#row = Update(location)
#connection = MySQLdb.connect(host='192.168.1.173', port=3306, user=ŧ'pi', password='7914920', db='flights')
sql = """INSERT INTO 3d (ts, cs, icao24, lat, lon, alt) VALUES (%s, %s, %s, %s, %s, %s)"""
print('connected')
row = Update(location)
try:
    connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi', password='7914920', db='flights')
    cursor = connection.cursor()
    cursor.executemany(sql, row.ss)
    connection.commit()
    connection.close()
    logit(len(row.ss))
    #print(f'updated {len(row.ss)} records')
    #print(row.ss)
except MySQLdb.Error as e:
    logit(e)
    #print(e)
    pass



