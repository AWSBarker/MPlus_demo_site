# v4 WORKING tr rollover and change data source
# v3 add auto step and server
# v2 add tooltips
from bokeh.plotting import figure, curdoc #show, output_file, save
from bokeh.tile_providers import get_provider, Vendors
from bokeh.models import ColumnDataSource, LabelSet
from opensky_api import OpenSkyApi
from pyproj import Transformer, CRS
from bokeh.driving import linear, repeat
import time

class ForbiddenTwoException(Exception):
    pass

class Update(object):
    """ # epsg.io WGS 84 (46.515229, 6.559460) - EPSG 3857 (730195.75, 5863302.96)
    get flight stats. bbox and extent are reversed lat/lon. d = scale factor
    USE PROJ FOR TUPLS GEODESIC TO CRS
    {   'baro_altitude': 10660.38,     'callsign': 'DLH02P  ',
    'geo_altitude': 11049,     'heading': 200.38,
    'icao24': '3c664e',    'last_contact': 1567365699,
    'latitude': 45.5358,    'longitude': 6.1761,
    'on_ground': False,    'origin_country': 'Germany',
    'position_source': 0,    'sensors': None,
    'spi': False,    'squawk': '2504',
    'time_position': 1567365699,    'velocity': 220.07,
    'vertical_rate': 0}
    """
    d = 0.01# size of map 0.01 about 1° square
    def __init__(self, loc):
        self.loc=loc
        self.lat,self.lon = locations[self.loc]
        self.states = api.get_states(time_secs=0,bbox=(self.get_bbox())).states
        if self.states == None:
            raise ForbiddenTwoException
        #else:
         #   print(self.states)

        self.ss=[] # callsigns
        self.sslat=[] # list of lats and longs
        self.sslon=[]
        self.ssorig=[]
        self.ssalt=[]
        self.ssair=[]
        self.ssv=[]
        self.ssvr=[]
        self.ssh=[]
        if len(self.states) == 0:
            print('nothing found')
        else:
            for s in self.states:
                #print('{} {},V{}m/s'.format(s.callsign,s.heading, s.velocity))
                self.east,self.north = self.lalo_en(s.latitude,s.longitude)
                #self.ss.update({s.callsign:[s.longitude, s.latitude, s.baro_altitude, s.velocity, s.callsign, s.origin_country]})
                self.ss.append(s.callsign)
                self.sslon.append(self.east)
                self.sslat.append(self.north)
                self.ssorig.append(s.origin_country)
                self.ssalt.append(s.baro_altitude)
                self.ssair.append(s.on_ground) # False = air
                self.ssvr.append(0) if s.on_ground == True else self.ssvr.append(s.vertical_rate)
                self.ssv.append(self.speed(s.velocity))
                self.ssh.append(s.heading)

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

@repeat(range(6))
def steps(value):
    # rolloverb= 6 x planes, rollover = planes
    try:
        b = Update(location)
        rob = len(b.ss) * 5
        print(f'step count {value}. tracker points= {rob}')
        bsource.stream({"lon": b.sslon, "lat": b.sslat}, rollover=rob)
        if value == 5:
            print(f'updated {len(b.ss)} planes ')
            source.stream({"lon": b.sslon, "lat": b.sslat, "cs": b.ss, "orig": b.ssorig,
                           "alt": b.ssalt, "air": b.ssair, "vel": b.ssv, "vr": b.ssvr, "head": b.ssh},
                          rollover=len(b.ss))
            bsource.stream({"lon": b.sslon, "lat": b.sslat}, rollover=len(b.ss))
    except Exception as e: #ForbiddenTwoException:
        print(f'ForbiddenTwoException : {e}')
        #print(f'states {b.ss}')
        pass

trans = Transformer.from_crs(4326, 3857)
api = OpenSkyApi(username='awsbarker', password=7914920)
locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.23631, 6.108055)}
location = 'HOM'
TOOLTIPS = [("Origin", "@orig"), ("Alt m", "@alt{0.0a}"),("Speed km/h", "@vel{0}"), ("Dir", "@head"), ("Climb m/s", "@vr")]

a = Update(location)
time.sleep(5)
tile_provider = get_provider(Vendors.CARTODBPOSITRON)  # STAMEN_TERRAIN)
source = ColumnDataSource(data=dict(lon=a.sslon, lat=a.sslat, cs=a.ss,orig=a.ssorig,
                                    alt=a.ssalt,air=a.ssair, vel=a.ssv, vr=a.ssvr, head=a.ssh))
p = figure(x_range=a.get_xrange(), y_range=a.get_yrange(), x_axis_type="mercator", y_axis_type="mercator", tooltips=TOOLTIPS)
p.add_tile(tile_provider)
labels1 = LabelSet(x='lon', y='lat', text='cs', x_offset=1, y_offset=1, source=source,text_font_size='8pt')
p.square(x=a.lalo_en(a.lat,a.lon)[0], y=a.lalo_en(a.lat,a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)
p.circle(x='lon', y='lat', size=10, fill_color="red", fill_alpha=0.8, source=source)
p.add_layout(labels1)

bsource = ColumnDataSource({"lon":[], "lat":[]})
p.circle(x='lon', y='lat', size=4, fill_color="red", fill_alpha=0.8, source=bsource)
curdoc().add_root(p)
curdoc().title=f"Flights Status {location}"
# Add a periodic callback to be run every 5001 milliseconds
curdoc().add_periodic_callback(steps, 5500)
