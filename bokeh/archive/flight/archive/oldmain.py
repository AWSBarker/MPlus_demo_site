# v7 with location selecctor
# v6 for RPI3 with pyproj=1.9 (Transfor and CRS not working)
# v5 with details from planes
# v4 WORKING tr rollover and change data source
# v3 add auto step and server
# v2 add tooltips
from bokeh.plotting import figure, curdoc #show, output_file, save
from bokeh.layouts import column
from bokeh.tile_providers import get_provider, Vendors
from bokeh.models import ColumnDataSource, LabelSet
from bokeh.models.widgets import Select
from opensky_api import OpenSkyApi
#from pyproj import Transformer, CRS
from functools import partial
from pyproj import Proj, transform
from bokeh.driving import linear, repeat
import time
from pandas.io.sql import read_sql_table
from sqlalchemy import create_engine

#trans = Transformer.from_crs(4326, 3857)
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
    d = 0.0075# size of map 0.01 about 1° square
    def __init__(self, planes, loc=None):
        self.loc='HOM' if loc is None else loc
        self.lat,self.lon = locations[self.loc]
        self.states = api.get_states(time_secs=0,bbox=(self.get_bbox())).states
        if self.states == None:
            raise ForbiddenTwoException
        #else:
         #   print(self.states)
        self.ss=[] # callsigns
        self.type = []  # plane details
        self.own = [] # owner
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
                #print(f'{self.loc}')
                self.east,self.north = self.lalo_en(s.latitude,s.longitude)
                #self.ss.update({s.callsign:[s.longitude, s.latitude, s.baro_altitude, s.velocity, s.callsign, s.origin_country]})
                self.ss.append(s.callsign)
                self.type.append(self.details(s.icao24)[0])
                self.own.append(self.details(s.icao24)[1])
                self.sslon.append(self.east)
                self.sslat.append(self.north)
                self.ssorig.append(s.origin_country)
                self.ssalt.append(s.baro_altitude)
                self.ssair.append(s.on_ground) # False = air
                self.ssvr.append(0) if s.on_ground == True else self.ssvr.append(s.vertical_rate)
                self.ssv.append(self.speed(s.velocity))
                self.ssh.append(s.heading)

    def details(self,cs): # from callsign get details from planes array
        a=planes.setdefault(cs,None) # 5 None?
        return [str(a[1]) + str(a[2]),a[3]] if a is not None else [None,None]

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
        #east, north = trans.transform(lat,lon) # Lat, Lon -> X East, Y North
        east1, north1 = trans1(lon,lat) # trans1 works lon.lat -> East North
        return east1,north1

def get_planes():
    cols = ['icao24', 'registration', 'manufacturername', 'model', 'owner', 'built']
    try:
        engine = create_engine('mysql+mysqldb://pi:7914920@192.168.1.173:3306/flights')
        con = engine.connect()
        df1 = read_sql_table("planes", con, index_col='icao24', columns=cols)
        con.close()
    except:
        print('error connecting database {}')
    return dict(zip(df1.index, df1.values))

@repeat(range(6))
def steps(value):
    # rolloverb= 6 x planes, rollover = planes
    try:
        b = Update(planes,location.value)
        rob = len(b.ss) * 5
        print(f'loc {location.value}, step count {value}. tracker points= {rob}, details {b.own}')
        bsource.stream({"lon": b.sslon, "lat": b.sslat}, rollover=rob)
        if value == 5:
            print(f'updated {len(b.ss)} planes ')
            source.stream({"lon": b.sslon, "lat": b.sslat, "cs": b.ss, "orig": b.ssorig, "type": b.type, "own": b.own,
                           "alt": b.ssalt, "air": b.ssair, "vel": b.ssv, "vr": b.ssvr, "head": b.ssh},
                          rollover=len(b.ss))
            bsource.stream({"lon": b.sslon, "lat": b.sslat}, rollover=len(b.ss))
    except Exception as e: #ForbiddenTwoException:
        print(f'ForbiddenTwoException : {e}')
        print(f'loc {location.value}')
        pass

def f_updatemap(attr, old, new):
    print(f'new location {location.value}')
    global p,source,bsource
    p, source, bsource, = init_plot()
    print(f"current fig {p} popping 0: {layout.children[0]} 1:{layout.children[1]}")
    layout.children.pop()
    new_layout = column(location,p)
    layout.children = new_layout.children
    print(f"current is {layout} {layout.children[0]} 1:{layout.children[1]}")
    print(f'curdoc size {len(curdoc()._all_former_model_ids)} {curdoc()._all_former_model_ids} ')


def init_plot():
    a = Update(planes, location.value)
    time.sleep(5)
    source = ColumnDataSource(data=dict(lon=a.sslon, lat=a.sslat, cs=a.ss, orig=a.ssorig, type=a.type, own=a.own,
                                        alt=a.ssalt, air=a.ssair, vel=a.ssv, vr=a.ssvr, head=a.ssh))
    p = figure(x_range=a.get_xrange(), y_range=a.get_yrange(), x_axis_type="mercator", y_axis_type="mercator",
               tooltips=TOOLTIPS)
    p.add_tile(tile_provider)
    labels1 = LabelSet(x='lon', y='lat', text='cs', x_offset=1, y_offset=1, source=source, text_font_size='8pt')
    p.square(x=a.lalo_en(a.lat, a.lon)[0], y=a.lalo_en(a.lat, a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)
    p.circle(x='lon', y='lat', size=10, fill_color="red", fill_alpha=0.8, source=source)
    p.add_layout(labels1)
    bsource = ColumnDataSource({"lon": [], "lat": []})
    p.circle(x='lon', y='lat', size=4, fill_color="red", fill_alpha=0.8, source=bsource)
    return p,source,bsource


p4326=Proj(init="epsg:4326")
p3857=Proj(init="epsg:3857")
trans1 = partial(transform, p4326,p3857)
api = OpenSkyApi(username='awsbarker', password=7914920)

locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.23631, 6.108055)}
location = Select(title='pick location', value='HOM', options=['BKK', 'GVA', 'HOM'])
location.on_change('value', f_updatemap)
print(f'initial loc {location.value}')

TOOLTIPS = [("Type", "@type"),("Own", "@own"),("Origin", "@orig"), ("Alt m", "@alt{0.0a}"),("Speed km/h", "@vel{0}"), ("Dir", "@head"), ("Climb m/s", "@vr")]
tile_provider = get_provider(Vendors.CARTODBPOSITRON)  # STAMEN_TERRAIN)
planes = get_planes()

p,source,bsource = init_plot()
layout = column(location,p)

curdoc().add_root(layout)
curdoc().title = f"Flights Status {location.value}"
curdoc().add_periodic_callback(steps, 5500)

