from bokeh.plotting import figure, show, output_file, save
from bokeh.tile_providers import get_provider, Vendors
from bokeh.models import ColumnDataSource, LabelSet
from opensky_api import OpenSkyApi
import math
import time
from pyproj import Transformer, CRS
trans = Transformer.from_crs(4326, 3857)
api = OpenSkyApi(username='awsbarker', password=7914920)

class Update(object):
    """ get flight stats. bbox and extent are reversed lat/lon. d = scale factor
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
        self.states = api.get_states(bbox=(self.get_bbox())).states
        self.ss=[]
        self.sslat=[] # list of lats and longs
        self.sslon=[]
        self.sstext=[]
        if len(self.states) == 0:
            print('nothing found')
        else:
            for s in self.states:
                print('{} {}m,V{}m/s'.format(s.callsign,s.baro_altitude, s.velocity))
                self.east,self.north = self.lalo_en(s.latitude,s.longitude)
                print('{} {:2f} '.format(self.east, self.north))
                #self.ss.update({s.callsign:[s.longitude, s.latitude, s.baro_altitude, s.velocity, s.callsign, s.origin_country]})
                self.ss.append(s.callsign)
                self.sslon.append(self.east)
                self.sslat.append(self.north)
                self.sstext.append(s.origin_country)

    def get_yrange(self): # north
        # x (mlon) tuple min,max = extent of the map blokeh plot
        east,north = self.lalo_en(self.lat, self.lon)
        return (north - north*Update.d , north + north*Update.d)

    def get_xrange(self): # east
        # y (lat) tuple min,max = extent of the map blokeh plot
        east,north = self.lalo_en(self.lat, self.lon)
        return (east - east*Update.d , east + east*Update.d)

    def get_bbox(self):
        # tuple (min latitude, max latitude, min longitude, max longitude)
        return (self.lat - 100*Update.d, self.lat + 100*Update.d, self.lon - 100*Update.d , self.lon + 100*Update.d)

    def lalo_en(self, lat,lon):
        east, north = trans.transform(lat,lon) # Lat, Lon -> X East, Y North
        return east,north
    #x,y =trans.transform(46.515229,6.559460) # Lat, Lon -> X East, Y North
# epsg.io WGS 84 (46.515229, 6.559460) - EPSG 3857 (730195.75, 5863302.96)
locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.23631, 6.108055),
             'JFK': (-73.778889, 40.639722)}
location = 'HOM'

while True:
    time.sleep(5)
    a = Update(location)
    tile_provider = get_provider(Vendors.CARTODBPOSITRON) # STAMEN_TERRAIN)
    #zoom=0.01
    #p = figure(x_range=(x-(x*zoom), x+(x*zoom)), y_range=(y-(y*zoom), y+(y*zoom)),x_axis_type="mercator", y_axis_type="mercator")
    p = figure(x_range=a.get_xrange(),y_range=a.get_yrange(),x_axis_type="mercator", y_axis_type="mercator")
    p.add_tile(tile_provider)
    source = ColumnDataSource(data=dict(lon=a.sslon,lat=a.sslat,cs=a.ss, txt=a.sstext))
    labels1 = LabelSet(x='lon', y='lat', text='cs', x_offset=1, y_offset=1, source=source,text_font_size='6pt')
    labels2 = LabelSet(x='lon', y='lat', text='txt', x_offset=1, y_offset=13, source=source,text_font_size='6pt')

    p.circle(x='lon', y='lat', size=5, fill_color="red", fill_alpha=0.8, source=source)
    #show(p)
    p.square(x=a.lalo_en(a.lat,a.lon)[0], y=a.lalo_en(a.lat,a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)
    p.circle(x='lon', y='lat', size=6, fill_color="red", fill_alpha=0.8, source=source)
    p.add_layout(labels1)
    p.add_layout(labels2)

    output_file("/mnt/RPi3-data/RPi1-Pictures/flight.html")
    # https://awsbarker.ddns.net/test/
    #show(p)
    save(p)

    for i in range(0,6):
        time.sleep(5)
        b = Update(location)
        source = ColumnDataSource(data=dict(lon=b.sslon,lat=b.sslat))
        p.circle(x='lon', y='lat', size=4, fill_color="red", fill_alpha=0.8, source=source)
        save(p)
