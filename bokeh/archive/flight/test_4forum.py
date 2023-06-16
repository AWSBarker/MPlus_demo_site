# v4 add restart after X steps
# v3 add auto step and server
# v2 add tooltips
from bokeh.plotting import figure, curdoc #show, output_file, save
from bokeh.tile_providers import get_provider, Vendors
from bokeh.models import ColumnDataSource, LabelSet
from opensky_api import OpenSkyApi
from pyproj import Transformer, CRS
from bokeh.driving import linear
trans = Transformer.from_crs(4326, 3857)
api = OpenSkyApi(username='awsbarker', password=7914920)
import time

class Update(object):
    d = 0.01# size of map 0.01 about 1° square
    def __init__(self, loc):
        self.loc=loc
        self.lat,self.lon = locations[self.loc]
        self.states = api.get_states(bbox=(self.get_bbox())).states
        self.ss=[] # callsigns
        self.sslat=[] # list of lats and longs
        self.sslon=[]
        if len(self.states) == 0:
            print('nothing found')
        else:
            for s in self.states:
                self.east,self.north = self.lalo_en(s.latitude,s.longitude)
                self.ss.append(s.callsign)
                self.sslon.append(self.east)
                self.sslat.append(self.north)

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

locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.23631, 6.108055)}
location = 'HOM'
TOOLTIPS = [("Origin", "@orig")]

a = Update(location)
tile_provider = get_provider(Vendors.CARTODBPOSITRON)  # STAMEN_TERRAIN)
source = ColumnDataSource(data=dict(lon=a.sslon, lat=a.sslat))
p = figure(x_range=a.get_xrange(), y_range=a.get_yrange(), x_axis_type="mercator", y_axis_type="mercator", tooltips=TOOLTIPS)
p.add_tile(tile_provider)
labels1 = LabelSet(x='lon', y='lat', text='cs', x_offset=1, y_offset=1, source=source,text_font_size='8pt')
p.circle(x='lon', y='lat', size=5, fill_color="red", fill_alpha=0.8, source=source)
p.square(x=a.lalo_en(a.lat,a.lon)[0], y=a.lalo_en(a.lat,a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)
p.circle(x='lon', y='lat', size=6, fill_color="red", fill_alpha=0.8, source=source)
p.add_layout(labels1)

@linear()
def steps(value):
    print(f'step count {value}')
    if value <5:
        b = Update(location)
        bsource.stream({"lon":b.sslon, "lat":b.sslat})
        p.circle(x='lon', y='lat', size=4, fill_color="red", fill_alpha=0.8, source=bsource)

time.sleep(5)
bsource = ColumnDataSource({"lon":[], "lat":[]})
curdoc().add_root(p)
curdoc().title="current satus"
# Add a periodic callback to be run every 5001 milliseconds
curdoc().add_periodic_callback(steps, 6001)
