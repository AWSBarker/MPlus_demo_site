# try get local IP into initial map
# tile select create class for city by country to avoid public using mysql. single updater
# download db-dict for countries but only call cities after country filter
# create city maps via country (city list 100k is too big, 5k utf8 coding
# v6 for RPI3 with pyproj=1.9 (Transfor and CRS not working)

from bokeh.plotting import figure, curdoc #show, output_file, save
from bokeh.tile_providers import get_provider, Vendors
from bokeh.layouts import column, row
#from pyproj import Transformer, CRS
from functools import partial
from pyproj import Proj, transform
from bokeh.models.widgets import Select
import MySQLdb
import MySQLdb.cursors
import requests

class Map():
    """ # epsg.io WGS 84 (46.515229, 6.559460) - EPSG 3857 (730195.75, 5863302.96)
    get flight stats. bbox and extent are reversed lat/lon. d = scale factor
    USE PROJ FOR TUPLS GEODESIC TO CRS
    for loc use ipinfo
    """
    d = 0.00075# size of map 0.01 about 1° square
    def __init__(self, loc):
        #self.loc=loc # loc is tuple (lat, lon) from dict key 'Ecublens, CH (46.6073, 6.80895)'
        #print(f'loc is {loc} location value is {coord}')#location is : {locations[loc]}')
        self.lat,self.lon = loc
        self.east,self.north = self.lalo_en(self.lat,self.lon)

    def __str__(self):
        return f'loc is {self.lat}, {self.lon} '

    def get_yrange(self): # north   # x (mlon) tuple min,max = extent of the map blokeh plot
        east,north = self.lalo_en(self.lat, self.lon)
        return (north - north*Map.d , north + north*Map.d)

    def get_xrange(self): # east   # y (lat) tuple min,max = extent of the map blokeh plot
        east,north = self.lalo_en(self.lat, self.lon)
        return (east - east*Map.d , east + east*Map.d)

    def get_bbox(self):        # tuple (min latitude, max latitude, min longitude, max longitude)
        return (self.lat - 100*Map.d, self.lat + 100*Map.d, self.lon - 100*Map.d , self.lon + 100*Map.d)

    def lalo_en(self, lat,lon):
        #east, north = trans.transform(lat,lon) # Lat, Lon -> X East, Y North
        east1, north1 = trans1(lat,lon) # trans1 works lon.lat -> East North
        return east1,north1

def db(sql, dbase='flights'):
    connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi',
                                 password='7914920', db=dbase, charset='utf8')#, cursorclass=MySQLdb.cursors.DictCursor)
    conn = connection.cursor()  # setup the dropdowns dicts
    conn.execute(sql)  # "SELECT * FROM countries")
    r = conn.fetchall()  # tuple of dicts ({},{}..  ,)
    conn.close()
    connection.close()
    return r

class CountryCities():
# produce cities by selected country each selection. allow code or name selection i.e. dict using name and code keys
    def __init__(self, c, place, latlon):
        'country, city = aplace(Zurich, CH (lat,lon), accord (lat,lon) picked from ipinfo,'
        if len(c) == 2:
            self.country = table[c]
            self.code = c
        else:
            self.code = table[c]
            self.country = c
        self.locations = {}

        for t in db(f"SELECT name, c_code, lat, lon, pop from cities WHERE c_code = '{self.code}' AND pop > 5000"):
            k = f'{t[0]}, {t[1]}, ({t[2]}, {t[3]})'
            self.locations[k] = (t[2], t[3])

        self.aplace = place # next(iter(self.locations)) 3 not exact coords ipinfo

        self.acoord = self.locations.setdefault(self.aplace, latlon)   # tuple (lat, lon)
        self.placelist = sorted(list(self.locations.keys()))

    def __str__(self):
        return f'Country is {self.code}, {self.country}. With {len(self.locations)} cities. i.e. {self.aplace}'

def init_plot(l):
    # location.value needs to be tuple (lat, lon) it
    a = Map(l)  # location exp as dict { 'Ecublens, CH (46.6073, 6.80895)' : (46.6073, 6.80895). }
    p = figure(plot_height=840, plot_width=840, x_range=a.get_xrange(), y_range=a.get_yrange(), x_axis_type="mercator", y_axis_type="mercator")
    p.add_tile(tile_provider)
    p.square(x=a.lalo_en(a.lat, a.lon)[0], y=a.lalo_en(a.lat, a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)
    return p

def cupdate(attr, old, new):
    global cc,p
    # need new location list for new country, no plot
    print(f'new country.value is {country.value} location.value {location.value}') #location.value is string 'lat, lon'
    cc = CountryCities(country.value)
    location.options = cc.placelist
    p = init_plot(cc.acoord)
    poplayout(p)

def lupdate(attr, old, new):
    # get new location list and plot
    global cc,p
    print(f'new country.value is {country.value} location.value {location.value}') #location.value is string 'lat, lon'
    p = init_plot(cc.locations[location.value])
    poplayout(p)

def tupdate(attr, old, new):
    global cc,tile_provider,p
    tile_provider = get_provider(f'{tile.value}')  # OSM
    print(f'new tile is {tile.value} place {cc.aplace} location {cc.locations[location.value]}') #location.value is string 'lat, lon'
    p = init_plot(cc.locations[location.value])
    poplayout(p)

def poplayout(p):
    layout.children.pop()
    new_layout = column(controls,p, name='g1')
    layout.children = new_layout.children

namecode = sorted(list(db('SELECT Code, Name FROM countries'))) # [(Switzerland, CH),]
table = {x[0]: x[1] for x in namecode}

p4326=Proj("epsg:4326") # init depreciated but lat/lon switched
p3857=Proj("epsg:3857")
trans1 = partial(transform, p4326,p3857)

r = requests.get('https://ipinfo.io')
if r.status_code == 200:
    content = r.json()
    city = content['city']
    country = content['country']
    lat, lon = (float(x) for x in content['loc'].split(','))
    place =  f"{city}, {country}, ({str(lat)},{str(lon)})"# "Zurich, CH (lat,lon)"
else:
    country = 'CH'
    place = "Lausanne, CH, (46.516, 6.63282)"
    lat, lon = (46.516, 6.63282)

cc = CountryCities(country, place, (lat,lon)) # current country from ipinfo

vendors = ['CARTODBPOSITRON', 'CARTODBPOSITRON_RETINA', 'STAMEN_TERRAIN',
           'STAMEN_TERRAIN_RETINA', 'STAMEN_TONER', 'STAMEN_TONER_BACKGROUND',
           'STAMEN_TONER_LABELS', 'OSM', 'WIKIMEDIA', 'ESRI_IMAGERY']
tile_provider = get_provider('OSM') #Vendors.STAMEN_TERRAIN) # OSM

country = Select(title='pick a country', value=cc.code, options=namecode) # key : value (CH . Switzerland)
country.on_change('value', cupdate) # updatecities filters on country -> list of tuples (cities)
location = Select(title='pick a city', value=cc.aplace, options=cc.placelist)
location.on_change('value', lupdate)
tile = Select(title='pick map type', value='OSM', options=vendors)
tile.on_change('value', tupdate)

print(f'V7. initial loc {location.value}')

p = init_plot(cc.locations[location.value])

controls = row(country, location, tile)
layout = column(controls, p, name='g1')
curdoc().add_root(layout)
curdoc().title=f"City Map {cc.aplace}"