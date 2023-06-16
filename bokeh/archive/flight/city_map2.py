# TODO - mercator rings to correct small scale
# CDS for map
# single updater
# download db-dict for countries but only call cities after country filter
# create city maps via country (city list 100k is too big
# v6 for RPI3 with pyproj=1.9 (Transfor and CRS not working)
from bokeh.plotting import figure, curdoc #show, output_file, save
from bokeh.tile_providers import get_provider, Vendors
from bokeh.layouts import column, WidgetBox
from bokeh.models import ColumnDataSource, LabelSet
#from pyproj import Transformer, CRS
from functools import partial
from pyproj import Proj, transform
from bokeh.models.widgets import Select
from pandas.io.sql import read_sql_table
from sqlalchemy import create_engine

#tile_provider = get_provider(Vendors.CARTODBPOSITRON)  # STAMEN_TERRAIN)
tile_provider = get_provider(Vendors.STAMEN_TERRAIN_RETINA)  # STAMEN_TERRAIN)

mapxy = ColumnDataSource(data=dict(x=[],y=[])) # selected city
mapxy2 = ColumnDataSource(data=dict(x=[],y=[])) # compass

map = figure(x_axis_type="mercator", y_axis_type="mercator")
map.add_tile(tile_provider)
map.square(x='x', y='y', source=mapxy)#, line_width=1, line_color="red", line_alpha=0.5)#x=a.lalo_en(a.lat, a.lon)[0], y=a.lalo_en(a.lat, a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)
map.square(x='x', y='y', source=mapxy2)#, line_width=1, line_color="red", line_alpha=0.5)#x=a.lalo_en(a.lat, a.lon)[0], y=a.lalo_en(a.lat, a.lon)[1], size=10, fill_color="blue", fill_alpha=0.8)


def updatemap(attr, old, new): # calc square coords from selected city
    """d = 0.0075 # size of map 0.01 about 1° square
    lat,lon = locations[loc]
    east, north = trans1(lon, lat) # 757968.8468368602, N 5878209.351929433
    yrange = (north - north*Map.d , north + north*Map.d)
    xrange =(east - east * Map.d, east + east * Map.d)1   bbox = (self.lat - 100*Map.d, self.lat + 100*Map.d, self.lon - 100*Map.d , self.lon + 100*Map.d)
    """
    e = 0.03
    n = 0.004
    print(f'old {old}, new {new} ')
    lat, lon = (float(location.value.split(',')[0]),float(location.value.split(',')[1]))   # string ('48.06, 6.05')
    east, north = trans1(lon, lat)
    eastbb = [east-east*e, east+east*e]
    northbb = [north, north]
    #print(f'E {east}, Ebb {eastbb} N {north} Nbb {northbb}')
    #map.x_range.factors = []
    mapxy.data = dict(x=[east, east] , y=[north-north*n,north+north*n])
    mapxy2.data = dict(x=eastbb , y=northbb)

    if len(new) == 2: # if new country
        print(f'country shoosen old {old}, new {new} ')
        ac=acities[acities.c_code == country.value]
        #cities list of tuples  [('name, ccode, lat, lon', ('lat','lon')), ]
        #locations { 'name, ccode, (lat, lon)' : ('lat', 'lon') }
        c =[]
        for row in ac.itertuples():
            k = f'{row[1]}, {row[2]}, ({row[3]}, {row[4]})'
            v = f'{str(row[3])}, {str(row[4])}'
            c.append((v,k))
            #locations[k] = (row[3], row[4])
        location.options = c

def get_cities(): # one time df
    cols = ['name', 'c_code', 'lat', 'lon']
    engine = create_engine('mysql+mysqldb://pi:7914920@192.168.1.173:3306/flights')
    con = engine.connect()
    acities = read_sql_table("cities", con, columns=cols)
    con.close()
    return acities

def get_countries(): # make locations dict and list - all options
    cols = ['name', 'c_code']
    engine = create_engine('mysql+mysqldb://pi:7914920@192.168.1.173:3306/flights')
    con = engine.connect()
    all_countries = read_sql_table("countries", con)
    print('connected')
    con.close()
    return list(zip(all_countries.Code, all_countries.Name)) # [(value=code, label=name)..]

def filter_cities(): # filter cities by country
    global cities
    cities = []
    ac=acities[acities.c_code == 'CH']
    #cities list of tuples  [('name, ccode, lat, lon', (lat,lon)), ]
    #locations { 'name, ccode, (lat, lon)' : (lat, lon) }
    for row in ac.itertuples():
        k = f'{row[1]}, {row[2]}, ({row[3]}, {row[4]})'
        v = f'{row[3]}, {row[4]}'
        cities.append((v,k))
    return cities

#trans = Transformer.from_crs(4326, 3857)
p4326=Proj(init="epsg:4326")
p3857=Proj(init="epsg:3857")
trans1 = partial(transform, p4326,p3857)
#location = []
home = ('46.6073, 6.80895') #value can't be float Ecublens, CH, (46.6073, 6.80895)
#locations = {home:(46.6073, 6.80895)} # initialise for first run

acountries = get_countries()
acities= get_cities()
cities = filter_cities()

country = Select(title='pick a country', value='CH', options=acountries) # label : value (CH . Switzerland)
location = Select(title='pick a location (hit reset or zoom to adjust view)', value=home, options=cities) # label = 'Ecublens, CH (46.6073, 6.80895)' value = (46.6073, 6.80895)

#alocations, alocation, acountries, df_aloc = make_locs() # all possible locations
#locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.23631, 6.108055)}

country.on_change('value', updatemap) # updatecities filters on country -> list of tuples (cities)
location.on_change('value', updatemap)

print(f'V7. initial loc {location.value}')
controls = WidgetBox(country, location)
layout = column(controls,map)

curdoc().add_root(layout)
curdoc().title=f"City Map {location}"
