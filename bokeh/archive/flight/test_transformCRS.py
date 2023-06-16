from pyproj import Transformer, CRS, Proj, transform
# transfor between dms GPS, EPSG:4326 WGS 84 -> X,Y EPSG:3857 WGS 84 / Pseudo-Mercator
#crs = CRS.from_epsg(4326, 3857)
trans = Transformer.from_crs(4326, 3857)
inProj = Proj(init='epsg:4326')
outProj = Proj(init='epsg:3857')
#crs = CRS.from_epsg(3857) # WGS 84 X East Lon, Y North Lat
# . epsg.io 4326 crs.geodetic_crs Lat North, Lon East (46.515229, 6.559460) - EPSG 3857 (730195.75, 5863302.96)
locations = {'BKK': (13.69269, 100.750465 ), 'HOM': (46.515229, 6.559460), 'GVA': (46.2044, 6.1432),
             'JFK': (-73.778889, 40.639722)}
location = 'HOM'
x,y =trans.transform(46.515229,6.559460) # Lat, Lon -> X East, Y North
print(x, y)

list = [(13.69269, 100.750465 ), (46.515229, 6.559460), (46.2044, 6.1432),(-73.778889, 40.639722)]
for x,y in trans.itransform(list):
    print(x,y)

lon,lat =6.559460, 46.515229
x,y = transform(inProj,outProj,lon,lat) #, always_xy=True)
print(x,y)