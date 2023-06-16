i = 1

while i % 6 == 0:
   i = i+1
   print(i, i % 6)


# import cartopy.crs as ccrs
# import matplotlib.pyplot as plt
#
# ax = plt.axes(projection=ccrs.Mollweide())
# ax.stock_img()
#
# ny_lon, ny_lat = 6, 46
# delhi_lon, delhi_lat = 77.23, 28.61
#
# plt.plot([ny_lon, delhi_lon], [ny_lat, delhi_lat],
#          color='blue', linewidth=2, marker='o',
#          transform=ccrs.Geodetic(),
#          )
#
# plt.plot([ny_lon, delhi_lon], [ny_lat, delhi_lat],
#          color='gray', linestyle='--',
#          transform=ccrs.PlateCarree(),
#          )
#
# plt.text(ny_lon -6, ny_lat +40, 'Ecublens',
#          horizontalalignment='right',
#          transform=ccrs.Geodetic())
#
# plt.text(delhi_lon + 3, delhi_lat - 12, 'Delhi',
#          horizontalalignment='left',
#          transform=ccrs.Geodetic())
#
# plt.show()