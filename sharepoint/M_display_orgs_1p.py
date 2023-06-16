# M+ main.py on awsb. M_display_orgs_1p.py
# pymssql required to avoid TypeError: Only valid with DatetimeIndex, TimedeltaIndex or PeriodIndex, but got an instance of 'Float64Index'
# Try M boxes with W inlay
# main need > bokeh 2 for multichoice -> try select
# test graphical one grouping,
#import datetime
#from bokeh.palettes import Magma, Viridis256
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, CDSView, DateRangeSlider, GroupFilter, BooleanFilter, Select, DatetimeTickFormatter,FactorRange
from bokeh.plotting import figure, curdoc
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
#pd.set_option('max_columns', 20)
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn' stops copying
from Mapi2 import Mapi, Org, DBconx
from bokeh.io import export_png

orgDict = Mapi.getOrg_ID_dict()
# df['2021-10-10'][(df.loc['2021-10-10', 'org'] == 6) & (df.loc['2021-10-10', 'count'] >=1)]
db_table = "M+_daily" # "M_daily1" "M+_daily_structure" #

#tuptup = DBconx('M').query(f"SELECT imei, checked_on, last_measure_at, count, org from `{db_table}`")
tuptup = DBconx('M').query(f"SELECT imei, checked_on, last_measure_at, count, org from `{db_table}` WHERE org IN (1,2,3,4,5,6)")
df1 = pd.DataFrame([i for i in tuptup]) #, columns=['imei', 'checked', 'last', 'count', 'org']) #.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
df1.columns = ['imei', 'checked', 'last', 'count', 'org']
df1.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})
df1.org = df1.org.map(Org.getOrg_Name_dict()) #.astype('category')
# non-zero df use lastMeasure as index
#df1.set_index('last', drop=False, inplace=True)
#df1.sort_index(inplace=True)
#df2 = df1.resample('M').max()

# total devices by org
dev_org = df1.groupby(['org'])['imei'].nunique()
dev_org_mon = df1.groupby(['org', pd.Grouper(key='last', freq='M')])['imei'].nunique().to_frame()

orgs = list(Org.getOrg_Name_dict().values())
#bool1 = [True if o in orgs else False for o,x in source.data['xaxis']]

#ts = dev_org_mon._get_label_or_level_values('last')
dev_org_mon['month'] = pd.to_datetime(dev_org_mon._get_label_or_level_values('last')).date
dev_org_mon['xax'] = dev_org_mon.month - timedelta(days=15) # just for xaxis drawing in mid month
dev_org_mon['org'] = dev_org_mon._get_label_or_level_values('org')

source = ColumnDataSource(dev_org_mon)
oselect = Select(title='Pick M+ Organisation  ', value=orgs[0], options=orgs, width_policy='min')

# start range end of this month back 3 months OR min/max
# end of this month < (dt.now().date().replace(day=1) + timedelta(days=32)).replace(day=1)
deom = (dt.now().date().replace(day=1) + timedelta(days=32)).replace(day=1)
dnow = dt.now().date()
drange1 = (deom - timedelta(days=360), deom)
drange2 = (dev_org_mon.month.min(), dev_org_mon.month.max())
dslider = DateRangeSlider(value=drange1, start= drange2[0], end=drange2[1], format = '%d  %b  %Y',
                          title='Period for Active Devices         .', bar_color='green', width_policy='min')
# initial view deom back 6 months
bool2 = (source.data['month'] <= deom) & (source.data['month'] >= dt.now().date()- timedelta(days=180))

view1 = CDSView(source=source, filters=[GroupFilter(column_name = 'org', group = 'GM_SL'), BooleanFilter(bool2)])

p = figure(plot_width=800, plot_height=350, x_axis_type = 'datetime',
           title="Active devices by Org by Month (device time, might be  in future", toolbar_location='above')
p.xaxis[0].formatter = DatetimeTickFormatter(months="%B %y")
p.vbar(x='xax', top='imei', width=24*60*60*1000*25, source=source, line_color="black", view = view1) #, fill_color=index_cmap, )

def updater(attr, old, new):
    if type(new) is tuple: # its a ts
        #print(f'new {new} {dslider.value_as_date}')
        bool2 = (view1.source.data['month'] <= dslider.value_as_date[1]) & (view1.source.data['month'] >= dslider.value_as_date[0])
        #print(f' bools {bool2}')
        view1.filters[1] = BooleanFilter(bool2)
    else: # its an org
        #print(f'new org is {new} {type(new)}') # list of orgs
        view1.filters[0] = GroupFilter(column_name='org', group=new)
        dslider.value = drange1 # reset slider
        #p.x_range.factors = list(view1.source.data['xaxis'])
        #print(view1.source.data['month'])
    #dslider new range

oselect.on_change('value', updater)
dslider.on_change('value_throttled', updater)

c = row(oselect,dslider)
layout = column(c, p, name = 'g1')
#show(p)
export_png(layout, filename=DBconx().data_folder + '/plot4monthly.png')
curdoc().add_root(layout)
#print('done')


'''
removed to Mapi2
def db_get(sql):
    db = pymysql.connect(host='192.168.1.173', port=3306, user='pi', password='7914920', db='Health', charset='utf8')
    with db.cursor() as cur:# setup the dropdowns dicts
        cur.execute(sql) #"SELECT * FROM countries")
        r = cur.fetchall() # tuple of dicts ({},{}..  ,)
    db.close()
    return r

'''
