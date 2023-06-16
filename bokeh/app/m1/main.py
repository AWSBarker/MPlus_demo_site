# M1 : Lightsail version Mapi2 + redis
# M_display_orgs_1p.py
# pymssql required to avoid TypeError: Only valid with DatetimeIndex, TimedeltaIndex or PeriodIndex, but got an instance of 'Float64Index'
# Try M boxes with W inlay
# main need > bokeh 2 for multichoice -> try select
# test graphical one grouping,
#from bokeh.palettes import Magma, Viridis256
import redis
import pickle
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, CDSView, DateRangeSlider, GroupFilter, BooleanFilter, Select, DatetimeTickFormatter,FactorRange
from bokeh.plotting import figure, curdoc
from datetime import datetime as dt
from datetime import timedelta
import sys
sys.path.append("/home/bitnami/sharepoint")
sys.path.append("..")
#sys.path.append(os.path.dirname(os.path.realpath(__file__)))
#pd.options.mode.chained_assignment = None  # default='warn' stops copying
from Mapi2 import Mapi

r = redis.Redis()
dev_org_mon = pickle.loads(r.get("df_M1_dev_org_mon"))

orgs = list(Mapi.getOrgs_showas_dict().values())
source = ColumnDataSource(dev_org_mon)
oselect = Select(title='Pick M+ Org ', value=orgs[0], options=orgs, width_policy='min')

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
view1 = CDSView(source=source, filters=[GroupFilter(column_name = 'org', group = orgs[0]), BooleanFilter(bool2)])

p = figure(plot_width=800, plot_height=350, x_axis_type = 'datetime',
           title="Active devices by Org by Month (device time, might be  in future", toolbar_location='above')
p.xaxis[0].formatter = DatetimeTickFormatter(months="%B %y")
p.vbar(x='xax', top='imei', width=24*60*60*1000*25, source=source, line_color="black", view = view1) #, fill_color=index_cmap, )

def updater(attr, old, new):
    if type(new) is tuple: # its a ts
        bool2 = (view1.source.data['month'] <= dslider.value_as_date[1]) & (view1.source.data['month'] >= dslider.value_as_date[0])
        view1.filters[1] = BooleanFilter(bool2)
    else: # its an org
        view1.filters[0] = GroupFilter(column_name='org', group=new)
        dslider.value = drange1 # reset slider

oselect.on_change('value', updater)
dslider.on_change('value_throttled', updater)

c = row(oselect,dslider)
layout = column(c, p, name = 'g1')
#show(p)
curdoc().add_root(layout)
#print('done')

