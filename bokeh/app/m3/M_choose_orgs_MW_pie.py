# add pie and datatable
# Try M boxes with W inlay
from bokeh.layouts import row, column
from bokeh.models import CustomAction, CustomJS, ColumnDataSource, CDSView, Button, DateFormatter, \
    DateRangeSlider, GroupFilter, BooleanFilter, Select, DatetimeTickFormatter, DataTable,\
    TableColumn, HoverTool
from bokeh.plotting import figure, curdoc
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
#pd.set_option('max_columns', 20)
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn' stops copying
from Mapi2 import Mapi, Org, DBconx
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral11
from Mdb2 import MonthlyReport_1
from bokeh.events import ButtonClick
from os.path import dirname, join

orgDict = Org.getOrg_Name_dict()
tuptup = DBconx().query("SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`")# WHERE org IN (1,2,3,4,5,6)")
df1 = pd.DataFrame([i for i in tuptup])
df1.columns = ['imei', 'checked', 'last', 'count', 'org']
df1.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8}) # no effect 'last' : '<M8[s]'})
df1.org = df1.org.map(orgDict) #.astype('category')

# total devices by org
dev_org = df1.groupby(['org'])['imei'].nunique()
dev_org_mon = df1.groupby(['org', pd.Grouper(key='last', freq='M')])['imei'].nunique().to_frame()
# weekly data (can be M)
orgs_wk = df1.groupby(['org', pd.Grouper(key='last', freq='W')])['imei'].nunique().to_frame()
no_orgs = orgs_wk.index.unique('org').to_list()
org_cmap = factor_cmap('xaxis', palette=Spectral11, factors= no_orgs, end=1)
orgs_wk['xaxis'] =pd.to_datetime(orgs_wk._get_label_or_level_values('last')).date
orgs_wk['org'] = orgs_wk._get_label_or_level_values('org')

orgs = list(Org.getOrg_Name_dict().values())
#ts = dev_org_mon._get_label_or_level_values('last')
dev_org_mon['month'] = pd.to_datetime(dev_org_mon._get_label_or_level_values('last')).date
dev_org_mon['xax'] = dev_org_mon.month - timedelta(days=15) # just for xaxis drawing in mid month
dev_org_mon['org'] = dev_org_mon._get_label_or_level_values('org')

source_mon = ColumnDataSource(dev_org_mon)
source_wk = ColumnDataSource(orgs_wk)
oselect = Select(title='Pick M+ Organisation  ', value=orgs[0], options=orgs, width_policy='min')

# start range end of this month back 3 months OR min/max
# end of this month < (dt.now().date().replace(day=1) + timedelta(days=32)).replace(day=1)
deom = (dt.now().date().replace(day=1) + timedelta(days=32)).replace(day=1)
dnow = dt.now().date()
drange1 = (deom - timedelta(days=360), deom)
drange2 = (dt(2021,1,1), deom) #dev_org_mon.month.max())
dslider = DateRangeSlider(value=drange1, start= drange2[0], end=drange2[1], format = '%d  %b  %Y',
                          title='Period for Active Devices         .',
                          bar_color='green', width_policy='min', step=86400000*30)
# initial view deom back 6 months
boolm = (source_mon.data['month'] <= deom) & (source_mon.data['month'] >= dt.now().date()- timedelta(days=180))
viewm = CDSView(source=source_mon, filters=[GroupFilter(column_name = 'org', group = 'GM_SL'), BooleanFilter(boolm)])

boolw = (source_wk.data['xaxis'] <= deom) & (source_wk.data['xaxis'] >= dt.now().date()- timedelta(days=180))
vieww = CDSView(source=source_wk, filters=[GroupFilter(column_name = 'org', group = 'GM_SL'), BooleanFilter(boolw)])

p = figure(plot_width=800, plot_height=250, x_axis_type = 'datetime',
           title="Active devices by Month", toolbar_location='right')
p.xaxis[0].formatter = DatetimeTickFormatter(months="1 %b %y")
p.xaxis[0].visible = True
#tool = CustomAction(icon="M+logo.png", callback=CustomJS(code='alert("foo")'))
#p.add_tools(tool)
p.vbar(x='xax', top='imei', width=24*60*60*1000*27, source=source_mon, line_color="black", view = viewm, fill_color='darkred') #, fill_color=index_cmap, )
p.add_tools(HoverTool(tooltips = [('Active', '@imei'), ("Month", "@xax{%b}")],
          formatters = {'@xax' : 'datetime'},
          mode='vline'
          ))

p1 = figure(plot_width=800, plot_height=250, x_axis_type = 'datetime', x_range=p.x_range,
           title="Active devices by week", toolbar_location='right')
p1.xaxis[0].formatter = DatetimeTickFormatter(months="1 %b %y")
p1.vbar(x='xaxis', top='imei', width=24*60*60*1000*7, source=source_wk, line_color="white", view = vieww, fill_color='darkred')

p1.add_tools(HoverTool(tooltips = [('Active', '@imei'), ("Week", "@xaxis{%W}")],
          formatters = {'@xaxis' : 'datetime'},
          mode='vline'
          ))
def callback():
    m,o = get_sele_month()
    print(f'm is {m}')
    m1 = m.replace(day=1)
    o1 = Org.getName_id_dict().get(o)
    mr = MonthlyReport_1((o1,),m1)
    print(f'Org :{(o1,)} Month {m1} Report {mr.filename[-28:]}')
    button.label = f"===> Downloaded to {mr.filename}"
    # else:
    #     print(f'm is False {m}')
    #     return CustomJS()
    #return CustomJS(args=dict(source=viewm.source), code=open(join(dirname(__file__), "download.js")).read())

def get_sele_month(): # get selected datatable month details
    if source_mon.selected.indices:
        i = source_mon.selected.indices[0]
        m = viewm.source.data['month'][i]
        o = viewm.source.data['org'][i]
        print(f"datatable selected {m}, {o}")
        return m,o
    else:
        print(f"nothing selected")

def updater(attr, old, new):
    if type(new) is tuple: # its a ts
#        boolm = (viewm.source.data['month'] <= np.float(dslider.value[1])) & (viewm.source.data['month'] >= np.float(dslider.value[0]))
        boolm = (viewm.source.data['month'] <= dslider.value_as_date[1]) & (viewm.source.data['month'] >= dslider.value_as_date[0])
        viewm.filters[1] = BooleanFilter(boolm)
#        boolw = (vieww.source.data['xaxis'] <= np.float(dslider.value[1])) & (vieww.source.data['xaxis'] >= np.float(dslider.value[0]))
        boolw = (vieww.source.data['xaxis'] <= dslider.value_as_date[1]) & (vieww.source.data['xaxis'] >= dslider.value_as_date[0])
        vieww.filters[1] = BooleanFilter(boolw)
        source_mon.selected.indices = []
        button.label = f"Select a period to Download. "
    elif type(new) is str: # its an org
        viewm.filters[0] = GroupFilter(column_name='org', group=new)
        vieww.filters[0] = GroupFilter(column_name='org', group=new)
        dslider.value = drange1 # reset slider
        source_mon.selected.indices = []
        button.label = f"Select a period to Download. "
    elif source_mon.selected.indices: # its datatable change
        m, o = get_sele_month()
        #i = source_mon.selected.indices[0]
        #m = viewm.source.data['month'][i]
        #print(f"datatable selected {m} {o}")
        button.label = f"Click to Download : {m} {o}"
    data_table.visible=False
    data_table.visible = True

button = Button(label=f"Download month as report ", button_type='success', background='red')
cols = [TableColumn(field='month', title='Month', formatter=DateFormatter()),
        TableColumn(field='imei', title='Active devices'),
        ]
data_table = DataTable(source=source_mon, columns=cols, view=viewm, index_position=None, height=200)
oselect.on_change('value', updater)
dslider.on_change('value_throttled', updater)
source_mon.selected.on_change('indices', updater)
button.on_click(callback)
#button.js_on_event(ButtonClick, callback())

c = row(oselect,dslider)
layout = column(c, p, p1, data_table, button, name = 'g1')
#show(p)
curdoc().add_root(layout)
#print('done')

