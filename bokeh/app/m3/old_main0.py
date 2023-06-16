# lightsail version M3 + redis
# add pie and datatable
# Try M boxes with W inlay
import redis
import pickle
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, CDSView, Button, DateFormatter, \
    DateRangeSlider, GroupFilter, BooleanFilter, Select, DatetimeTickFormatter, DataTable,\
    TableColumn, HoverTool
from bokeh.plotting import figure, curdoc
from datetime import datetime as dt
from datetime import timedelta
from bokeh.models import CustomJS
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral11
import sys
sys.path.append("..")
sys.path.append("/home/bitnami/sharepoint")
from Mapi2 import Mapi, Org
from Mdb2 import MonthlyReport_1

r = redis.Redis()
orgs_wk = pickle.loads(r.get("df_M3_orgs_wk"))
dev_org_mon = pickle.loads(r.get("df_M3_dev_org_mon"))

no_orgs = orgs_wk.index.unique('org').to_list()
org_cmap = factor_cmap('xaxis', palette=Spectral11, factors= no_orgs, end=1)
orgs = list(Mapi.getOrgs_showas_dict().values())

source_mon = ColumnDataSource(dev_org_mon)
source_wk = ColumnDataSource(orgs_wk)
oselect = Select(title='Pick M+ Org  ', value=orgs[0], options=orgs, width_policy='min')

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
viewm = CDSView(source=source_mon, filters=[GroupFilter(column_name = 'org', group = orgs[0]), BooleanFilter(boolm)])

boolw = (source_wk.data['xaxis'] <= deom) & (source_wk.data['xaxis'] >= dt.now().date()- timedelta(days=180))
vieww = CDSView(source=source_wk, filters=[GroupFilter(column_name = 'org', group = orgs[0]), BooleanFilter(boolw)])

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
    try:
        m,o = get_sele_month() # date with end of month
        m1 = m.replace(day=1) # date with start
        o1 = Org.getOrgs_showas_dict(True).get(o) # inverted dict
        MonthlyReport_1((o1,),m1)
        reporter.label = f"===> Completed OrgReport_{o}_{m1} report. Click below for download"
    except:
        reporter.label = "ERROR Choose an month by clicking on above table"

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
        reporter.label = f"Select a period to Download. "
    elif type(new) is str: # its an org
        viewm.filters[0] = GroupFilter(column_name='org', group=new)
        vieww.filters[0] = GroupFilter(column_name='org', group=new)
        dslider.value = drange1 # reset slider
        source_mon.selected.indices = []
        reporter.label = f"Select a period to Download. "
    elif source_mon.selected.indices: # its datatable change
        m, o = get_sele_month()
        reporter.label = f"Click to Generate report for {o} month ending {m}"
    data_table.visible=False
    data_table.visible = True

reporter = Button(label=f"Select Month in above table, Then click here to create report ", button_type='success', background='red')
downloader = Button(label=f"Download the report", button_type='warning', background='red')

cols = [TableColumn(field='month', title='Month', formatter=DateFormatter()),
        TableColumn(field='imei', title='Active devices'),
        ]

editbutton = CustomJS(args=dict(s=source_mon), code="""
    const uri = s.data.uri[0];
    const inds = s.selected.indices;
    const fname = s.data.filename[inds];
    if (inds.length == 0)
        return;
    console.log(uri, inds, fname);
    var link = document.createElement('a');
    link.download = fname;
    link.href = uri+fname;
    link.click();
    s.change.emit();""")

data_table = DataTable(source=source_mon, columns=cols, view=viewm, index_position=None, height=200)
oselect.on_change('value', updater)
dslider.on_change('value_throttled', updater)
source_mon.selected.on_change('indices', updater)
reporter.on_click(callback)
downloader.js_on_click(editbutton)
c = row(oselect,dslider)
layout = column(c, p, p1, data_table, reporter, downloader, name = 'g1')
#show(p)
curdoc().add_root(layout)
#print('done')

