# Lightsail version
# NOTE : WHERE org startswith GM
# TODO pie all orgs active/total
from math import pi
import pandas as pd
from Mapi2 import Org, DBconx
from bokeh.palettes import Spectral11
from bokeh.plotting import figure, show #¼, column
from bokeh.layouts import  column # 1.4
from bokeh.transform import cumsum
import numpy as np
from bokeh.io import curdoc, show
from bokeh.models import ColumnDataSource, LabelSet,FactorRange, FuncTickFormatter, HoverTool, Label, DataTable, TableColumn
from datetime import datetime as dt, timedelta
from bokeh.transform import factor_cmap
db_table = "M+_daily"  #"M+_daily_structure" # "M_daily1"
orgDict = Org.getOrg_Name_dict() # {ID : NAME, }
gm_orgs = tuple([k for k,v in orgDict.items() if v.startswith('GM')])
tuptup = DBconx('T').query(f"SELECT imei, checked_on, last_measure_at, count, org from `{db_table}` WHERE org IN {gm_orgs}")
df1 = pd.DataFrame([i for i in tuptup], columns=['imei', 'checked', 'last', 'count', 'org']) #.astype({'imei': np.int64, 'count' : np.int16, 'org': np.int8})

df1.org = df1.org.map(orgDict) #.astype('category')
df1.set_index('last', drop=False, inplace=True)
df1.sort_index(inplace=True)

# summary data for pie, total devices by org
dev_org = df1.groupby(['org'])['imei'].nunique().to_frame().rename(columns={'imei':'Total'})
dm = df1.groupby(['org', pd.Grouper(level=0, freq='M')])['imei'].nunique().to_frame() # monthly data by org
# get month ends
d_end_month = (dt.now().replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)-timedelta(days=1)
d_end_month_1 = (dt.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)-timedelta(days=1))
#lst_mon = dm.loc[(slice(None), d_end_month_1),].droplevel('last').rename(columns={'imei': d_end_month_1.strftime('%b%y')})
lst_mon = dm.loc[(slice(None), d_end_month_1),].rename(columns={'imei': d_end_month_1.strftime('%b%y')})
mtd = dm.loc[(slice(None), d_end_month),].droplevel('last').rename(columns={'imei': d_end_month.strftime('%b%y')})
# shift xaxis 1 month
lst_mon['xaxis'] = [(str(x[0]), (x[1] - timedelta(days=60)).date().strftime('%W %y')) for x in lst_mon.index]
lst_mon['xaxis1'] = [(str(x[0]), (x[1] - timedelta(days=5)).date().strftime('%W %y')) for x in lst_mon.index]
dfpie = dev_org.join(lst_mon, how='inner')
dfpie = dfpie.join(mtd, how='inner') #pd.concat([lst_mon,dev_org], axis=1, join='inner')# active devices (last not NaT) by org by month
by=d_end_month_1.strftime('%b%y')
mtd_txt=d_end_month.strftime('%b%y')
piescale= 15
dfpie['ang'] = dfpie[by]/dfpie.Total * 2*pi
dfpie['rad'] = piescale/(dfpie.Total.sum()/dfpie.Total)
dfpie['Tlab'] = dfpie['Total'].astype(str)
dfpie['Llab'] = dfpie[by].astype(str)
dfpie['Rlab'] = dfpie[mtd_txt].astype(str)
dfpie['ang1'] = dfpie[mtd_txt]/dfpie.Total * 2*pi
dfpie['rad1'] = piescale/(dfpie.Total.sum()/dfpie.Total)

# weekly data (can be M)
orgs_wk = df1.groupby(['org', pd.Grouper(level=0, freq='W')])['imei'].nunique().to_frame()
orgs_mn = df1.groupby(['org', pd.Grouper(level=0, freq='M')])['imei'].nunique().to_frame()

no_orgs = orgs_wk.index.unique('org').to_list()
org_cmap = factor_cmap('xaxis', palette=Spectral11, factors= no_orgs, end=1)
#x = [(str(x[0]), x[1].date().strftime('%b%y')) for x in dev_org_wk.index]
orgs_wk['xaxis'] = [(str(x[0]), x[1].date().strftime('%W %y')) for x in orgs_wk.index]

# stacked by org == dm?
org_stack_wk = df1.groupby([pd.Grouper(level=0, freq='W'), 'org'])['imei'].nunique().to_frame().unstack(level=1, fill_value=0).T.reset_index(level=0, drop=True).T#.unstack(level=1, fill_value=0)
org_stack_wk = org_stack_wk[sorted(org_stack_wk.columns)]
org_stack_wk['xaxis'] = [x.date().strftime('%W %y') for x in org_stack_wk.index] #mon_org_stack._get_label_or_level_values('last').astype('M8[D]').astype('str')
org_stack_wk['TWk'] = org_stack_wk.sum(axis=1, numeric_only=True)

ts = orgs_wk._get_label_or_level_values('last').astype('<M8[s]') # obhects to pd-timestmp
orgs_wk['ts'] = ts # converted to numpy.dt64 (int64) in CDS
#=== cut app here ===
source_orgs_wk = ColumnDataSource(orgs_wk)
spie = ColumnDataSource(dfpie)
source_org_stack_wk = ColumnDataSource(org_stack_wk)

p = figure(plot_width=800, plot_height=120, title=f"GM : Total vs Active devices by Org, for {by}",
           x_range = FactorRange(*source_orgs_wk.data['xaxis'].tolist()), outline_line_color=None, toolbar_location=None)
p.circle(x='xaxis', y=100, radius = 'rad', fill_color='white', fill_alpha=0.5, line_width=0.5, line_color='gray',source = spie)
p.wedge(x='xaxis', y=100, radius = 'rad', start_angle=0, end_angle='ang', source = spie, fill_color=org_cmap, fill_alpha=0.5)
#p.circle(x='xaxis1', y=100, radius = 'rad1', fill_color='white', fill_alpha=0.5, line_width=0.5, line_color='gray',source = spie)
#p.wedge(x='xaxis1', y=100, radius = 'rad1', start_angle=0, end_angle='ang1', source = spie, fill_color=org_cmap, fill_alpha=0.5)
p.xaxis.visible = False
p.xgrid.visible = False
p.yaxis.visible = False
p.ygrid.visible = False

label1 = Label(x=1, y=100, text='Fleet =', text_color='black', text_font_size = '10px', y_offset=15)
label2 = Label(x=1, y=100, text='Active =', text_color='black', text_font_size = '10px',y_offset=-30)
Tlabels = LabelSet(x='xaxis', y=100, text='Tlab', x_offset=-9, y_offset=15,
                  source=spie, text_color='black', text_font_size = '10px')
Llabels = LabelSet(x='xaxis', y=100, text='Llab', x_offset=-9, y_offset=-30,
                  source=spie, text_color='black', text_font_size = '10px')
Rlabels = LabelSet(x='xaxis1', y=100, text='Rlab', x_offset=-9, y_offset=-30,
                  source=spie, text_color='black', text_font_size = '10px')
p.add_layout(Tlabels)
p.add_layout(Llabels)
#p.add_layout(Rlabels)
p.add_layout(label1)
p.add_layout(label2)

p1 = figure(plot_width=800, plot_height=300, x_range =  p.x_range,
            title="Active by Org by week", title_location = 'left',
            outline_line_color=None, toolbar_location='right')

p1.add_tools(HoverTool(tooltips = [('Active', '@imei'), ("Week", "@ts{%W},@ts{%b}")],
          formatters = {'@ts' : 'datetime'},
          mode='vline'
          ))

p1.vbar(x='xaxis', top='imei', width=0.9, line_color="black",  source= source_orgs_wk, fill_color=org_cmap) #, fill_color=index_cmap, )
p1.xaxis[0].formatter = FuncTickFormatter(code="""
    if (index % 10 == 0)
    {
        return tick;
    }
    else
    {
        return "";
    }
    """)
p1.xaxis.major_tick_line_color = None  # turn off x-axis major ticks
p1.xaxis.minor_tick_line_color = None
p1.xaxis.major_label_text_font_size ='8px'
#p1.xaxis[0].major_tick_line_color = None
p1.xaxis.group_text_font_size = '11px'
p1.xaxis[0].major_label_orientation = "vertical"
p1.xgrid.visible = False
p1.ygrid.visible = False

p2 = figure(plot_width=800, plot_height=300, title="All Orgs : active devices by week",title_location = 'left',
            outline_line_color=None, toolbar_location=None, tools="", x_range=source_org_stack_wk.data['xaxis'])
p2.vbar_stack(x='xaxis', stackers=no_orgs, width=0.9,  source=source_org_stack_wk, color=org_cmap['transform'].palette[:len(no_orgs)]) #, view=view_org_stack_wk) #, source=source1
p2.xaxis[0].major_label_orientation = "vertical"
p2.xgrid.visible = False
p2.ygrid.visible = False
p2.xaxis[0].formatter = FuncTickFormatter(code="""
    if (index % 5 == 0)
    {
        return tick;
    }
    else
    {
        return "";
    }
    """)
p2.xaxis.major_tick_line_color = None  # turn off x-axis major ticks
p2.xaxis.minor_tick_line_color = None
columns = [TableColumn(field="xaxis", title="Weeks")]
for i in no_orgs:
    columns.append(TableColumn(field=i))
columns.append(TableColumn(field="TWk", title="Sum"))
data_table = DataTable(source=source_org_stack_wk, columns=columns, width=650, index_position=None, margin=(5,5,5,5))
#p2.extra_x_ranges = {'mon': }

layout = column(p,p1,p2, data_table)
#show(layout)
curdoc().add_root(layout)


'''
p = figure(height=350, title="Pie Chart", toolbar_location=None,
           tools="hover", tooltips="@country: @value", x_range=(-0.5, 1.0))

p.wedge(x=0, y=1, radius=0.4,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend_field='country', source=data)
'''

'''


N = 9
x = np.linspace(-2, 2, N)
y = x**2
r = x/15.0+0.3

source = ColumnDataSource(dict(x=x, y=y, r=r))

plot = Plot(
    title=None, width=300, height=300,
    min_border=0, toolbar_location=None)

glyph = Wedge(x="x", y="y", radius="r", start_angle=0.6, end_angle=4.1, fill_color="#b3de69")
plot.add_glyph(source, glyph)

xaxis = LinearAxis()
plot.add_layout(xaxis, 'below')

yaxis = LinearAxis()
plot.add_layout(yaxis, 'left')

plot.add_layout(Grid(dimension=0, ticker=xaxis.ticker))
plot.add_layout(Grid(dimension=1, ticker=yaxis.ticker))

curdoc().add_root(plot)

show(plot)
'''
