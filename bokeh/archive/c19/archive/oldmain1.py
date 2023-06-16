'''
columns of text inside bars
add panel 2 cases and deaths
try optimise sources and add 2nd panel sources
TRY with df sql
1. select country , age, sex and disease to get death rate vs region vs global
Present an interactive function explorer with slider widgets.
Scrub the sliders to change the properties of the ``sin`` curve, or
type into the title cord box to update the title of the plot.
Use the ``bokeh serve`` command to run the example by executing:
    bokeh serve sliders.py
at your command prompt. Then navigate to the URL
    http://localhost:5006/sliders
in your browser.

death risk: age, locale, comorb (db,cv,can, resp, oth)
A select cases or death
1. your age
2. c19 local deaths / local population
cases risk
'''
from playhouse.dataset import DataSet
import MySQLdb
import MySQLdb.cursors
import numpy as np
import pandas as pd
pd.set_option('use_inf_as_na', False)
from bokeh.io import curdoc
from bokeh.layouts import row, widgetbox, column
from bokeh.models import ColumnDataSource, Select, CDSView, GroupFilter, BooleanFilter, DatetimeTickFormatter, LabelSet, Label
from bokeh.models.glyphs import Text
from bokeh.models.widgets import Slider, TextInput
from bokeh.plotting import figure
from bokeh.models import LinearAxis, Range1d
from bokeh.models.widgets import Tabs, Panel
from bokeh.palettes import Spectral6
import datetime as dt

def db(sql):
    connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi',
                             password='7914920',  db='Health', cursorclass=MySQLdb.cursors.DictCursor)
    conn = connection.cursor()# setup the dropdowns dicts
    conn.execute(sql) #"SELECT * FROM countries")
    r = conn.fetchall() # tuple of dicts ({},{}..  ,)
    conn.close()
    connection.close()
    return r

df_pays = pd.DataFrame(db("SELECT * FROM countries"))
ghes = pd.DataFrame(db("SELECT ghe, C FROM ghe"))

dfall = pd.DataFrame(db("SELECT * FROM C19daily"))
dfall.set_index(['geoId', 'date'], drop=False, inplace=True)
dfall = dfall.sort_index(ascending=True)

# map subregions
am = df_pays.set_index('Code')[['SRegion', 'Name']]
dfall['sr'] = dfall['geoId'].map(am['SRegion']).astype('str')
dfall['Name'] = dfall['geoId'].map(am['Name'])
dfall['geoId'] = dfall['geoId'].astype('str')

gbl0 = dfall.groupby(level=0)
gb10_cases = gbl0['cases']
gb10_deaths = gbl0['deaths']

dfall['ccases'] = gb10_cases.cumsum()
dfall['dpc_cases'] = gb10_cases.pct_change(fill_method='pad', periods=7)
dfall['cdeaths'] = gb10_deaths.cumsum()
dfall['dpc_deaths'] = gb10_deaths.pct_change(fill_method='pad', periods=7)
dfall = dfall.replace([np.inf, -np.inf], np.nan)
dfall['dmax'] = gb10_deaths.rolling(7, min_periods=1).mean().reset_index(0,drop=True)
dfall['cmax'] = gb10_cases.rolling(7, min_periods=1).mean().reset_index(0,drop=True)
dfall['ccolor'] = ['bisque' if c >= 0 else 'lightgreen' for c in dfall.dpc_cases]
dfall['dcolor'] = ['bisque' if c >= 0 else 'lightgreen' for c in dfall.dpc_deaths]
# id in each group
dfall['id'] = gbl0.cumcount()

gball = dfall.groupby(level=0)
gbcm = gball.cmax
i = gbcm.transform('idxmax')
dfall['id'] = dfall.id - dfall.loc[i, 'id'].values

# normalise cases again max in each group
#dfall['dfm_cmax'] = dfall.groupby(level=0).cmax.transform('max') - dfall.cmax
dfall['dfm_cmax'] = dfall.cmax / gbcm.transform('max')

# post peak days and last data for labelset
gbtmp = gball.last().reset_index(drop=True)
gblast = gbtmp.set_index('sr')
postpeakd = dt.date.today().timetuple().tm_yday - 60

# swiss
age_group_pct = {0: 0.0001, 1: 0.0002, 2: 0.005, 3: 0.01, 4: 0.01, 5: 0.07, 6: 0.90}
gender_pct = {'Males': 0.60, 'Females': 0.40}
gender = ['Males', 'Females']
ages = {0:list(range(5)), 1 : list(range(5,15)), 2 : list(range(15,30)), 3 : list(range(30,50)),
        4 : list(range(50,60)), 5 : list(range(60,70)), 6 : list(range(70,100))} #0.01,'20-30':0.03,'30-40':0.08,'40-50': 0.16,'50-60':0.6,'60-70':1.9, '70-80':4.3, '80':7.8}
ages1 = {} # age : range i.e. 75 : 6
for k,v in ages.items():
    for i in v:
        ages1[i] = k

# Set up initial plots data
x = [0,0,0,0,0,0]
y = [0,0,0,0,0,0]
y0 = [0,0,0,0,0,0]
#l = ['','','','','','C19']
z = [0,0,0,0,0,0]
text = ['', '', '', '', '', '']


source = ColumnDataSource(data=dict(x=x, y=y, y0=y0, text= text, color=Spectral6)) # dict of x:values= death rates and y : values = country/region/world
source1 = ColumnDataSource(dfall)
source2 = ColumnDataSource(df_pays)
source3 = ColumnDataSource(gblast)

bools = [True if c == 'CH' else False for c in source1.data['geoId']]
view3 = CDSView(source=source1, filters=[GroupFilter(column_name='sr', group='Western Europe')])
view4 = CDSView(source=source1, filters=[GroupFilter(column_name='geoId', group='CH')])
view1 = CDSView(source = source1,
                filters=[GroupFilter(column_name='geoId', group='CH'),
                        BooleanFilter([True if c > 250 else False for c in source1.data['ccases']]
                        )])
TOOLTIPS = [(("(days, cases"), "($x, $y)"), ("Country", "@Name")]

# panel1 plot morbidity risk
plot = figure(plot_height=400, plot_width=640, title='Top 5 causes of death vs C19',
               tools="save", toolbar_location="right")
#plot.vbar(x='x', top='y', source=source, width = 0.5, color='color') #, legend_field = 'label'
plot.hbar(y='x', left = 0, right = 'y', source=source, height = 0.6, color='color') #, legend_field = 'label'

plot.xaxis[0].axis_label = 'Avg deaths per 1k pop. adjusted to period YTD'
#plot.legend.background_fill_alpha = 0.0
#plot.legend.label_text_font_size = '8pt'
plot.xgrid.grid_line_color = None
plot.ygrid.grid_line_color = None
plot.yaxis.visible = False

textcol = Text(x = 'y0', y = 'x', text = 'text', y_offset = 10, x_offset = 1, text_color="black", text_font_size = '1em')
plot.add_glyph(source, textcol)

textbox = Label(x=350, y=250, x_units='screen', y_units='screen', text=('.'), render_mode='css', border_line_color='grey',
                  border_line_alpha=0.8, background_fill_color='grey', background_fill_alpha=0.3, text_font_size = '1.1em', text_font_style= 'bold')
textbox1 = Label(x=350, y=230, x_units='screen', y_units='screen', text=('.'), render_mode='css', border_line_color='grey',
                  border_line_alpha=0.8, text_font_size = '1em')
textbox2 = Label(x=350, y=210, x_units='screen', y_units='screen', text=('.'), render_mode='css', border_line_color='grey',
                  border_line_alpha=0.8, text_font_size = '1em')

#x_units='screen', y_units='screen', text=(''), render_mode='css',
#                  border_line_color='grey', border_line_alpha=0.8, background_fill_color='grey', background_fill_alpha=0.3, text_font_size = '1em')
plot.add_layout(textbox)
plot.add_layout(textbox1)
plot.add_layout(textbox2)

# Set up plot1 cases
plot1 = figure(plot_height=200, x_axis_type="datetime", y_axis_label='cases', title = 'Cases, Deaths, Daily changes, Post peak regional comparison', tools="save", toolbar_location="above")
plot1.vbar(x = 'date', top = 'dpc_cases', width=dt.timedelta(days=0.5), source=source1, view=view1, color='ccolor', y_range_name ='foo')
plot1.circle(x = 'date', y = 'ccases', size=5, source = source1, view = view1) #, legend_label = 'cases')
#plot1.vbar(x = 'Region_geoId', top = 'deaths_mean', width=0.7, source=source1, color = 'red')
# Setting the second y axis range name and range
plot1.extra_y_ranges = {"foo": Range1d(start=-2, end=5)}
# Adding the second axis to the plot.
plot1.add_layout(LinearAxis(y_range_name="foo", axis_label=' %  daily  change  vs ', major_tick_line_color = None), 'right')
plot1.background_fill_color = "grey"
plot1.background_fill_alpha = 0.1
plot1.xaxis.visible = False
plot1.x_range.range_padding = 0.1
plot1.xgrid.grid_line_color = None
#plot1.xaxis.major_label_orientation = 1.2
plot1.outline_line_color = None
plot1.toolbar.autohide = True
plot1.yaxis[0].ticker.desired_num_ticks = 3
plot1.yaxis[1].ticker.desired_num_ticks = 3

#plot1.legend.location = 'top_left'

# Arrange plot2 deaths
plot2 = figure(plot_height=200, x_axis_type="datetime", y_axis_label='deaths', tools="save", toolbar_location="above")
plot2.vbar(x = 'date', top = 'dpc_deaths', width=dt.timedelta(days=0.5), source=source1, view=view1, color='dcolor', y_range_name ='foo')
plot2.circle(x = 'date', y = 'cdeaths', size=5, source = source1, view = view1, color = 'red') #, legend_label = 'deaths' )
# Setting the second y axis range name and range
plot2.extra_y_ranges = {"foo": Range1d(start=-2, end=5)}
# Adding the second axis to the plot.
plot2.add_layout(LinearAxis(y_range_name="foo", axis_label=' 1 week rolling mean  ', minor_tick_line_color = None), 'right')
plot2.background_fill_color = "grey"
plot2.background_fill_alpha = 0.2
#plot2.x_range.range_padding = 0.1
plot2.xgrid.grid_line_color = None
plot2.xaxis.formatter=DatetimeTickFormatter(days=["%d/%m"])
#plot2.xaxis.major_label_orientation = 1.2
plot2.outline_line_color = None
plot2.toolbar.autohide = True
plot2.yaxis[0].ticker.desired_num_ticks = 3
plot2.yaxis[1].ticker.desired_num_ticks = 3

# plot 3 is subregional compaison
plot3 = figure(plot_height=400, plot_width = 600, title = 'Regional comparison of Post peak performance', x_axis_type='linear', x_range = (-1,postpeakd), x_axis_label='days since peak', y_axis_label='Post peak case ratio (country/region) Peak = 1', y_range = (0, 1.1), tooltips=TOOLTIPS, tools="save", toolbar_location="above")

plot3.line(x='id', y='dfm_cmax', source=source1, view=view3)
plot3.line(x='id', y='dfm_cmax', source=source1, view=view4, color = 'red', line_width = 2.0) #,  color = Category20)
source3.data = gblast.loc['Western Europe']
label = LabelSet(x='id', y='dfm_cmax', text='Name', text_font_size='0.9em', x_offset=1, y_offset=1, source=source3)


pays = Select(title="Country of residence", value="Switzerland", options = sorted(df_pays['Name'].to_list()))
reg = df_pays[df_pays['Name'] == pays.value].Region.iloc[0]  #rdict[pays.value] #Select(title="Region", value="Vd", options = ddd['Switzerland'])
print(f'reg {reg}')
xx = df_pays[df_pays['Name'] == pays.value].Code.iloc[0]

age = Slider(title="Your Age", value=55.0, start=0.0, end=100.0, step=1.0)
position = Select(title='SDS position /shirt No.', value = 'midfield 10', options=['goalio 10', 'midfield 10 ', 'attack 10', 'substitutio 10', 'elswhere 10'])
gender = Select(title = "Current gender", value ="Females", options = gender)
#ghe = Select(title="Comparator Disease Group", value="3.HIV/AIDS", options = ghes['C'].tolist())
timer = Slider(title="Look back X days", value=0, start=0, end=100, step=1)
#ghe2 = Select(title="Comparator Diseases", value="1.Tuberculosis", options = g['C'].tolist())

def update_data(attr, old, new):
    print(f'attr {attr} {old} {new}')
    # Get the current slider values
    reg = df_pays[df_pays['Name'] == pays.value].SRegion.iloc[0] #rdict[pays.value]  # Select(title="Region", value="Vd", options = ddd['Switzerland'])
    xx = df_pays[df_pays['Name'] == pays.value].Code.iloc[0]

    view1.filters[0] = GroupFilter(column_name='geoId', group=xx)
    view3.filters[0] = GroupFilter(column_name='sr', group=reg)
    view4.filters[0] = GroupFilter(column_name='geoId', group=xx)
    source3.data = gblast.loc[reg]

    a = age.value
    ag = ages1[a] # age group 0-6
    p = df_pays.loc[df_pays['Name'] == pays.value, 'Code3'].values[0] # country code ARG
    s = gender.value
    #g = ghes.loc[ghes['C'] == ghe.value, 'ghe'].values[0] # ghe code
    # initial DoY adjusted by timer
    t = timer.value
    DoY = dt.date.today().timetuple().tm_yday - t # DoY and C19 lookback = DoY plus timer (-1)

    def topx(a, c, s, DoY, x=5):
        # find topx by ghe / ghe desc, adjust DoY factor
        # create labels = C and values
#       print(f' s {s} a {a}')
        d0 = db(f"SELECT g.ghe, g.C4, g.C, d.{c} FROM death d "
                f"inner join ghe g on g.ghe = d.ghe "
                f"WHERE g.C4!='' AND d.sex='{s}' AND d.age='{a}'")
        dp = pd.DataFrame(d0)

        top = dp.nlargest(x, {c})
        topx = top[f'{c}'].to_list()
        topx = [i * DoY/365 for i in topx] # df DoY
        topl = top['C'].apply(lambda x: x[2:]).to_list()
        print(topl)
        #topl = [' '.join(topl[i:i + 3]) + '\n' for i in range(0, len(topl), 3)]
        return topx, topl

    def alldeath(c, s=('Males', 'Females')):
        # all causes death by country
            #DoY = dt.date.today().timetuple().tm_yday - t
            g = (10, 600, 1510)
            a = (0, 1, 2, 3, 4, 5, 6)
            d = pd.DataFrame(db(f"SELECT {c} FROM death WHERE sex IN {s} AND ghe IN {g} AND age IN {a}"))
            td = d[c].sum()  # tot deaths
            # pop = pd.DataFrame(db(f"SELECT {c} FROM pop WHERE sex = '{s}' AND age = '{a}'")) # pop of sex, age, country
            p = db(f"SELECT {c} FROM pop WHERE sex IN {s} AND age IN {a}")  # tuples of 6 dicts per age group { CHE : pop}
            pop = sum(i[c] for i in p)
            dY = 1 #DoY / 365
            print(f'pop {pop} and td {td}')
            return [(dY * 1000 * td / float(pop))]

    def selectdeath(c, a, s):
        # mortality rate in selected group
            #DoY = dt.date.today().timetuple().tm_yday - t
            g = (10, 600, 1510)
            a = a
            s = s
            d = pd.DataFrame(db(f"SELECT {c} FROM death WHERE g IN {g} AND sex ='{s}'' AND age = '{a}'"))
            td = d[c].sum()  # tot deaths
            # pop = pd.DataFrame(db(f"SELECT {c} FROM pop WHERE sex = '{s}' AND age = '{a}'")) # pop of sex, age, country
            p = db(f"SELECT {c} FROM pop WHERE sex IN {s} AND age IN {a}")  # tuples of 6 dicts per age group { CHE : pop}
            pop = sum(i[c] for i in p)
            dY = 1 #DoY / 365
            print(f'pop {pop} and td {td}')
            return [(dY * 1000 * td / float(pop))]

    def getC19daily(s, c, cxx, agx, t):
        DoYdate = dt.date.today() - dt.timedelta(t)
        print(f'DoYdate = {DoYdate}')
        try:
            d = pd.DataFrame(db(f"SELECT cases, deaths, date FROM C19daily WHERE geoId = '{cxx}' AND date <= '{DoYdate}'"))
            #d = pd.DataFrame(db(f"SELECT deaths FROM C19daily WHERE geoId = '{cxx}'"))
        except:
            d = pd.DataFrame(db(f"SELECT deaths, date FROM C19daily WHERE geoId = '{cxx}'"))
            print(f'exception d = {d}')
        #print(f' dinfo = {d.info()} {d.head()}')

        d['dpc_cases'] = d['cases'].pct_change(periods=7)
        d['dpc_deaths'] = d['deaths'].pct_change(periods=7)
        #posneg = ['g' if c >= 0 else 'r' for c in d.dpc_cases]

        td = d['deaths'].sum()
        mf = gender_pct[s] # 60:40
        agp = age_group_pct[agx] # grp 6 = 90%
        po = db(f"SELECT {c} FROM pop WHERE sex = '{s}' AND age = '{agx}'")  # tuples of 6 dicts per age group { CHE : pop}
        pop = sum(i[c] for i in po)
        # tot death rate per  '000 pop =   td * m/f / float(pop))
        tdr = (td * mf * agp) / pop
        #print(f' po {po} mf {mf} td {td} tdrc19 {tdr} pays {c} cxx {cxx} s {s} ag {agx} agp {agp}')
        return tdr

    def getC19daily_lastupd(c):
        return db(f"SELECT date FROM C19daily WHERE geoId = '{c}' ORDER by date DESC")[0]['date'].strftime('%m-%d')

    # Generate the new data country, region and labels
    d1,labs = topx(ag, p, s, DoY)
    cxx = df_pays[df_pays['Code3'] == p].Code.iloc[0]
    tdrc19 = getC19daily(s, p, cxx, ag, t)
    ld = getC19daily_lastupd(cxx)
    d1.append(tdrc19)
    labs.append(f'Covid19') # at date=
    #print(f'DoY-t {DoY} group {ag} country {p} reg , {reg} sex {s} , d1 {d1}  labs = {labs}')
    x = list(range(1,len(d1)+1))# +2 for C19
    #x = labs
    y = d1
    z = alldeath(p)

    source.data = dict(y=y, x=x, y0 = y0, text=labs, color=Spectral6)
    textbox.text=(f'Annual Mortality per 1k pop')
    textbox1.text = (f'All causes + all persons =  {z[0]:.2f}')
    textbox2.text = (f'Within the selected group = {y[1]:.2f}')

    plot.title.text = f'{pays.value[:15]} {s} of {a} years: Top 5 causes of death vs C19 (updated {ld})'
    #source1.data = dict(x=x, y=y) #, label=labs)

for w in [pays, age, gender, timer, position]:
    w.on_change('value', update_data)

# Set up slider layouts and add to document
inputs2 = column(pays)
row1 = row(pays, age)
row2 = row(position)
row3 = row(gender, timer)
inputs1 = column(row1, row2, row3) #pays, age, position, gender, timer)

#layout = column(location,p, name='g1')
#curdoc().add_root(layout)
plot3.add_layout(label)
layout2 = column(inputs2, plot1, plot2, plot3) #, name='g1')
layout1 = column(inputs1, plot) #, name='g1')

pan2 = Panel(child=layout2, title = 'Cases, Deaths vs Region')
pan1 = Panel(child=layout1, title = 'Top Mortality Rates vs C19')
tabs = Tabs(tabs = [pan1, pan2], name = 'g1')

curdoc().add_root(tabs) #row(inputs, plot, width=800, name='g1'))
curdoc().title = "C19 Risks"