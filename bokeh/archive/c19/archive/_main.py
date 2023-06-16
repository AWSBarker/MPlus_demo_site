'''
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
2.

cases risk

'''
from playhouse.dataset import DataSet
# MySQL database implementation that utilizes mysql-connector driver.
#db = MySQLConnectorDatabase('my_database', host='1.2.3.4', user='mysql')
#db = MySQLConnectorDatabase('Health', user='pi', password='7914920', host='192.168.1.173', port=3306)
import MySQLdb
import MySQLdb.cursors
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import row, widgetbox, column
from bokeh.models import ColumnDataSource, Select, CheckboxGroup, LabelSet
from bokeh.models.widgets import Slider, TextInput
from bokeh.plotting import figure
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

c = pd.DataFrame(db("SELECT * FROM countries"))
ghes = pd.DataFrame(db("SELECT ghe, C FROM ghe"))
xx = pd.DataFrame(db("SELECT Code, Code3 FROM countries"))

# setup lookup dicts
rdict = dict(zip(c['Name'],c['Region']))
xx_xxx = dict(zip(xx['Code3'], xx['Code'])) # Code GB = UK

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


# Set up initial data deaths to global average
x1 = [1,2,3,4,5,'']
y1 = [0,0,0,0,0,0]
l1 = ['','','','','','C19']
source1 = ColumnDataSource(data=dict(x=x1, y=y1, label= l1, color=Spectral6)) # dict of x:values= death rates and y : values = country/region/world

# Set up plot1
plot1 = figure(plot_height=400, plot_width=200, title='Standard Mortality Rate (per 1k)',
               tools="save", toolbar_location="above",
               #x_range=[0, 5], y_range=[0, 100], y_axis_type = 'log'
              )
labels = LabelSet(x='x', y='y', text='y', level='glyph',
        x_offset=-13.5, y_offset=0, source=source1, render_mode='canvas')

plot1.vbar(x='x', top='y', source=source1, width = 0.5, legend_field = 'l')

plot1a = figure(plot_height=400, plot_width=200, title='Plot 1a',
               #tools="save"
               #x_range=[0, 5], y_range=[0, 100], y_axis_type = 'log'
               )

plot1a.vbar(x='x', top='y', source=source1, width = 0.5)


# Set up plot2 (Pan1 top
x = [1,2,3,4,5,'']
y = [0,0,0,0,0,0]
l = ['','','','','','C19']
source2 = ColumnDataSource(data=dict(x=x, y=y, label= l, color=Spectral6)) # dict of x:values= death rates and y : values = country/region/world


plot2 = figure(plot_height=400, plot_width=400, title='Top 5 causes of death vs C19',
               tools="save", toolbar_location="above"
               #x_range=[0, 5], y_range=[0, 100], y_axis_type = 'log'
               )

plot2.vbar(x='x', top='y', source=source2, width = 0.5, legend_field = 'label', color='color')
plot2.yaxis[0].axis_label = 'Avg deaths per 1000 population vs C19.  Date adjusted'
plot2.legend.background_fill_alpha = 0.0
plot2.legend.label_text_font_size = '8pt'
plot2.xgrid.grid_line_color = None
plot2.ygrid.grid_line_color = None


pays = Select(title="Country of residence", value="Switzerland", options = sorted(c['Name'].to_list()))
reg = rdict[pays.value] #Select(title="Region", value="Vd", options = ddd['Switzerland'])
print(f'reg {reg}')
age = Slider(title="Your Age", value=0.0, start=0.0, end=100.0, step=1.0)
position = Select(title='SDS position /shirt No.', value = 'midfield', options=['goalio 10', 'midfield 10 ', 'attack 10', 'substitutio 10'])
gender = Select(title = "Current gender", value ="Females", options = gender)
ghe = Select(title="Comparator Disease Group", value="3.HIV/AIDS", options = ghes['C'].tolist())
timer = Slider(title="Look back X days", value=0, start=0, end=100, step=1)
#ghe2 = Select(title="Comparator Diseases", value="1.Tuberculosis", options = g['C'].tolist())

def update_data(attr, old, new):
    print(f'attr {attr} {old} {new}')

    # Get the current slider values
    #ps = pays.value
    reg = rdict[pays.value]  # Select(title="Region", value="Vd", options = ddd['Switzerland'])
    pinreg = c.Code3[c['Region'] == reg].to_list() # country list in region
    a = age.value
    ag = ages1[a] # age group 0-6

    p = c.loc[c['Name'] == pays.value, 'Code3'].values[0] # country code ARG

    s = gender.value
    g = ghes.loc[ghes['C'] == ghe.value, 'ghe'].values[0] # ghe code

    # initial DoY adjusted by timer
    t = timer.value
    DoY = dt.date.today().timetuple().tm_yday - t # DoY and C19 lookback = DoY plus timer (-1)

    def topx(a, c, s, DoY, x=5):
        # find topx by ghe / ghe desc, adjust DoY factor
        # create labels = C and values
        print(f' s {s} a {a}')
        d0 = db(f"SELECT g.ghe, g.C4, g.C, d.{c} FROM death d "
                f"inner join ghe g on g.ghe = d.ghe "
                f"WHERE g.C4!='' AND d.sex='{s}' AND d.age='{a}'")
        dp = pd.DataFrame(d0)

        top = dp.nlargest(x, {c})
        topx = top[f'{c}'].to_list()
        topx = [i * DoY/365 for i in topx] # df DoY
        topl = top['C'].apply(lambda x: x[2:]).to_list()
        return topx, topl

    def alldeath(s, c):
        # calc overall standardised death rate /1k all ages given sex, country
        # 10, 600, 1510 M + F per '000 poulation
        # adjust for DoY to compare with C19 (assume linear)
        g = (10, 600, 1510)
        a = (0, 1, 2, 3, 4, 5, 6)
        d = pd.DataFrame(db(f"SELECT {c} FROM death WHERE sex = '{s}' AND ghe IN {g} AND age IN {a}"))
        td = d[c].sum()  # tot deaths
        # pop = pd.DataFrame(db(f"SELECT {c} FROM pop WHERE sex = '{s}' AND age = '{a}'")) # pop of sex, age, country
        p = db(f"SELECT {c} FROM pop WHERE sex = '{s}' AND age IN {a}")  # tuples of 6 dicts per age group { CHE : pop}
        pop = sum(i['CHE'] for i in p)
        dY = DoY/365
        print(f'pop {pop} and td {td}')
        return (dY * 1000 * td / float(pop))

    def getC19daily(s, c, cxx, agx, t):
        DoYdate = dt.date.today() - dt.timedelta(t)
        print(f'DoYdate = {DoYdate}')
        try:
            d = pd.DataFrame(db(f"SELECT deaths, date FROM C19daily WHERE geoId = '{cxx}' AND date <= '{DoYdate}'"))
            #d = pd.DataFrame(db(f"SELECT deaths FROM C19daily WHERE geoId = '{cxx}'"))
        except:
            d = pd.DataFrame(db(f"SELECT deaths, date FROM C19daily WHERE geoId = '{cxx}'"))
            print(f'exception d = {d}')
        print(f' dinfo = {d.info()} {d.head()}')
        td = d['deaths'].sum()
        mf = gender_pct[s] # 60:40
        agp = age_group_pct[agx] # grp 6 = 90%
        po = db(f"SELECT {c} FROM pop WHERE sex = '{s}' AND age = '{agx}'")  # tuples of 6 dicts per age group { CHE : pop}
        pop = sum(i[c] for i in po)

        # tot death rate per  '000 pop =   td * m/f / float(pop))
        tdr = (td * mf * agp) / pop

        print(f' po {po} mf {mf} td {td} tdrc19 {tdr} pays {c} cxx {cxx} s {s} ag {agx} agp {agp}')
        return tdr

    # Generate the new data country, region and labels
    d1,labs = topx(ag, p, s, DoY)

    cxx = xx_xxx[f'{p}']
    tdrc19 = getC19daily(s, p, cxx, ag, t)

    d1.append(tdrc19)
    labs.append('Covid19')

    # Generate the data for region
    #d2, = db(f"SELECT {p} FROM death WHERE sex = '{s}' AND ghe = '{g}' AND age = '{ag}'")
    #d2 = sum(list(d2.values()))
    print(f'DoY-t {DoY} group {ag} country {p} reg , {reg} sex {s} , ghe {g} d1 {d1}  labs = {labs}')

    x = list(range(1,len(d1)+1))# +2 for C19
    y = d1

    source2.data = dict(x=x, y=y, label=labs, color=Spectral6)

# pays.on_change('value', update_data)
# reg.on_change('value', update_data)

for w in [pays, age, gender, timer, position]:
    w.on_change('value', update_data)

# Set up layouts and add to document
inputs = column(pays, age, position, gender, timer)

#layout = column(location,p, name='g1')
#curdoc().add_root(layout)
layout1 = row(inputs, plot1, plot1a) #, name='g1')
layout2 = row(inputs, plot2) #, name='g1')

pan2 = Panel(child=layout1, title = 'Cases, Deaths vs Region')
pan1 = Panel(child=layout2, title = 'Top Mortality Rates vs Covid19')
tabs = Tabs(tabs = [pan1, pan2], name = 'g1')

curdoc().add_root(tabs) #row(inputs, plot, width=800, name='g1'))
curdoc().title = "C19 Risks"