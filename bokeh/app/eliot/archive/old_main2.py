# main view copied 8.10
# see is view filter changes data
#
# try 2nd axis with glucose

import pandas as pd
import numpy as np
pd.set_option('use_inf_as_na', False)
pd.options.mode.chained_assignment = None  # default='warn' stops copying
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import CDSView, GroupFilter, BooleanFilter, ColumnDataSource, Select,  BoxAnnotation, DatetimeTickFormatter, Label,  Range1d, LinearAxis, Slider
from bokeh.models.widgets import DateRangeSlider, Tabs, Panel
from bokeh.plotting import figure
import MySQLdb

class BodyComp:
    ''' Bone is in Kg, height cm, age years, res in Ohm as array
    returns df t, age, height, weight, res, fat, mus, h20, bone, bmr
    '''

    def __init__(self,df, age, height):
        self.df = df
        self.df['age'] = age
        self.df['height'] = height
        #self.weight = weight
        #self.res = res

    def man(self):
        self.df['fat'] = 24.1911 + (0.0463 * self.df['age']) - (0.460888 * self.df['height']) + (0.6341581 * self.df['w']) + (
                        0.0566524 * self.df['r'])
        self.df['mus'] =  66.4907 - (0.1919*self.df['age'])+(0.2279*self.df['height'])-(0.402*self.df['w'])-(0.0514*self.df['r'])
        self.df['h20'] = (84 - self.df['fat']) * 0.92
        self.df['bone'] = 2.1191 - (0.00213 * self.df['age']) + (0.0059 * self.df['height']) + (0.010501 * self.df['w']) - (0.001599 * self.df['r'])
        self.df['bmr'] = 382.3347 - (2.441487 * self.df['age']) + (4.5998 * self.df['height']) + (14.5974 * self.df['w']) - (1.097371 * self.df['r'])
        return self.df

    def woman(self):
        self.df['fat'] = 40.6912 + (0.0443 * self.df['age']) -(0.5008 * self.df['height']) + (0.7042 * self.df['w']) + (0.0449 * self.df['r'])
        self.df['mus'] = 65.9907 - (0.1919 * self.df['age']) + (0.2278 * self.df['height']) - (0.402 * self.df['w']) - (0.0514 * self.df['r'])
        self.df['h20'] = (80 - self.df['fat']) * 0.96
        self.df['bone'] = 1.4191 - (0.00213 * self.df['age']) + (0.0105 * self.df['height']) + (0.0205 * self.df['w']) - (
                    0.0026 * self.df['r'])
        self.df['bmr'] = 58.3347 - (2.4414 * self.df['age']) + (6.7997 * self.df['height']) + (12.5974 * self.df['w']) - (
                    0.6073 * self.df['r'])
        return self.df


def db(sql, dbase='Health'):
    connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi', password='7914920',  db=dbase, charset='utf8')#, cursorclass=MySQLdb.cursors.DictCursor)
    with connection.cursor() as conn:# setup the dropdowns dicts
        conn.execute(sql) #"SELECT * FROM countries")
        r = conn.fetchall() # tuple of dicts ({},{}..  ,)
    connection.close()
    return r

cols = ['device_IMEI', 'measurements_timestamp', 'measurements_pulse_value', 'measurements_systolicBloodPressure_value',
        'measurements_diastolicBloodPressure_value', 'measurements_glucose_value', 'measurements_bodyWeight_value',
        'measurements_pulse_value', 'r', 'metadata_measurementType'
        ]

dba = db("SELECT device_IMEI, measurements_timestamp, measurements_pulse_value, measurements_systolicBloodPressure_value,"
          " measurements_diastolicBloodPressure_value, measurements_glucose_value, measurements_bodyWeight_value, "
          "measurements_pulse_value, measurements_bodyComposition_value, metadata_measurementType from eliot2")

dfall = pd.DataFrame([i for i in dba], columns=cols)
dfall.set_index('measurements_timestamp', drop=True, inplace=True)
dfall = dfall.sort_index(ascending=True)

dfbc = dfall.loc[dfall['metadata_measurementType'] == 'BodyWeightComposition']
dfbc.drop(columns=['metadata_measurementType','measurements_systolicBloodPressure_value', 'measurements_diastolicBloodPressure_value', 'measurements_glucose_value',  'measurements_pulse_value'], inplace=True)
dfbc.rename_axis(index={'measurements_timestamp': 'ts'}, inplace=True)
dfbc.rename(columns={'measurements_bodyWeight_value' : 'w' }, inplace=True)
dfbc.r.replace(0, pd.NA, inplace=True)
dfbc['bmr'] = 0 # initial bmr line that will be overridden in BodyComp
dfbp = dfall.loc[dfall['metadata_measurementType'] != 'BodyWeightComposition']

#devices = dfall.device_IMEI.unique().tolist()
bp_devices = dfbp['device_IMEI'].unique().tolist()
bc_devices = dfbc['device_IMEI'].unique().tolist()

bp_initvalue = (dfbp.index.min(), dfbp.index.max())
bc_initvalue = (dfbc.index.min(), dfbc.index.max())
bpbools = (dfbp.index > dfbp.index.min()) & (dfbp.index < dfbp.index.max()) # truncates min , max
bcbools = (dfbp.index > dfbp.index.min()) & (dfbp.index < dfbp.index.max()) # truncates min , max

source1 = ColumnDataSource(dfbp)
view1 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])
view2 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])
view3 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])

source2 = ColumnDataSource(dfbc)
source3 = ColumnDataSource(dfbc)
viewbc = CDSView(source=source2, filters= [GroupFilter(column_name='device_IMEI', group=bc_devices[0]), BooleanFilter(bcbools)])

# Create plots and widgets
plot1 = figure(plot_height=350, x_axis_type="datetime", y_axis_label='Pulse bpm. Systolic / Diastolic mmHg', tools="save", toolbar_location="below")
#plot1.circle(x = 'measurements_timestamp', y = 'measurements_diastolicBloodPressure_value', size=5, fill_color= 'red', source = source1, view=view2)
#plot1.circle(x = 'measurements_timestamp', y = 'measurements_systolicBloodPressure_value', size=4, fill_color = 'orange', source = source1, view=view2)
plot1.circle(x = 'measurements_timestamp', y = 'measurements_pulse_value', legend_label= 'Pulse', size=6, fill_color = 'red', source = source1, view=view2)
plot1.vbar(x = 'measurements_timestamp', top = 'measurements_systolicBloodPressure_value', bottom = 'measurements_diastolicBloodPressure_value',
           legend_label= 'BP', width = 0.2, fill_color = 'blue', source = source1, view=view1)

plot1.circle(x = 'measurements_timestamp', y = 'measurements_glucose_value', legend_label= 'BG', size=7, fill_color = 'yellow', source = source1,view=view3, y_range_name ='foo' )
plot1.extra_y_ranges = {"foo": Range1d(start=50, end=180)}
plot1.add_layout(LinearAxis(y_range_name="foo", axis_label=' BG mg/dL ', major_tick_line_color = None), 'right')

plot1.add_layout(BoxAnnotation(bottom = 120,  top= 140, fill_alpha=0.1, fill_color='green', line_color='green'))
plot1.add_layout(BoxAnnotation(bottom=80, top = 90, fill_alpha=0.1, fill_color='green', line_color='green'))
textbox = Label(x=100, y=280, x_units='screen', y_units='screen', text='', render_mode='css', border_line_color='grey',
                  border_line_alpha=0.8, background_fill_color='grey', background_fill_alpha=0.3, text_font_size = '1em', text_font_style= 'bold')

plot1.add_layout(textbox)
plot1.legend.location = 'bottom_left'
plot1.legend.click_policy='hide'
plot1.legend.orientation='horizontal'
plot1.legend.title = 'click a item to toggle view'
plot1.x_range.range_padding = 0.05
plot1.xgrid.grid_line_color = None
#plot1.xaxis.major_label_orientation = 1.2
plot1.outline_line_color = None
plot1.xaxis.formatter=DatetimeTickFormatter(days=["%d/%m"])

TOOLS = "box_select,hover,help"
left = figure(plot_height=350, plot_width= 480, x_axis_type="datetime", y_axis_label='Kg', tools=TOOLS, toolbar_location = 'above', title = '1. Select your weight measurements')
left.circle(x = 'ts', y = 'w', size=8, fill_color = 'red', hover_color="firebrick", source=source2, selection_color="orange", view=viewbc)

# create another new plot, add a renderer that uses the view of the data source
right = figure(plot_height=350, plot_width= 550,x_axis_type="datetime", y_axis_label='4. Body Composition estimate', toolbar_location = None, y_range=(0,130), title='3. Body composition for selected measurements')
right.varea_stack(stackers=['fat', 'mus', 'h20', 'bone'], x = 'ts', source=source3, color=['yellow', 'brown', 'blue', 'grey'])#brewer['Spectral'][4]) #, view=view)

right.extra_y_ranges = {"foo": Range1d(start=1000, end=2500)}
right.add_layout(LinearAxis(y_range_name="foo", axis_label=' basal metabolic rate kcal', major_tick_line_color = None), 'right')
right.line(x='ts', y='bmr', line_width = 4, line_color='black', y_range_name='foo', legend_label='metabolic rate', source=source3)
right.legend.location='top_right'
right.legend.background_fill_alpha=0
right.legend.border_line_color = None

labelstyle = {'x_units' :'screen', 'y_units' :'screen', 'text' :'', 'render_mode' :'css', 'border_line_color' : 'grey',
                  'border_line_alpha' : 0.8, 'background_fill_color' : 'white', 'background_fill_alpha' : 0.5, 'text_font_size' : '1.2em', 'text_font_style' : 'bold'}
fatbox = Label(x = 100, y = 20, **labelstyle)
musbox = Label(x=100, y=110, **labelstyle)
h20box = Label(x=100, y=200, **labelstyle)
right.add_layout(musbox)
right.add_layout(fatbox)
right.add_layout(h20box)

bcslider = DateRangeSlider(title='2 Period ', value=bc_initvalue, start=bc_initvalue[0], end=bc_initvalue[1], width_policy='auto')
bcdevices = Select(title="1. Select Device identifier IMEI", value=bc_devices[0], options=bc_devices, width=140)

gender = Select(title="3. Select your statistics", value='Woman', options=['Man', 'Woman'], width_policy='min')
height = Slider(start=100, end=200, value=155, step=1, title="Height cm", width_policy='auto')
age = Slider(start=18, end=99, value=49, step=1, title="Age", width_policy='auto')

def selection_change(attrname, old, new):
    #print(attrname, new)
    global selected
    selected = source2.selected.indices
    #pre.text = f'{source2.selected.indices}'

def updater(attr, old, new):
    if gender.value == 'Man':
        source3.data = BodyComp(dfbc.iloc[selected], age.value, height.value).man()
    else:
        source3.data = BodyComp(dfbc.iloc[selected], age.value, height.value).woman()
    #print(f'new is {new}, gender {gender.value} age {age.value} height {height.value}')
    #print(f'{source3.data}')
    fatbox.text = f"Fat : {np.nanmean(source3.data['fat']):.1f} %"
    musbox.text = f"Muscle : {np.nanmean(source3.data['mus']):.1f} %"
    h20box.text = f"Water : {np.nanmean(source3.data['h20']):.1f} %"

def newbc(attr, old, new):
    viewbc.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = dfbc.loc[dfbc.device_IMEI == new]
    #textbox.text = bp_stats(d)
    beg,end = (d.index.min(), d.index.max())
    bcslider.update(value = (beg,end), start = beg, end = end)

def bcperiod(attr, old, new):
    beg, end = bcslider.value_as_datetime
    bcbools = (dfbc.index >= beg) & (dfbc.index <= end)
    viewbc.filters[1] = BooleanFilter(bcbools)
    #textbox.text = bp_stats(dfbp[beg:end].loc[dfbp.device_IMEI == bpdevices.value])

bcdevices.on_change('value', newbc)
bcslider.on_change('value', bcperiod)
source2.selected.on_change('indices', selection_change, updater)
gender.on_change('value', updater)
height.on_change('value', updater)
age.on_change('value', updater)

def bp_stats(d):
    #print(f"bp_stats {len(d['device_id'])}")
    if len(d['device_IMEI']) > 1:
        s_mean = np.nanmean(d['measurements_systolicBloodPressure_value'])
        d_mean = np.nanmean(d['measurements_diastolicBloodPressure_value'])
        p_mean = np.nanmean(d['measurements_pulse_value'])
        n = len(d['device_IMEI'])
        return f'Period average (N={n}): {s_mean:.0f}/{d_mean:.0f},   {p_mean:.0f}bpm    '
    else:
        return f'Period average not available, not enough data '

textbox.text = bp_stats(source1.data)
bpslider = DateRangeSlider(title='Period ', value=bp_initvalue, start=bp_initvalue[0], end=bp_initvalue[1], width_policy='auto')
bpdevices = Select(title="Device identifier IMEI", value=bp_devices[0], options=bp_devices, width=140)

def newbp(attr, old, new):
    view1.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    view2.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    view3.filters[0] = GroupFilter(column_name='device_IMEI', group=new)
    d = dfbp.loc[dfbp.device_IMEI == new]
    textbox.text = bp_stats(d)
    beg,end = (d.index.min(), d.index.max())
    bpslider.update(value = (beg,end), start = beg, end = end)

def bpperiod(attr, old, new):
    beg, end = bpslider.value_as_datetime
    bpbools = (dfbp.index >= beg) & (dfbp.index <= end)
    view1.filters[1] = BooleanFilter(bpbools)
    view2.filters[1] = BooleanFilter(bpbools)
    view3.filters[1] = BooleanFilter(bpbools)
    textbox.text = bp_stats(dfbp[beg:end].loc[dfbp.device_IMEI == bpdevices.value])

def newbc(attr, old, new):
    viewbc.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = dfbc.loc[dfbc.device_IMEI == new]
    #textbox.text = bp_stats(d)
    beg,end = (d.index.min(), d.index.max())
    bcslider.update(value = (beg,end), start = beg, end = end)

def bcperiod(attr, old, new):
    beg, end = bcslider.value_as_datetime
    bcbools = (dfbc.index >= beg) & (dfbc.index <= end)
    viewbc.filters[1] = BooleanFilter(bcbools)
    #textbox.text = bp_stats(dfbp[beg:end].loc[dfbp.device_IMEI == bpdevices.value])

bpdevices.on_change('value', newbp)
bpslider.on_change('value', bpperiod)

layout1 = column(bpdevices, bpslider, plot1)
pan1 = Panel(child=layout1, title = 'BP & BG')

tt = column(bcdevices, bcslider)
top = row(left)
mid = row(gender)
bot = row(age)
layout2 = column(tt, top, mid, height, bot, right)
pan2 = Panel(child=layout2, title = 'BC')

tabs = Tabs(tabs = [pan1,pan2], name = 'g1')

curdoc().add_root(tabs)
