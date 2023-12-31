# TODO one DF with views GroupFilter = device.
# bokeh 2.3 used in conda c19 on acer -  on VESA source /media/Data2/ve_sharepoint/bin/activate
# date slider not min/max but start last 5
# view1 only for Plot1 bp
# try bc plot with bars, varea_stack with index not ts
# main view copied 17.10
# add colour db
# see is view filter changes data
## try 2nd axis with glucose
import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
pd.set_option('use_inf_as_na', False)
pd.options.mode.chained_assignment = None  # default='warn' stops copying
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import CDSView, GroupFilter, BooleanFilter, ColumnDataSource, Select,  BoxAnnotation, DatetimeTickFormatter, Label,  Range1d, LinearAxis, Slider, HoverTool, Legend
from bokeh.models.widgets import DateRangeSlider, Tabs, Panel
from bokeh.plotting import figure
import MySQLdb
import random

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


def db_get(sql):
    db = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi', password='7914920', db='Health', charset='utf8')
    with db.cursor() as cur:# setup the dropdowns dicts
        cur.execute(sql) #"SELECT * FROM countries")
        r = cur.fetchall() # tuple of dicts ({},{}..  ,)
    db.close()
    return r

def db_post(data, sql): #data is list of tuples
    try:
        db = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi', password='7914920', db='Health', charset='utf8')
        with db.cursor() as cur:
            cur.executemany(sql, data)
            db.commit()
        print(f'updated {data}')
    except MySQLdb.Error as e:
        print(f'mysql error {e}')
    finally:
        db.close()

def makecolour(): # return single random coour
    return ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)])]

cols = ['device_IMEI', 'device_model','measurements_timestamp', 'measurements_pulse_value','measurements_pulse_unit', 'measurements_systolicBloodPressure_value',
        'measurements_diastolicBloodPressure_value', 'measurements_glucose_value', 'measurements_glucose_unit', 'measurements_bodyWeight_value', 'r',
        'measurements_cholesterol_value', 'measurements_cholesterol_unit',
        'measurements_uricacid_value', 'measurements_uricacid_unit',
        'measurements_ketone_value', 'measurements_ketone_unit',
        'measurements_temperature_value', 'measurements_temperature_unit',
        'measurements_SpO2_value', 'measurements_SpO2_unit',
        'metadata_measurementType'
        ]

dba = db_get("SELECT device_IMEI, device_model, measurements_timestamp, "
             "measurements_pulse_value, measurements_pulse_unit, measurements_systolicBloodPressure_value, measurements_diastolicBloodPressure_value, measurements_glucose_value, measurements_glucose_unit, measurements_bodyWeight_value, measurements_bodyComposition_value, "
             "measurements_cholesterol_value, measurements_cholesterol_unit,"
             "measurements_uricacid_value, measurements_uricacid_unit,"
             "measurements_ketone_value, measurements_ketone_unit,"
             "measurements_temperature_value, measurements_temperature_unit,"
             "measurements_SpO2_value, measurements_SpO2_unit,"
             "metadata_measurementType from eliot2"
             )

dfall = pd.DataFrame([i for i in dba], columns=cols)
dfall.set_index('measurements_timestamp', drop=True, inplace=True)
dfall = dfall.sort_index(ascending=True)


for bm in ['pulse','cholesterol', 'ketone', 'uricacid', 'glucose', 'temperature', 'SpO2']:
    bmv = f'measurements_{bm}_value'
    bmu = f'measurements_{bm}_unit'
    dfall.loc[dfall[bmv].notnull(),'value'] = dfall[bmv]
    dfall.loc[dfall[bmu].notnull(),'unit'] = dfall[bmu]

# BC data
tuptup = db_get("SELECT device_IMEI, measurements_bodyComposition_value, measurements_bodyWeight_value, measurements_timestamp, colour from eliot2 WHERE metadata_measurementType = 'BodyWeightComposition'")
dfbc = pd.DataFrame([i for i in tuptup],columns=['device_IMEI', 'r', 'w', 'ts', 'colour'])
dfbc.set_index('ts', drop=True, inplace=True)
dfbc = dfbc.sort_index(ascending=True)
dfbc.r.replace(0, pd.NA, inplace=True)
dfbc = dfbc.assign(bmr = 0, fat = 0, h2o = 0, bone = 0, id = np.arange(len(dfbc))) # initial bmr line that will be overridden in BodyComp

# BP data
#dfbp = dfall.loc[dfall['metadata_measurementType'] != 'BodyWeightComposition']
dfbp = dfall.loc[(dfall.device_model.str.contains('BP800')) | (dfall.device_model.str.contains('D40'))]

# Gtel data
dfgtel = dfall.loc[dfall['device_model'] == 'GTEL']

# GW data
dfgw = dfall.loc[dfall['device_model'] == 'GW9017']

bp_devices = dfbp['device_IMEI'].unique().tolist()
bc_devices = dfbc['device_IMEI'].unique().tolist()
gtel_devices = dfgtel['device_IMEI'].unique().tolist()
gw_devices = dfgw['device_IMEI'].unique().tolist()

# gwinitvalue back 1 month as default all
oneMonBack = dt.datetime.now()-timedelta(days=30)
bp_initvalue = (oneMonBack, dfbp.index.max())
bc_initvalue = (oneMonBack, dfbc.index.max())
gtel_initvalue = (oneMonBack, dfgtel.index.max())
gw_initvalue = (oneMonBack, dfgw.index.max())

bpbools = (dfbp.index > dfbp.index.min()) & (dfbp.index < dfbp.index.max()) # truncates min , max
bcbools = (dfbp.index > dfbp.index.min()) & (dfbp.index < dfbp.index.max()) # truncates min , max
gtelbools = (dfgtel.index >= dfgtel.index.min()) & (dfgtel.index <= dfgtel.index.max()) # truncates min , max
gwbools = (dfgw.index >= dfgw.index.min()) & (dfgw.index <= dfgw.index.max()) # truncates min , max

source1 = ColumnDataSource(dfbp)
view1 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])
#view2 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])
#view3 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])

source2 = ColumnDataSource(dfbc)
source3 = ColumnDataSource(dfbc)
viewbc = CDSView(source=source2, filters= [GroupFilter(column_name='device_IMEI', group=bc_devices[0]), BooleanFilter(bcbools)])

source4 = ColumnDataSource(dfgtel)
mggtel = CDSView(source=source4, filters= [GroupFilter(column_name='device_IMEI', group=gtel_devices[0]),GroupFilter(column_name='unit', group='mg/dL'), BooleanFilter(gtelbools)])
mmolgtel = CDSView(source=source4, filters= [GroupFilter(column_name='device_IMEI', group=gtel_devices[0]), GroupFilter(column_name='unit', group='mmol/L'), BooleanFilter(gtelbools)])

source5 = ColumnDataSource(dfgw)
view5 = CDSView(source=source5, filters= [GroupFilter(column_name='device_IMEI', group=gw_devices[0]), BooleanFilter(gwbools)])

# create GTEL plot
# TCH 50 -300 mg/dL | 2 - 10 mmol/L # bK 0 - 18 mg/dL | 0 - 3 mmol/L # BG 50 - 200 mg/dL | 4 - 10 mmol/L # UA 1 - 10 mg/dL   | 0 - 1 mmol/L
plot_gtel = figure(plot_width= 640, plot_height=350, x_axis_type="datetime", x_axis_label='measurement date',
                   y_range = (0,250), y_axis_label='Circles = mg/dL ', tools="", toolbar_location=None)
plot_gtel.square(x = 'measurements_timestamp', y = 'measurements_ketone_value', legend_label= 'bKetone', size=8, fill_color = 'blue', source = source4, view=mmolgtel, y_range_name='mmol')
plot_gtel.square(x = 'measurements_timestamp', y = 'measurements_uricacid_value', legend_label= 'Uric Acid', size=8, fill_color = 'orange', source = source4, view=mmolgtel, y_range_name='mmol')
plot_gtel.square(x = 'measurements_timestamp', y = 'measurements_cholesterol_value', legend_label= 'Total Cholesterol', size=8, fill_color = 'green', source = source4, view=mmolgtel, y_range_name='mmol')
plot_gtel.square(x = 'measurements_timestamp', y = 'measurements_glucose_value', legend_label= 'Glucose', size=8, fill_color = 'yellow', source = source4, view=mmolgtel, y_range_name='mmol')

plot_gtel.circle(x = 'measurements_timestamp', y = 'measurements_ketone_value', legend_label= 'bKetone', size=8, fill_color = 'blue', source = source4, view=mggtel)
plot_gtel.circle(x = 'measurements_timestamp', y = 'measurements_uricacid_value', legend_label= 'Uric Acid', size=8, fill_color = 'orange', source = source4, view=mggtel)
plot_gtel.circle(x = 'measurements_timestamp', y = 'measurements_cholesterol_value', legend_label= 'Total Cholesterol', size=8, fill_color = 'green', source = source4, view=mggtel)
plot_gtel.circle(x = 'measurements_timestamp', y = 'measurements_glucose_value', legend_label= 'Glucose', size=8, fill_color = 'yellow', source = source4, view=mggtel)

plot_gtel.legend.location = 'center'
plot_gtel.legend.click_policy='hide'
plot_gtel.legend.orientation='horizontal'
plot_gtel.legend.title = 'Biomarkers : Click to view. Hover for units'
plot_gtel.xaxis.formatter=DatetimeTickFormatter(days=["%d.%m.%y"], microseconds=["%T"]) #, hours=['%h'], microseconds=["%c"])
plot_gtel.extra_y_ranges = {"mmol": Range1d(start=0, end=12)}
plot_gtel.add_layout(LinearAxis(y_range_name="mmol", axis_label='Squares =  mmol/L ', major_tick_line_color = None), 'right')
h = HoverTool(tooltips = [("units", "@unit"), ("value", "$y{0.0}"), ("time", "$x{%H:%M:%S}")], formatters={'$x':'datetime'})
plot_gtel.add_tools(h)
plot_gtel.add_layout(plot_gtel.legend[0], 'above')

# Create BP plots and widgets
plot1 = figure(plot_height=350, plot_width= 640, x_axis_type="datetime", x_axis_label = 'measurement date',
               y_range=(10,230), y_axis_label='Pulse bpm. Sys/Dia mmHg', tools="save", toolbar_location= None)
plot1.circle(x = 'measurements_timestamp', y = 'measurements_pulse_value', legend_label= 'Pulse', size=6, fill_color = 'red', source = source1, view=view1)
plot1.vbar(x = 'measurements_timestamp', top = 'measurements_systolicBloodPressure_value', bottom = 'measurements_diastolicBloodPressure_value',
           legend_label= 'BP', width = 0.2, fill_color = 'blue', source = source1, view=view1)

plot1.circle(x = 'measurements_timestamp', y = 'measurements_glucose_value', legend_label= 'BG', size=7, fill_color = 'yellow', source = source1,view=view1, y_range_name ='bg' )
plot1.extra_y_ranges = {"bg": Range1d(start=20, end=300)}
p1textbox = Label(x=150, y=200, x_units='screen', y_units='screen', text='-', render_mode='css',  text_font_size = '1em', text_font_style= 'bold') # border_line_color='grey',border_line_alpha=0.8, background_fill_color='grey', background_fill_alpha=0.3,
plot1.add_layout(p1textbox)
plot1.legend.location = 'center'
plot1.legend.click_policy='hide'
plot1.legend.orientation='horizontal'
plot1.legend.title = 'BP Pulse BG - click to toggle view'
plot1.x_range.range_padding = 0.05
plot1.xgrid.grid_line_color = None
#plot1.xaxis.major_label_orientation = 1.2
plot1.outline_line_color = None
plot1.xaxis.formatter=DatetimeTickFormatter(days=["%d.%m.%y"])
plot1.add_layout(LinearAxis(y_range_name="bg", axis_label=' BG mg/dL ', major_tick_line_color = None), 'right')
plot1.add_layout(BoxAnnotation(bottom = 120,  top= 140, fill_alpha=0.1, fill_color='green', line_color='green'))
plot1.add_layout(BoxAnnotation(bottom=80, top = 90, fill_alpha=0.1, fill_color='green', line_color='green'))
plot1.add_layout(plot1.legend[0], 'above')

# create a new plot and add a renderer
TOOLS = "box_select,reset, hover"
weights = figure(plot_height=350, plot_width= 640, x_axis_type="datetime", y_axis_label='Kg', tools=TOOLS, toolbar_location = 'above', title = '3. Select your weight measurements')
weights.background_fill_color = "grey"
weights.background_fill_alpha = 0.1
weights.circle(x = 'ts', y = 'w', size=8, line_color='colour', line_width=2, fill_color = 'colour', hover_color="firebrick", source=source2, selection_color="orange", view=viewbc)

# create another new plot, add a renderer that uses the view of the data source
bodycomp = figure(plot_height=350, plot_width= 640, y_axis_label='Body Composition estimate', toolbar_location = None, y_range=(0,130), title='5. Body composition for selected measurements')
bodycomp.varea_stack(stackers=['fat', 'mus', 'h20', 'bone'], x = 'id', source=source3, color=['yellow', 'brown', 'blue', 'grey'])#brewer['Spectral'][4]) #, view=view)
#bodycomp.vbar_stack(['fat', 'mus', 'h20', 'bone'], x = 'id',  source=source3, color=['yellow', 'brown', 'blue', 'grey'])

labelstyle = {'x_units' :'screen', 'y_units' :'screen', 'text' :'', 'render_mode' :'css', 'border_line_color' : 'grey',
                  'border_line_alpha' : 0.8, 'background_fill_color' : 'white', 'background_fill_alpha' : 0.5, 'text_font_size' : '1.2em', 'text_font_style' : 'bold'}
fatbox = Label(x = 100, y = 20, **labelstyle)
musbox = Label(x=100, y=110, **labelstyle)
h20box = Label(x=100, y=200, **labelstyle)
bodycomp.add_layout(musbox)
bodycomp.add_layout(fatbox)
bodycomp.add_layout(h20box)
bodycomp.extra_y_ranges = {"foo": Range1d(start=1000, end=2500)}
bodycomp.add_layout(LinearAxis(y_range_name="foo", axis_label=' basal metabolic rate kcal', major_tick_line_color = None), 'right')
bodycomp.line(x='id', y='bmr', line_width = 4, line_color='black', y_range_name='foo', legend_label='metabolic rate', source=source3)
bodycomp.legend.location='top_right'
bodycomp.legend.background_fill_alpha=0
bodycomp.legend.border_line_color = None

# Create GW plots
gwplot1 = figure(plot_height=250, plot_width= 640, x_axis_type="datetime", x_axis_label = None,
               y_range=(10,230), y_axis_label='BP Sys/Dia mmHg ', tools= '', toolbar_location=None)
gwplot1.vbar(x = 'measurements_timestamp', top = 'measurements_systolicBloodPressure_value', bottom = 'measurements_diastolicBloodPressure_value',
           legend_label= 'Blood Pressure', width = timedelta(minutes=120), fill_color = 'blue', source = source5, view=view5)
gwplot1.circle(x = 'measurements_timestamp', y = 'measurements_pulse_value',   y_range_name='P', legend_label= 'Pulse', size=8, fill_color = 'red', source = source5, view=view5)

gwplot2 = figure(plot_height=250, plot_width= 640, x_axis_type="datetime", x_axis_label = 'measurement date',
               y_range=(88,101), y_axis_label='SpO2 %', tools='', toolbar_location= None)
gwplot2.star(x = 'measurements_timestamp', y = 'measurements_SpO2_value',  legend_label= 'SpO2', size=10, fill_color = 'blue', source = source5,view=view5)
gwplot2.circle(x = 'measurements_timestamp', y = 'measurements_temperature_value',  y_range_name='T', legend_label= 'Body Temp', size=8, fill_color = 'orange', source = source5, view=view5)

gwplot1.extra_y_ranges = {"P": Range1d(start=30, end=90)}
gwtextbox = Label(x=150, y=10, x_units='screen', y_units='screen', text='-', render_mode='css',  text_font_size = '1em', text_font_style= 'bold') # border_line_color='grey',border_line_alpha=0.8, background_fill_color='grey', background_fill_alpha=0.3,
gwplot1.add_layout(gwtextbox)
gwplot1.legend.location = 'center'
gwplot1.legend.click_policy='hide'
gwplot1.legend.orientation='horizontal'
gwplot1.legend.title = 'Gateway devices : BP, Pulse - click to toggle'
gwplot1.xaxis.visible = False
gwplot1.yaxis.axis_label_text_color = 'blue'
gwplot1.yaxis.axis_line_color = 'blue'
gwplot1.outline_line_color = None
gwplot1.add_layout(BoxAnnotation(bottom = 120,  top= 140, fill_alpha=0.1, fill_color='green', line_color='green'))
gwplot1.add_layout(BoxAnnotation(bottom=80, top = 90, fill_alpha=0.1, fill_color='green', line_color='green'))
gwplot1.add_layout(LinearAxis(y_range_name="P", axis_label='Pulse bpm', axis_line_color = 'red', axis_label_text_color='red', major_label_text_color = 'red'), 'right')

gwplot2.extra_y_ranges = {"T": Range1d(start=30, end=42)}
#gwplot2.add_layout(gwtextbox)
gwplot2.legend.location = 'center'
gwplot2.legend.click_policy='hide'
gwplot2.legend.orientation='horizontal'
#gwplot2.legend.title = 'Gateway devices : SpO2, Temperature - click one to toggle view'
gwplot2.xaxis.formatter=DatetimeTickFormatter(days=["%d.%m.%y"])
gwplot2.yaxis.axis_label_text_color = 'blue'
gwplot2.add_layout(LinearAxis(y_range_name="T", axis_label='body Temp ', axis_label_text_color='orange', major_label_text_color = 'orange'), 'right')

h1 = HoverTool(tooltips = [("units", "@unit"), ("bpm", "@measurements_pulse_value{0}"), ("sys", "@measurements_systolicBloodPressure_value"),
                           ("dia", "@measurements_diastolicBloodPressure_value"), ("time", "$x{%H:%M:%S}")], formatters={'$x':'datetime'})
h2 = HoverTool(tooltips = [("units", "@unit"), ("value", "$y{0.0}"), ("time", "$x{%H:%M:%S}")], formatters={'$x':'datetime'})
gwplot1.add_tools(h1)
gwplot2.add_tools(h2)
gwplot1.add_layout(gwplot1.legend[0], 'above')
gwplot2.add_layout(gwplot2.legend[0], 'above')

def colour_post(newcolour): # input colour post to eliot2-colour db
    c = [(newcolour, gender.value, age.value, height.value)]
    db_post(c, """INSERT INTO eliot2_colour (colour, sex, age, height) VALUES (%s,%s,%s,%s) 
        ON DUPLICATE KEY UPDATE colour=VALUES(colour),sex=VALUES(sex),age=VALUES(age),height=VALUES(height)""")

def selection_change(attrname, old, new):
    global selected
    selected = source2.selected.indices
    # get existing colour values from selection and find most freqent to get s,a,h into slider init values
    cs = source2.data[('colour')][selected].astype(str).tolist()
    if len(cs) >= 1:
        cs_maxf = max(set(cs), key=cs.count)
        _, gender.value, age.value, height.value = db_get(f"SELECT colour, sex, age, height from eliot2_colour WHERE colour = '{cs_maxf}'")[0]
        newcolour = makecolour() * len(selected)
        patches = {'colour': list(zip(selected, newcolour)),}
        source2.patch(patches)
        LofT = list(zip(source2.data[('ts')][selected].astype(str).tolist(), source2.data[('colour')][selected].astype(str).tolist()))
        db_post(LofT, """INSERT INTO eliot2 (measurements_timestamp, colour) VALUES (%s,%s) ON DUPLICATE KEY UPDATE colour=VALUES(colour)""")
        colour_post(newcolour[0])
    else:
        print('nothon selected')
        pass

def updater(attr, old, new):
    if selected:
        if gender.value == 'Man':
            source3.data = BodyComp(dfbc.iloc[selected], age.value, height.value).man()
        else:
            source3.data = BodyComp(dfbc.iloc[selected], age.value, height.value).woman()
        try:
            fatbox.text = f"Fat : {np.nanmean(source3.data['fat']):.1f} %"
            musbox.text = f"Muscle : {np.nanmean(source3.data['mus']):.1f} %"
            h20box.text = f"Water : {np.nanmean(source3.data['h20']):.1f} %"
        except:
            print(f"some error {source3.data['fat']}")
        colour_post(source2.data['colour'][selected][0])

def newbc(attr, old, new):
    viewbc.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = dfbc.loc[dfbc.device_IMEI == new]
    beg,end = (d.index.min(), d.index.max())
    bcslider.update(value = (beg,end), start = beg, end = end)

def bcperiod(attr, old, new):
    beg, end = bcslider.value_as_datetime
    bcbools = (dfbc.index >= beg) & (dfbc.index <= end)
    viewbc.filters[1] = BooleanFilter(bcbools)

def bp_stats(d):
    if len(d['device_IMEI']) >= 1:
        s_mean = np.nanmean(d['measurements_systolicBloodPressure_value'])
        d_mean = np.nanmean(d['measurements_diastolicBloodPressure_value'])
        p_mean = np.nanmean(d['measurements_pulse_value'])
        n = len(d['device_IMEI'])
        return f'Period average (N={n}): {s_mean:.0f}/{d_mean:.0f},   {p_mean:.0f}bpm    '
    else:
        return f'Period average not available, not enough data '

def newbp(attr, old, new):
    view1.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = dfbp.loc[dfbp.device_IMEI == new]
    beg,end = (d.index.min(), d.index.max())
    bpslider.update(value = (beg,end), start = beg, end = end)

def bpperiod(attr, old, new):
    beg, end = bpslider.value_as_datetime
    bpbools = (dfbp.index >= beg) & (dfbp.index <= end)
    view1.filters[1] = BooleanFilter(bpbools)
    p1textbox.text = bp_stats(dfbp[beg:end].loc[dfbp.device_IMEI == bpdevices.value])

def newgtel(attr, old, new):
    mggtel.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    mmolgtel.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = dfgtel.loc[dfgtel.device_IMEI == new]
    beg,end = (d.index.min(), d.index.max())
    gtelslider.update(value = (beg,end), start = beg, end = end)

def gtelperiod(attr, old, new):
    beg, end = gtelslider.value_as_datetime
    gtelbools = (dfgtel.index >= beg) & (dfgtel.index <= end)
    mggtel.filters[2] = BooleanFilter(gtelbools)
    mmolgtel.filters[2] = BooleanFilter(gtelbools)

def newgw(attr, old, new):
    view5.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = dfgw.loc[dfgw.device_IMEI == new]
    beg,end = (d.index.min(), d.index.max())
    gwslider.update(value = (beg,end), start = beg, end = end)

def gwperiod(attr, old, new):
    beg, end = gwslider.value_as_datetime
    gwbools = (dfgw.index >= beg) & (dfgw.index <= end)
    view5.filters[1] = BooleanFilter(gwbools)
    gwtextbox.text = bp_stats(dfgw[beg:end].loc[dfgw.device_IMEI == gwdevices.value])

bcslider = DateRangeSlider(title='2 Period ', value=bc_initvalue, start=dfbc.index.min(), end=bc_initvalue[1], width_policy='auto')
bcdevices = Select(title="1. Select Select by IMEI", value=bc_devices[0], options=bc_devices, width=140)

gender = Select(title="4. Select your statistics", value='Woman', options=['Man', 'Woman'], width_policy='min')
height = Slider(start=100, end=200, value=155, step=1, title="Height cm", width_policy='auto')
age = Slider(start=18, end=99, value=49, step=1, title="Age", width_policy='auto')

gteldevices = Select(title="Select by IMEI", value=gtel_devices[0], options=gtel_devices, width=140)
gtelslider = DateRangeSlider(title='Period ', value=gtel_initvalue, start=dfgtel.index.min(), end=gtel_initvalue[1], width_policy='auto')

p1textbox.text = bp_stats(source1.data)
bpslider = DateRangeSlider(title='Period ', value=bp_initvalue, start=dfbp.index.min(), end=bp_initvalue[1], width_policy='auto')
bpdevices = Select(title="Select by IMEI", value=bp_devices[0], options=bp_devices, width=140)

gwtextbox.text = bp_stats(source5.data)
gwslider = DateRangeSlider(title='Period ', value=gw_initvalue, start=dfgw.index.min(), end=gw_initvalue[1], width_policy='auto')
gwdevices = Select(title="Select by IMEI", value=gw_devices[0], options=gw_devices, width=140)

gteldevices.on_change('value', newgtel)
gtelslider.on_change('value', gtelperiod)

bpdevices.on_change('value', newbp)
bpslider.on_change('value', bpperiod)

bcdevices.on_change('value', newbc)
bcslider.on_change('value', bcperiod)
source2.selected.on_change('indices', selection_change, updater)
gender.on_change('value', updater)
height.on_change('value', updater)
age.on_change('value', updater)

gwdevices.on_change('value', newgw)
gwslider.on_change('value', gwperiod)

layout1 = column(bpdevices, bpslider, plot1)
pan1 = Panel(child=layout1, title = 'BP800 or D40g')

tt = column(bcdevices, bcslider)
top = row(weights)
mid = row(gender)
bot = row(age)
layout2 = column(tt, top, mid, height, bot, bodycomp)
pan2 = Panel(child=layout2, title = 'BC800')

layout3 = column(gteldevices, gtelslider, plot_gtel)
pan3 = Panel(child=layout3, title = 'GTEL')

layout4 = column(gwdevices, gwslider, gwplot1, gwplot2)
pan4 = Panel(child=layout4, title = 'Gateway')

tabs = Tabs(tabs = [pan1, pan2, pan3, pan4], name = 'g1')
curdoc().add_root(tabs)
