# main view
# see is view filter changes data
#
# try 2nd axis with glucose

from playhouse.dataset import DataSet
import pandas as pd
import numpy as np
pd.set_option('use_inf_as_na', False)
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import CDSView, GroupFilter, BooleanFilter, ColumnDataSource, Select,  BoxAnnotation, DatetimeTickFormatter, Label,  Range1d, LinearAxis
from bokeh.models.widgets import DateRangeSlider, Tabs, Panel
from bokeh.plotting import figure

db = DataSet('mysql://pi:7914920@192.168.1.173/Health')

bp = db['eliot2'] #.find(metadata_measurementType = 'BloodPressure')
dfall = pd.DataFrame([i for i in bp])
dfall.set_index('measurements_timestamp', drop=True, inplace=True)
dfall = dfall.sort_index(ascending=True)

#devices = dfall.device_IMEI.unique().tolist()
bp_devices = dfall['device_IMEI'].loc[dfall.metadata_measurementType != 'BodyWeightComposition'].unique().tolist()
bc_devices = dfall['device_IMEI'].loc[dfall.metadata_measurementType == 'BodyWeightComposition'].unique().tolist()

# initial values for last device on list full period
df_bp = dfall.loc[dfall['device_IMEI'].isin(bp_devices)]
df_bc = dfall.loc[dfall['device_IMEI'].isin(bc_devices)]

bp_initvalue = (df_bp.index.min(), df_bp.index.max())
bc_initvalue = (df_bc.index.min(), df_bc.index.max())
bpbools = (df_bp.index > df_bp.index.min()) & (df_bp.index < df_bp.index.max()) # truncates min , max
bcbools = (df_bp.index > df_bp.index.min()) & (df_bp.index < df_bp.index.max()) # truncates min , max

source1 = ColumnDataSource(df_bp)
view1 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])
view2 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])
view3 = CDSView(source=source1, filters= [GroupFilter(column_name='device_IMEI', group=bp_devices[0]), BooleanFilter(bpbools)])

source2 = ColumnDataSource(df_bc)
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

plot2 = figure(plot_height=350, x_axis_type="datetime", y_axis_label='BC', tools="save", toolbar_location="below")
plot2.circle(x = 'measurements_timestamp', y = 'measurements_bodyWeight_value', legend_label= 'Weight', size=6, fill_color = 'red', source = source2, view=viewbc)
plot2.xaxis.formatter=DatetimeTickFormatter(days=["%d/%m"])

def bp_stats(d):
    #print(f"bp_stats {len(d['device_id'])}")
    if len(d['device_id']) > 1:
        s_mean = np.nanmean(d['measurements_systolicBloodPressure_value'])
        d_mean = np.nanmean(d['measurements_diastolicBloodPressure_value'])
        p_mean = np.nanmean(d['measurements_pulse_value'])
        n = len(d['device_id'])
        return f'Period average (N={n}): {s_mean:.0f}/{d_mean:.0f},   {p_mean:.0f}bpm    '
    else:
        return f'Period average not available, not enough data '

textbox.text = bp_stats(source1.data)
bpslider = DateRangeSlider(title='Period ', value=bp_initvalue, start=bp_initvalue[0], end=bp_initvalue[1], width_policy='auto')
bpdevices = Select(title="Device identifier IMEI", value=bp_devices[0], options=bp_devices, width=140)

bcslider = DateRangeSlider(title='Period ', value=bc_initvalue, start=bc_initvalue[0], end=bc_initvalue[1], width_policy='auto')
bcdevices = Select(title="Device identifier IMEI", value=bc_devices[0], options=bc_devices, width=140)

def newbp(attr, old, new):
    view1.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    view2.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    view3.filters[0] = GroupFilter(column_name='device_IMEI', group=new)
    d = df_bp.loc[df_bp.device_IMEI == new]
    textbox.text = bp_stats(d)
    beg,end = (d.index.min(), d.index.max())
    bpslider.update(value = (beg,end), start = beg, end = end)

def bpperiod(attr, old, new):
    beg, end = bpslider.value_as_datetime
    bpbools = (df_bp.index >= beg) & (df_bp.index <= end)
    view1.filters[1] = BooleanFilter(bpbools)
    view2.filters[1] = BooleanFilter(bpbools)
    view3.filters[1] = BooleanFilter(bpbools)
    textbox.text = bp_stats(df_bp[beg:end].loc[df_bp.device_IMEI == bpdevices.value])

def newbc(attr, old, new):
    viewbc.filters[0] = GroupFilter(column_name='device_IMEI', group= new)
    d = df_bc.loc[df_bc.device_IMEI == new]
    #textbox.text = bp_stats(d)
    beg,end = (d.index.min(), d.index.max())
    bcslider.update(value = (beg,end), start = beg, end = end)

def bcperiod(attr, old, new):
    beg, end = bcslider.value_as_datetime
    bcbools = (df_bc.index >= beg) & (df_bc.index <= end)
    viewbc.filters[1] = BooleanFilter(bcbools)
    #textbox.text = bp_stats(df_bp[beg:end].loc[df_bp.device_IMEI == bpdevices.value])

bpdevices.on_change('value', newbp)
bpslider.on_change('value', bpperiod)
bcdevices.on_change('value', newbc)
bcslider.on_change('value', bcperiod)

layout1 = column(bpdevices, bpslider, plot1)
layout2 = column(bcdevices, bcslider, plot2)

pan2 = Panel(child=layout2, title = 'BC')
pan1 = Panel(child=layout1, title = 'BP & BG')
tabs = Tabs(tabs = [pan1,pan2], name = 'g1')

curdoc().add_root(tabs)
