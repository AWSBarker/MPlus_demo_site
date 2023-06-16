# main bokeh app
'''
BP reading select device, dates
Health.eliot2
ranges
'''

from playhouse.dataset import DataSet
import pandas as pd
pd.set_option('use_inf_as_na', False)
from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Select,  BoxAnnotation, DatetimeTickFormatter, Label
from bokeh.models.widgets import DateRangeSlider
from bokeh.plotting import figure

db = DataSet('mysql://pi:7914920@192.168.1.173/Health')

bp = db['eliot2'].find(metadata_measurementType = 'BloodPressure')
dfall = pd.DataFrame([i for i in bp])
dfall.set_index('measurements_timestamp', drop=True, inplace=True)
dfall = dfall.sort_index(ascending=True)

devices = dfall.device_IMEI.unique().tolist()
dfall['dr'] = True

# initial values for first device on list full period
dfdev = dfall.loc[dfall['device_IMEI'] == devices[0]]
initvalue = (dfdev.index.min(), dfdev.index.max())

source1 = ColumnDataSource(dfdev)

# Create plots and widgets
plot1 = figure(plot_height=350, x_axis_type="datetime", y_axis_label='Pulse bpm. Systolic / Diastolic mmHg')
plot1.circle(x = 'measurements_timestamp', y = 'measurements_diastolicBloodPressure_value', size=5, fill_color= 'red', source = source1)
plot1.circle(x = 'measurements_timestamp', y = 'measurements_systolicBloodPressure_value', size=4, fill_color = 'orange', source = source1)
plot1.circle(x = 'measurements_timestamp', y = 'measurements_pulse_value', legend_label= 'Pulse', size=6, fill_color = 'blue', source = source1)
plot1.vbar(x = 'measurements_timestamp', top = 'measurements_systolicBloodPressure_value', bottom = 'measurements_diastolicBloodPressure_value', width = 0.1, fill_color = 'grey', source = source1)
plot1.add_layout(BoxAnnotation(bottom = 120,  top= 140, fill_alpha=0.1, fill_color='green', line_color='green'))
plot1.add_layout(BoxAnnotation(bottom=80, top = 90, fill_alpha=0.1, fill_color='green', line_color='green'))
textbox = Label(x=100, y=300, x_units='screen', y_units='screen', text='', render_mode='css', border_line_color='grey',
                  border_line_alpha=0.8, background_fill_color='grey', background_fill_alpha=0.3, text_font_size = '1em', text_font_style= 'bold')

plot1.add_layout(textbox)
plot1.legend.location = 'bottom_left'
plot1.x_range.range_padding = 0.05
plot1.xgrid.grid_line_color = None
#plot1.xaxis.major_label_orientation = 1.2
plot1.outline_line_color = None
plot1.xaxis.formatter=DatetimeTickFormatter(days=["%d/%m"])

def stats(d):
    print(f"stats {len(d['device_id'])}")
    if len(d['device_id']) > 1:
        s_mean = d['measurements_systolicBloodPressure_value'].mean()
        d_mean = d['measurements_diastolicBloodPressure_value'].mean()
        p_mean = d['measurements_pulse_value'].mean()
        n = len(d['device_id'])
        return f'Period average (N={n}): {s_mean:.0f}/{d_mean:.0f},   {p_mean:.0f}bpm    '
    else:
        return f'Period average not available '

textbox.text = stats(source1.data)
slider = DateRangeSlider(title='Period ', value=initvalue, start=initvalue[0], end=initvalue[1], width_policy='fit')
devices = Select(title="Device identifier IMEI", value=devices[0], options=devices, width_policy='min')

def newdev(attr, old, new):
    # need to reset daterange data
    dfdev = dfall.loc[dfall['device_IMEI'] == new]
    initvalue = (dfdev.index.min(), dfdev.index.max())
    slider.value = slider.start,slider.end = initvalue
    #slider.update()
    source1.data = dfdev
    textbox.text = stats(source1.data)

def newperiod(attr, old, new):
    beg, end = slider.value_as_datetime
    source1.data = dfdev[beg:end]
    textbox.text = stats(source1.data)

devices.on_change('value', newdev)
slider.on_change('value', newperiod)

layout = column(devices, slider, plot1)
curdoc().add_root(layout)
