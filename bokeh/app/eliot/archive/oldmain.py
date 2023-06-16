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
from bokeh.layouts import row, widgetbox, column
from bokeh.models import ColumnDataSource, CDSView, GroupFilter, Select,  BoxAnnotation, DatetimeTickFormatter
from bokeh.models.widgets import Slider, DateRangeSlider
from bokeh.models.filters import BooleanFilter
from bokeh.plotting import figure

db = DataSet('mysql://pi:7914920@192.168.1.173/Health')

bp = db['eliot2'].find(metadata_measurementType = 'BloodPressure')
dfall = pd.DataFrame([i for i in bp])
dfall.set_index('measurements_timestamp', drop=True, inplace=True)
dfall = dfall.sort_index(ascending=True)

dfall['dr'] = True
#dfall['dr'] = (dfall.index >= '2020-7-23') & (dfall.index <= '2020-8-30')

source1 = ColumnDataSource(dfall)
view1 = CDSView(source = source1, filters=[GroupFilter(column_name='device_IMEI', group='358173054439511'),
                                           BooleanFilter(dfall['dr'])
                                           ])

# Create plots and widgets
plot1 = figure(plot_height=350, x_axis_type="datetime", y_axis_label='Pulse bpm. Systolic / Diastolic mmHg')
plot1.circle(x = 'measurements_timestamp', y = 'measurements_diastolicBloodPressure_value', size=5, fill_color= 'red', source = source1, view = view1 )
plot1.circle(x = 'measurements_timestamp', y = 'measurements_systolicBloodPressure_value', size=4, fill_color = 'orange', source = source1, view = view1 )
plot1.circle_x(x = 'measurements_timestamp', y = 'measurements_pulse_value', size=6, fill_color = 'blue', source = source1, view = view1 )
plot1.vbar(x = 'measurements_timestamp', top = 'measurements_systolicBloodPressure_value', bottom = 'measurements_diastolicBloodPressure_value', width = 0.1, fill_color = 'grey', source = source1, view = view1)
plot1.add_layout(BoxAnnotation(bottom = 120,  top= 140, fill_alpha=0.1, fill_color='green', line_color='green'))
plot1.add_layout(BoxAnnotation(bottom=80, top = 90, fill_alpha=0.1, fill_color='green', line_color='green'))

plot1.x_range.range_padding = 0.05
plot1.xgrid.grid_line_color = None
#plot1.xaxis.major_label_orientation = 1.2
plot1.outline_line_color = None
plot1.xaxis.formatter=DatetimeTickFormatter(days=["%d/%m"])
# Arrange plots and widgets in layouts
last30 = dfall[-30:]
initvalue = (last30.index.min(), last30.index.max())

slider = DateRangeSlider(title='Time period select', value=initvalue, start=dfall.index.min(), end=initvalue[1])
devices = Select(title="Device identifier IMEI", value="358173054439511", options = ['358173054439511', '358173054439511'])

def callback(attr, old, new):
    if type(new) == tuple:
        beg, end = slider.value_as_datetime
        print(beg,end)
        source1.data['dr'] = (dfall.index >= beg) & (dfall.index <= end)
        #s_mean =
        view1.filters[1] = BooleanFilter(booleans=source1.data['dr'])
    else:
        # need to reset daterange data
        view1.filters[0] = GroupFilter(column_name='device_IMEI', group=new)
    #source.data = dict(newest.data)

devices.on_change('value', callback)
slider.on_change('value', callback)

layout = column(devices, slider, plot1)
curdoc().add_root(layout)
