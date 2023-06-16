# run  set to 30mins
import time
#import datetime as dt
from datetime import timedelta as td, datetime as dt
import threading

# v2 add tooltips
from bokeh.plotting import curdoc, figure #show, output_file, save
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Toggle, Slider, CustomJS
from bokeh.driving import repeat

@repeat(range(1))
def steps(value):
    if Clock1['on']:
        Clock1['u'] = Clock1['u']  + td(seconds=1)
        Clock1['b'] = Clock1['gt'] - Clock1['u']
        data1.data['d1'] = [str(Clock1['b'])[2:]]
    elif Clock2['on']:
        Clock2['u'] = Clock2['u'] + td(seconds=1)
        Clock2['b'] = Clock2['gt'] - Clock2['u']
        data1.data['d2'] = [str(Clock2['b'])[2:]]
    else :
        pass

def switcher(attr):
    global Clock1, Clock2
    print(f"{attr}  {Clock1['b']} {Clock2['b']}")
    #if attr:
    if Clock1['on']:
        Clock1['on'] = False
        Clock2['on'] = True
    elif Clock2['on']:
        Clock1['on'] = True
        Clock2['on'] = False
    else : # both off, start white
        Clock1['on'] = True
        Clock2['on'] = False


def starter(attr, old, new):
    global Clock1, Clock2
    print(attr, new)
    #gametime = td(minutes=30)
#    return {'on' : False, 'gt' : gametime, 'b' : gametime, 'u' : td(seconds=0)}

gametime = td(minutes=30)
Clock1 = {'on' : False, 'gt' : gametime, 'b' : gametime, 'u' : td(seconds=0)}
Clock2 = {'on' : False, 'gt' : gametime, 'b' : gametime, 'u' : td(seconds=0)}

#start = 0
#end = 30
#Clock1 = Clock2 = starter('',start, end)

#gametimer = Slider(start=start, end=end, value=end, step=1, title="Game time")
#gametimer.on_change('value', starter)

d = {'x0' : [0.6], 'y0' : [0.6], 'x' : [0.4], 'y' : [0.4],  'x1' : [0.6], 'y1' : [0.9], 'x2' : [0.4], 'y2' : [0.1], 'd1' : [str(Clock1['b'])[2:]], 'd2' : [str(Clock2['b'])[2:]]}
data1 = ColumnDataSource(data=d)

plot1 = figure(x_range=(0,1),  y_range=(0,1), plot_width=400, plot_height=200, background_fill_color = 'white', toolbar_location = None)
plot1.text(x = 'x0', y = 'y0', text = 'd1', text_font_size = '5em', angle = 3.141, source = data1)
plot1.text(x = 'x1', y = 'y1', text = 'd2', text_font_size = '1em', angle = 3.141, source = data1)
plot1.xgrid.grid_line_color = None
plot1.ygrid.grid_line_color = None
plot1.axis.visible = False

plot2 = figure(x_range=(0,1), y_range=(0,1), plot_width=400, plot_height=200, background_fill_color = 'black', toolbar_location = None)
plot2.text(x = 'x', y = 'y', text = 'd2', text_font_size = '5em', text_color = 'white', source = data1)
plot2.text(x = 'x2', y = 'y2', text = 'd1', text_font_size = '1em', text_color = 'white', source = data1)
plot2.xgrid.grid_line_color = None
plot2.ygrid.grid_line_color = None
plot2.axis.visible = False

toggle = Toggle(label="Switch", button_type="success", height = 150)
toggle.on_click(switcher)

#r1 = row(gametimer)
#r2 = row(plot1, plot2)
layout = column(plot1, toggle, plot2)
curdoc().add_root(layout)
curdoc().title = f"Chess"
curdoc().add_periodic_callback(steps, 999)