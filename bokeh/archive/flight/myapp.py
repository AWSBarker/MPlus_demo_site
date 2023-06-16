import numpy as np
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import Button
from bokeh.layouts import column, widgetbox

def button_callback():
    """Add `num_points` circles to figure `fig`."""
    data_scr.stream({"x":np.random.normal(size=(10,)),
                     "y":np.random.normal(size=(10,))},
                     rollover=1)

bokeh_doc = curdoc()

# Creating data source
data_scr = ColumnDataSource({"x":[], "y":[]})

sample_plot = figure(plot_height=400, plot_width=400)
sample_plot.circle(x="x", y="y", source=data_scr)

button = Button(label="Generate", button_type="success")
button.on_click(button_callback)

bokeh_doc.add_root(column([sample_plot, widgetbox(button, align="center")]))

bokeh_doc.title = "Bokeh App with data streaming."