from functools import partial
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, Button
from bokeh.layouts import column
from bokeh.events import ButtonClick
from tornado.ioloop import IOLoop
import asyncio

def view(doc):
    source = ColumnDataSource(data=dict(x=[0, 1], y=[0, 0]))

    def update_source(new_data):
        source.data = new_data

    async def loop():
        for i in range(10):
            doc.add_next_tick_callback(partial(update_source, dict(x=[0, 1], y=[i, i**2])))
            await asyncio.sleep(0.5)

    def button_pushed():
        IOLoop.current().spawn_callback(loop)

    p = figure(plot_width=600, plot_height=300)
    p.line(source=source, x='x', y='y')

    button = Button(label='Draw')
    button.on_event(ButtonClick, button_pushed)

    doc.add_root(column(button, p))

show(view)