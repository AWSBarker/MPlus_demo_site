from bokeh.plotting import figure
from bokeh.io import show
from bokeh.models import ColumnDataSource, Rect, DatetimeTickFormatter, HoverTool
from Mdb2 import GetIMEI
# output to static HTML file
#output_file("line.html")
#  'last_measure_at', 'count'
i = GetIMEI(359804081343156).imeidf() # 359804081382097 354033090698526  358244086394972
i['d'] = i['count'].diff()
imax = i['count'].max()
print(i.head(), i.tail())
source1 = ColumnDataSource(i)

p = figure(title=f"IMEI Measures (total {imax})",width=600, height=100,
			   x_axis_type='datetime', y_range=(0,1), x_range=(i.index.min(), i.index.max()),
			   tools = "pan,wheel_zoom,reset", toolbar_location="above")

p.add_tools(HoverTool(tooltips=[('N measures', "@d"), ('dated', "@last_measure_at{%d%b%y}")],
					  formatters = {"@last_measure_at": "datetime"}))

p.add_tools(HoverTool(tooltips=[('N measures', "@d"), ('dated', "@last_measure_at{%d%b%y}")],
          formatters = {"@last_measure_at": "datetime"}))
p.circle(x='last_measure_at', y=0.5, size='d', source=source1)
p.yaxis.visible = False
p.ygrid.visible = False
p.xaxis[0].formatter = DatetimeTickFormatter(days='%d%b%y')
# show the results
show(p, browser='firefox')
