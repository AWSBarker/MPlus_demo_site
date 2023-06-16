import pandas as pd
from django.shortcuts import render
from django.db.models import Sum, Max
from django.http import HttpResponse
from .models import MDevOwn, MDaily, MOrg
from django.views.generic.base import TemplateView
from django.views import View
from django.views.generic import ListView
from slick_reporting.views import SlickReportView
from slick_reporting.fields import SlickReportField
from datetime import timedelta, datetime as dt
from .filters import MDailyFilter

from bokeh.embed import server_document, components, server_session
from bokeh.layouts import row, column
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, CDSView, DatetimeTickFormatter, HoverTool

class HomePage(TemplateView):
	template_name = 'mdaily/homepage.html'

class Search1(MDailyFilter):
	model = MDaily

class MorgListView1(ListView):
	model = MOrg

class D_1ListView1(ListView):
	model = MDaily
	queryset = MDaily.objects.exclude(last_measure_at__lt = dt.now()-timedelta(days=1)).exclude(last_measure_at = None)

class View1(View):
	model = MDaily
	lt = dt.now()-timedelta(days=30)
	queryset = MDaily.objects.exclude(last_measure_at__lt = lt).exclude(last_measure_at = None)
	df = pd.DataFrame(list(queryset.values('imei', 'last_measure_at', 'count', 'org__showas')))
	df['org'] = df['org__showas'].astype('str')
	df['imei'] = df['imei'].astype('str')
	template_name = 'mdaily/view1.html'

	def get(self, request):
		gb = self.df.groupby(['org'])['imei']
		source1 = ColumnDataSource(gb)
		print(source1.data)

		plot1 = figure(plot_width=640, title=f"active by org. since {self.lt.strftime('%x %X')}", x_range=gb, y_axis_label='mdaily')
		plot1.vbar(x = 'org', top='unique', source=source1) #, fill_color="#b3de69")
		layout = row(plot1)
		script,div = components(layout)
		my_dict = {'script' : script, 'div' : div, 'updated' : 'Active mdaily'}
		return render(request, self.template_name, my_dict)

# try bokeh script with slider
class ActiveByCountry(SlickReportView):
	report_model = MDaily
	date_field = 'last_measure_at'
	report_model = MDaily
	group_by = 'imei'
	columns = ['imei', SlickReportField.create(Max, 'count', name='count_sum')]

	chart_settings = [{'type': 'column',
        'data_source': ['count_sum'],
        'plot_total': False,
        'title_source': 'imei',
        #'title': _('Detailed Columns'),
    }, ]

class ImeiByCountry(SlickReportView):
	report_model = MDaily
	date_field = 'last_measure_at'
	columns = ['imei','count']
