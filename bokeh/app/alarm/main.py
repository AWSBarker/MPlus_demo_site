# alarms model : Lightsail version Mapi2 + redis
# use poisson distribution hourly means
# ref : https://timeseriesreasoning.com/contents/poisson-process/
# probability of zero P(0) for hour x given λ = ,
import datetime

from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, Select, Legend, DatePicker
from bokeh.plotting import figure, curdoc
from bokeh.transform import dodge
import sys
import pandas as pd
import numpy as np
sys.path.append("sharepoint")
sys.path.append("..")
from Mapi2 import Mapi, DBconx
from scipy.stats import poisson
import datetime as dt
from datetime import timedelta
import redis
import pickle

def initial_df(): # from redis
    with DBconx('t') as d:
        tuptup = d.query(f"SELECT md.imei, md.last_measure_at, mo.showas from `M+_daily` md "
                         f"JOIN `M+_orgs` mo ON md.org = mo.id "
                         f"WHERE md.last_measure_at BETWEEN '{beg}' AND curdate()"
                         )
    df1 = pd.DataFrame([i for i in tuptup])
    df1.columns = ['imei', 'hours', 'id']
    df1 = df1.astype({'imei': np.int64, 'id': str}) # no effect 'last' : '<M8[s]'})
    df1.set_index('hours', drop=False, inplace=True)
    return  df1.pivot_table(columns=df1.index.hour, index=[df1.id, df1.index.date], values='imei',
                                     aggfunc=np.count_nonzero, fill_value=0)
def an_org(df, o, b):
    #dftemp = df1.loc
    _dftemp = df.loc[(o, slice(b, dt.date.today())), :]
    _Nd = _dftemp.index.__len__() # dftemp
    o_a_m = _dftemp.mean() # org actual means
    _m = o_a_m
    _t = o_a_m.apply(lambda x: poisson.pmf(0, x))
    _z = (_dftemp == 0).sum() / _Nd  # for each hour how many zeros over days
    _tm = _dftemp.sum().sum()
    del _dftemp
    return (_m,_t,_z, _Nd, _tm)

def first_org(df1): # slice for Org = 1, all dates
    dfA = pd.DataFrame()
    _beg = dt.date.today() - timedelta(days=90)
    #dftemp = df1.loc[(1,)]
    _dftemp = df1.loc[(org, slice(_beg, dt.date.today())) , :]
    _Nd = _dftemp.index.__len__()
    #Tmeasures = _dftemp.sum().sum()
    o_a_m = _dftemp.mean() # org actual means
    dfA['m'] = o_a_m
    dfA['t'] = o_a_m.apply(lambda x: poisson.pmf(0, x))
    dfA['z'] = (_dftemp == 0).sum() / _Nd  # for each hour how many zeros over days
    return ColumnDataSource(dfA), _dftemp.sum().sum(), _Nd

def plotit():
    # top is Theory based on X days history for Org
    p = figure(width=800, height=350, title="Probability of zero measures per hour",
               toolbar_location='below', x_range=(-1,24),
               x_axis_label ='Hour of day UTC',
               y_axis_label = 'Probability of zero measures'
               )
    p.add_layout(Legend(), 'right')
    p.vbar(x=dodge('hours', 0.2), top='t', width=0.4, fill_color = 'blue', legend_label='theory', source=source) #, view=view1)  # , fill_color=index_cmap, )
    p.vbar(x=dodge('hours', -0.2), top='z', width=0.4, fill_color = 'red', legend_label='actual', source=source)
    p.xaxis.ticker = list(range(0,24))
    p.legend.background_fill_alpha = 0.4

    p1 = figure(width=800, height=350, title=f"{org}: Mean hourly measures since {beg}  ({Ndays} days, {Tmeasures} measures)",
               toolbar_location=None, x_range=(-1,24),
               x_axis_label = None,
               y_axis_label = 'Mean hourly measures'
               )
    p1.add_layout(Legend(), 'right')
    p1.vbar(x='hours', top='m', width=0.8, fill_color = 'red', legend_label='actual', source=source)
    p1.xaxis.ticker = list(range(0,24))
    p1.legend.background_fill_alpha = 0.4
    p1.legend.label_text_font_size = '10pt'
    return (p1, p)
def updater(attr, old, new):
    global org
    if len(new) < 9:
        org = new
        startd = dt.datetime.strptime(date_picker.value, '%Y-%m-%d').date()
    else:
       startd = dt.datetime.strptime(new, '%Y-%m-%d').date()
    source.data['m'], source.data['t'], source.data['z'], Ndays, Tmeasures = an_org(df0, org, startd)
    print(f'stratd is {startd}, org is {org} Ndays {Ndays}, Tmeasures {Tmeasures}')
    p1.title.text = f"{org} : Mean hourly measures since {startd} ({Ndays} days, {Tmeasures} measures)"

orgdict = Mapi.getOrgs_showas_dict(inv=True)  # {id:showas}
orgs = list(orgdict.keys())
beg = dt.date(2022,1,1) # begining of database NOT start date for analysis
org = 'Al'  # ('OCH', 'AL', 'GMU', 'IN', 'ODE')

oselect = Select(title='Pick M+ Org ', value=org, options=orgs, width_policy='min')
date_picker = DatePicker(title='Measures since :', value=(dt.date.today() - timedelta(days=90)).strftime('%Y-%m-%d'), min_date="2022-01-01", max_date= dt.date.today().strftime('%Y-%m-%d'), width_policy='min')

r = redis.Redis()
#df0 = initial_df()
df0 = pickle.loads(r.get("initial_df"))
#Ndays = df0.loc[org].index.__len__()
source, Tmeasures, Ndays = first_org(df0) #an_org(orgs_days_by_h, 1)
oselect.on_change('value', updater)
date_picker.on_change('value', updater)

r = column(oselect, date_picker)
p1,p = plotit()
c = column(p1,p)
layout = row(r, c, name = 'g1')
#show(p)
curdoc().add_root(layout)
#print('done')


"""

# start range end of this month back 3 months OR min/max
# # end of this month < (dt.now().date().replace(day=1) + timedelta(days=32)).replace(day=1)
# deom = (dt.now().date().replace(day=1) + timedelta(days=32)).replace(day=1)
# dnow = dt.now().date()
# drange1 = (deom - timedelta(days=360), deom)
# drange2 = (dev_org_mon.month.min(), dev_org_mon.month.max())
# dslider = DateRangeSlider(value=drange1, start= drange2[0], end=drange2[1], format = '%d  %b  %Y',
#                           title='Period for Active Devices         .', bar_color='green', width_policy='min')
# initial view deom back 6 months
#bool2 = (source.data['month'] <= deom) & (source.data['month'] >= dt.now().date()- timedelta(days=180))

#view1 = CDSView(source=source, filters=[GroupFilter(column_name = 'org', group = orgs[0]), BooleanFilter(bool2)])

"""

