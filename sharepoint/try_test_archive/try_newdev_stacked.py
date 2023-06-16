from Mapi2 import DBconx, Org
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, DatetimeTickFormatter
from bokeh.transform import factor_cmap
from bokeh.palettes import magma

def remove_outliers(df): #, columns, n_std):
    mean = df['imei'].mean()
    sd = df['imei'].std()
    df = df[(df['imei'] <= mean + (2 * sd))]
    return df

with DBconx() as d:
    tuptup = DBconx('t').query(
        f"SELECT imei, checked_on, last_measure_at, count, org from `M+_daily`" \
        f"WHERE last_measure_at IS NOT NULL AND last_measure_at BETWEEN '2021-12-30' AND NOW()")
#fm = MDaily.objects.values('imei').exclude(last_measure_at=None).annotate(first=Min('last_measure_at'))
df = pd.DataFrame([i for i in tuptup])
orgDict  = Org.getOrg_Name_dict()
df.columns=['imei', 'checked_on', 'last_measure_at', 'count', 'org']
df.org = df.org.map(orgDict)
df.set_index('last_measure_at', drop=False, inplace=True)
df.sort_index(inplace=True)
# get list of active device with earliest First Measure
df['xaxis'] = df.last_measure_at.dt.strftime('%W%y')
fm = df.groupby(['org', 'imei'])['last_measure_at'].min().to_frame().reset_index()
df = fm.groupby([pd.Grouper(key='last_measure_at', freq='D'), 'org'])['imei'].count().to_frame()
#df = fm.groupby([pd.Grouper(level=0, freq='W'), 'org'])['imei'].nunique().to_frame().unstack(level=1, fill_value=0).T.reset_index(level=0, drop=True).T#.unstack(level=1, fill_value=0)


#df = remove_outliers(df)
#mon_org_stack._get_label_or_level_values('last').astype('M8[D]').astype('str')
#df['xaxis'] = df.index # pd.to_datetime(df.index).date mon_org_stack._get_label_or_level_values('last').astype('M8[D]').astype('str')
df['TWk'] = df.sum(axis=1, numeric_only=True)
df.info()

source_df = ColumnDataSource(df)

#orgs_wk = df1.groupby(['org', pd.Grouper(level=0, freq='W')])['imei'].nunique().to_frame()
no_orgs = df.groupby('org').nunique().index.to_list()

org_cmap = factor_cmap('xaxis', palette=magma(len(no_orgs)), factors= no_orgs, end=1)

p2 = figure(plot_width=800, plot_height=300,
            title="All Orgs : active devices by week", outline_line_color=None,
            x_range=source_df.data['xaxis']) # toolbar_location=None, tools="",
p2.vbar_stack(x='xaxis', stackers=no_orgs, width=0.9,  source=source_df, color=org_cmap['transform'].palette[:len(no_orgs)]) #, view=view_df) #, source=source1
#p2.xaxis[0].major_label_orientation = "vertical"
p2.xgrid.visible = False
p2.ygrid.visible = False
#p2.xaxis.formatter = DatetimeTickFormatter(months="%d %b %y")

show(p2)
