# TODO db the filters, menu for create, log of history
# beware sharepoint location on server
# plo3hr but filter on 1hr
# 1. drops out from 0000 and 0300
# # org/time specific filters
# check alarms, send email using andrew.barker account!
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
from email.mime.image import MIMEImage
from datetime import datetime as dt, timedelta
import datetime
import pandas as pd
import numpy as np
import sys, os
sys.path.append("..")
from sharepoint.Mapi2 import DBconx
from matplotlib import pyplot as plt, ticker
import matplotlib as mpl
#import matplotlib.ticker as MaxNLocator
class Logger:
    def __init__(self, t):
        self.l = f"data/email_{dt.now().strftime('%Y%m%d')}.log"
        self.t = t

        if os.path.exists(self.l):
            with open(self.l, 'a') as f:
                f.write(f"{dt.now().strftime('%X')} : {self.t}")
                f.write("\n")
        else:
            with open(self.l, 'w') as f:
                f.write(f"{dt.now().strftime('%X')} : {self.t}")
                f.write("\n")
        print(f"{dt.now().strftime('%X')} : {self.t}")


class SendMail: #

    def __init__(self, filter, st = 'AB'):
        self.st,self.en,self.org,self.zp,self.em,self.de = FilterCheck().filter_dict.get(filter)
        self.username = st # 'AB' is email server account
        self.sendto = self.getsendtolist() #['andrew.barker@medisante-group.com']  # get list from self.em lookup
        self.filtername = filter
        msg = MIMEMultipart('related')
        msg['Subject'] = self.de[:40] # first 40 chars"TEST M+hub outage alarms"
        msg['From'] = 'andrew.barker@medisante-group.com'
        msg['To'] = ", ".join(self.sendto)
        #msg['Cc'] = 'andrew.barker@medisante-group.com'
        msg.attach(MIMEText(f"""
                <html>
                    <body>
                        <h1 style="text-align: center;">{self.de[:40]}</h1>
                        <p>You are getting this because filter {self.filtername} was triggered (see footer for filter)</p>
                        <p>There were at least {self.zp} periods triggered</p>
                        <p><a href="https://demo.medisante-group.com/tools/uptime">Live status here</a></p> 
                        <p><img src="cid:0" alt = "pic"></p>
                        <p>Filter details: {self.de}</p>
                        <p>Filter data : {self.st,self.en,self.org,self.zp,self.em}</p>
                    </body>
                </html>""", 'html', 'utf-8'))

        with open('data/past2hours.png', 'rb') as fp:
            img = MIMEImage(fp.read())
            img.add_header('Content-Disposition', 'inline', filename='past2hours.png')
#            img.add_header('X-Attachment-Id', '0')
            img.add_header('Content-ID', '<0>')
            msg.attach(img)
        # with open('test.png', 'rb') as fp:
        #     img = MIMEImage(fp.read())
        #     img.add_header('Content-Disposition', 'inline', filename='test.png')
        #     #            img.add_header('X-Attachment-Id', '0')
        #     img.add_header('Content-ID', '<1>')
        #     msg.attach(img)

        try:
            with SMTP("smtp.office365.com", 587) as server:
                server.starttls()
                server.login('andrew.barker@medisante-group.com', 'Zirege73')
                server.send_message(msg)
            Logger(f'email sent to {self.sendto} ({self.filtername} {self.st} {self.en})')
        except Exception as e:
            Logger(f"error emailing alarms : {e}")

    """
    def addlog(self, t): # add to log
        self.t = t
        Logger(self.t)
      
        l = f"data/email_{dt.now().strftime('%Y%m%d')}.log"
        if os.path.exists(l):
            with open(l, 'a') as f:
                f.write(f"{dt.now().strftime('%X')} : {self.t}")
        else:
            with open(l, 'w') as f:
                f.write(f"{dt.now().strftime('%X')} : {self.t}")
        """

    def getsendtolist(self): # from self.em ('AB',) ('AB', 'LC')  return a list [andrew@, lionel@..] no '*'
        AB2email_dict = {
            'AB' : 'andrew.barker@medisante-group.com',
            'LC' : 'lionel.cavalliere@medisante-group.com',
            'MS' : 'manuel.stocker@medisante-group.com',
            'RK' : 'robin.kleiner@medisante-group.com',
            'GL' : 'gilles.lunzenfichter@medisante-group.com',
            'SS': 'support@medisante-group.com'
        #            '*' : 'andrew.barker@medisante-group.com', 'manuel.stocker@medisante-group.com' # 'lionel.cavalliere@medisante-group.com']
            }
        em_list =[]

        for n in self.em:
            em_list.append(AB2email_dict.get(n))

        return em_list

class FilterCheck():
    """
    - zp = number of zero periods in 3hr window to trigger i.e. 3 or 4 in prod
    - zp_value is 0 by default, set higher for testing in T1
    Filters in filter_dict are rules, truefilter collects all triggered
    - G1 global all orgs all times, threshold > 3 or 4 zps
    - T1 testing set zp threshold to 1

    org    Al  GK  GMA  GMS  GMU  GMUS  Ins  Och  VS
last
09:30   9   0    2    0    6     0    0    4   0
09:45   5   0    0    1    3     0    0    0   0
10:00   0   1    1    0    4     0    0    0   0
10:15   7   0    1    0    1     0    0    1   0
10:30   2   0    1    0    2     0    0    1   1
10:45   3   0    1    0    1     0    0    1   0
11:00   1   0    0    1    2     2    3    2   0
11:15   0   0    1    0    2     2    0    0   1
11:30   0   0    1    1    1     0    0    0   0
11:45   2   0    4    1    0     0    0    0   0
12:00   2   0    0    0    3     2    0    0   0
12:15   2   0    1    1    2     1    0    2   0

self.df.sum(axis=1)
last
09:30    21
09:45     9
10:00     6
10:15    10
10:30     7
10:45     6
11:00    11
11:15     6
11:30     3
11:45     7
12:00     7
12:15     9

check is any period is below/equal zp_value (0)
self.df.sum(axis=1) <= 3).any()
True

count periods below zp_value
(self.df.sum(axis=1) <= 7).sum()

count of periods below zp_value for an Org
self.df.loc[(self.df['Al'] <= 3), 'Al'].count() >= self-zp
    # create filter rules, name :  st, end, org, zp, email, dess (1st 40 appear in email subect and header)
"""
    filter_dict = {
        'G1': [datetime.time(0,0), datetime.time(23,59,59), '*', 4, ('AB','MS','LC', 'GL', 'RK', 'SS'),
               '24hr Global alarm +++ NOT A TEST +++.    Trigger: at least 4 periods of 15min with 0 measures in past 1 hour'],
        'T1': [datetime.time(0,0), datetime.time(23,59,59), '*', 3, ('AB','MS', 'SS'), #,'MS','LC', 'GL', 'RK'),
               'TEST ONLY. 24hr Global alarm.            Trigger: at least 3 periods of 15min with 1 or less measures in past 1 hour'],
        'AB1': [datetime.time(0,0), datetime.time(3,45,00), 'GMS', 1, ('AB',),
                'TEST ONLY 00:00 - 05:30 for GMS org.    Trigger: at least 1 periods of 15min with 0 measures in last past 1 hour'],
        'LC1': [datetime.time(4, 0), datetime.time(8, 59, 59), 'Al', 3, ('LC','SS'),
                'ALARM 04:00 - 09:00 for Al org.     Trigger: at least 3 periods of 15min with 0 measures in past 1 hour']
    }

    def __init__(self, df = None, now=dt.utcnow()): #plot3hr df
        self.df = df
        self.utcnow = now.time()
        self.zp_value = 0 # zp is zero
        self.truefilters =[]

    def filters(self):

        def is_between(): # required to avoid #@ 02:00 UTC problem is  23:00 < 02:00 < 03:00
            if self.en < self.st: # normal day 0200 < 1800
                return self.utcnow >= self.st or self.utcnow <= self.en
            return self.st <= self.utcnow <= self.en # else its now 0100 and 2300 < 0800

        for k,v in self.filter_dict.items():
            self.filtername = k
            self.st,self.en,self.org,self.zp,self.em,self.de = v
            self.zp_value = 1 if self.filtername == 'T1' else 0   # testing only adust zp_value
            if self.org == '*':
                if is_between():
                    if (self.df.sum(axis=1) <= self.zp_value).sum() >= self.zp: # if any periods found equal/below zp_value
                        self.truefilters.append(self.filtername)
            else: # one Org (need 1 rule per Org)
                if is_between():
                    # could have no measures i.e not in df, or some measures zp_value but not less than zp
                    if self.org not in self.df.columns:
                        self.truefilters.append(self.filtername)
                    elif self.df.loc[(self.df[self.org] <= self.zp_value), self.org].count() >= self.zp:
                        self.truefilters.append(self.filtername)
        return self.truefilters
    #
    # def G1(self):
    #     # verify st, en
    #     # count zero periods against zp(threshold) - df.sum(axis=0) is sum counts by org, axis1 by 15T
    #     # self.df.sum(axis=1).value_counts()[0] how many zero - need no find default
    #     if self.st < self.utcnow < self.en :
    #         if (self.df.sum(axis=1).value_counts() <= self.zp_value).any():  # any 0
    #             if self.df.sum(axis=1).value_counts()[self.zp_value] >= self.zp: # count them check against zp
    #                 self.truefilters.append('G1')
    #
    # def AB1(self):
    #     if self.st < self.utcnow < self.en :
    #         if 0 in self.df.sum(axis=1).value_counts():  # there is at least one zero period
    #             zero_periods = self.df.sum(axis=1).value_counts()[0]
    #             self.truefilters.append('AB1')
    #
    # def T1(self):
    #     self.zp_value = 3 # raise threshold to 1 measures as zp (s <=1).any()
    #     # verify st, en
    #     # count periods below or equal to zp_value (self.df.sum(axis=1) <= 7).sum()
    #     if self.st < self.utcnow < self.en :
    #         if (self.df.sum(axis=1) <= self.zp_value).sum(): # if any periods found equal/below zp_value
    #             self.truefilters.append('T1')

def drop_or_plot(state='plotting'): # if state not 'testing' AND filter > 0, send email otherwise drop (always make plot for demo)
    lookback = 180  # rounded down 15 mins 120 #minutes UTC vs CET
    now = dt.utcnow() #dt(2022,11,29, 1, 59,1) # fake time dt.utcnow()  # BEWARE SERVER TIME = utc
    beg = (now - (now - dt.min) % timedelta(minutes=15)) - timedelta(minutes=lookback)  # 1 week back 7 x 24 grid, top is today
    end = beg + timedelta(hours=3, minutes=15)  # for index24 slice
    back1hr = (now - (now - dt.min) % timedelta(minutes=15)) - timedelta(minutes=60)
    lastmonth = beg - timedelta(days=30)

    with DBconx('t') as d:
        tuptup = d.query(f"SELECT md.last_measure_at, md.count, mo.showas from `M+_daily` md "
                         f"JOIN `M+_orgs` mo ON md.org = mo.id "
                         f"WHERE md.last_measure_at >= '{lastmonth}' AND md.last_measure_at <= '{now}'"
                         )
    dflm = pd.DataFrame([i for i in tuptup], columns=['last', 'count', 'org'])
    dflm = dflm.astype({'count': np.int16, 'org': 'category'})
    dflm.set_index('last', drop=True, inplace=True)
    dflm.sort_index(inplace=True)  # avoid future error on index not existing

    # get color map dict by org -
    orgs = dflm['org'].unique().to_list()
    cmap = mpl.colormaps['tab20'].resampled(len(orgs))
    cmap_dict = dict(zip(orgs, cmap.colors))
    # try to keep same org same colour between charts
    fixed = {'Al': np.array([0.01, 0.1, 0.1, 0.9]), 'GMA': np.array([0.3, 0.28, 0.9, 0.7])}
    for k,v in fixed.items():
        if k in cmap_dict:
            cmap_dict.update({k:v})

    # get 3 hour df  TODO - if zeros????!!!
    df3hr = dflm[beg:] #.strftime('%Y-%m-%d %H:%M'):]
    last_ = df3hr.index.max().strftime('%H:%M')
    first_ = df3hr.index.min().strftime('%H:%M')
    # resample to get all 15m periods, count fails across midnight/days
#    plot3hr = df1.groupby('org').resample('15T')['count'].count().unstack(fill_value=0).T
    plot3hr = df3hr.groupby('org').resample('15T')['count'].nunique().unstack(fill_value=0).T
    plot3hr.index = pd.to_datetime(plot3hr.index).strftime('%H:%M')
    no_measures = set(orgs).difference(set(plot3hr.columns.to_list()))
    plot3hr = plot3hr.loc[:, (plot3hr != 0).any(axis=0)]  # remove the zero orgs

    df1hr = dflm[back1hr:].groupby('org').resample('15T')['count'].nunique().unstack(fill_value=0).T
    df1hr.index = pd.to_datetime(df1hr.index).strftime('%H:%M')
    df1hr = df1hr.loc[:, (df1hr != 0).any(axis=0)]  # remove the zero orgs

    plot1mon = dflm.groupby([dflm.index.floor('15T').strftime('%H:%M'), 'org'])['count'].count().unstack(fill_value=0) / 30
    # 96 row of 15min iloc 0 (00.00) to 96 (23.45). converts beg/end if now between 0000 and 0300
    # shift index 3hrs during midnight blindspot
    if datetime.time(0,0,0) <= now.time() <= datetime.time(3,0,0):
        index24 = ['21:00', '21:15', '21:30', '21:45', '22:00', '22:15', '22:30',
                   '22:45', '23:00', '23:15', '23:30', '23:45', '00:00', '00:15',
                   '00:30', '00:45', '01:00', '01:15', '01:30', '01:45',
           '02:00', '02:15', '02:30', '02:45', '03:00', '03:15', '03:30', '03:45',
           '04:00', '04:15', '04:30', '04:45', '05:00', '05:15', '05:30', '05:45',
           '06:00', '06:15', '06:30', '06:45', '07:00', '07:15', '07:30', '07:45',
           '08:00', '08:15', '08:30', '08:45', '09:00', '09:15', '09:30', '09:45',
           '10:00', '10:15', '10:30', '10:45', '11:00', '11:15', '11:30', '11:45',
           '12:00', '12:15', '12:30', '12:45', '13:00', '13:15', '13:30', '13:45',
           '14:00', '14:15', '14:30', '14:45', '15:00', '15:15', '15:30', '15:45',
           '16:00', '16:15', '16:30', '16:45', '17:00', '17:15', '17:30', '17:45',
           '18:00', '18:15', '18:30', '18:45', '19:00', '19:15', '19:30', '19:45',
           '20:00', '20:15', '20:30', '20:45']
        plot1mon = plot1mon.reindex(index24)

    # slice on new index at 15m exact beg:end
    plot1mon = plot1mon[beg.time().strftime('%H:%M'):end.time().strftime('%H:%M')]#beg.strftime('%H:%M'):now.strftime('%H:%M')]
    plot1mon = plot1mon.loc[:, (plot1mon != 0).any(axis=0)]  # remove the zero orgs
    fig, axs = plt.subplots(2, 1, figsize=(8, 7))
    fig.suptitle(f"M+hub Measures by Org \n Reported at {now.strftime('%Y-%m-%d %H:%M')} UTC")

    plot3hr.plot(ax=axs[0], kind='bar', stacked=True, fontsize=9, rot=0, width=0.9,
                 ylabel='Measures',
                 xlabel='',
                 color=cmap_dict,
                 )
    axs[0].set_xlim(0,12)  # 2 hours of 15 min blocks = 8
    axs[0].tick_params(bottom=False)
    axs[0].set_title(f'Measures between {first_} {last_}UTC per 15min period. \n No measures from {no_measures}', fontsize=10, loc='center')
    axs[0].set_xlabel(            f'First {first_}                                          Periods UTC                                                Last {last_}',fontsize=9, loc='left')
    axs[0].set_xticklabels(axs[0].get_xticklabels(), fontdict={'fontsize': 8, 'horizontalalignment': 'right'})
    handles, labels = axs[0].get_legend_handles_labels()
    axs[0].legend(handles[::-1], labels[::-1], bbox_to_anchor=(1, 1), loc="upper left", fontsize=6)
    axs[0].axvspan(0,7.5, facecolor='grey', alpha=0.1)
    axs[0].annotate("--- Alarm period ---", xy=(0.75, 0.85), xycoords=axs[0].transAxes)

    axs[0].yaxis.set_major_locator(ticker.MaxNLocator(3, integer=True))

    plot1mon.plot(ax=axs[1], kind='bar', stacked=True, fontsize=8, rot=0, width=0.9,
           ylabel='Measures',
           xlabel='Periods UTC ',
           color=cmap_dict,
           )
    axs[1].set_title(f'Average measurements during last month', fontsize=10, loc='center')
    axs[1].set_xlim(0,12)  # 2 hours of 15 min blocks = 8
    axs[1].set_xticklabels(axs[1].get_xticklabels(), fontdict={'fontsize': 8, 'horizontalalignment': 'right'})
    axs[1].tick_params(bottom=False)
    axs[1].yaxis.set_major_locator(ticker.MaxNLocator(3, integer=True))
    handles, labels = axs[1].get_legend_handles_labels()
    axs[1].legend(handles[::-1], labels[::-1], bbox_to_anchor=(1, 1), loc="upper left", fontsize=6)

    fig.tight_layout(pad=4.0)
    fig.savefig('data/past2hours.png', transparent=True)
    print('saved png')

    # filters on 1 hour back df
    filter_list = FilterCheck(df1hr, now).filters()
    # send or not
    if len(filter_list) > 0 and state == 'production':
            for f in filter_list:
                SendMail(f)
    else:
        Logger(f'Alarm checked, but nothing to do. [{state}] ')#SendMail().addlog('testing email log : no zeros no test')

if __name__ == "__main__":
    drop_or_plot('production') # when called from crontab poduction state, otherwise for plotting
    #drop_or_plot()
    #drop_or_plt('prod')