# M_alarms version using db, API to get ActiveFilterDict
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
from matplotlib import pyplot as plt, ticker
import matplotlib as mpl
# import matplotlib.ticker as MaxNLocator
import MySQLdb
import MySQLdb.cursors
from MySQLdb import _mysql  # fastest
from MySQLdb.constants import FIELD_TYPE
import requests
from sharepoint.Mapi2 import DBconx

class Logger:
    pass

class Api2Dict():
    """
    - create dict from api calls 1. models/alarms ACTIVE only 2. emailers
    - convert api/list of dicts to filter_dict {name :[start,end, target_org, t15, email_to, email_subject, description],}
    - start and end as datetime.datetime

    """
    api_url = 'https://dev.medisante-group.com/M_alarms/'  #

    def __init__(self, type='alarms'):
        self.aurl = self.api_url + type + '/' if type == 'alarms' else self.api_url + 'emails/'
        self.fd = {}
        self.api_lod = requests.get(self.aurl).json()

    def make_alarm_dict(self):
        for a in self.api_lod:
            self.fd[a['name']] = [dt.strptime(a['start'], '%X').time(), dt.strptime(a['end'], '%X').time(),
                                  [d['showas'] for d in a['target_org']],
                                  int(a['t15']), int(a['s15']),
                                  [d['username'] for d in a['email_to']],
                                  a['email_subject'],
                                  a['description']]
        return self.fd

    def make_emails_dict(self):
        for a in self.api_lod:
            self.fd[a['username']] = [a['email']]
        return self.fd

    def make_emails_list(self):
        return [e['email'] for e in self.api_lod if e['email']]


class SendMail:  #
    """
    - send email for TrueFilters/amlar filters to desginated email addresses using AB account
    - driven by drop_or_plot
    """
    pass

class FilterCheck():
    """
    - check dataframe against each filter to see if any activated
    - replace zp by th15 and zp_value by s15 (number of zero periods in 3hr window to trigger i.e. 3 or 4 in prod,     - zp_value, zero point, is 0 by default, set higher for testing in T1
    Filters in filter_dict are rules, truefilter collects all triggered
    - G1 global all orgs all times, threshold > 3 or 4 zps,     - T1 testing set zp threshold to 1
    filter_dict = {'G1': [datetime.time(0,0), datetime.time(23,59,59), '*', 4, ('AB','MS','LC', 'GL', 'RK', 'SS'),
               '24hr Global alarm +++ NOT A TEST +++.    Trigger: at least 4 periods of 15min with 0 measures in past 1 hour'],
    """
    filter_dict = Api2Dict().make_alarm_dict()

    def __init__(self, df=None, now=dt.utcnow()):  # plot3hr df
        self.df = df
        self.utcnow = now.time()
        # self.s15 = 0 # zp is zero
        self.truefilters = []

    def filters(self):

        def is_between():  # required to avoid #@ 02:00 UTC problem is  23:00 < 02:00 < 03:00
            if self.en < self.st:  # normal day 0200 < 1800
                return self.utcnow >= self.st or self.utcnow <= self.en
            return self.st <= self.utcnow <= self.en  # else its now 0100 and 2300 < 0800

        for k, v in self.filter_dict.items():
            self.filtername = k
            self.st, self.en, self.org, self.th15, self.s15, self.em, self.de, self.su = v
            #            self.zp_value = 1 if self.filtername == 'T1' else 0   # testing only adust zp_value

            if len(self.org) > 1:  # if self.org == '*':
                if is_between():
                    if (self.df.sum(
                            axis=1) <= self.s15).sum() >= self.th15:  # if any periods found equal/below zp_value
                        self.truefilters.append(self.filtername)
            else:  # one Org (need 1 rule per Org)
                self.org = self.org[0] # conv to str
                if is_between():
                    # could have no measures i.e not in df, or some measures zp_value but not less than zp
                    if self.org not in self.df.columns:
                        self.truefilters.append(self.filtername)
                    elif self.df.loc[(self.df[self.org] <= self.s15), self.org].count() >= self.th15:
                        self.truefilters.append(self.filtername)
        return self.truefilters


def drop_or_plot(state='plotting'):  # if state not 'testing' AND filter > 0, send email otherwise drop (always make plot for demo)
    lookback = 180  # rounded down 15 mins 120 #minutes UTC vs CET
    now = dt.utcnow()  # dt(2022,11,29, 1, 59,1) # fake time dt.utcnow()  # BEWARE SERVER TIME = utc
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
    for k, v in fixed.items():
        if k in cmap_dict:
            cmap_dict.update({k: v})

    # get 3 hour df  TODO - if zeros????!!!
    df3hr = dflm[beg:]  # .strftime('%Y-%m-%d %H:%M'):]
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

    plot1mon = dflm.groupby([dflm.index.floor('15T').strftime('%H:%M'), 'org'])['count'].count().unstack(
        fill_value=0) / 30
    # 96 row of 15min iloc 0 (00.00) to 96 (23.45). converts beg/end if now between 0000 and 0300
    # shift index 3hrs during midnight blindspot
    if datetime.time(0, 0, 0) <= now.time() <= datetime.time(3, 0, 0):
        index24 = list((dt(2023, 5, 4, 21) + timedelta(days=i / 96)).time().strftime('%H:%M') for i in range(0, 96))
        """
        index24 = ['21:00', '21:15', '21:30', '21:45', '22:00', '22:15', '22:30',
           '20:00', '20:15', '20:30', '20:45']
        """

        plot1mon = plot1mon.reindex(index24)

    # slice on new index at 15m exact beg:end
    plot1mon = plot1mon[beg.time().strftime('%H:%M'):end.time().strftime(
        '%H:%M')]  # beg.strftime('%H:%M'):now.strftime('%H:%M')]
    plot1mon = plot1mon.loc[:, (plot1mon != 0).any(axis=0)]  # remove the zero orgs
    fig, axs = plt.subplots(2, 1, figsize=(8, 7))
    fig.suptitle(f"M+hub Measures by Org \n Reported at {now.strftime('%Y-%m-%d %H:%M')} UTC")

    plot3hr.plot(ax=axs[0], kind='bar', stacked=True, fontsize=9, rot=0, width=0.9,
                 ylabel='Measures',
                 xlabel='',
                 color=cmap_dict,
                 )
    axs[0].set_xlim(0, 12)  # 2 hours of 15 min blocks = 8
    axs[0].tick_params(bottom=False)
    axs[0].set_title(f'Measures between {first_} {last_}UTC per 15min period. \n No measures from {no_measures}',
                     fontsize=10, loc='center')
    axs[0].set_xlabel(
        f'First {first_}                                          Periods UTC                                                Last {last_}',
        fontsize=9, loc='left')
    axs[0].set_xticklabels(axs[0].get_xticklabels(), fontdict={'fontsize': 8, 'horizontalalignment': 'right'})
    handles, labels = axs[0].get_legend_handles_labels()
    axs[0].legend(handles[::-1], labels[::-1], bbox_to_anchor=(1, 1), loc="upper left", fontsize=6)
    axs[0].axvspan(0, 7.5, facecolor='grey', alpha=0.1)
    axs[0].annotate("--- Alarm period ---", xy=(0.75, 0.85), xycoords=axs[0].transAxes)

    axs[0].yaxis.set_major_locator(ticker.MaxNLocator(3, integer=True))

    plot1mon.plot(ax=axs[1], kind='bar', stacked=True, fontsize=8, rot=0, width=0.9,
                  ylabel='Measures',
                  xlabel='Periods UTC ',
                  color=cmap_dict,
                  )
    axs[1].set_title(f'Average measurements during last month', fontsize=10, loc='center')
    axs[1].set_xlim(0, 12)  # 2 hours of 15 min blocks = 8
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
        Logger(f'Alarm checked, but nothing to do. [{state}] ')  # SendMail().addlog('testing email log : no zeros no test')

if __name__ == "__main__":

    drop_or_plot()  # when called from crontab poduction state, otherwise for plotting

