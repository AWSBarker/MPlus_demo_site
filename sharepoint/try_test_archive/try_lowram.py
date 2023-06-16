#500kb ram
# lightsail version [VESA] - tested 24.6.22 Health_test
# converted tables to show NULL instead None
# db.table should come from Study_Studies.Env OR DBconx
# ensure emailed_studies_date.txt is clean 'NCT9999999' and weekly_studies_date.log is logger
# crontab run combined CTgov_3 and Study_Studies
import pandas as pd
import datetime as dt
from Study_Studies import Enviro
from memory_profiler import profile
import dask.dataframe as dd

@profile

def read_csv(): # set env, download, assess, save

    cols = ['nct_id', 'brief_title', 'official_title', 'overall_status', 'phase',  'enrollment', 'last_update_posted_date']
    tp = pd.read_csv(source+'/'+filename1, sep='|', low_memory=False, iterator=True, chunksize=5000, usecols=cols, parse_dates=['last_update_posted_date'], dayfirst=True)
    studies= pd.concat(tp, ignore_index=True)
    studies.rename(columns= {'last_update_posted_date' : 'updated'}, inplace=True)
    filter400d = dt.datetime.now()-dt.timedelta(days=401)
    phase34r = studies[studies.phase.str.contains('3|4|Not Applicable', case=False, na=True, regex=True)
                       & (studies.overall_status.str.contains('Recruiting', case=False, na=False, regex=False))
                       & (studies.updated >= filter400d)]
    return phase34r

#@profile
def ddread_csv(): # set env, download, assess, save

    cols = ['nct_id', 'brief_title', 'official_title', 'overall_status', 'phase',  'enrollment', 'last_update_posted_date']
    dcols = {'nct_id' : str, 'brief_title' : str, 'official_title' : str, 'overall_status' : 'category', 'phase' : 'category',  'enrollment' : object, 'last_update_posted_date' : object}

    d_studies = dd.read_csv(source+'/'+ filename1, sep='|', usecols=cols, dtype=dcols, parse_dates=[], dayfirst=True, assume_missing=True) # , blocksize=25e6

    #d_studies.rename(columns= {'last_update_posted_date' : 'updated'}, inplace=True)
    filter400d = dt.datetime.now()-dt.timedelta(days=401)
    #phase34r = d_studies[d_studies.phase.str.contains('3|4|Not Applicable', case=False, na=True, regex=True)
    #                   & (d_studies.overall_status.str.contains('Recruiting', case=False, na=False, regex=False))
    #                   & (d_studies.updated >= filter400d)]
    d_studies2 = d_studies[(d_studies.phase == 'Phase 3') |
                           (d_studies.phase == 'Phase 4') |
                           (d_studies.phase == 'Not Applicable') |
                           (d_studies.overall_status == 'Not Recruiting')
    ]
    return d_studies2

if __name__ == "__main__":

    enviro = Enviro()
    tab = enviro.table
    source = enviro.data_folder #"/media/Data2/ownCloud/PycharmProjects/sharepoint/data/"
    filename2 = "detailed_descriptions.txt"
    filename1 = "studies.txt"
    filename3 = "design_outcomes.txt"
    filename4 = "central_contacts.txt"


    dphase34r = ddread_csv()
    df = dphase34r.compute()
    df.info()
"""
    phase34r = read_csv()
    phase34r.info()
"""


