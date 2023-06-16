# try using dask for speed
# enable email summary/Drank of specific NCT_ID from csv
# try to add classes for modular
# ensure emailed_studies_date.txt is clean 'NCT9999999' and weekly_studies_date.log is logger
# crontab daily try to download aact Zipfiles -> csv
import pandas as pd
import numpy as np
#from io import BytesIO
#from urllib.request import urlopen
#from urllib.error import HTTPError, URLError
#from zipfile import ZipFile
#import os
import datetime as dt
from Study_Studies import Study, Studies, csvStudies
import time
#import dask.dataframe as dd
from os.path import exists

if __name__ == "__main__":

## assumed crontab used to get latest days < 1, csvStudies().downloadzip()
    c = csvStudies()
    if c.getfilenames2():
        #c.downloadzip()

        st = time.time()
        cols = ['nct_id', 'brief_title', 'official_title', 'overall_status', 'phase',  'enrollment', 'last_update_posted_date']
        dcols = {'nct_id' : str, 'brief_title' : str, 'official_title' : str, 'overall_status' : 'category', 'phase' : 'category',  'enrollment' : object, 'last_update_posted_date' : object}
        studies = c.get_csv_df(cols, dcols, c.filename1, ['last_update_posted_date'])

        cols = ['id', 'nct_id', 'outcome_type' , 'measure' , 'time_frame', 'description']
        dcols = {'id': int, 'nct_id' : str, 'outcome_type' : 'category', 'measure' : str, 'time_frame' : object, 'description' : str}
        outcome_m = c.get_csv_df(cols, dcols, c.filename3)

        cols = ['id', 'nct_id', 'description']
        dcols = {'id' : int, 'nct_id' : str, 'description' : str}
        desc = c.get_csv_df(cols, dcols, c.filename2)

        studies.rename(columns= {'last_update_posted_date' : 'updated'}, inplace=True)
        filter400d = dt.datetime.now()-dt.timedelta(days=401)
        phase34r = studies[studies.phase.str.contains('3|4|NaN', case=False, na=True, regex=True) &
                           studies.overall_status.str.contains('Recruiting', case=False, na=False, regex=False) &
                           (studies.updated >= filter400d)]
        del studies
        outP34r = c.mergeLR(phase34r, outcome_m)
        descP34r = c.mergeLR(phase34r, desc)
        outP34r, descP34r = c.getDrank_outcomes(outP34r, descP34r)

        print(f'df timed at {st - time.time()}')

# dash version

        st = time.time()
        cols = ['nct_id', 'brief_title', 'official_title', 'overall_status', 'phase',  'enrollment', 'last_update_posted_date']
        d_studies = c.get_csv_ddf(cols, c.filename1,['last_update_posted_date'])

        cols = ['id', 'nct_id', 'outcome_type' , 'measure' , 'time_frame', 'description']
        d_outcome_m = c.get_csv_ddf(cols, c.filename3)

        cols = ['id', 'nct_id', 'description']
        d_desc = c.get_csv_ddf(cols, c.filename2)


        d_studies = d_studies.rename(columns= {'last_update_posted_date' : 'updated'})
        filter400d = dt.datetime.now()-dt.timedelta(days=401)
        d_phase34r = d_studies[d_studies.phase.str.contains('3|4|NaN', case=False, na=True, regex=True) &
                           d_studies.overall_status.str.contains('Recruiting', case=False, na=False, regex=False) &
                           (d_studies.updated >= filter400d)]

        del d_studies
        d_outP34r = c.d_mergeLR(d_phase34r, d_outcome_m)
        d_descP34r = c.d_mergeLR(d_phase34r, d_desc)

        d_outP34r, d_descP34r = dd.compute(d_outP34r, d_descP34r, traverse=False)
        #d_descP34r = dd.compute(d_descP34r)
        print(f'dd timed at {st - time.time()}')
        d_outP34r, d_descP34r = c.getDrank_outcomes(d_outP34r, d_descP34r)




        # # ranking keywords in outcome_m and description
        # kw_outcome = [i['kw'] for i in Studies().kw('outcome')]
        # kw_desc = [i['kw'] for i in Studies().kw('desc')]
        #
        # # added 'other' outcomes to ranking
        # for kw in kw_outcome:
        #     outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
        #                 (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
        #                 (outP34r.outcome_type.str.contains('primary', na=False)), 'Drank'] += 20
        #
        #     outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
        #                 (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
        #                 (outP34r.outcome_type.str.contains('secondary', na=False)), 'Drank'] += 10
        # # capture 'other' outcomes
        #     outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
        #                 (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
        #                 (outP34r.outcome_type.str.contains('other', na=False)), 'Drank'] += 10
        #
        #     descP34r.loc[descP34r.official_title.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=1
        #     descP34r.loc[descP34r.description.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=1

        outP34r, descP34r = c.getDrank_outcomes(outP34r, descP34r)
        print(f" kw_outcomes {outP34r.Drank.value_counts().sort_index()}, {descP34r.Drank.value_counts().sort_index()}")

        # for kw in kw_desc:
        #     outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
        #                 (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
        #                 (outP34r.outcome_type.str.contains('primary', na=False)), 'Drank'] += 20
        #
        #     outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
        #                 (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
        #                 (outP34r.outcome_type.str.contains('secondary', na=False)), 'Drank'] += 10
        # # capture 'other' outcomes
        #     outP34r.loc[(outP34r.description.str.contains(kw, case=False, na=False, regex=False)) |
        #                 (outP34r.measure.str.contains(kw, case=False, na=False, regex=False)) &
        #                 (outP34r.outcome_type.str.contains('other', na=False)), 'Drank'] += 10
        #
        #     #outP34r.loc[outP34r.description.str.contains(kw, case=False, na=False, regex=False), 'Drank'] +=100
        #     descP34r.loc[(descP34r.official_title.str.contains(kw, case=False, na=False, regex=False)) |
        #                  (descP34r.description.str.contains(kw, case=False, na=False, regex=False)), 'Drank'] +=100

        outP34r, descP34r = c.getDrank_desc(outP34r, descP34r)
        print(f" kw_descriptions {outP34r.Drank.value_counts().sort_index()}, {descP34r.Drank.value_counts().sort_index()}")

        # get unique nct_id and sum Drank
        UoutP34r = outP34r[outP34r.Drank > 1].groupby(['nct_id']).sum().reset_index()
        UdescP34r = descP34r[descP34r.Drank > 1].groupby(['nct_id']).sum().reset_index()

        # filter and sort
        dr = pd.merge(UoutP34r, UdescP34r, left_on='nct_id', right_on='nct_id', how='inner', suffixes=('_out', '_desc'))
        dr['Drank'] = dr.loc[(dr.Drank_out >= 30), 'Drank_out'] + dr.loc[(dr.Drank_desc >= 2), 'Drank_desc']
        dr = dr.loc[dr.Drank.notna()]

        sorted_Drank = dr[dr.Drank >= 32].sort_values('Drank', ascending=False)[['nct_id','Drank', 'Drank_out', 'Drank_desc']].values.tolist()

        db_desc = dr.join(descP34r.set_index('nct_id'), on='nct_id', lsuffix='_final')
        db_desc.drop(['id', 'Drank','enrollment_out', 'enrollment_desc'], axis = 1, inplace=True)
        db_desc = db_desc.replace({np.nan: None})
        db_desc['url'] = 'https://clinicaltrials.gov/ct2/show/' + db_desc['nct_id'].astype(str)
        #db_desc['updated'] = db_desc['updated'].astype(str)
        db_desc['updated'] = db_desc['updated'].dt.date

        # get new from latest.difference(original)
        latest = db_desc.nct_id.to_list()
        # whats new and use as email targets
        original = set(Studies().nct_df().nct_id.values)
        newones = set(latest).difference(original) # removed NCT02365974, NCT04182464
        mask = db_desc.index.isin(newones)
        Studies().log_newones(db_desc.iloc[mask].to_records().tolist())

    # NewStudies.log created. but note E = Existing below needs editing for manual re-runs else newonesGT doesn't work
        sql= f"INSERT INTO {tab} " \
             f"(nct_id, id_out, Drank_out, id_desc, Drank_desc, Drank_final, updated, brief_title, official_title, overall_status, phase, enrollment, description, url) " \
             f"VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE " \
             f"nct_id = VALUES(nct_id),id_out = VALUES(id_out),Drank_out = VALUES(Drank_out)," \
             f"id_desc = VALUES(id_desc),Drank_desc = VALUES(Drank_desc),Drank_final = VALUES(Drank_final),updated = VALUES(updated),brief_title = VALUES(brief_title)," \
             f"official_title = VALUES(official_title),overall_status = VALUES(overall_status),phase = VALUES(phase)," \
             f"enrollment = VALUES(enrollment),description = VALUES(description),url = VALUES(url), new = 'E'"
        # if duplicate, Existing record new = E otherwise default is N = New. emailAB = 0,

        Studies().allocate(db_desc.to_records(index=False, column_dtypes={'Drank_desc': int, 'Drank_final': int, 'Drank_out': int, 'enrollment': int}).tolist(), sql)
        #df2db(db_desc.to_records(index=False, column_dtypes={'Drank_desc' : int, 'Drank_final' : int, 'Drank_out' : int}).tolist(), sql)

        # update contact db
        contacts = pd.read_csv(csvStudies().data_folder+filename4, sep='|')
        contacts = contacts.replace({np.nan: None})
        # connection = MySQLdb.connect(host='192.168.1.173', port=3306, user='pi', password='7914920', db='Health')
        # with connection as con:
        #     studies = read_sql("SELECT nct_id FROM CTgov1", con)
        studies = Studies().nct_df()
        cs = studies.merge(contacts, how='inner', on='nct_id')

        # will update changes and add new
        sql= """INSERT INTO CTgovContacts (nct_id, id, contact_type, name, phone, email)    
        VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
        nct_id = VALUES(nct_id),
        id = VALUES(id),
        contact_type = VALUES(contact_type),
        name = VALUES(name),
        phone = VALUES(phone),
        email = VALUES(email)
        """

        Studies().allocate(cs.to_records(index=False).tolist(), sql)
        #df2db(cs.to_records(index=False).tolist(), sql)

        # update outcomes db
        o = studies.merge(outcome_m, how='left', on='nct_id')
        o = o.replace({np.nan: None})
        del outcome_m
        sql= """INSERT INTO CTgovOutcomes (nct_id, id, outcome_type, measure, time_frame, description)    
        VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE 
        nct_id = VALUES(nct_id),
        id = VALUES(id),
        outcome_type = VALUES(outcome_type),
        measure = VALUES(measure),
        time_frame = VALUES(time_frame),
        description = VALUES(description)
        """
        Studies().allocate(o.to_records(index=False).tolist(), sql)
        Studies().table_backup(tab)

    # 0. Setup ranking filter cutoffs
        existing_rankfrom = 142  # filter existing studies from 150 down
        rankGT = 145  # only include new studies GT 150

        getnew = Studies().new_rankedGT(rankGT)
        na150 = Studies().not_mailed_alloc_new(existing_rankfrom)
        cut_at = 3 * len(Study.maildict)
    # 1. If send any new studies send all immediately greater than rank X

        if len(getnew) > 0: # only if some new ones
            newna150 = getnew.nct_id.to_list()
            Studies().send_email(newna150, 'N')

    # 2. send only 3 each to mailing list of Existing studies until ranked > 150 (18 on Jan5 2022)
        # if > 3*len, send 18 from na150[-18:],  < 3*len send na150[len:], if 0 skip

        if len(na150) > 0:
            na150s = na150.sort_values(by= ['Drank_final']).nct_id.to_list()
            Study.logger(f'Mailing Exiting trials: {len(na150s)} found to send')
            if len(na150s) >= cut_at:
                Studies().send_email(na150s[-cut_at:], 'E')
            else :
                Studies().send_email(na150s, 'E')
        else:
            Study.logger(f'no Exiting trials Rank > {existing_rankfrom} found to send')

    #3 allocate (update new = E) to db only what has been sent using /data/emailed_studies_20111104.txt
        # update DB emailAB = 1, Alloc = st , new = E
        s = Studies()
        s.table='CTgov1'  # tab?
        asql= f"INSERT INTO {s.table} (nct_id, Alloc) VALUES(%s,%s) ON DUPLICATE KEY UPDATE nct_id = VALUES(nct_id)," \
                     f"new = 'E', Alloc=VALUES(Alloc), emailAB = 1"
        # test allocate what has been sent from file emailed_studies_20111104.txt
        lot=[]
        try:
            with open(f"{s.data_folder}/emailed_studies_{time.strftime('%Y%m%d')}.txt", 'r') as f:
                #(f"{self.nct_id}, {st}\n")
                for line in f:
                    lot.append(tuple(line.replace(' ','').replace('\n','').split(',')))
            Studies().allocate(lot, asql)
            Study.logger(f'completed writing to db new studies emailed {s.table} ')
        except:
            Study.logger('some error in writing emailed files')

    else:
        Study.logger('The Zipfiles are not updated')



'''
    #new100 = getpkl.nct_id.loc[(getpkl.new == 'N') & (getpkl.Drank_final >= 100)].to_list()
    #if len(newna150) > 0:
    #if new150b > 0:
        #newX = Studies().new_rankedGT(rankGT).nct_id.to_list()
   
        

    totalsent = 0
    while existing_rankfrom > existing_rankTo:
        #na150 = getpkl.nct_id.loc[(getpkl.Alloc =='') & (getpkl.Drank_final >= existing_rankfrom) & (getpkl.new != 'N')].to_list()
        na150 = Studies().not_mailed_alloc_new(existing_rankfrom)
        if len(na150) + totalsent <= 3 * len(Study.maildict): # only send max 3x maildict i.e. 18
            Studies().send_email(na150, 'E')
            totalsent = len(na150)+ totalsent
            break
        else:
            existing_rankfrom-=1
'''


"""
    #s = Study('NCT04518306') #NCT04518293
    # detect which pc
    if os.uname()[1]=='VESA-ubuntu':
        source = "/media/Data2/ownCloud/PycharmProjects/sharepoint/data/"
    else: # acer
        source = "/home/ab/ownCloud/PycharmProjects/sharepoint/data/"

    #with open(f"{source}/emailed_studies_{time.strftime('%Y%m%d')}.txt", 'a') as f:
    Study.logger("started GTgov weekly")

    tab = 'CTgov1'    #tab = 'test'
    filename2 = "detailed_descriptions.txt" # /data2/downloads/CTgov/
    filename1 = "studies.txt"
    filename3 = "design_outcomes.txt"
    filename4 = "central_contacts.txt"

    newfiles = getfilenames2()
    if newfiles: # if any files are more than a day old
        downloadzip(source)
"""
