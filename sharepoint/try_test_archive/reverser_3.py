# correct maildict / username error 27.6.2022
# correct allocation in .txt fil but under email instead id
# open log, get nct_id, email, conert email to id, insert
# get list of files emailed for sql query
from Study_Studies import Studies, Study
import time
s = Studies()   # table inherited from Env (s.table='CTgov1')
filelist = []
# get email, id by reversing usermaildict
revdict = {v: k for k, v in s.maildict.items()}
fl2 = []
with open(f"{s.data_folder}/Health_{s.table}_emailed_studies_20220627.txt", 'r') as f:
        for line in f:
            pair = line.replace(' ','').replace('\n','').split(',') # [nct, ab]
            pair2 = revdict.get(pair[1])   # email to id
            filelist.append((pair[0], pair2))
            fl2.append(pair[0])
print(filelist)
print(fl2)


#Alloc needs usid
sql= f"INSERT INTO CTgov1 " \
         f"(nct_id, Alloc) " \
         f"VALUES(%s,%s) ON DUPLICATE KEY UPDATE " \
         f"nct_id = VALUES(nct_id), Alloc=VALUES(Alloc), ts = '2022-06-27 22:00:00'"

s.allocate(filelist, sql)



"""
SELECT * FROM `CTgov1` where nct_id IN ('NCT05417490', 'NCT05196854', 'NCT05334342', 'NCT05159999', 'NCT03588520', 'NCT05330247', 'NCT05339841', 'NCT04981002', 'NCT05289310', 'NCT05184933', 'NCT05428514', 'NCT04467021', 'NCT05394883', 'NCT05352633', 'NCT05172466', 'NCT05426707', 'NCT04803435', 'NCT04098354', 'NCT05169788', 'NCT05211648', 'NCT05390502', 'NCT05228860', 'NCT05388071', 'NCT05295823', 'NCT05138601', 'NCT05236725', 'NCT05423652', 'NCT04936243', 'NCT05222711', 'NCT05390541', 'NCT05345340', 'NCT05319418', 'NCT05319613') 
"""
