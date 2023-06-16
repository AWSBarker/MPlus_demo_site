# chk why Mapi2 Mdb2 GM report gives GMP 333 and Mapi2 332
# get a list from MDaily and Mapi2
from Mapi2 import Org, Dev
from Mdb2 import GetDF

gdf = GetDF()

#Mapi2 Org
odict = Org.getOrg_ID_name_dict()
gmp_org = odict['GM_POL']

bdevs = Org(gmp_org).get_org_fleet()


adevs = Org(gmp_org).getalldevs()


MAPIgmp_set = set([int(i['imei']) for i in adevs])
print(MDgmp_set.difference(MAPIgmp_set))
# device 354033090666903 has moved to GM_UK

d = Dev('354033090666903')
d.get_device_details()

#GETDF for GMP = 5

gmp = GetDF((5,))
MDgmp_set = gmp.all_dev_set

