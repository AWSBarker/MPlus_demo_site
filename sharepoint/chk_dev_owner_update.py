# chk why Mapi2 Mdb2 GM report gives GMP 333 and Mapi2 332
# get a list from MDaily and Mapi2
from Mapi2 import Org, Dev, Mapi, DBconx
from Mdb2 import GetDF

allorgs = Mapi().getOrgs()
odict = Org.getOrg_ID_name_dict()
iodict= Org.getOrg_ID_dict()
asql= f"INSERT INTO `M+_dev_own` (imei, owner) VALUES(%s,%s) " \
                   f"ON DUPLICATE KEY UPDATE imei=VALUES(imei), owner=VALUES(owner)"

for o in allorgs:
    adevs = Org(o).getalldevs()
    own = iodict.get(o)
    try :
        LoT_imei_owner = [(int(i['imei']),own) for i in adevs]
        Dev.update_owner(LoT_imei_owner)
    except Exception as e:
        print(f'update_owner error in {own} : {e}')
