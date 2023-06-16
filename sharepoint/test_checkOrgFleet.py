# check Alermrys '-' IMEI
# api after IMEI 358244086392232 returns '-'
from Mapi2 import Org, Dev
import pprint
# 872b62b0-9527-4e1b-9822-bdeb48c5ac27
org_al = Org('872b62b0-9527-4e1b-9822-bdeb48c5ac27')
print(org_al.get_org_details())

dev = Dev(358244086392232)
pprint.pprint(dev.get_device_details())


