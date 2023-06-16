from Mapi2 import DBconx, Org
import os

o = Org('6e66e7c3-f425-4a56-84a4-cb425859b7fc')
l = f"{DBconx.data_folder}/Test_22.06.17.log"

print(o.name)
for d in o.getdev_gt_1():
            with open(l, 'a') as f:
                f.write(f" device > 1 {d['id']} \n")


