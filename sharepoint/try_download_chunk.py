# try chunks vs simple request
# on vesa with bitnami mounted sshfs 620s

import os, time, requests, shutil
from Study_Studies import Study
from Weekly_run_CTgov3 import Aact

def download_file(url):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dl_file, 'wb') as f:
            # chunk 214s 10*1024
            #for chunk in r.iter_content(chunk_size=10 * 1024):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
            #    print(len(chunk))
            #    f.write(chunk)
                shutil.copyfileobj(r, f)
    return dl_file

a = Aact()
aurl = a.aurl
print(aurl)
dl_file = 'b.zip' #os.path.join(a.source, 'b.zip')
st = time.time()
download_file(aurl)
print(f'finished download in {int(time.time() - st)}s')

