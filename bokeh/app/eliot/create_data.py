# one off recreate initial ECG data for eliot in redis : Lightsail version Mapi2
import redis
import pickle
import pandas as pd
import numpy as np
import sys
sys.path.append("/home/bitnami/sharepoint")
sys.path.append("..")
from Mapi2 import DBconx, Mapi
from pdf2image import convert_from_path, convert_from_bytes
from PIL import Image
import base64
from os.path import exists
from os import mknod

class TempFileURL:
    '''
    - save created pdf/png to share folder
    - input is a PDF page 1 of 2 from PIL image
    - in df and redis store file location URL relative to ../sharepoint so bokeh can display .png
    '''

    def __init__(self, im, cid, ts, r):
        self.cid = cid
        self.ts = ts #2022-11-10 10:12:39
        self.url = f'{static_folder}/PM100_{self.cid}_{self.ts}.pdf'
        self.png = f'{static_folder}/PM100_{self.cid}_{self.ts}.png'
        if not exists(self.url):
            mknod(self.url)
            #mknod(self.png)
            im_pages = convert_from_bytes(base64.b64decode(im))
            for i in im_pages:
                i.save(self.url, append=True)

            self.pngs =convert_from_path(self.url)
            self.stackPNG()
            print(f"Created a Named Temporary File {self.url} ")
        r.set(f'PM100_{self.cid}_{self.ts}.png', im)  # save image png with filename key

    def stackPNG(self):
        sz = 0.2
        resz = (int(self.pngs[0].width * sz), int(self.pngs[0].height * sz))
        hx2 = (int(self.pngs[0].width * sz), int(self.pngs[0].height * sz * 2))
        new = Image.new('RGB', (hx2))
        new.paste(self.pngs[0].resize(resz), (0,0))
        new.paste(self.pngs[1].resize(resz), (0, int(new.height * 0.5)))
        new.save(self.png, format='png')

# ECG data DataTable ***** NOTE eliot2_bk for testing ********
sql = """SELECT device_IMEI, device_model, device_timezone, measurements_timestamp, metadata_measurementType, metadata_correlationid,
measurements_ecgSamples_minValue, measurements_ecgSamples_maxValue, measurements_ecgSamples_sampleRate, 
measurements_ecgSamples_sampleRateUnit, measurements_ecgSamples_factor, measurements_ecgSamples_factorUnit, 
measurements_ecgSamples_samples from eliot2 WHERE metadata_measurementType = 'ECG'"""

cols = ['IMEI', 'model', 'tz', 'ts', 'type', 'ucid', 'min', 'max', 'rate', 'rateunit', 'factor', 'factorunit', 'samples']

static_folder = '/home/bitnami/bokeh/app/eliot/static'  # PROD bokeh server = eliot/static
#static_folder = 'static'  # DEV oC/eliot/static
relative_folder = '/eliot/static'  # static_folder URL replaced by this value for bokeh display

dfecg = pd.DataFrame([i for i in DBconx('T').query(sql)],columns=cols)
dfecg['format'] = 'pdf'
dfecg.loc[dfecg.samples.str.startswith('[{'), 'format'] ='raw'

r = redis.Redis()
dfecg['aurl'] = dfecg[dfecg['format']=='pdf'].apply(lambda  x: TempFileURL(x.samples,x.ucid, x.ts.strftime('%y%m%d_%H%M%S'), r).png, axis=1)
dfecg['aurl']= dfecg['aurl'].str.replace(static_folder, relative_folder, regex=False)

r.set("eliot_dfecg", pickle.dumps(dfecg))  #