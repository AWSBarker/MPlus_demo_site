# ccti moved to pop-up window, eed bs4 to ID seledct url

import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt
# 2. Create a User Agent (Optional)
headers = {"User-Agent": "Mozilla/5.0 (Linux; U; Android 4.2.2; he-il; NEO-X5-116A Build/JDQ39) AppleWebKit/534.30 ("
                         "KHTML, like Gecko) Version/4.0 Safari/534.30"}
# 3. Send get() Request and fetch the webpage contents
url = "https://aact.ctti-clinicaltrials.org/download"
response = requests.get(url) #"https://aact.ctti-clinicaltrials.org/pipe_files", headers=headers)
if response.status_code == 200:
    webpage = response.content
    soup = BeautifulSoup(webpage, "html.parser")
    # first dropdown list element is latest zip file
    dropbox = soup.find_all(class_='form-select')[2].contents[3]
    aurl = dropbox.attrs['value']
    filename = dropbox.text.strip()
    filedate = dt.strptime(filename[:8],'%Y%m%d').date()
    #first_drop = soup.find('option',attrs={'value': True})
    #aurl = first_drop.get('value')
    #filedate = dt.strptime(first_drop.text.lstrip('\n /app/public/static/tmp/export/')[:8],'%Y%m%d').date()
    print(aurl, filename, filedate)
