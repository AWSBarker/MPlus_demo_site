from selenium import webdriver
from selenium.webdriver.firefox.options import Options
#from selenium.webdriver.firefox.service import Service
#from webdriver_manager.firefox import GeckoDriverManager

options = Options()
options.headless = True
options.log = 'debug'
#service=Service('/home/bitnami/miniconda3/envs/sharepoint_CTgov/bin/firefox') #'usr/local/bin/')
browser = webdriver.Firefox()


#options.executable_path = binpath
# Don't put the path to geckodriver in the following. But the firefox executable
# must be in the path. If not, include the path to firefox, not geckodriver below.
browser.get('https://google.com/')
print("Firefox Headless Browser Invoked")
# Print the first 300 characters on the page.
print(browser.page_source[:300])
browser.quit()
