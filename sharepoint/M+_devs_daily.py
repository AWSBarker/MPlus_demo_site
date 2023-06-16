# Lightsail versions
# daily crontab to update M+_daily DB with devs
# TODO check link Orgs list with admin add M+_orgs

from Mapi2 import Org

if __name__ == "__main__":

    orgs = Org.getOrgs()
    for o in orgs:
       Org(o).dailyupdate()
