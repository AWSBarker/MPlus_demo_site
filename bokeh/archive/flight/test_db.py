import time
from pandas.io.sql import read_sql_table
from sqlalchemy import create_engine
#import MySQLdb

def get_planes():
    cols = ['icao24', 'registration', 'manufacturername', 'model', 'owner', 'built']
    try:
        engine = create_engine('mysql+mysqldb://pi:7914920@192.168.1.173:3306/flights')
        con = engine.connect()
        df1 = read_sql_table("planes", con, index_col='icao24', columns=cols)
        con.close()
    except Exception as e:
        print(f'error connecting database {e}')
    return dict(zip(df1.index, df1.values))

planes = get_planes()
print(planes)
