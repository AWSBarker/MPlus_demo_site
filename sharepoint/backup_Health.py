from Mapi2 import DBconx

with DBconx() as con:
    #cur = con.cur
    tables = list([i['Tables_in_Health'] for i in con.query('SHOW TABLES')])

    for t in tables:
        con.backup(t)
