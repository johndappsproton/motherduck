# insert_floachar.py
#
import duckdb

con = duckdb.connect('floachar.db')

idno 	= 1
idlimit = 1000
string = ''
string += str(idno)
sqlstr = (f"{idno}, {idno}, '{string}'")
stmt   = (f'insert into floachar (id,floa1,varch1) values({sqlstr})')
con.begin()
while idno < idlimit:
    string = str(idno)
    sqlstr = (f"{idno}, {idno}, '{string}'")
    stmt   = (f'insert into floachar (id,floa1,varch1) values({sqlstr})')
    rows = con.execute(stmt)
    idno = idno + 1
con.commit()    
print(f"{idno} rows inserted")
