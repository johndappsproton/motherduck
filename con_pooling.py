# con_pooling.py
#
import duckdb
import datetime
import time

beg = datetime.datetime.now()

rowit    = 0
idx      = 0
nocons   = 3
noloops  = 1
cons     = []
nocons   = 2
db_name  = "md:meters_db"
con = duckdb.connect(db_name)
#con = duckdb.connect("meters_db.db")
con.execute('set max_memory="12GB"')
con.execute('set memory_limit="12GB"')
con.execute("set temp_directory='D:\\temp\\duckdb'")
con.execute('set worker_threads = 6')
con.execute('set threads=6')
con1 = duckdb.connect(db_name)
con2 = duckdb.connect(db_name)
con3 = duckdb.connect(db_name)

cons = [con, con, con]
rows = con.execute("pragma md_version")
row = rows.fetchone()

print(f"Start con_pooling with DB {db_name} and {noloops} loops , \
with MD {row[0]}, DuckDB {duckdb.__version__}")
cur  = cons[0].cursor()
rows = cur.execute \
("select distinct(modus, avg(ta), \
max(trl),max(tvl), max(tbw), max(tfb1)) \
from rawlwp$ group by modus;")

for row in rows.fetchall():
    rowit = row
#    print (row)

rows = cur.execute("select modus, count(*) from rawlwp$ group by modus")
for row in rows.fetchall():
    rowit = row
#    print (row)

cur  = cons[1].cursor()
rows = cur.execute("select dateshort, count(*), avg(ta) from \
rawlwp$ group by dateshort")
row = rows.fetchone()
rowit = row
    #print (row)

cur  = cons[2].cursor()
rows = cur.execute("select strftime(datumuhrzeit,'%Y'),count(*) \
from rawlwp$ group by strftime(datumuhrzeit,'%Y')")
row = rows.fetchone()
rowit = row
#print (row)

cntloops = 0 
prep = cur.execute("prepare rawq as select modus,count(*) as cnt \
from rawlwp$ group by rollup(modus) order by modus")
countcons = 3
nocons    = 0
loopcons  = 0
while cntloops < noloops:
    cur = cons[nocons]
    rows = cur.execute("select modus,count(*) as cnt \
    from rawlwp$ group by rollup(modus) order by modus")
    for row in rows.fetchall():
        rowit = row
    while loopcons < countcons:
        nocons = nocons + 1
        loopcons = loopcons +1
        if nocons == 2:
            stat = con1.close()
            stat = con2.close()
            stat = con3.close() 
            con1 = duckdb.connect(db_name)
            con2 = duckdb.connect(db_name)
            con3 = duckdb.connect(db_name)
            cons = [con1, con2, con3]
            nocons = 0
            loopcons = 0
#            print("Change con")
    cntloops = cntloops + 1


end = datetime.datetime.now()
t = end - beg
print ("Total time ", t)

stat = cons[0].close()
stat = cons[1].close()
stat = cons[2].close()        