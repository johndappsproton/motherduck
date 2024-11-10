# single_threads_update.py

import duckdb
import random
from datetime import datetime
import time
import sys
import psutil
import random
import setup_duck
setup_duck.setup_duck('W')

norows = 0
totrows = 0
txn_count = 0
try_count = 0 
rr = random.randint(1,17)
ss = random.randint(1,59)
mm = random.randint(1,59)   
hh = random.randint(1,23)
if ss < 10:
    ss = ss + 10
if hh < 10:
    hh = hh + 10
if mm < 10:
    mm = mm + 10
if len(sys.argv) < 2:
    db_name = "md:meters_db"
else:
    db_name = sys.argv[2]

duckdb_con = duckdb.connect(db_name)
local_con = duckdb_con.cursor()
rows = duckdb_con.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
beg_rows = row[0]   

idkey = (ss+mm+hh+rr)
if idkey > beg_rows:
    idkey = idkey - begrows
print(idkey)

print("Starting", sys.argv[0], " and", db_name, duckdb.__version__) 
print("Count of MY_INSERTS at beginning", row[0])

startCPU = time.process_time_ns()
beg = datetime.now()

noloops = 10
while try_count < noloops:
    try:
        local_con.begin()
        result = local_con.execute(f" \
                UPDATE my_inserts SET thread_name = '{datetime.now()}' \
                where id = {idkey}")
        norows = result.fetchone()
        totrows = totrows + norows[0]
        try_count = try_count + 1
        local_con.commit()
    except duckdb.TransactionException as e:
        local_con.rollback()
        print ('duckdb.TransactionException')
        txn_count = txn_count + 1
#        time.sleep(1)
        continue
    except Exception as e:
        print("some other error", e)

endCPU = time.process_time_ns()
end = datetime.now()
tot = end - beg    
totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

con1 = duckdb.connect("md:meters_db")
rows = con1.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
end_rows = row[0]
print("Count of my_inserts table", row[0], "total updates", totrows)
print("Total running time:", tot)
