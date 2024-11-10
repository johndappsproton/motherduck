# while_test1_eng.py
#
import sys
import timeit
import duckdb
#import duckdb_engine
from sqlalchemy import create_engine
from sqlalchemy import text
import os
import psutil
import time
import setup_duck
setup_duck.setup_duck('R')

# Getting loadover15 minutes
load1, load5, load15 = psutil.getloadavg()
# print(psutil.net_io_counters())
netstart = psutil.net_io_counters()
startbsend = netstart[0]
startbrecv = netstart[1]

num_loops = 10
if len(sys.argv) < 2:
    db_name = "md:meters_db"
else:
    db_name = sys.argv[1]
# con = duckdb.connect ('meters_db.db')
eng = create_engine("duckdb:///md:meters_db", \
        pool_size=20, pool_timeout=100, \
        query_cache_size=1000,
        max_overflow=0,  pool_recycle=3600)
with eng.begin() as connection:
    rows = connection.execute(text("pragma md_version"))
    row = rows.fetchone()
    print("starting", sys.argv[0], num_loops, " loops and ", db_name, \
    duckdb.__version__, "DB", row[0])
    connection.rollback()
    
index = 0
numModus = 0
number = 0
#
# Start the timer
#
start = timeit.default_timer()
startCPU = time.process_time_ns()

while number < num_loops:
    with eng.begin() as con:
        rows = con.execute(text(\
            "select count(distinct datumuhrzeit) from rawlwp$"))
        for row in rows.fetchone():
            rowCount = row
        number = number + 1
con.rollback()
num_loops = 1
number    = 0
#+
# Thefollowing does not work due to 'server is shutting down: SIGTERM'
# Try now using Modus to limit the number of rows 
#-
while number < num_loops:
    with eng.begin() as con:
        rows = con.execute(text(\
            "select distinct (*) from rawlwp$ \
            where modus = 'Notbetrieb' fetch first 1 rows only"))
        row = rows.fetchone()
#        print (row[0], "Notbetrieb")
        con.rollback()
    with eng.begin() as con:
        rows = con.execute(text("select distinct (*) from rawlwp$ \
        where modus = 'Brauchwasser' fetch first 1 rows only"))
        row = rows.fetchone()
#        print (row[0], "Brauchwasser")
        con.rollback()
    with eng.begin() as con:
        rows = con.execute(text("select distinct (*) from rawlwp$ \
        where modus = 'Heizung' fetch first 1 rows only"))
        row = rows.fetchone()
#        print (row[0], "Heizung")
        con.rollback()
    number = number + 1
#
# Stop the timer
#
endCPU = time.process_time_ns()
stop = timeit.default_timer()

print("number loops was: ", number, "num rows ", rowCount)
netend = psutil.net_io_counters()
endbsend = netend[0]
endbrecv = netend[1]
print(
    "Mega bytes sent and recceived \n",
    (endbsend - startbsend) / 1000,
    (endbrecv - startbrecv) / 1000,)

totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

total_time = stop - start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)

sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
