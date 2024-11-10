# while_test1_eng_local.py
#
import sys
import timeit
import duckdb
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

num_loops = 5
print("starting", sys.argv[0], num_loops, " loops and , mcdb.db Engine")
#eng = create_engine("duckdb:///D:\duckdb\mcdb.db", \
eng = create_engine("duckdb:///md:meters_db", \
        pool_size=7, \
        pool_timeout=10, query_cache_size=1000,
        max_overflow=0,  pool_recycle=10)
        
    
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
        con.rollback()
        print (row[0], "Notbetrieb")
    with eng.begin() as con:
        rows = con.execute(text("select distinct (*) from rawlwp$ \
        where modus = 'Brauchwasser' fetch first 1 rows only"))
        row = rows.fetchone()
        print (row[0], "Brauchwasser")
        con.rollback()
    with eng.begin() as con:
        rows = con.execute(text("select distinct (*) from rawlwp$ \
        where modus = 'Heizung' fetch first 1 rows only"))
        row = rows.fetchone()
        print (row[0], "Heizung")
        con.rollback()
    number = number + 1
#
# Stop the timer
#
endCPU = time.process_time_ns()
stop = timeit.default_timer()

print("number loops was: ", number, "num rows ", rowCount)
#
# Getting % usage of virtual_memory ( 3rd field)
#
#print("RAM memory % used:", psutil.virtual_memory()[2])

# Getting usage of virtual_memory in GB ( 4th field)
#print("RAM Used (GB):", psutil.virtual_memory()[3] / 1000000000)

# print (psutil.net_io_counters())
netend = psutil.net_io_counters()
endbsend = netend[0]
endbrecv = netend[1]
print(
    "Mega bytes sent and recceived ",
    (endbsend - startbsend) / 1000,
    (endbrecv - startbrecv) / 1000,)

totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

total_time = stop - start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)

sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
