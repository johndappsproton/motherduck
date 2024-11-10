# while_test2.py
#
import sys
import timeit
import duckdb
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

num_loops = 100
if len(sys.argv) < 2:
    db_name = "md:meters_db"
else:
    db_name = sys.argv[1]
print("starting test", sys.argv[0], "with ", num_loops,\
 "loops on", db_name)
# con = duckdb.connect ('meters_db.db')
con = duckdb.connect(db_name)
cur = con.cursor()
res = cur.execute("set threads = 1")
#
# get the current MD version
#
rows = con.execute("pragma md_version")
ver = rows.fetchone()
threads = cur.execute(\
"select value from duckdb_settings() where name='threads'")
thread = threads.fetchone()
print("MD", ver[0], "DB", duckdb.__version__, "threads", thread[0])

index = 0
numModus = 0
number = 0
#
# Start the timer
#
start = timeit.default_timer()
startCPU = time.process_time_ns()
try:
    cur.begin()
    rows = cur.execute("prepare rawq as pivot rawlwp$ on year(datumuhrzeit) \
    in ('2018','2019','2021','2020') \
    using avg(ta) as ta, avg(tbw) as tbw, avg(tfb1) \
    as tfb1 group by modus")
    cur.rollback()
except (RuntimeError, TypeError, NameError) as inst:
    print (type(inst))
    print (inst)
except (duckdb.IOException, duckdb.CatalogException) as duckdb:
    print ("DuckDB error ", duckdb)
    exit(0)
except Exception as e:
    print("Abort with ", e)
except KeyboardInterrupt:
    print ("Please do not interrupt while I'm working")
    
cur.begin()
while number < num_loops:
    rows = cur.execute("execute rawq")
    row = rows.fetchone()
    stringit = row
    number = number + 1
cur.rollback()
#
# Stop the timer
#
stop = timeit.default_timer()
endCPU = time.process_time_ns()
#print("number loops was: ", number)
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
    "\n",
    (endbsend - startbsend) / 1000,
    (endbrecv - startbrecv) / 1000,)

totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

total_time = stop - start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)
print (f"{sys.argv[0]} time: {total_time}")
#sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
