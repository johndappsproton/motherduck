# while_test1.py
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
#db_name = "md:meters_db"

print("starting", sys.argv[0], "with ", num_loops, " loops and ", db_name)
# con = duckdb.connect ('meters_db.db')
con = duckdb.connect(db_name)
cur = con.cursor()
cur.execute("set temp_directory='D:\\temp\\duckdb'")
cur.execute("set threads=1")
cur.execute("set worker_threads=1")
#
# get the current MD version
#
if db_name == "md:meters_db":
    rows    = cur.execute("pragma md_version")
    ver     = rows.fetchone()
    threads = cur.execute(\
        "select value from duckdb_settings() where name='threads'")
    thread = threads.fetchone()
    print("Ver", ver[0], "DB", \
        duckdb.__version__,"threads", thread[0])

index = 0
numModus = 0
number = 0
#
# Start the timer
#
start = timeit.default_timer()
startCPU = time.process_time_ns()

rawq = con.execute("prepare rawq as select count(distinct datumuhrzeit) from rawlwp$")
con.begin()
while number < num_loops:
    rows = con.execute("execute rawq")
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
cur.begin()
while number < num_loops:
    rows = cur.execute("select distinct (*) from rawlwp$ \
    where modus = 'Notbetrieb' fetch first 1 rows only")
    row = rows.fetchone()
#    print (row[0], "Notbetrieb")
    rows = cur.execute("select distinct (*) from rawlwp$ \
    where modus = 'Brauchwasser' fetch first 1 rows only")
    row = rows.fetchone()
#    print (row[0], "Brauchwasser")
    rows = cur.execute("select distinct (*) from rawlwp$ \
    where modus = 'Heizung' fetch first 1 rows only")
    row = rows.fetchone()
#    print (row[0], "Heizung")
    number = number + 1
cur.rollback()
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
print (f"{sys.argv[0]} time: {total_time}")
# sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
