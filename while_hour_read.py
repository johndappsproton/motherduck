# while_hour_read.py
#
# Good article explaining real, user and sys time in DuckDB CLI
#
# https://stackoverflow.com/questions/556405/what-do-real-user-and-sys-mean-in-the-output-of-time1
#
import duckdb
import random
from datetime import datetime
import sys
import timeit
import duckdb
import os
import psutil
import time
import setup_duck

setup_duck.setup_duck('R')
noloops  = 0
maxloops = 1
#db_name = 'md:meters_db'
if len(sys.argv) < 2:
    db_name = "md:meters_db"
else:
    db_name = sys.argv[1]
con = duckdb.connect(db_name)
cur = con.cursor()

cur.execute("SET temp_directory = 'd:\\temp\\duckdb'")
cur.execute("set threads=1")
con.begin()
rows = con.execute("pragma md_version")
ver = rows.fetchone()
threads = con.execute(\
"select value from duckdb_settings() where name='threads'")
thread = threads.fetchone()
rows = con.execute("select count(*) from rawlwp$")
norows = rows.fetchone()
con.rollback()
print("===================================================================")
print (f"Starting {db_name},\
Loops {maxloops}, Version {ver[0]}\
{duckdb.__version__} rows {norows} threads {thread[0]}")
print("===================================================================")

netstart = psutil.net_io_counters()
startbsend = netstart[0]
startbrecv = netstart[1]

beg = datetime.now()
cur.begin()
while noloops < maxloops:
    rows = cur.execute("select distinct(year(r.dateshort)) Dat_e, \
    modus, count(*) coun_t \
    from rawlwp$ r, pump$ p, timeline t \
    where year(r.dateshort) = year(p.dateshort) \
    and hh = (select max(hh) from timeline) \
    and hh = strftime(datumuhrzeit,'%H')  \
    group by year(r.dateshort),modus  \
    order by year(r.dateshort), modus;")
    row = rows.fetchone()
#    print ("year ", row[0])
    noloops = noloops + 1
cur.rollback()
netend = psutil.net_io_counters()
endbsend = netend[0]
endbrecv = netend[1]

print("Mega bytes sent and recceived ","\n",
    (endbsend - startbsend) / 1000, " ",
    (endbrecv - startbrecv) / 1000)
print("CPU time     ", time.process_time_ns() / 1000000000)
end = datetime.now()
tot = end - beg    
print("Total running time:", tot)