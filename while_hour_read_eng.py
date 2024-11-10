# while_hour_read_eng.py
#
# Good article explaining real, user and sys time in DuckDB CLI
#
# https://stackoverflow.com/questions/556405/what-do-real-user-and-sys-mean-in-the-output-of-time1
#
import duckdb_engine
from sqlalchemy import create_engine
from sqlalchemy import text
import random
from datetime import datetime
import sys
import timeit
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

eng = create_engine("duckdb:///md:meters_db", \
        pool_size=20, pool_pre_ping="False", \
        pool_timeout=3600, query_cache_size=10000)
with eng.begin() as connection:
    rows = connection.execute(text("pragma md_version"))
    ver = rows.fetchone()
    rows = connection.execute(text("select count(*) from rawlwp$"))
    norows = rows.fetchone()
    connection.rollback()

print (f"Starting with {db_name},\
Num Loops {maxloops}, Version {ver[0]}\
and {duckdb_engine.__version__} and no rows {norows[0]}")

netstart = psutil.net_io_counters()
startbsend = netstart[0]
startbrecv = netstart[1]

beg = datetime.now()
while noloops < maxloops:
    with eng.begin() as con:
        rows = con.execute(text("select distinct(year(r.dateshort)) Dat_e, \
        modus, count(*) coun_t \
        from rawlwp$ r, pump$ p, timeline t \
        where year(r.dateshort) = year(p.dateshort) \
        and hh = (select max(hh) from timeline) \
        and hh = strftime(datumuhrzeit,'%H')  \
        group by year(r.dateshort),modus  \
        order by year(r.dateshort), modus;"))
        row = rows.fetchone()
    noloops = noloops + 1
    con.rollback()
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