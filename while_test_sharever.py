# while_test_sharever.py
#
import duckdb
from datetime import datetime
import time
import uuid
import psutil
import tracemalloc
import setup_duck
setup_duck.setup_duck('R')

# disk I/O counters [0] and [1] for reads and writes
bdread  = 0
bdwrite = 0
edread  = 0
edwrite = 0
# network I/O counters [0] and [1] for sent and received
bnread  = 0
bnwrite = 0
enread  = 0
enwrite = 0
# memory counters
#https://www.geeksforgeeks.org/monitoring-memory-usage-of-a-running-python-program/
# tracemalloc.start()
# allocmem = tracemalloc.get_traced_memory()
# tracemalloc.stop()
curmem = 0  # [0] memory currently allocated
totmem = 0  # [1] total memory used

verno   = 0
loopcnt = 10000
loops   = 0
con = duckdb.connect("md:meters_db")
con.execute("set memory_limit='1GB';")
con.execute("set max_memory='1GB';")
con.execute("set immediate_transaction_mode = false;")
con.execute("set threads = 4")

while 1:
    con.begin()
    rows = con.execute("select max(verno) from ver")
    row  = rows.fetchone()
    verno = row[0]
    con.rollback()
    print (verno)
    beg = datetime.now()
    while loops < loopcnt:
        con.begin()
        rows = con.execute("select max(verno)from ver")
        row  = rows.fetchone()
        vernew = row[0]
        stmt= (f"select verdatetime from ver where verno = {row[0]}")
        rows = con.execute(stmt)
        row = rows.fetchone()
        vertime = row[0]
        con.rollback()
        end = vertime
        elapsed = vertime - beg
        if vernew > verno:
            print (f"Verno has changed from {verno} to {vernew} \n")
            print (f"Received update at {datetime.now().strftime('%H:%M:%S')} sent at {vertime}\n")
            print (f"Seconds elapsed {elapsed}") 
            break
        loops = loops + 1
end = datetime.now()
tot = end - beg    
print (f"Thats total time after ", tot)
