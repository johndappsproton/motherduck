# sharever_check.py
#
import duckdb
from datetime import datetime
import time
import uuid

verno   = 0
loopcnt = 10000
loops   = 0
con = duckdb.connect("md:sharever")
#con = duckdb.connect('sharever.db')
con.execute("set memory_limit='1GB';")
con.execute("set max_memory='1GB';")
con.execute("set immediate_transaction_mode = false;")
con.execute("set threads = 4")

rows = con.execute("select max(verno) from ver")
row  = rows.fetchone()
verno = row[0]
print (verno)
beg = datetime.now()
while loops < loopcnt:
    rows = con.execute("select max(verno)from ver")
    row  = rows.fetchone()
    vernew = row[0]
    stmt= (f"select verdate from ver where verno = {row[0]}")
    rows = con.execute(stmt)
    row = rows.fetchone()
    vertime = row[0]
    end = vertime
    elapsed = vertime - beg
    if vernew > verno:
        print (f"Verno has changed from {verno} to {vernew} \n")
        print (f"Received update at {datetime.now().strftime('%H:%M:%S')} sent at {vertime}\n")
        print (f"Seconds elapsed {elapsed}") 
        break
    time.sleep(1)
    loops = loops + 1
    
print (f"Thats it fo rnow")
   
   
   