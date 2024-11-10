# pyarrow_t1.py

import duckdb
from datetime import datetime
import time
import uuid
import psutil
import sys

db_name = "md:t1t2"
if len(sys.argv) < 2:
    db_name  = "md:t1t2"
else:
    db_name  = sys.argv[1]
db_share = "t1t2"
verno       = 0
loopcnt     = 1
loops       = 0
recordcnt   = 0
maxrecord   = 1 # number rows to select from other table

con1= duckdb.connect("t1t2.db")
con = duckdb.connect("md:t1t2")
con.execute('set streaming_buffer_size = "8192 MiB"');
con.execute('SET preserve_insertion_order = false;')
print (f"DuckDB {duckdb.__version__} DB Name {db_name}")
print(f"Starting{sys.argv[0]} on {db_name} with  \
{maxrecord} rows from pytable\n")

con.begin()
resultb = con.execute("select count(*) recs from t1").fetchone()
con.rollback()
#print("starting", resultb[0])

beg = time.time()
netstart            = psutil.net_io_counters()
startbsend          = netstart[0]
startbrecv          = netstart[1]
startpackssend      = netstart[2]
startpacksreceive   = netstart[3]
start_time          = time.time()
startCPU            = time.process_time_ns()
recordcnt  = 0

con1.begin()
arrow_table = con1.execute(f"SELECT * FROM t2 order by id \
    limit {maxrecord}").arrow()
con1.rollback()

con.begin()
rows = con.execute("insert into t1 (decimalnr, date,time,comment) \
 select decimalnr, date,time,comment from arrow_table")
con.commit()

netend   = psutil.net_io_counters()
endbsend = netend[0]
endbrecv = netend[1]
endpackssend    = netend[2]
endpacksreceive = netend[3]
endCPU = time.process_time_ns()
totCPU = (endCPU - startCPU) / 1000000000
packstotsend = endpackssend - startpackssend
packstotreceive = endpacksreceive - startpacksreceive
packstot = packstotreceive + packstotsend
end = time.time()
tot = end - beg

veruuid = str(uuid.uuid4())
verdate = str(datetime.today())
vertime = datetime.now().strftime('%H:%M:%S')
verdatetime = datetime.now()

con.begin()
verno = con.execute("select count(*) recs from ver").fetchone()
sqlstr  = (f"{verno[0]+1},'{verdate}','{vertime}','{verdatetime}','{veruuid}'")
con.execute(f"insert into ver values({sqlstr})")
con.commit()
con.execute(f"update share {db_share}")    

con.begin()
resulte = con.execute("select count(*) recs from t1").fetchone()
con.rollback()
print("Total rows inserted", resulte[0] - resultb[0],"\n")
print(
    "Mega bytes sent and recceived ",
    (endbsend - startbsend) / 1000,
    (endbrecv - startbrecv) / 1000,"\n")

print(f"Packs SENT {packstotsend} RECEIVED {packstotreceive}\
 packs TOTAL {packstot}\n")
print("Total CPU time", totCPU, "\n")
print(f"inserted rows {maxrecord} in {tot:.2f} time:")
