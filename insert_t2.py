# insert_t2.py
#
# Insert some rows into T2 which has no constraints
#
#CREATE TABLE t2 (
#    id INTEGER not null,
#     decimalnr DOUBLE CHECK (decimalnr < 10),
#     date DATE not null,
#     time TIMESTAMP default current_timestamp,
#     comment varchar default 'Lovely'
#);
import duckdb
from datetime import datetime
import time
import uuid
import psutil
import sys

if len(sys.argv) < 2:
    db_name  = "md:t1t2"
else:
    db_name  = sys.argv[1]

db_share = "t1t2"
con = duckdb.connect(db_name)
cur = con.cursor()

def setparams():
    con.execute("SET preserve_insertion_order=false")
    con.execute("set memory_limit='16GB';")
    con.execute("set max_memory='16GB';")
    con.execute("set immediate_transaction_mode = false;")
    con.execute("set threads = 4")
    cur.execute("set streaming_buffer_size = '16384MiB'")

verno       = 0
loopcnt     = 1
loops       = 0
recordcnt   =0
maxrecord   =1000 # 1 million rows

#rows = con.execute("pragma md_version")
#row = rows.fetchall()
#verno = row[0]
print (f"DuckDB {duckdb.__version__} DB Name {db_name}")
print(f"Starting{sys.argv[0]} on {db_name} with  \
{maxrecord} rows at 768 at a time\n")

setparams()
beg = time.time()
netstart            = psutil.net_io_counters()
startbsend          = netstart[0]
startbrecv          = netstart[1]
startpackssend      = netstart[2]
startpacksreceive   = netstart[3]
diskstart  = psutil.disk_io_counters()
startread  = diskstart[0]
startwrite = diskstart[1]
start_time = time.time()
startCPU   = time.process_time_ns()
recordcnt  = 0

cur.begin()
while recordcnt < maxrecord:
    now   = str(datetime.now())
    today = str(datetime.today())
    recordcnt = recordcnt+ 1
    stmt = f"9, '{today}', '{datetime.now()}', 'Original'"
#
# there are 518 rows being inserted or just 6 or 624
#
    cur.execute(f"insert into t1 \
    (decimalnr, date, time, comment) \
    values ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),\
        ({stmt}),({stmt}),({stmt}),({stmt}),({stmt}),({stmt})")
cur.commit()    

veruuid = str(uuid.uuid4())
verdate = str(datetime.today())
vertime = datetime.now().strftime('%H:%M:%S')
verdatetime = datetime.now()
sqlstr  = (f"{verno},'{verdate}','{vertime}','{verdatetime}','{veruuid}'")
# print (sqlstr)
con.begin()
con.execute(f"insert into ver values({sqlstr})")
con.commit()
#begupdate = datetime.now()
#con.execute(f"update share {db_share}")    
#endupdate = datetime.now()
#print (f"Update share time {endupdate - begupdate}")
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
print(f"Packs SENT {packstotsend} RECEIVED {packstotreceive}\
 packs TOTAL {packstot}")
#print(
#    "Mega bytes sent and recceived ",
#    (endbsend - startbsend) / 1000,
#    (endbrecv - startbrecv) / 1000)
diskstart = psutil.disk_io_counters()
endread = diskstart[0]
endwrite = diskstart[1]
#print("disk read ", endread - startread, "disk write ", endwrite-startwrite)
print("Total CPU time", totCPU, "\n")
print(f"inserted rows {maxrecord} in {tot:.2f} time:")