# insert_t1.py
#
# Insert some rows into T1 which has a primary key and an unique date column
#
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
    # disable preservation of insertion order
    con.execute("SET preserve_insertion_order=false")
    con.execute("set memory_limit='16B';")
    con.execute("set max_memory='16GB';")
    con.execute("set immediate_transaction_mode = false;")
    con.execute("set threads = 4")
    cur.execute("set streaming_buffer_size = '16384MiB'")

verno   = 0
loopcnt = 1
loops   = 0
recordcnt=0

maxrecord = 10 # 1 million rows

#rows = con.execute("pragma md_version")
#row = rows.fetchall()
#verno = row[0]
print (f"DuckDB {duckdb.__version__} DB Name {db_name}")
print("Starting",sys.argv[0]," on ", db_name, "with ", \
    maxrecord, "rows", "768 at a time\n")

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
today = str(datetime.today())
recordcnt  = 0
cur.begin()
try:
    while recordcnt < maxrecord:
        now   = str(datetime.now())
        today = str(datetime.today())
        recordcnt = recordcnt+ 1
        stmt = f" 9, '{today}', '{datetime.now()}', 'Original'"
    #
    # there are 522 rows being inserted or 624 or 768
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
except duckdb.ConstraintException as e:
    print ('duckdb.TransactionException', e)
    cur.rollback()
    
cur.commit()    
veruuid = str(uuid.uuid4())
verdate = str(datetime.today())
vertime = datetime.now().strftime('%H:%M:%S')
verdatetime = datetime.now()
# print (sqlstr)
con.begin()
verno = con.execute("select count(*) recs from ver").fetchone()
sqlstr  = (f"{verno[0]+1},'{verdate}','{vertime}','{verdatetime}','{veruuid}'")
con.execute(f"insert into ver values({sqlstr})")
con.commit()
#begupdate = datetime.now()
#con.execute(f"update share {db_share}")    
#endupdate = datetime.now()
#print (f"Update share time {endupdate - begupdate}")
netend = psutil.net_io_counters()
endbsend = netend[0]
endbrecv = netend[1]
endpackssend    = netend[2]
endpacksreceive = netend[3]
packstotsend = endpackssend - startpackssend
packstotreceive = endpacksreceive - startpacksreceive
packstot = packstotreceive + packstotsend
#print(
#    "Mega bytes sent and recceived ",
#    (endbsend - startbsend) / 1000,
#    (endbrecv - startbrecv) / 1000)
end = time.time()
tot = end - beg
endCPU = time.process_time_ns()
totCPU = (endCPU - startCPU) / 1000000000
diskstart = psutil.disk_io_counters()
endread = diskstart[0]
endwrite = diskstart[1]
print(f"Packs SENT {packstotsend} RECEIVED {packstotreceive}\
 total packs {packstot}")
print("Total CPU time", totCPU, "\n")
#print ("disk read ", endread - startread, "disk write ", endwrite-startwrite)
print (f"{maxrecord} rows inserted in {tot:.2f} time")