# con_pooling_test.py
#
# Version modified by Guen
#
import duckdb
import datetime
import time
import sys
import os
import psutil
import timeit
import setup_duck
from dotenv import load_dotenv

setup_duck.setup_duck('R')

#
# set to 1 if detailed prints required
#
ifprint = 1

#
# Define how often the connections are re-esablished
#
loopcontrol = 10
if len(sys.argv) < 2:
    db_name = "md:meters_db"
else:
    db_name = sys.argv[1]

idx = 0
nocons = 7
con = duckdb.connect(db_name)
cur = con.cursor()
con = duckdb.connect("md:meters_db?conn=bla")
cur = con.cursor()

con.execute("set temp_directory='D:\\temp\\duckdb'")
con.execute("PRAGMA enable_checkpoint_on_shutdown;")

# Get MotherDuck tokens
load_dotenv()
motherduck_token_1 = os.environ["motherduck_token_1"]
motherduck_token_2 = os.environ["motherduck_token_2"]
motherduck_token_3 = os.environ["motherduck_token_3"]
motherduck_token_4 = os.environ["motherduck_token_4"]
motherduck_token_5 = os.environ["motherduck_token_5"]
motherduck_token_5 = os.environ["motherduck_token_6"]
motherduck_token_5 = os.environ["motherduck_token_7"]

# Recording the user accounts (not used in the code)
# motherduck_token_1
user_1 = "john.d.apps.gmail.com"
# motherduck_token_2
user_2 = "johndapps.gmail.com"
# motherduck_token_3
user_3 = "john.apps.outlook.com"
# motherduck_token_4
user_4 = "woog.alex.andra.gmail.com"
# motherduck_token_5 
user_5 = "joe.chiemgau@outlook.com"
# motherduck_token 6
user_6 = "johndapps@protonmail.com"
# motherduck_token_7
user_7 = "woog.alex.andra@gmail.com"

# Create long-lived DuckDB connections

# user 1
duckdb_con1 = duckdb.connect(\
    f"md:meters_db?motherduck_token={motherduck_token_1}")
# user 2
duckdb_con2 = duckdb.connect(\
    f"md:meters_db?motherduck_token={motherduck_token_2}")
# user 3
duckdb_con3 = duckdb.connect(\
    f"md:meters_db?motherduck_token={motherduck_token_3}")
# user 4
duckdb_con4 = duckdb.connect(\
    f"md:meters_db?motherduck_token={motherduck_token_4}")
# user 5
duckdb_con5 = duckdb.connect(\
    f"md:meters_db?motherduck_token={motherduck_token_3}")
# user 6
duckdb_con6 = duckdb.connect(\
    f"md:t1t2?motherduck_token={motherduck_token_6}")
# user 7
duckdb_con7 = duckdb.connect(\
    f"md:meters_db?motherduck_token={motherduck_token_7}")

# user 1
cur1 = duckdb_con1.cursor()
# user 2
cur2 = duckdb_con2.cursor()
# user 3
cur3 = duckdb_con3.cursor()
# user 4
cur4 = duckdb_con4.cursor()
# user 5
cur5 = duckdb_con5.cursor()
# user 6
cur6 = duckdb_con6.cursor()
# user 7
cur7 = duckdb_con7.cursor()

cntloops = 0
noloops  = 10
forloops = 5
fortotalbeg = datetime.datetime.now()
fortotalend = datetime.datetime.now()
fortotal = 0.0
rows = cur.execute('pragma md_version')
row = rows.fetchone()
res = cur.execute("set threads=1")
threads = cur.execute(\
    "select value from duckdb_settings() where name='threads'")
thread = threads.fetchone()
db_version = duckdb.__version__
print (f"MD {row[0]} and {db_version} and DB {db_name} \
 and {noloops} loops and loopcontrol {loopcontrol} \
 threads = {thread[0]}")

beg = datetime.datetime.now()
begs = beg.timestamp()

cur1.begin()
prep = cur1.execute("prepare rawq1 as select distinct(modus, \
    avg(ta), max(trl),\
    max(tvl), max(tbw), max(tfb1)) from rawlwp$ \
    group by modus order by modus;")
cur1.rollback()

cur2.begin()
prep = cur2.execute("prepare rawq2 as select modus, count(*) \
        from rawlwp$ group by modus")
cur2.rollback()

cur3.begin()
prep = cur3.execute("prepare rawq3 as select strftime(dateshort,'%Y-%m') as YYDD,\
        count(*) as number, avg(ta) as avgTA \
        from rawlwp$ group by dateshort order by dateshort limit 10")
cur3.rollback()

cur4.begin()
prep = cur4.execute("prepare rawq4 as select strftime(datumuhrzeit,'%Y'), \
        count(*) from rawlwp$ group by strftime(datumuhrzeit,'%Y')\
        order by strftime(datumuhrzeit,'%Y')")
cur4.rollback()

cur5.begin()
prep = cur5.execute("prepare rawq5 as select modus,count(*) as cnt \
        from rawlwp$ group by rollup(modus) order by modus")
cur5.rollback()

cur7.begin()
prep = cur7.execute("prepare rawq7 as pivot rawlwp$ \
    on year(datumuhrzeit) \
    in ('2018','2019','2021','2020') \
    using avg(ta) as ta, avg(tbw) as tbw, avg(tfb1) \
    as tfb1 group by modus order by modus")
cur7.rollback()
#
# here we go in the main loop
#
startCPU = time.process_time_ns()

while cntloops < noloops:
    forbeg = datetime.datetime.now()
    forbegs = forbeg.timestamp()
    netstart = psutil.net_io_counters()
    startbsend = netstart[0]
    startbrecv = netstart[1]
# cur1
    cur1.begin()
    for _ in range(forloops):
        rows = cur1.execute("execute rawq1")
        row = rows.fetchall()
    cur1.rollback()
    netend = psutil.net_io_counters()
    endbsend = netend[0]
    endbrecv = netend[1]
    sent = (endbsend - startbsend) / forloops
    received = (endbrecv - startbrecv) / forloops
    forend = datetime.datetime.now()
    forends = forend.timestamp()
    fortotal = forends - forbegs
    if ifprint:
        print (f"{cntloops+1}, {sent}, {received}, {fortotal}")
    
    forbeg = datetime.datetime.now()
    forbegs = forbeg.timestamp()
    netstart = psutil.net_io_counters()
    startbsend = netstart[0]
    startbrecv = netstart[1]
# cur2
    cur2.begin()
    for _ in range(forloops):
        rows = cur2.execute("execute rawq2")
        row = rows.fetchall()
    cur2.rollback()
    netend = psutil.net_io_counters()
    endbsend = netend[0]
    endbrecv = netend[1]
    sent = (endbsend - startbsend) / forloops
    received = (endbrecv - startbrecv) / forloops
    forend = datetime.datetime.now()
    forends = forend.timestamp()
    fortotal = forends - forbegs
    if ifprint:
        print (f"{cntloops+1}, {sent}, {received}, {fortotal}")

    forbeg = datetime.datetime.now()
    forbegs = forbeg.timestamp()
    netstart = psutil.net_io_counters()
    startbsend = netstart[0]
    startbrecv = netstart[1]
# cur3
    cur3.begin()
    for _ in range(forloops):
        rows = cur3.execute("execute rawq3")
        row = rows.fetchone()
    cur3.rollback()
    netend = psutil.net_io_counters()
    endbsend = netend[0]
    endbrecv = netend[1]
    sent = (endbsend - startbsend) / forloops
    received = (endbrecv - startbrecv) / forloops
    forend = datetime.datetime.now()
    forends = forend.timestamp()
    fortotal = forends - forbegs
    if ifprint:
        print (f"{cntloops+1}, {sent}, {received}, {fortotal}")

    forbeg = datetime.datetime.now()
    forbegs = forbeg.timestamp()
    netstart = psutil.net_io_counters()
    startbsend = netstart[0]
    startbrecv = netstart[1]
# cur4
    cur4.begin()
    for _ in range(forloops):
        rows = cur4.execute("execute rawq4")
        row = rows.fetchall()
    cur4.rollback()
    netend = psutil.net_io_counters()
    endbsend = netend[0]
    endbrecv = netend[1]
    sent = (endbsend - startbsend) / forloops
    received = (endbrecv - startbrecv) / forloops
    forend = datetime.datetime.now()
    forends = forend.timestamp()
    fortotal = forends - forbegs
    if ifprint:
        print (f"{cntloops+1}, {sent}, {received}, {fortotal}")

    forbeg = datetime.datetime.now()
    forbegs = forbeg.timestamp()
    netstart = psutil.net_io_counters()
    startbsend = netstart[0]
    startbrecv = netstart[1]
# cur5
    cur5.begin()
    for _ in range(forloops):
        rows = cur5.execute("execute rawq5")
        row = rows.fetchall()
    cur5.rollback()
    netend = psutil.net_io_counters()
    endbsend = netend[0]
    endbrecv = netend[1]
    sent = (endbsend - startbsend) / forloops
    received = (endbrecv - startbrecv) / forloops
    forend = datetime.datetime.now()
    forends = forend.timestamp()
    fortotal = forends - forbegs
    if ifprint:
        print (f"{cntloops+1}, {sent}, {received}, {(sent + received)/1000000}, {fortotal}")
# cur7
    cur7.begin()
    for _ in range(forloops):
        rows = cur7.execute("execute rawq7")
        row = rows.fetchall()
    cur7.rollback()
    netend = psutil.net_io_counters()
    endbsend = netend[0]
    endbrecv = netend[1]
    sent = (endbsend - startbsend) / forloops
    received = (endbrecv - startbrecv) / forloops
    forend = datetime.datetime.now()
    forends = forend.timestamp()
    fortotal = forends - forbegs
    if ifprint:
        print (f"{cntloops+1}, {sent}, {received}, {(sent + received)/1000000}, {fortotal}")
# cur6
    cur6.begin()
    rows = cur6.execute("select count(*) from t1t2.t2")
    row = rows.fetchone()
    if ifprint:
        print(f"Number rows in t1t2.t2 is {row[0]}")
    cur6.rollback()
    if (noloops % loopcontrol) == 1:
        stat = cur.close() 
        stat = cur1.close() 
        stat = cur2.close() 
        stat = cur3.close() 
        stat = cur4.close() 
        stat = cur5.close() 
        stat = cur6.close() 
        # user 1
        cur1 = duckdb_con1.cursor()
        # user 2
        cur2 = duckdb_con2.cursor()
        # user 3
        cur3 = duckdb_con3.cursor()
        # user 4
        cur4 = duckdb_con4.cursor()
        # user 5
        cur5 = duckdb_con5.cursor()
        # user 6
        cur6 = duckdb_con6.cursor()
        # user 7
        cur7 = duckdb_con7.cursor()

        prep = cur1.execute("prepare rawq1 as select distinct(modus, avg(ta), max(trl),\
                max(tvl), max(tbw), max(tfb1)) from rawlwp$ \
                group by modus order by modus;")
        prep = cur2.execute("prepare rawq2 as select modus, count(*) \
                    from rawlwp$ group by modus")
        prep = cur3.execute("prepare rawq3 as select strftime(dateshort,'%Y-%m') as YYDD,\
                    count(*) as number, avg(ta) as avgTA \
                    from rawlwp$ group by dateshort order by dateshort limit 10")
        prep = cur4.execute("prepare rawq4 as select strftime(datumuhrzeit,'%Y'), \
                    count(*) from rawlwp$ group by strftime(datumuhrzeit,'%Y')")
        prep = cur5.execute("prepare rawq5 as select modus,count(*) as cnt \
                    from rawlwp$ group by rollup(modus) order by modus")
        prep = cur7.execute("prepare rawq7 as pivot rawlwp$ \
            on year(datumuhrzeit) \
            in ('2018','2019','2021','2020') \
            using avg(ta) as ta, avg(tbw) as tbw, avg(tfb1) \
            as tfb1 group by modus order by modus")
        if ifprint:
            print ("Closing and re-opning connection", cntloops+1)
    cntloops = cntloops + 1
endCPU = time.process_time_ns()    
end = datetime.datetime.now()
totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

t = end - beg
print (sys.argv[0],"time:", t)

# Close connections
cur1.close()
cur2.close()
cur3.close()
cur4.close()
cur5.close()
cur6.close()
cur7.close()

duckdb_con1.close()
duckdb_con2.close()
duckdb_con3.close()
duckdb_con4.close()
duckdb_con5.close()
duckdb_con6.close()
duckdb_con7.close()
