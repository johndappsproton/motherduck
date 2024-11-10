# con_pooling_test.py
#
import duckdb
import datetime
import time
import sys
import os
import psutil
import timeit
import setup_duck
setup_duck.setup_duck('R')
#
# set to 1 if detailed prints required
#
ifprint= 1
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
con.execute("set threads=12")

# user 2
cur1 = duckdb.connect(\
    "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTcxMTgyMjQyOCwiZXhwIjoxNzQzMzgwMDI4fQ.ilGMY49gBy2bXsSAZ1zBFWQunktnlESb5xiG8pjbIo0")
# user 2
cur2 = duckdb.connect(\
    "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTcxMTgyMjQyOCwiZXhwIjoxNzQzMzgwMDI4fQ.ilGMY49gBy2bXsSAZ1zBFWQunktnlESb5xiG8pjbIo0")
# user 4
cur3 = duckdb.connect(\
    "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5hcHBzLm91dGxvb2suY29tIiwiZW1haWwiOiJqb2huLmFwcHNAb3V0bG9vay5jb20iLCJ1c2VySWQiOiJmZjQwM2Q0Yy1kNTVjLTQ1MTQtOGY1Zi0wMzQ4YmNiZTk1MmEiLCJpYXQiOjE3MTE4MjI1MzMsImV4cCI6MTc0MzM4MDEzM30.Hq8raVv3TPcMffYPOszDVWWq4IE0daHaQhaP6L8K-wE")
# user2
cur4 = duckdb.connect(\
    "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTcxMTgyMjQyOCwiZXhwIjoxNzQzMzgwMDI4fQ.ilGMY49gBy2bXsSAZ1zBFWQunktnlESb5xiG8pjbIo0")
# user 7
cur5 = duckdb.connect(\
    "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoid29vZy5hbGV4LmFuZHJhLmdtYWlsLmNvbSIsImVtYWlsIjoid29vZy5hbGV4LmFuZHJhQGdtYWlsLmNvbSIsInVzZXJJZCI6ImU1Mzc3OGVjLWZkNTctNDQ1Yy04MWVmLWJmZDU5MzQxYjMyYiIsImlhdCI6MTcyMDYyOTU2NSwiZXhwIjoxNzUyMTg3MTY1fQ.vgiqXfw88vdJQKOTn1gQ7cTopciqEGG3ZNRExpLgTyw")
# user 5
cur6 = duckdb.connect(\
    "md:t1t2?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9lLmNoaWVtZ2F1Lm91dGxvb2suY29tIiwiZW1haWwiOiJqb2UuY2hpZW1nYXVAb3V0bG9vay5jb20iLCJ1c2VySWQiOiI0NjE5NDEwMi04Nzg1LTRlYmEtOWMyOS1mZjYyZjFmNmFmMWUiLCJpYXQiOjE3MTE4MjI2MTUsImV4cCI6MTc0MzM4MDIxNX0.q-niiC63ALFdAFohU8TVtSDK1T9YIu0StQHSk7RzfE0")
cur7 = duckdb.connect(\
    "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoid29vZy5hbGV4LmFuZHJhLmdtYWlsLmNvbSIsImVtYWlsIjoid29vZy5hbGV4LmFuZHJhQGdtYWlsLmNvbSIsInVzZXJJZCI6ImU1Mzc3OGVjLWZkNTctNDQ1Yy04MWVmLWJmZDU5MzQxYjMyYiIsImlhdCI6MTcyMDYyOTU2NSwiZXhwIjoxNzUyMTg3MTY1fQ.vgiqXfw88vdJQKOTn1gQ7cTopciqEGG3ZNRExpLgTyw")

#
# Define how often the connections are re-esablished
#
loopcontrol = 13    # defines how often to close and open connections
cntloops    = 0     # holds the loop counter
noloops     = 10    # determines the number of loops
forloops    = 1     # determines the number of times each execute is run
fortotalbeg = datetime.datetime.now()
fortotalend = datetime.datetime.now()
fortotal = 0.0
rows = cur.execute('pragma md_version')
row = rows.fetchone()
res = cur.execute("set threads=12")
res = cur.execute('set streaming_buffer_size = "953.6 MiB"')

db_version = duckdb.__version__

print (f"MD {row[0]} and {db_version} and DB {db_name} \
 and {noloops} loops and loopcontrol {loopcontrol} ")

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
    if (noloops % loopcontrol) == 0:
        stat = cur.close() 
        stat = cur1.close() 
        stat = cur2.close() 
        stat = cur3.close() 
        stat = cur4.close() 
        stat = cur5.close() 
        stat = cur6.close() 
        # user 2
        cur1 = duckdb.connect(\
            "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTcxMTgyMjQyOCwiZXhwIjoxNzQzMzgwMDI4fQ.ilGMY49gBy2bXsSAZ1zBFWQunktnlESb5xiG8pjbIo0")
        # user 3
        cur2 = duckdb.connect(\
            "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5hcHBzLnNraWZmLmNvbSIsImVtYWlsIjoiam9obi5hcHBzQHNraWZmLmNvbSIsInVzZXJJZCI6ImM4YTI1NjI0LTY1MmEtNGNiZi1iZWI2LWY2YmViYTIwOTU2OCIsImlhdCI6MTcxMTgyMjQ5MiwiZXhwIjoxNzQzMzgwMDkyfQ.GbgeQyEjEKPhfISVYhpHD9a88AXa7BRLvXfUcGXrceg")
        # user 4
        cur3 = duckdb.connect(\
            "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5hcHBzLm91dGxvb2suY29tIiwiZW1haWwiOiJqb2huLmFwcHNAb3V0bG9vay5jb20iLCJ1c2VySWQiOiJmZjQwM2Q0Yy1kNTVjLTQ1MTQtOGY1Zi0wMzQ4YmNiZTk1MmEiLCJpYXQiOjE3MTE4MjI1MzMsImV4cCI6MTc0MzM4MDEzM30.Hq8raVv3TPcMffYPOszDVWWq4IE0daHaQhaP6L8K-wE")
        cur4 = duckdb.connect(\
            "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5kLmFwcHMuZ21haWwuY29tIiwiZW1haWwiOiJqb2huLmQuYXBwc0BnbWFpbC5jb20iLCJ1c2VySWQiOiI3MjYyNzQxMS1lZjE4LTRkNGEtYTg3Ny1iM2U1ZDUxOTRhOWQiLCJpYXQiOjE3MTE4MjIzNTgsImV4cCI6MTc0MzM3OTk1OH0.t8dAyF9GLMxgMSPDGnMKRTO7_FUudxSw0Q3MXlVE8hU")
        # user 7
        cur5 = duckdb.connect(\
            "md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoid29vZy5hbGV4LmFuZHJhLmdtYWlsLmNvbSIsImVtYWlsIjoid29vZy5hbGV4LmFuZHJhQGdtYWlsLmNvbSIsInVzZXJJZCI6ImU1Mzc3OGVjLWZkNTctNDQ1Yy04MWVmLWJmZDU5MzQxYjMyYiIsImlhdCI6MTcyMDYyOTU2NSwiZXhwIjoxNzUyMTg3MTY1fQ.vgiqXfw88vdJQKOTn1gQ7cTopciqEGG3ZNRExpLgTyw")
        # user 2
        cur6 = duckdb.connect(\
            "md:t1t2?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5kLmFwcHMuZ21haWwuY29tIiwiZW1haWwiOiJqb2huLmQuYXBwc0BnbWFpbC5jb20iLCJ1c2VySWQiOiI3MjYyNzQxMS1lZjE4LTRkNGEtYTg3Ny1iM2U1ZDUxOTRhOWQiLCJpYXQiOjE3MTE4MjIzNTgsImV4cCI6MTc0MzM3OTk1OH0.t8dAyF9GLMxgMSPDGnMKRTO7_FUudxSw0Q3MXlVE8hU")
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
    
#       if ifprint:
        print ("Closing and re-opning connection", cntloops+1)
    cntloops = cntloops + 1
endCPU = time.process_time_ns()    
end = datetime.datetime.now()
totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

t = end - beg
print (sys.argv[0],"time:", t)