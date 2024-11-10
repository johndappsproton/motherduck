# multi-process.py
#
# https://bard.google.com/chat/959f077821e147c8
#
import multiprocessing
import duckdb
import time
import os
import psutil
import sys
import timeit

stmt = 'select modus, count(*) as Countmodus,min(ta) \
as minTA, avg(ta) as ta, max(ta) as maxTA, \
avg(tvl) as tvl, avg(tbw) as tbw, avg(tfb1) as tfb1 \
from rawlwp$ group by modus order by modus'
stmt1 = "select a.modus, count (a.*) as countmod,\
strftime(start,'%Y') from alldatalwp$ a, \
rawlwp$ r where a.start = r.datumuhrzeit \
group by a.modus,strftime(start,'%Y') \
order by strftime(start,'%Y'),modus,countmod"

def calculate_square(number):
    number_loops = 10
    num_loops = 0
    process_name = multiprocessing.current_process().name
    con1 = duckdb.connect('md:meters_db')
    cur1 = con1.cursor()

    while num_loops < number_loops:
        rows  = con1.execute(stmt)
        row = rows.fetchall()
        num_rows = row[0]
        rows = con1.execute(stmt1)
        row = rows.fetchall()
        num_rows = row[0]	
        rows = con1.execute('select count(distinct rawlwp$) from rawlwp$')
        row = rows.fetchall()
        result = row[0]
        rows = cur1.execute("pivot rawlwp$ \
        on year(datumuhrzeit) in ('2018','2019','2021','2020') \
        using avg(ta) as ta, avg(tbw) as tbw, avg(tfb1) as tfb1 \
        group by modus")
        for row in rows.fetchone():
            modus = row[0]
#	    print ("Modus: ", row[0])
        rows = cur1.execute('select count(distinct rawlwp$) from rawlwp$')
        for row in rows.fetchone():
            distinct_rawlwp = row[0]
#        print("Count of distinct RAWLWP$ rows")
        rows = cur1.execute("select modus, count(*) from rawlwp$ \
        group by modus order by modus")
        for row in rows.fetchone():
            modus = row[0]
        num_loops = num_loops + 1
    return result
###################################################
if __name__ == "__main__":
#
# determine how many processes to run
#    
    num_processes = 2
    num_loops     = 10
    processes = []
    con = duckdb.connect('md:meters_db')
#
# Get the MD version
#
    rows = con.execute("pragma md_version")
    row = rows.fetchone()
    ver = row[0]
    print ("---", ver, "---")

    t = time.localtime()
    start_time = time.strftime("%H:%M:%S", t)
    print ("Starting at ", start_time, \
    "with ", num_processes, "started running ", num_loops,\
    os.path.basename(__file__))
#
# Getting loadover15 minutes
#
    load1, load5, load15 = psutil.getloadavg()
    netstart = psutil.net_io_counters()
    startbsend = (netstart[0])
    startbrecv = (netstart[1])
    start = timeit.default_timer()
#
# Here we go...
#
    for num in range(1, num_processes):
        process = multiprocessing.Process(target=calculate_square, args=(num_loops,))
        processes.append(process)
    for process in processes:
        process.start()
    for process in processes:
        process.join()
#
# Stop the timer
#
    stop = timeit.default_timer()
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
#
# Get netowork statistics
#
    netend   = psutil.net_io_counters()
    endbsend = (netend[0])
    endbrecv = (netend[1])
    print ("Network bytes sent and recceived ", "\n", (endbsend - startbsend)/1000, " ",(endbrecv - startbrecv)/1000)
#
# CPU time of the _main_ process
#
    print ("CPU time", time.process_time_ns() / 1000000000)
    total_time = stop - start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
#
# How much time did the whole run take
#
    sys.stdout.write("Elapsed time: %d:%d:%d.\n" \
    % (hours, mins, secs))	
    print("All ", num_processes, "processes completed at", current_time)
    
    seqno = 1
    print(f"{seqno},\
    {start_time},\
    {current_time},\
    {total_time},\
    {time.process_time_ns() / 1000000000},\
    {num_processes}\
    {num_loops},\
    {(endbsend - startbsend)/1000},\
    {(endbrecv - startbrecv)/1000},\
    {os.path.basename(__file__)},\
    {ver}")
