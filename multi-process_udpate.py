# multi-process_update.py
#
# https://bard.google.com/chat/959f077821e147c8
#
import datetime
import multiprocessing
import duckdb
import time
import os
import psutil
import sys
import timeit

def calculate_square(number):
    md_db_token = 'motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTY5Mjk3NzMzMSwiZXhwIjoxNzI0NTM0OTMxfQ.HcSXyc1_kYs1dDus5hUcPIIoEPRh-LiKynFZoLtqu0M'
    mdb_db_con = 'md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTY5Mjk3NzMzMSwiZXhwIjoxNzI0NTM0OTMxfQ.HcSXyc1_kYs1dDus5hUcPIIoEPRh-LiKynFZoLtqu0M'

    mdb_sh_token = 'motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTY5Mjk3NzMzMSwiZXhwIjoxNzI0NTM0OTMxfQ.HcSXyc1_kYs1dDus5hUcPIIoEPRh-LiKynFZoLtqu0M'
    mdb_sh_con = 'md:meters_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTY5Mjk3NzMzMSwiZXhwIjoxNzI0NTM0OTMxfQ.HcSXyc1_kYs1dDus5hUcPIIoEPRh-LiKynFZoLtqu0M'
#
# Number of loops should be passed in
#
    number_loops = number
    num_loops = 0
    result = number
    process_name = multiprocessing.current_process().name
    
    os.environ["motherduck_token"] = \
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5kLmFwcHMuZ21haWwuY29tIiwiZW1haWwiOiJqb2huLmQuYXBwc0BnbWFpbC5jb20iLCJ1c2VySWQiOiI3MjYyNzQxMS1lZjE4LTRkNGEtYTg3Ny1iM2U1ZDUxOTRhOWQiLCJpYXQiOjE3MDA0MTI3OTAsImV4cCI6MTczMTk3MDM5MH0.q5vz8ph0FXi6QQvPypXf1tEDyiTZkkb2d616GFukymE"    
    con_mdb = duckdb.connect('md:meters_db')
    cur_mdb = con_mdb.cursor()
    con_mdsh = duckdb.connect(mdb_sh_con)
    cur_mdsh = con_mdsh.cursor()
      
    while num_loops < number_loops:
#
# This is the only process which will perform updates
#
        if process_name != 'joe':
#        print ("In process", process_name)
            rows = cur_mdb.execute("update rawlwp$ set modus = \
            'Brauchwasser-haha' where \
            datumuhrzeit='2021-03-31 13:43:00'")
            row = rows.fetchone()
            num_updates = row[0]
            rows = cur_mdb.execute('update share meters_db')
            row = rows.fetchone()
            num_updates = row[0]
        if process_name != 'joe':
#
# Reverse the update from above
#
            rows = cur_mdb.execute("update rawlwp$ set modus = \
            'Brauchwasser' where \
            datumuhrzeit='2021-03-31 13:43:00'")
            row =rows.fetchone()
            num_updates = row[0]
            rows = cur_mdb.execute('update share meters_db')
            row = rows.fetchone()
            num_updates = row[0]
#            print(f"Process '{process_name}' completed with update successful", num_updates, "to the share")
        num_loops = num_loops +1
    result = num_updates
    return result



###########################
# Main thread
###########################
if __name__ == "__main__":
    num_processes = 2
    num_loops     = 10
    processes = []
#
# this env is for the meters_db
#
    os.environ["motherduck_token"] = \
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTY5Mjk3NzMzMSwiZXhwIjoxNzI0NTM0OTMxfQ.HcSXyc1_kYs1dDus5hUcPIIoEPRh-LiKynFZoLtqu0M"
    con = duckdb.connect('md:meters_db')
#
# Get the MD version
#
    rows = con.execute("pragma md_version")
    row = rows.fetchone()
    ver = row[0]
    print ("---", ver, "---")
    print ("\n")
    print ("Check for number of modus")
    rows = con.execute("select modus, count(*) CNT from rawlwp$ group by modus order by modus")
    row = rows.fetchall()
    print(f"Count {row}")
    print(f"Modus {row}")
#    rows = con.execute("select * from duckdb_databases order by database_name").df()
#    print(rows)
    t = time.localtime()
    start_time = time.strftime("%H:%M:%S", t)
    print ("Starting at ", start_time, \
    "with ", num_processes, \
    "processes running", os.path.basename(__file__))
#
# Getting loadover15 minutes
#
    load1, load5, load15 = psutil.getloadavg()
    netstart = psutil.net_io_counters()
    startbsend = (netstart[0])
    startbrecv = (netstart[1])
    start = timeit.default_timer()
    proc_ret = 0
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
    rows = con.execute("select modus, count(*) from rawlwp$ group by modus order by modus")
    print ("\n")
    print ("Check for number of modus")
    for row in rows.fetchall():
        print (row[0], row[1])
#
# Get netowork statistics
#
    netend   = psutil.net_io_counters()
    endbsend = (netend[0])
    endbrecv = (netend[1])
    print ("\n")
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
    sys.stdout.write("Elapsed time: %d:%d:%d \n" \
    % (hours, mins, secs))	
    print("All ", num_processes, "processes completed at", \
    current_time, "with", num_loops)
#
# try generating some CSV
#
    seqno = 1
    print(f"{seqno},\
    {start_time},\
    {current_time},\
    {total_time},\
    {time.process_time_ns() / 1000000000},\
    {num_processes},\
    {num_loops},\
    {(endbsend - startbsend)/1000},\
    {(endbrecv - startbrecv)/1000},\
    {os.path.basename(__file__)}\
    {ver}")