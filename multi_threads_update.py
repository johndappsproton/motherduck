# multi_threads_update.py
# https://duckdb.org/docs/guides/python/multiple_threads.html
import duckdb
from threading import Thread, current_thread
import random
from datetime import datetime
import time
import psutil
import sys
import setup_duck
setup_duck.setup_duck('W')

if len(sys.argv) < 2:
    db_name = "md:meters_db"
else:
    db_name = sys.argv[1]
norows = 0
noloops = 100
totrows = 0
write_thread_count = 20
read_thread_count  = 20
db_name = 'md:meters_db'
duckdb_con = duckdb.connect(db_name)
duckdb_con.execute("set threads=1")

def write_from_thread(duckdb_con):
    global totrows    
    txn_count = 0
    try_count = 0
    noloops = 5 # Should be total of 10 loops
    # Create a DuckDB connection specifically for this thread
    local_con = duckdb_con.cursor()
    local_con.execute("set threads = 1")
    # Update  a row set thread_name to time
    thread_name = str(current_thread().name)
    while try_count < noloops:
        try:
            hh = random.randint(1,23)
            mm = random.randint(1,59)
            ss = random.randint(1,59)
            rr = random.randint(1,999)
            idkey = ss + mm + hh + rr
            local_con.begin()
            result = local_con.execute(f" \
                UPDATE my_inserts SET thread_name = '{datetime.now()}' \
                where id = {idkey}")
            norows = result.fetchone()
            totrows = totrows + norows[0]
            try_count = try_count + 1
            local_con.commit()
        except duckdb.TransactionException as e:
            local_con.rollback()
            print ('duckdb.TransactionException')
            txn_count = txn_count + 1
#            time.sleep(1)
            continue
        except Exception as e:
            print("some other error", e)
def read_from_thread(duckdb_con):
    # Create a DuckDB connection specifically for this thread
    local_con = duckdb_con.cursor()
    # Query the current row count
    thread_name = str(current_thread().name)
    results = local_con.execute("""
        SELECT 
            ? AS thread_name,
            count(*) AS row_counter,
            current_timestamp 
        FROM my_inserts
    """, (thread_name,)).fetchall()
#    print(results)
rows = duckdb_con.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
print("Starting multi_threads_update.py \n with readers", read_thread_count,
   "writers", write_thread_count, "and", db_name, "loops", noloops) 
print("Count of MY_INSERTS at beginning", row[0])
beg_rows = row[0]   

threads = []
# Create multiple writer and reader threads (in the same process) 
# Pass in the same connection as an argument
beg = datetime.now()
startCPU = time.process_time_ns()

for i in range(write_thread_count):
    threads.append(Thread(target = write_from_thread,
                            args = (duckdb_con,),
                            name = 'write_thread_'+str(i)))

for j in range(read_thread_count):
    threads.append(Thread(target = read_from_thread,
                            args = (duckdb_con,),
                            name = 'read_thread_'+str(j)))

# Shuffle the threads to simulate a mix of readers and writers
random.seed(6) # Set the seed to ensure consistent results when testing
random.shuffle(threads)

# Kick off all threads in parallel
for thread in threads:
    thread.start()

# Ensure all threads complete before printing final results
for thread in threads:
    thread.join()
end = datetime.now()
endCPU = time.process_time_ns()
totCPU = (endCPU - startCPU) / 1000000000
tot = end - beg    

con1 = duckdb.connect("md:meters_db")
rows = con1.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
end_rows = row[0]
print("Count of my_inserts", row[0], "total updates", totrows)
print("CPU time", totCPU)
print("Total running time:", tot)
