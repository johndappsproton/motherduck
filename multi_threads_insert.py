# multi_python_threads.py
# https://duckdb.org/docs/guides/python/multiple_threads.html
import duckdb
from threading import Thread, current_thread
import random
import psutil
from datetime import datetime
import setup_duck
import time
setup_duck.setup_duck()

write_thread_count = 2
read_thread_count  = 0
iiii               = 0

db_name = 'md:meters_db'
duckdb_con = duckdb.connect(db_name)
rows = duckdb_con.execute("select max(id) +1 from my_inserts")
row  = rows.fetchone()
iiii = row[0]
if iiii == None:
    iiii = 1
#duckdb_con = duckdb.connect('meters_db.db')
# Use connect without parameters for an in-memory database
# duckdb_con = duckdb.connect()
#duckdb_con.execute("""
#    CREATE TABLE my_inserts(thread_name VARCHAR, 
#    insert_time TIMESTAMP DEFAULT(current_timestamp), 
#    id BIGINT);
#""")
def write_from_thread(duckdb_con):
    # Create a DuckDB connection specifically for this thread
    global iiii 
    noloops = 100
    iiii = 1
    idd = 1
    local_con = duckdb_con.cursor()
    # Insert a row with the name of the thread. 
    #insert_time is auto-generated.
    thread_name = str(current_thread().name)
    local_con.begin()
    while idd < noloops:
        result = local_con.execute(
            f"insert into my_inserts(id, thread_name) \
            values({idd},'{thread_name}')")
        idd = idd +1
    iiii = idd
    local_con.commit()
#    result = local_con.execute("""
#        INSERT INTO my_inserts (id,thread_name) 
#        VALUES (?, ?)
#    """, (thread_name,)).fetchall()
def read_from_thread(duckdb_con):
    # Create a DuckDB connection specifically for this thread
    local_con = duckdb_con.cursor()
    # Query the current row count
    thread_name = str(current_thread().name)
    local_con.begin()
    results = local_con.execute("""
        SELECT 
            ? AS thread_name,
            count(*) AS row_counter,
            current_timestamp 
        FROM my_inserts
    """, (thread_name,)).fetchall()
    local_con.rollback()
#    print(results)
duckdb_con.begin()
rows = duckdb_con.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
duckdb_con.execute("set threads = 1")
duckdb_con.rollback()
print("Starting multi_threads_insert.py \n with readers", read_thread_count,
   "writers", write_thread_count, "and", db_name) 
print("MY_INSERTS at beginning", row[0])
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
tot = end - beg

endCPU = time.process_time_ns()
totCPU = (endCPU - startCPU) / 1000000000
print("CPU time", totCPU)

con1 = duckdb.connect("md:meters_db")
con1.begin()
rows = con1.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
end_rows = row[0]
print("Count of my_inserts", row[0], "total inserts", end_rows - beg_rows)
con1.rollback()
print("Total running time:", tot)