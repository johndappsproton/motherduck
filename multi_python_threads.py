# multi_python_threads.py
# https://duckdb.org/docs/guides/python/multiple_threads.html
import duckdb
from threading import Thread, current_thread
import random
import datetime

db_name = 'md:meters_db'
duckdb_con = duckdb.connect(db_name)

#duckdb_con.execute("""
#    CREATE OR REPLACE TABLE my_inserts (
#        thread_name VARCHAR,
#        insert_time TIMESTAMP DEFAULT current_timestamp
#    )
#""")

def write_from_thread(duckdb_con):
    # Create a DuckDB connection specifically for this thread
    local_con = duckdb_con.cursor()
    # Insert a row with the name of the thread. insert_time is auto-generated.
    thread_name = str(current_thread().name)
    local_con.begin()
    result = local_con.execute("""
        INSERT INTO my_inserts (thread_name) 
        VALUES (?)
    """, (thread_name,)).fetchall()
    local_con.commit()
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

write_thread_count = 10
read_thread_count  = 10
threads = []
print("stating multi_python_threads with", read_thread_count, "readers and",\
    write_thread_count, "writers")
# Create multiple writer and reader threads (in the same process) 
# Pass in the same connection as an argument
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
beg = datetime.datetime.now()
# Kick off all threads in parallel
for thread in threads:
    thread.start()

# Ensure all threads complete before printing final results
for thread in threads:
    thread.join()
#con1 = duckdb.connect("md:meters_db")
rows = duckdb_con.execute("SELECT count(*) from my_inserts")
row = rows.fetchone()
end = datetime.datetime.now()

print(write_thread_count,"writers")
print(read_thread_count,"readers")
print("Count my_inserts", row[0])
print("Total running time:", end-beg)