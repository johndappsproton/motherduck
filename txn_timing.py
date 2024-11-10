# txn_timing.py
#
# Check out txn times with duckdb
#
from time import process_time 
import duckdb

txnrange = 100_000
db_name='md:meters_db'
#db_name='t1t2.db'
con = duckdb.connect('db_name')
print("starting txn_timing.py with range of", txnrange)
# Start the stopwatch / counter 
t1_start = process_time() 

beg = process_time() 
for i in range(txnrange): 
    con.begin()
    con.commit()
end = process_time() 
print("Commit ended in", end-beg)
beg = process_time() 
for i in range(txnrange): 
    con.begin()
    con.rollback()
end = process_time() 
print("rollback ended in", end-beg)
# Stop the stopwatch / counter 
t1_stop = process_time() 
print("Elapsed time:", t1_stop, t1_start) 
print("Elapsed time during the whole program in seconds:", t1_stop-t1_start) 
