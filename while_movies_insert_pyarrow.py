# while_movies_insert_pyarrow.py
import duckdb
import pyarrow as pa
import sys
import time
import psutil

# Connect to DuckDB
duckdb_conn = duckdb.connect('movies_db.db')

# Connect to MotherDuck
# Replace 'your_motherduck_token' with your actual MotherDuck token
md_conn = duckdb.connect('md:movies_db')
md_conn.execute("set immediate_transaction_mode = false;")
startCPU    = time.process_time_ns()
nets = nets = psutil.net_io_counters()
packbsent   = nets[2]
packbrecv   = nets[3]

beg = time.time()
# Read data from DuckDB table into a PyArrow table
query = "SELECT * FROM movies"
duckdb_conn.begin()
duck_arrow_table = duckdb_conn.execute(query).arrow()
duckdb_conn.rollback()

# Insert PyArrow table into MotherDuck
md_conn.begin()
md_conn.execute("CREATE TABLE IF NOT EXISTS movies AS SELECT * FROM duck_arrow_table")
md_conn.commit()
end = time.time()
nets  = nets = psutil.net_io_counters()
packesent = nets[2]
packerecv = nets[3]
packesenttot = packesent - packbsent
packsbrecvtot= packerecv - packbrecv
endCPU   = time.process_time_ns()
print(f"Packets sent & recv {packesenttot} and {packsbrecvtot}\
    total packets {packesenttot + packsbrecvtot}\n")
print(f"CPU time {(endCPU - startCPU) / 1000000000}\n")

# Close connections
duckdb_conn.close()
rows = md_conn.execute("select count(*) recs from movies").fetchall()
for row in rows:
    print(f"Number rows {row[0]}")
md_conn.close()

print(f"{sys.argv[0]} INSERT time: {end - beg:2f} ")