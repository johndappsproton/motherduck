#while_monitor_server.py

import duckdb
import sys
import time
import datetime

db_name = "md:tpcds"
if len(sys.argv) < 2:
    db_name = "md:tpcds"
else:
    db_name = sys.argv[1]
    
con = duckdb.connect("md:")
cur = con.cursor()
res = cur.execute('set http_keep_alive = "False"')
print(f"Starting at {time.time()}")
print("ConID,                                 CLID,    Txtime,          Qtime            Stage")
for _ in range(10000):
    rows = cur.execute("select \
        client_connection_id,\
        client_transaction_id,\
        server_transaction_elapsed_time,\
        server_query_elapsed_time,  \
        server_transaction_stage \
        FROM md_active_server_connections() \
        where client_connection_id != \
        (md_current_client_connection_id()) \
        order by client_connection_id,\
        client_transaction_id;").fetchall()
    for row in rows:
        print(f"{str(row[0]):24},{row[1]:4d},\
        {row[2]},\
        {row[3]},\
        {row[4]:20}")
        time.sleep(1)
print(f"Finished at {time.time()}")
 
