# while_fetchall.py
import sys
import timeit
import duckdb
#import duckdb_engine
from sqlalchemy import create_engine
from sqlalchemy import text
import os
import psutil
import time
import setup_duck
setup_duck.setup_duck('R')

db_name = 'md:meters_db'
eng = create_engine("duckdb:///md:meters_db", \
        pool_size=7, pool_timeout=1, \
        query_cache_size=1000, \
        max_overflow=0,  pool_recycle=36)
stmt = "select dt,hup,tfb1,tbw,ta,modus,id \
    from rawmini order by id limit 10000000"
idx     = 0
ids     = 0
start   = time.time()
with eng.begin() as con:
# https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection.execution_options.params.isolation_level
# preserve_rowcount for update and delete
    with con.execution_options \
        (yield_per=100,stream_results=True).execute( \
        text(stmt)) as result:
        for rows in result.partitions():
            for row in rows:
                for rr in row:
                    idx = idx +1
                    if idx % 7 == 0:
                        ids = ids+1
    con.rollback()
    print(f"Number ids {ids}")
end = time.time()
print(f"Total time: {end-start:.2f}")
    