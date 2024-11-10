# con_pooling_alch.py
#https://python.plainenglish.io/mastering-connection-pooling-in-python-optimizing-database-connections-72d66ec2bfcb

from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from time import time
import duckdb
import datetime

# Create a DuckDB test with connection pooling
# recycle pool after 120 seconds
#
engine = create_engine('duckdb:///md:meters_db', poolclass=QueuePool,\
pool_size=5,pool_pre_ping=True,pool_recycle=10,echo_pool=False, \
isolation_level="REPEATABLE READ")

Session = sessionmaker(bind=engine)
rowcnt = 0
moduscnt = 0
rowcount = 1
beg = datetime.datetime.now()
for _ in range(100):
    rowcount = 1
    with engine.begin() as con:
        con.execute('set max_memory="12GB"')
        con.execute('set memory_limit="12GB"')
        con.execute("set temp_directory='D:\\temp\\duckdb'")
        con.execute('set worker_threads = 6')
        con.execute('set threads=6')
        result = con.execute("select distinct(modus) as mod, count(*) as number \
        from rawlwp$ group by modus order by modus desc ")
        for row in result:
            print(f"Rowcount {rowcount} Modus {row[0]}         Count: {row[1]}")
            moduscnt = moduscnt + (row[1])
            rowcount = rowcount + 1
            rowcnt = rowcnt + 1

end = datetime.datetime.now()

tot = (end - beg) 
print(f"\nTime taken with connection pooling with {rowcnt} rowws \n in {tot} time and\
 a total modus count of {moduscnt}")