# con_pooling_alch_yield.py
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
pool_size=5,pool_pre_ping=False,pool_recycle=60,echo_pool=False, \
isolation_level="REPEATABLE READ",pool_timeout=1,query_cache_size=1000)

rowcnt = 0
moduscnt = 0
noloops = 100
minTA = 0
con = engine.connect()
#
# https://docs.sqlalchemy.org/en/20/core/connections.html
# connection = engine.connect()
# trans = connection.begin()
# connection.execute(text("insert into x (a, b) values (1, 2)"))
# trans.commit()
#
trans = con.begin()
con.execute('set max_memory="12GB"')
con.execute('set memory_limit="12GB"')
con.execute("set temp_directory='D:\\temp\\duckdb'")
con.execute('set worker_threads = 6')
con.execute('set threads=6')
trans.commit()

beg = datetime.datetime.now()
for _ in range(noloops):
    rowcount =1
    with engine.begin() as con:
        result = con.execution_options(yield_per=13).execute\
        ("select year(datumuhrzeit) as date, min(ta) as MTA, \
            avg(ta), max(ta) as MTA, modus, count(*) as count, \
            sum(tvl - trl) as TVL, \
            from rawlwp$ group by year(datumuhrzeit),  \
            modus order by year(datumuhrzeit)")
        for rows in result:
            rowcount = rowcount + 1
            print(f"{rowcount} Date: {rows[0]}, minTA: {rows[1]}, Modus: {rows[4]}")
            minTA = minTA + rows[1]
            rowcnt = rowcnt +1
                    
end = datetime.datetime.now()

tot = (end - beg) 
print(f"\n Connection pooling with {rowcnt} rowws \
    \n with {noloops} loops and total minTA = {minTA} \
    \n in {tot} time")