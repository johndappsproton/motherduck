# while_guen_tpcds.py
"""
Run queries from multiple threads.
"""
import sys
import time
import logging
import os
from threading import Lock, Thread
from time import perf_counter

import duckdb
import sqlalchemy.pool as pool
from dotenv import load_dotenv
#
# add this to control memory, threads etc.
#
#import setup_duck
#setup_duck.setup_duck('W')

_log = logging.getLogger(__name__)
# added filename='example.log'
logging.basicConfig(filename='logs\\while_guen_tpcds.log',level=logging.INFO)



class DuckDBPool(pool.QueuePool):
    """Connection pool for DuckDB databases (MD or local).
    When you run con = pool.connect(), it will return a cached copy of one of the
    database connections in the pool.
    When you run con.close(), it doesn't close the connection, it just
    returns it to the pool.

    Args:
        database_paths: A list of unique databases to connect to.
    """
    def __init__(self, database_paths, *args, timeout=3600, **kwargs):
        self.database_paths = database_paths
        self.index = 0
        self.lock = Lock()

        if "pool_size" not in kwargs:
            kwargs["pool_size"] = len(database_paths)
        if "max_overflow" not in kwargs:
            kwargs["max_overflow"] = 0
        if "reset_on_return" not in kwargs:
            kwargs["reset_on_return"] = None

        super().__init__(
            self._next_conn,
            *args,
			timeout=timeout,
            **kwargs
        )

    def _next_conn(self):
        with self.lock:
            path = self.database_paths[self.index]
            self.index += 1
            duckdb_conn = duckdb.connect(path)
            duckdb_conn.execute("set threads=12")
            #_log.debug(f"Connected to database: {self._redact_token(path)}")
            return duckdb_conn
    
    @staticmethod
    def _redact_token(database_path: str):
        if "motherduck_token" in database_path.lower():
            return database_path[:database_path.index("?")]
        return database_path

    def reset(self):
        self.dispose()
        self.recreate()

    def dispose(self):
        #_log.debug("Closing connections")
        self.index = 0
        super().dispose()

# Load tokens from .env file
load_dotenv()
token_worker_1 = os.environ["token_worker_1"]
token_worker_2 = os.environ["token_worker_2"]
token_worker_3 = os.environ["token_worker_3"]
token_worker_4 = os.environ["token_worker_4"]

# These are some test databases we'll use.
# The list has the form (token, database_name).
# In this example I just use one token (for account #4) but you can change this.
# Just make sure that each thread connects to a separate database.
databases = [
    (token_worker_1, "my_test_db_1"),
    (token_worker_1, "my_test_db_2"),
    (token_worker_1, "my_test_db_3"),
    (token_worker_1, "my_test_db_4"),
    (token_worker_1, "my_test_db_5"),
]
database_paths = [
    f"md:{database_name}?motherduck_token={token}&threads=12"
    for token, database_name in databases
]

#########################
## INITIALIZATION STEPS #
#########################
# This initalizes your test databases ont he account(s) specified.
# There is an option to skip this if you've already run it once.

# The default database name to connect to initially for generating test data.
# Make sure this is an existing database on your MotherDuck account.
default_database_name = "my_db"

# This is the tpcds scaling factor used to generate test data.
sf = 1

# Set this to True if you've run this script before
# and want to skip creating a new database.
# Set it to False if you want to recreate it for a different scaling factor.
skip_init = False

# Keep track of the connections so we can close them in the end.
connections = []

# Run the initial set up queries.
# We run these sequentially for now.
if not skip_init:
    for token, database_name in databases:
        # Connect to MotherDuck
        conn = duckdb.connect(f"md:{default_database_name}?motherduck_token={token}")
    # added threads
        conn.execute("set threads=12")
        connections.append(conn)

        # start the timer
        tstart = perf_counter()

        #_log.debug(f"Create test database {database_name} and generate data")
        conn.sql(f"CREATE OR REPLACE DATABASE {database_name};")

        #_log.debug(f"Generate TPC-DS data with scaling factor {sf}")
        conn.sql(f"USE {database_name};")
        conn.sql(f"INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf={sf});")

        # end the timer
        elapsed_time = perf_counter() - tstart
        _log.info(f"Create & install tpcds in elapsed time: {elapsed_time}")

    # Close the connections.
    for conn in connections:
        conn.close()

################################
## TESTS USING CONNECTION POOL #
################################
# This runs tests using the TPC-DS extension in DuckDB.
# For more info see: https://duckdb.org/docs/extensions/tpcds.html

# This function runs a query from a thread using the connection pool.
def query_from_thread(pool: DuckDBPool, query: str):
    tstart = perf_counter()
    local_con = pool.connect()
    #_log.debug(f"Running query on connection {local_con}")
    res = local_con.execute(query)
    rows = res.fetchall()
    _log.info(f"Number of rows returned for query {query}: {len(rows)}")
    local_con.close()
    elapsed_time = perf_counter() - tstart
    _log.info(f"Elapsed time: for query {query}: {elapsed_time}")


# Create connection pool with one connection for each unique database path
# you can change this to use different tokens as well
conn_pool = DuckDBPool(database_paths)

# Create queries
queries = [f"PRAGMA tpcds({n+1});" for n in range(10)]

#
# Added total elapsed and CPU
#
startCPU   = time.process_time_ns()
start_time = time.time()

# Run queries in threads
threads = []
for query in queries:
    threads.append(Thread(
        target=query_from_thread,
        args=(conn_pool, query)
    ))

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
#
# Calulacte total CPU and elapsed
#
endCPU   = time.process_time_ns()
_log.info(f"CPU time: {(endCPU - startCPU) / 1000000000:.2f}")
end_time = time.time()
_log.info(f"{sys.argv[0]} time: {end_time - start_time:.2f} seconds")

# Close all connections in the end.
conn_pool.dispose()

# Note: if you want to recreate the pool, you can run this lines:
# conn_pool.recreate()