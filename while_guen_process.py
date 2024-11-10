# while_guen_process.py
"""
Script for running a list of queries in a queue and process them with 4 parallel workers.
In this example, each worker has their own MotherDuck account and writes to their own database.
"""
import multiprocessing
import os
from queue import Empty

import time
import sys

from dotenv import load_dotenv
import duckdb

# Load tokens from .env file
load_dotenv()
token_worker_1 = os.environ["token_worker_1"]
token_worker_2 = os.environ["token_worker_2"]
token_worker_3 = os.environ["token_worker_3"]
token_worker_4 = os.environ["token_worker_4"]

project_name = "test_project"
workload_name = "parallel_queries"
# Default database name. This is the database we initially connect to.
# It is different from the test database such that we can drop the test db afterwards.
default_database_name = "my_db"

# Target database name.
# It will be dropped at the end so make sure it is not used!
# If you don't want to drop it after, remove it from the cleanup_queries.
target_database_name = "my_test_db"

# Databases the workers will connect to
# worker_id: (database_path, database_name)
# If you want to use a different target database per worker,
# you can change the target databases here.
worker_databases = {
    "worker_1": (
        f"md:{default_database_name}?motherduck_token={token_worker_1}",
        target_database_name
    ),
    "worker_2": (
        f"md:{default_database_name}?motherduck_token={token_worker_2}",
        target_database_name
    ),
    "worker_3": (
        f"md:{default_database_name}?motherduck_token={token_worker_3}",
        target_database_name
    ),
    "worker_4": (
        f"md:{default_database_name}?motherduck_token={token_worker_4}",
        target_database_name
    ),
}

num_workers = len(worker_databases)

# Create queries to run on init.
# This creates your target database if it doesn't yet exist.
init_queries = [
    f"CREATE DATABASE IF NOT EXISTS {target_database_name};",
    f"USE {target_database_name};",
]

# Create a list of queries to run
# These queries will be added to a queue and run in parallel by {num_workers} workers
queries = [
    f"SELECT 'Hello, world! [{n}]';" for n in range(42)
]

# Create queries to run on shutdown.
# This drops the MotherDuck database used for testing.
# Uncomment the lines below if you want to drop it after!
cleanup_queries = [
    f"USE {default_database_name}",
    f"DROP DATABASE IF EXISTS {target_database_name};",
]

# Don't drop database after running the script
#cleanup_queries = []

def worker(worker_id, database_path, database_name, task_queue, results_queue):
    print(f"Connecting to {worker_id}...")
    conn = duckdb.connect(database_path)
    resu = conn.execute("set threads=1")
    print(f"Connected to {worker_id}")

    for query in init_queries:
        cursor = conn.cursor()
        print(f"[{worker_id}] Running init query: {query}")
        cursor.sql(query)
        cursor.close()

    while True:
        try:
            query = task_queue.get(timeout=1)
            if query is None:
                break

            if database_name:
                query = f"USE {database_name};\n" + query

            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            results_queue.put((worker_id, query, results))
            cursor.close()

        except Empty:
            continue

        except Exception as e:
            results_queue.put((worker_id, query, str(e)))

    for query in cleanup_queries:
        cursor = conn.cursor()
        print(f"[{worker_id}] Running cleanup query: {query}")
        cursor.sql(query)
        cursor.close()

    conn.close()


def main():
    task_queue = multiprocessing.Queue()
    results_queue = multiprocessing.Queue()
    
    # Create worker processes
    workers = []
    for worker_id, (database_path, database_name) in worker_databases.items():
        p = multiprocessing.Process(
            target=worker,
            args=(worker_id, database_path, database_name, task_queue, results_queue)
        )
        workers.append(p)
        p.start()

    for query in queries:
        task_queue.put(query)
    
    # Add termination signals for workers
    for _ in range(num_workers):
        task_queue.put(None)
    
    # Wait for all workers to finish
    for p in workers:
        p.join()
    
    # Process results
    while not results_queue.empty():
        worker_id, query, result = results_queue.get()
        print(worker_id)
        print(f"Query: {query}")
        print(f"Result: {result}")
        print()

if __name__ == "__main__":
    start_time = time.time()
    startCPU   = time.process_time_ns()    
    main()
    endCPU   = time.process_time_ns()
    end_time = time.time()
    print(f"CPU time {(endCPU - startCPU) / 1000000000:.2f}")
    print(f"{sys.argv[0]} time: {end_time - start_time:.2f} seconds")    