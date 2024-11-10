#while_copy_dbs.py

import duckdb

def copy_duckdb_database(src_db_path, dst_db_path):
    """
    Copy a DuckDB database from one file to another.
    
    Parameters:
    src_db_path (str): Path to the source DuckDB database file.
    dst_db_path (str): Path to the destination DuckDB database file.
    """
    # Connect to the source database
    src_conn = duckdb.connect(src_db_path)
    
    # Get a list of all tables in the source database
    tables = [row[0] for row in src_conn.execute("SHOW TABLES;").fetchall()]
    
    # Connect to the destination database
    dst_conn = duckdb.connect(dst_db_path)
    
    # Copy each table from the source to the destination
    for table in tables:
        src_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM {table};").fetchall()
        dst_conn.execute(f"CREATE TABLE {table} AS SELECT * FROM {table};").fetchall()
    
    # Close the connections
    src_conn.close()
    dst_conn.close()
copy_duckdb_database("h:\\duckdb\\t1t2.db", \
    "h:\duckdb\\backups\\t1t2.backup.db")