# https://github.com/duckdb/duckdb/issues/8261
import os
import duckdb

# Export database
con = duckdb.connect('t1t2.db') # has big file size
con.execute("export database 't1t2';")
con.close()

# Remove database
os.remove('t1t2.db')

# Re-import database
con = duckdb.connect('t1t2.db')
con.execute("import database 't1t2';")
con.close()
