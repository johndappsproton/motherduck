# sqlserver-to-duckdb.py
import duckdb
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import setup_duck

setup_duck.setup_duck('R')
# Connect to SQL Server

conn = pyodbc.connect(\
 'DRIVER={ODBC Driver 17 for SQL Server};\
  TRUSTED_CONNECTION=YES;SERVER=localhost\sqlexpress01;\
  DATABASE=meters_db;')

conn1 = pyodbc.connect(\
 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost\sqlexpress;\
  DATABASE=metereadings21;TRUSTED_CONNECTION=yes;')

conn.autocommit = False
conn1.autocommit= False

query = "select count(*) recs from rawlwp$"
df = pd.read_sql(query, conn1)
print (f"Count of rows in SQL Server {df}")

query = "SELECT * FROM rawlwp$"
df = pd.read_sql(query, conn1)
conn1.commit()

# Connect to DuckDB and write the DataFrame
con = duckdb.connect('mcdb.db')
#con.execute("CREATE TABLE rawlwp$ AS SELECT * FROM df")
con.begin()
result = con.execute("insert into rawlwp$  select * FROM df")
con.commit()
# Verify data in DuckDB
result = con.execute("SELECT count(*) FROM rawlwp$").fetchall()
print (result[0])
#print(result)