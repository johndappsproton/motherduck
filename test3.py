from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import text
import duckdb_engine

#engine = create_engine("duckdb:///t1t2.db",future="True")
engine = create_engine("duckdb:///md:meters_db",future="True")

# don't rely on autocommit for DML and DDL
with engine.begin() as connection:
    rows = connection.execute(text("pragma md_version"))
    row = rows.fetchone()
    print(row[0])

with engine.begin() as connection:
    rows = connection.execute(text("select count(*) from rawmini"))
    row = rows.fetchone()
    print(row[0])