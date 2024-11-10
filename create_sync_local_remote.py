#
# https://motherduckcommunity.slack.com/archives/C058PJL4R0S/p1697912523434049?thread_ts=1697884148.583929&cid=C058PJL4R0S
#
import duckdb

conn = duckdb.connect("md:")
conn.execute("ATTACH '/tmp/local.db' AS local")
conn.execute("USE local")

schema_list = conn.execute(
    "select distinct(schema_name) from information_schema.schemata where catalog_name = 'local'"
).fetchone()
table_list = conn.execute(
    "select table_schema, table_name from information_schema.tables where table_catalog = 'local'"
).fetchone()

# sync to remote
conn.execute("CREATE DATABASE IF NOT EXISTS clouddb")

# create schemas if needed
for schema in schema_list:
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema[0]}"')

for schema, table in table_list:
    conn.execute(
        f'CREATE OR REPLACE TABLE clouddb."{schema}"."{table}" AS SELECT * FROM local."{schema}"."{table}"'
    )
