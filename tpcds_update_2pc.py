import duckdb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import column
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.orm import Session

# Connect to the local DuckDB database
local_db = duckdb.connect("tpcds.db")

# Connect to the MotherDuck database
motherduck_db = duckdb.connect('md:tpcds')

# Create a SQLAlchemy engine for the MotherDuck database
engine = create_engine('duckdb:///md:tpcds')

# Create a sessionmaker with two-phase commit
Session = sessionmaker(bind=engine, twophase=True)

# Fetch data from the local DuckDB database
local_data = local_db.execute("SELECT * FROM inventory limit 10").fetchall()

inventory = []
# Begin a two-phase commit transaction
with Session() as session:
#    session.begin(subtransactions=True)
    session.begin()
    # Iterate over the fetched data and update the MotherDuck table
    for row in local_data:
        inventory = local_data
        inv_date_sk             = row[0]
        inv_item_sk             = row[1]
        inv_warehouse_sk        = row [2]
        inv_quantity_on_hand    = [3]
        session.add(inv_date_sk)
        session.add(inv_item_sk)
        print (inv_date_sk)
    # Commit the transaction
    session.commit()

# Close the connections
local_db.close()
motherduck_db.close()