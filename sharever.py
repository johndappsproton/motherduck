# sharever.py
#
import duckdb
from datetime import datetime
import time
import uuid

con = duckdb.connect("md:sharever")
def setparams():
    con.execute("SET preserve_insertion_order=false")
    con.execute('PRAGMA enable_progress_bar')
    con.execute('PRAGMA enable_print_progress_bar;')
    # disable preservation of insertion order
    con.execute("SET preserve_insertion_order=false")
    con.execute("set memory_limit='6GB';")
    con.execute("set max_memory='6GB';")
    con.execute("set immediate_transaction_mode = false;")
    con.execute("set threads = 16")

verno   = 0
loopcnt = 1
loops   = 0

setparams()
#con = duckdb.connect('sharever.db')

rows = con.execute("select max(verno) +1 vernomax from ver")
row  = rows.fetchone()
verno = row[0]

veruuid = str(uuid.uuid4())
verdate = str(datetime.today())
vertime = datetime.now().strftime('%H:%M:%S')
verdatetime = datetime.now()
#    verno   = verno + 1
sqlstr  = (f"{verno},'{verdate}','{vertime}'")
print (sqlstr)
con.execute(f"insert into ver (verno,verdate,vertime) values({sqlstr})")
#    time.sleep(1)

con.execute("update share sharever")    
print (f"1 row inserted with {verno} at {vertime} \n on {verdate} with {veruuid}")