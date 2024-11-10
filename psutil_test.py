# psutil_test.py
import os
import psutil
import duckdb
from datetime import datetime

noloops = 100
cntloops = 0
db_name = 'md:meters_db'

beg = datetime.now()
while cntloops < noloops:
    con0 = duckdb.connect(db_name)
    con0.begin()
    con0.execute("pragma md_version")
    con0.rollback()
    con0.close()
    cntloops = cntloops +1
    if cntloops %10 == 0:
        Print("this many loops", cntloops)
    
end = datetime.now()
tot = end - beg
print(f"Total running time:{tot})")