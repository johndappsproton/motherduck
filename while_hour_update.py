# while_hour_update.py
#
import duckdb
import random
from datetime import datetime
#from dask.diagnostics import ProgressBar
#import dask.dataframe as dd

db_name = 'md:meters_db'
con = duckdb.connect(db_name)
cur = con.cursor()

# con.execute("create table timeline (hh int, mm int, no bigint)")

#con.execute("insert into timeline select \
#strftime(datumuhrzeit,'%H'), \
#strftime(datumuhrzeit,'%M'),count(*) \
#from rawlwp$ \
#group by strftime(datumuhrzeit,'%H'),\
#strftime(datumuhrzeit,'%M')")
#
desired_width = 2
max_loops = 10
noloops = 1
print("Staring while_hour_update.py with",max_loops)
beg = datetime.now()
while noloops < max_loops:
    ii = random.randint(1,23)
    mm = random.randint(1,59)
    ss = random.randint(1,59)
    da = random.randint(1,28)    # day
    if ii < 10:
        ii = "{:0>{}}".format(ii, desired_width)
    if mm < 10:
        mm = "{:0>{}}".format(mm, desired_width)
    if ss < 10:
        ss = "{:0>{}}".format(ss, desired_width)
    if da < 10:
        da = "{:0>{}}".format(da, desired_width)

    stmt1 = f"select count(*) from rawmini where "
    stmt2 = stmt1 + f"(select hh from timeline "
    stmt3 = stmt2 + f"where hh = strftime(dt,'%H') "
    stmt4 = stmt3 + f"and hh = {ii} and strftime(dt, '%M') = {mm} "
    stmt  = stmt4 + f"and strftime(dt,'%d') = {da} and Modus = 'Brauchwasser')"
    rows = cur.execute(stmt)
    
    row = rows.fetchone()
#    print (row[0], "---> ", ii)
    rows = cur.execute(f"select  \
    count(*) from rawmini r, pump$ p, timeline t \
    where month(r.dt) = month(p.dateshort) \
    and strftime(dt,'%H') = {ii} \
    group by month(r.dt) \
    order by month(r.dt)")

    row = rows.fetchone()
    print (row[0], "--> ", ii, " noloops", noloops)
    cur.close()
    con = duckdb.connect(db_name)
    cur = con.cursor()
    noloops = noloops + 1
end = datetime.now()
tot = end - beg    
print("Thank you very much, what fun that was in \n", tot, \
" seconds with ", noloops, " loops\n with ", db_name)
