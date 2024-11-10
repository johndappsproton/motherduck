# threading_test.py
# https://www.geeksforgeeks.org/multithreading-python-set-1/
#
import duckdb
import threading
from datetime import datetime

def print_cube(num):
    #    con = duckdb.connect('meters_db.db')
    for _ in range(num):
        con.begin()
        rows = con.execute("select count(*) from rawlwp$")
        row = rows.fetchone()
        con.commit()
#    print("Count of rawlwp$ ", row[0])

def print_square(num):
    #    con1 = duckdb.connect('meters_db_1.db')
    for _ in range(num):
        cur1.begin()
        results = cur1.execute("select datefull, conwoog from meters_db_1.woog$ limit 10")
        row = results.fetchone()
        cur1.commit()
#    print(row[0], "---", row[1])
    for _ in range(num):
        cur1.begin()
        results = cur1.execute("select count(distinct rawlwp$) from meters_db_1.rawlwp$")
        row = results.fetchone()
        cur1.commit()
#    print("Unique rows in rawlwp$ ", row[0])
    for _ in range(num):
        cur1.begin()
        rows = cur1.execute("update rawlwp$ r  \
        set modus = (select modus from meters_db_1.rawlwp$ rr\
        where r.datumuhrzeit = rr.datumuhrzeit)"    )
        cur1.commit()

if __name__ == "__main__":
    noloops = 10
    con = duckdb.connect("md:meters_db")
    con1 = duckdb.connect("md:meters_db_1")
    cur1 = con1.cursor()
#    con.begin
#    con1.begin
    print(f"Starting threading_test.py with {noloops} loops")
    t1 = threading.Thread(target=print_square, args=(noloops,))
    t2 = threading.Thread(target=print_cube, args=(noloops,))
    beg = datetime.now()
    t1.start()
    t2.start()

    t1.join()
    t2.join()

#    con.commit
#    con1.commit
    end = datetime.now()
    print("Total running tie:", end - beg)
