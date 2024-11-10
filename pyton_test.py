# python_test.py
#
import duckdb

try:
    con = duckdb.connect("md:meters_db")
except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
    raise
except:
    print("An error occurred")

idx = 1
conwoog = 1
date = "2017-01-01"
modus = "Heizung"

try:
    con.execute("select dateshort,conwoog from woog$ where conwoog > ?", [conwoog])
    print(con.fetchone())
except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
    raise
except duckdb.CatalogException:
    print("Oh dear, we just hit a CatalogException")

try:
    con.execute("select dateshort, ta from alldatalwp$ where modus = ?", [modus])
    print(con.fetchone())
except duckdb.InvalidInputException:
    raise duckdb.InvalidInputException
    
str1 = "update rawlwp$ as ro set ta = "
str2 = "(select new.ta as ta from rawlwp$ new where "
str4 = "ro.dateshort=new.dateshort and new.ta < ro.ta)"
str5 = str1 + str2 + str4

print("")
try:
    con.execute(str5)
except duckdb.InvalidInputException:
    raise duckdb.InvalidInputException
finally: 
    print ("All is lost...")
    
#print(con.fetchone())

# con.execute('update t1 set f2=f1')
