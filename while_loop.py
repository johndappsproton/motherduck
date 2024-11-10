import sys
import timeit
import duckdb

# con= duckdb.connect('meters_db.db')
con = duckdb.connect("md:meters_db")
c = con.cursor()

str = "this is a string"
number = 1
rawCount = 1
tvl = 1.00
ta = 0.00
number = 1
index = 0

con.execute("select count(distinct datumuhrzeit) from rawlwp$")
rawCount = con.fetchone()
## print ("\n Distinct count of ", rawCount)

print("\n alldatalwp$.Modus")

index = 1
loops = 0
res = ""
rows = c.execute(
    "select Dateshort,Modus,laengemm,pausemm,ta from alldatalwp$ order by Modus"
)
for row in c.fetchone():
    res = row
    print(index, res)
    index = index + 1

print("\n alldatalwp$.max TA and max tvl")

str1 = "select max(a.ta),max(r.tvl) from rawlwp$ r, alldatalwp$ a where r.ta = a.ta"
index = 1
res = ""
rows = con.execute(str1)
for row in c.fetchone():
    res = row
    print(index, " - ", res)
    index = index + 1
print("\n")

# rows = c.execute('update rawlwp$ set tvl = a.tvl + 0.1 from rawlwp$ r inner join alldatalwp$ a on a.dateshort = r.dateshort')
# for row in c.fetchone() :
# 	print ("number of rows updated", row)
#
# Start the timer
#

start = timeit.default_timer()

while number < 10:
    rows = c.execute(
        "select dateshort,modus,ta,tvl,trl,tbw,tfb1,from rawlwp$ order by dateshort,modus "
    )
    index = 0
    for row in c.fetchone():
        index = index + 1
        ta = row
    # 		print (ta, " ", index)
    number = number + 1
    if number == 100:
        print(number)
    if number == 200:
        print(number)
    if number == 290:
        print(number)
#
# Stop the timer
#
stop = timeit.default_timer()
print("number was: ", loops)

total_time = stop - start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)

sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
