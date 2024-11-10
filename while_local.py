# while_local.py
import sys
import timeit
import duckdb
import os
import psutil

# Getting loadover15 minutes
load1, load5, load15 = psutil.getloadavg()
# print(psutil.net_io_counters())
netstart = psutil.net_io_counters()
startbsend = netstart[0]
startbrecv = netstart[1]


# con = duckdb.connect ('meters_db.db')
con = duckdb.connect("md:meters_db")
# enable the progress bar
con.execute('PRAGMA enable_progress_bar')
con.execute('PRAGMA enable_print_progress_bar;')
# disable preservation of insertion order
con.execute("SET preserve_insertion_order=false")
con.execute("set memory_limit='6GB';")
con.execute("set max_memory='12GB';")

cur = con.cursor()

str = "this is a string"
number = 1
rawCount = 1
tvl = 1.00
ta = 0.00
number = 1
index = 0

con.execute("select count(distinct datumuhrzeit) from rawlwp$")
rawCount = con.fetchone()
print("\n Distinct count of RAWLWP$", rawCount)

index = 0
loops = 0
res = ""
rows = cur.execute(
    "select Dateshort,Modus,laengemm,pausemm,ta from alldatalwp$ order by Modus"
)
for row in cur.fetchone():
    res = row
    index = index + 1
# 	print("Dateshort -> ",res[0])
# 	print ("Modus -> ", res[1])

##print ("\n alldatalwp$.max TA and max tvl", res)

str1 = "select  max(a.ta),max(r.tvl) from rawlwp$ r, alldatalwp$ a where r.ta = a.ta"
index = 0
res = ""
rows = con.execute(str1)
for row in rows.fetchone():
    res = row
    index = index + 1
# 	print("max TA -> ", res[0])
# 	print("max TVL -> ", res[1])
print("\n")

#
# Start the timer
#
start = timeit.default_timer()
index = 0
numModus = 0
modus = " "
number = 0
while number < 100:
    rows = cur.execute(
        "select datumuhrzeit, modus from rawlwp$ order by datumuhrzeit,modus "
    )
    for row in rows.fetchone():
        # 		print("Daate -> ", row[0])
        # 		print("Modus -> ", row[1])
        modus = row[1]
        if row[1] == "Heizung" and number == 0:
            numModus = numModus + 1
    # 			print("We've got a Heizung!")
    number = number + 1
#
# Stop the timer
#
stop = timeit.default_timer()
print("Number modus Heizung ", numModus)
print("number loops was: ", number)

# Getting % usage of virtual_memory ( 3rd field)
print("RAM memory % used:", psutil.virtual_memory()[2])

# Getting usage of virtual_memory in GB ( 4th field)
print("RAM Used (GB):", psutil.virtual_memory()[3] / 1000000000)


# print (psutil.net_io_counters())
netend = psutil.net_io_counters()
endbsend = netend[0]
endbrecv = netend[1]
print(
    "Mega bytes sent and recceived ",
    "\n",
    (endbsend - startbsend) / 1000,
    (endbrecv - startbrecv) / 1000,
)

total_time = stop - start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)

print("\n")
sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
