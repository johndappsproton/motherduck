import duckdb
import sys
import timeit


#
# Start the timer
#
start = timeit.default_timer()

con = duckdb.connect("md:")
con = duckdb.connect("md:")
con = duckdb.connect("md:")
con = duckdb.connect("md:")
con = duckdb.connect("md:")

#
# Stop the timer
#
stop = timeit.default_timer()
total_time = stop - start
mins, secs = divmod(total_time, 60)
hours, mins = divmod(mins, 60)

sys.stdout.write("Total running time: %d:%d:%d.\n" % (hours, mins, secs))
