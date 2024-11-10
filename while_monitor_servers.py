# while_monitor_servers.py
import time
import duckdb
import sys

def print_db(token, username):
#    print(f"=== {username} connecting ===")
    db_name = f"md:?motherduck_token={token}"
    con = duckdb.connect(db_name)
    cur = con.cursor()
    res = cur.execute('set http_keep_alive = "False"')
    rows = cur.execute("select \
        client_connection_id,\
        client_transaction_id,\
        server_transaction_elapsed_time,\
        server_query_elapsed_time,  \
        server_transaction_stage \
        FROM md_active_server_connections() \
        where client_connection_id != \
        (md_current_client_connection_id()) \
        order by client_connection_id,\
        client_transaction_id;").fetchall()
    print(f"---- {username} ----")
    for row in rows:
        print(f"{str(row[0]):24},{row[1]:4d},\
        {row[2]},\
        {row[3]},\
        {row[4]:20}")
        time.sleep(1)
    con.close()
    cur.close()
#    print(f"=== {username} closed ===")
#
# Begin of main program
#
maxloops = 10000
loops    = 0
while loops < maxloops:
    loops = loops +1
    print(f"Starting {sys.argv[0]}")
    user1 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5kLmFwcHMuZ21haWwuY29tIiwiZW1haWwiOiJqb2huLmQuYXBwc0BnbWFpbC5jb20iLCJ1c2VySWQiOiI3MjYyNzQxMS1lZjE4LTRkNGEtYTg3Ny1iM2U1ZDUxOTRhOWQiLCJpYXQiOjE3MTE4MjIzNTgsImV4cCI6MTc0MzM3OTk1OH0.t8dAyF9GLMxgMSPDGnMKRTO7_FUudxSw0Q3MXlVE8hU"
    user2 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLmdtYWlsLmNvbSIsImVtYWlsIjoiam9obmRhcHBzQGdtYWlsLmNvbSIsInVzZXJJZCI6IjBhOTY3YjFmLWVhZGItNDFmOS04M2I1LWNmYThhMDEzMWRiYSIsImlhdCI6MTcxMTgyMjQyOCwiZXhwIjoxNzQzMzgwMDI4fQ.ilGMY49gBy2bXsSAZ1zBFWQunktnlESb5xiG8pjbIo0"
    user3 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5hcHBzLnNraWZmLmNvbSIsImVtYWlsIjoiam9obi5hcHBzQHNraWZmLmNvbSIsInVzZXJJZCI6ImM4YTI1NjI0LTY1MmEtNGNiZi1iZWI2LWY2YmViYTIwOTU2OCIsImlhdCI6MTcxMTgyMjQ5MiwiZXhwIjoxNzQzMzgwMDkyfQ.GbgeQyEjEKPhfISVYhpHD9a88AXa7BRLvXfUcGXrceg"
    user4 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5hcHBzLm91dGxvb2suY29tIiwiZW1haWwiOiJqb2huLmFwcHNAb3V0bG9vay5jb20iLCJ1c2VySWQiOiJmZjQwM2Q0Yy1kNTVjLTQ1MTQtOGY1Zi0wMzQ4YmNiZTk1MmEiLCJpYXQiOjE3MTE4MjI1MzMsImV4cCI6MTc0MzM4MDEzM30.Hq8raVv3TPcMffYPOszDVWWq4IE0daHaQhaP6L8K-wE"
    user5 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9lLmNoaWVtZ2F1Lm91dGxvb2suY29tIiwiZW1haWwiOiJqb2UuY2hpZW1nYXVAb3V0bG9vay5jb20iLCJ1c2VySWQiOiI0NjE5NDEwMi04Nzg1LTRlYmEtOWMyOS1mZjYyZjFmNmFmMWUiLCJpYXQiOjE3MTE4MjI2MTUsImV4cCI6MTc0MzM4MDIxNX0.q-niiC63ALFdAFohU8TVtSDK1T9YIu0StQHSk7RzfE0"
    user6 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obmRhcHBzLnByb3Rvbm1haWwuY29tIiwiZW1haWwiOiJqb2huZGFwcHNAcHJvdG9ubWFpbC5jb20iLCJ1c2VySWQiOiIyNzU5OWZlMy1jMjMwLTRjYzUtYjMwNS01MGZjMjU2MWU4YTQiLCJpYXQiOjE3MTE4MjMyNDMsImV4cCI6MTc0MzM4MDg0M30.8VBiXqLwJVEwulBVkH43U6Mchw4_Wf1kLYKJrDqmo9E"
    user7 = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoid29vZy5hbGV4LmFuZHJhLmdtYWlsLmNvbSIsImVtYWlsIjoid29vZy5hbGV4LmFuZHJhQGdtYWlsLmNvbSIsInVzZXJJZCI6ImU1Mzc3OGVjLWZkNTctNDQ1Yy04MWVmLWJmZDU5MzQxYjMyYiIsImlhdCI6MTcyMDYyOTU2NSwiZXhwIjoxNzUyMTg3MTY1fQ.vgiqXfw88vdJQKOTn1gQ7cTopciqEGG3ZNRExpLgTyw"
    #cur = duckdb.connect(f"md:?motherduck_token={user1}")

    print_db(user1, 'user1')
    print_db(user2, 'user2')
    print_db(user3, 'user3')
    print_db(user4, 'user4')
    print_db(user5, 'user5')
    print_db(user6, 'user6')
    print_db(user7, 'user7')
