# con_pool.py
# Taken from Bard

import duckdb
import threading

ret = ''
class ConnectionPool:
    def __init__(self, database="md:meters_db"):
        self.database = database
        self.connections = []
        self.lock = threading.Lock()

    def get_connection(self):
        with self.lock:
            if self.connections:
                return self.connections.pop()
            else:
                return duckdb.connect(self.database)

    def release_connection(self, conn):
        with self.lock:
            self.connections.append(conn)

# Example usage
db_name='md:meters_db'
pool = ConnectionPool("db_name")

print("Here we go ")
def execute_query(query):
    conn = pool.get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    pool.release_connection(conn)
#    print (results[0])
    ret = results
#    print(ret)
    return results

# Execute multiple queries concurrently
threads = []
for i in range(1):
    thread = threading.Thread(target=execute_query, 
    args=(f"SELECT modus FROM 'rawlwp$' limit 10",))
    threads.append(thread)
    thread.start()
    
for thread in threads:
    thread.join()
print (ret)
print("That's it for now ")