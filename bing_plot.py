# 

import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the database
con = duckdb.connect('meters_db.db')

# Query the data
query = "SELECT totalkw FROM pump$"
df = pd.read_sql_query(query, con)

# Plot the data
plt.plot(df['totalkw'])
plt.title('Total KW')
plt.xlabel('Index')
plt.ylabel('Total KW')
plt.show()