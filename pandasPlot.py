# pandasPlot.py
# https://gemini.google.com/app/e9ede46a1ed0457c

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import pandas as pd
import matplotlib.pyplot as plt
import duckdb

engine = create_engine('duckdb:///md:meters_db', poolclass=QueuePool)
df = pd.read_sql("select datefull, conwoog from woog$",con=engine)
plt.scatter(df["DateFull"], df["ConWoog"], color="skyblue")
plt.xlabel("y DateFull")
plt.ylabel("y ConWoog")
plt.title("Scatter Plot")
plt.show()
