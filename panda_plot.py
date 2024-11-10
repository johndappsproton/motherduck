# panda_plot.py
# https://medium.com/@samuelfriend/an-introduction-to-plotting-csv-data-using-matplotlib-and-pandas-ecdc18fd2393
#
#
import pandas as pd
from matplotlib import pyplot as plt
bb_data = pd.read_csv('excel\pump$.csv')
bb_data
bb_data.TotalKW
plt.plot(bb_data.TotalKW)
bb_data.DateShort
plt.plot(bb_data.TotalKW,bb_data.DateShort)
plt.show()