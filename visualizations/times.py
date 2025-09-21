# -*- coding: utf-8 -*-
import os
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import numpy as np

# Create images folder if it doesn't exist
os.makedirs("images", exist_ok=True)

# Data
queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
rdd_times = [26, 24, 14, 31, 29]
csv_times = [72, 41, 32, 34, 36]
parquet_times = [54, 36, 30, 33, 33]

x = np.arange(len(queries))
width = 0.25

fig, ax = plt.subplots()
rects1 = ax.bar(x - width, rdd_times, width, label='RDD API')
rects2 = ax.bar(x, csv_times, width, label='Spark SQL (CSV)')
rects3 = ax.bar(x + width, parquet_times, width, label='Spark SQL (Parquet)')

ax.set_xlabel('Queries')
ax.set_ylabel('Execution Time (seconds)')
ax.set_title('Execution Time per Query per Scenario')
ax.set_xticks(x)
ax.set_xticklabels(queries)
ax.legend()

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha='center', va='bottom')

autolabel(rects1); autolabel(rects2); autolabel(rects3)
fig.tight_layout()

fig.savefig("images/execution_times.png", dpi=200, bbox_inches="tight")
print("Saved images/execution_times.png")
