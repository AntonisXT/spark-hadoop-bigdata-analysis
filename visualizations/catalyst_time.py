# -*- coding: utf-8 -*-
import os
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import numpy as np

os.makedirs("images", exist_ok=True)

labels = ['Join']
a1 = 19.7221  # Catalyst Disabled
a2 = 5.5824   # Catalyst Enabled

x = np.arange(len(labels))
width = 0.2

fig, ax = plt.subplots()
ax.bar(x - width/2, a1, width, label="Catalyst Disabled")
ax.bar(x + width/2, a2, width, label="Catalyst Enabled")

ax.set_ylabel('Join Time (s)')
ax.set_title('Join Execution Time (Broadcast vs SortMerge)')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

fig.tight_layout()
fig.savefig("images/catalyst_times.png", dpi=200, bbox_inches="tight")
print("Saved images/catalyst_times.png")
