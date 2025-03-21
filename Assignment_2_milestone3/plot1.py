import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Experiment data:
data1 = pd.read_csv('two_pubs_1_sub.csv', header=None)
data1.drop(data1.columns[[0,1]], axis=1, inplace=True)
end = 1000 if len(data1) > 1000 else len(data1)
df1 = data1.iloc[:end]

data2 = pd.read_csv('two_pubs_2_subs.csv', header=None)
data2.drop(data2.columns[[0,1]], axis=1, inplace=True)
end = 1000 if len(data2) > 1000 else len(data2)
df2 = data2.iloc[:end]

data3 = pd.read_csv('two_pubs_3_subs.csv', header=None)
data3.drop(data3.columns[[0,1]], axis=1, inplace=True)
end = 1000 if len(data3) > 1000 else len(data3)
df3 = data3.iloc[:end]

data4 = pd.read_csv('two_pubs_4_subs.csv', header=None)
data4.drop(data4.columns[[0,1]], axis=1, inplace=True)
end = 1000 if len(data4) > 1000 else len(data4)
df4 = data4.iloc[:end]

data5 = pd.read_csv('two_pubs_5_subs.csv', header=None)
data5.drop(data5.columns[[0,1]], axis=1, inplace=True)
end = 1000 if len(data5) > 1000 else len(data5)
df5 = data5.iloc[:end]

# Define bin edges and number of bins
bins = 1000

# Plot multi-histogram
plt.figure(figsize=(10, 6))

plt.hist(data1, bins=bins, alpha=0.6, label='1 subscriber', color='blue')
plt.hist(data2, bins=bins, alpha=0.6, label='2 subscribers', color='orange')
plt.hist(data3, bins=bins, alpha=0.6, label='3 subscribers', color='green')
plt.hist(data4, bins=bins, alpha=0.6, label='4 subscribers', color='red')
plt.hist(data5, bins=bins, alpha=0.6, label='5 subscribers', color='purple')

# Add labels and title
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.title('Experiment Histograms: Broker Strategy')
plt.legend(loc='upper right')

# Show plot
plt.show()
