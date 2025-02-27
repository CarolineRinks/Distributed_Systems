import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Broker Strategy experiment data:
data1 = pd.read_csv('two_pubs_1_sub.csv', header=None)
data1.drop(data1.columns[[0,1]], axis=1, inplace=True)
data2 = pd.read_csv('two_pubs_2_subs.csv', header=None)
data2.drop(data2.columns[[0,1]], axis=1, inplace=True)
data3 = pd.read_csv('two_pubs_3_subs.csv', header=None)
data3.drop(data3.columns[[0,1]], axis=1, inplace=True)
data4 = pd.read_csv('two_pubs_4_subs.csv', header=None)
data4.drop(data4.columns[[0,1]], axis=1, inplace=True)
data5 = pd.read_csv('two_pubs_5_subs.csv', header=None)
data5.drop(data5.columns[[0,1]], axis=1, inplace=True)

# Direct Strategy experiment data:
# data1 = csv.read_csv('two_pubs_1_sub_d.csv').drop(data1.columns[[0,1]], axis=1)
# data2 = csv.read_csv('two_pubs_2_subs_d.csv').drop(data2.columns[[0,1]], axis=1)
# data3 = csv.read_csv('two_pubs_3_subs_d.csv').drop(data3.columns[[0,1]], axis=1)
# data4 = csv.read_csv('two_pubs_4_subs_d.csv').drop(data4.columns[[0,1]], axis=1)
# data5 = csv.read_csv('two_pubs_5_subs_d.csv').drop(data5.columns[[0,1]], axis=1)

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
