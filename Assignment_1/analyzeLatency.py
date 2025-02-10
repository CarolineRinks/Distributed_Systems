
import pandas as pd
import matplotlib.pyplot as plt

# Load latency data
df = pd.read_csv("results.csv")

# Plot latency distribution for a specific publisher
publisher_id = "pub1"
subset = df[df["Publisher ID"] == publisher_id]
plt.hist(subset["Latency (ns)"], bins=50)
plt.xlabel("Latency (ns)")
plt.ylabel("Frequency")
plt.title(f"Latency Distribution for {publisher_id}")
plt.show()

# Plot latency vs. topic
for topic in df["Topic"].unique():
    subset = df[df["Topic"] == topic]
    plt.plot(subset["Publish Time (ns)"], subset["Latency (ns)"], label=topic)
plt.xlabel("Publish Time (ns)")
plt.ylabel("Latency (ns)")
plt.legend()
plt.title("Latency vs. Topic")
plt.show()