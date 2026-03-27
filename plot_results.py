import glob
import pandas as pd
import matplotlib.pyplot as plt

# Find all latency CSV files
files = glob.glob("latency_peer_*.csv")

if not files:
    print("No latency CSV files found.")
    exit()

dfs = []
for f in files:
    try:
        df = pd.read_csv(f)
        if df.empty:
            continue
        dfs.append(df)
    except Exception as e:
        print(f"Skipping {f}: {e}")

if not dfs:
    print("No usable data.")
    exit()

# Combine all data
all_data = pd.concat(dfs, ignore_index=True)

latencies = all_data["latency_ms"]

# Compute stats
avg_latency = latencies.mean()
min_latency = latencies.min()
max_latency = latencies.max()

print(f"Samples: {len(latencies)}")
print(f"Average latency: {avg_latency:.3f} ms")
print(f"Min latency: {min_latency:.3f} ms")
print(f"Max latency: {max_latency:.3f} ms")

# Plot histogram
plt.figure(figsize=(8, 5))
plt.hist(latencies, bins=30)
plt.xlabel("Latency (ms)")
plt.ylabel("Frequency")
plt.title("Latency Distribution (Lookup → First Reply)")
plt.grid(True)

# Save figure
plt.savefig("latency_histogram.png")
plt.show()