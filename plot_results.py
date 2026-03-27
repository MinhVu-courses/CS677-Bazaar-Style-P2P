import glob
import csv
from collections import Counter

files = glob.glob("latency_peer_*.csv")

if not files:
    print("No latency CSV files found.")
    raise SystemExit

latencies = []

for filename in files:
    try:
        with open(filename, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    latencies.append(float(row["latency_ms"]))
                except (ValueError, KeyError):
                    continue
    except Exception as e:
        print(f"Skipping {filename}: {e}")

if not latencies:
    print("No usable latency data found.")
    raise SystemExit

avg_latency = sum(latencies) / len(latencies)
min_latency = min(latencies)
max_latency = max(latencies)

print(f"Samples: {len(latencies)}")
print(f"Average latency: {avg_latency:.3f} ms")
print(f"Min latency: {min_latency:.3f} ms")
print(f"Max latency: {max_latency:.3f} ms")

# simple histogram buckets
bucket_size = 10  # ms
hist = Counter()

for value in latencies:
    bucket_start = int(value // bucket_size) * bucket_size
    bucket_label = f"{bucket_start:03d}-{bucket_start + bucket_size - 1:03d}"
    hist[bucket_label] += 1

print("\nLatency histogram:")
for bucket in sorted(hist):
    count = hist[bucket]
    bar = "#" * min(count, 50)
    print(f"{bucket} ms | {bar} ({count})")

with open("latency_summary.txt", "w") as f:
    f.write(f"Samples: {len(latencies)}\n")
    f.write(f"Average latency: {avg_latency:.3f} ms\n")
    f.write(f"Min latency: {min_latency:.3f} ms\n")
    f.write(f"Max latency: {max_latency:.3f} ms\n\n")
    f.write("Latency histogram:\n")
    for bucket in sorted(hist):
        count = hist[bucket]
        bar = "#" * min(count, 50)
        f.write(f"{bucket} ms | {bar} ({count})\n")

print("\nWrote latency_summary.txt")
