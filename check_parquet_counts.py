from pathlib import Path
import pyarrow.parquet as pq

paths = {
    "Bronze": "data/processed/bronze",
    "Silver": "data/processed/silver/network_events_streaming_v2",
    "Quarantine": "data/processed/quarantine/network_events_streaming_v2"
}

counts = {}

for name, path in paths.items():
    files = list(Path(path).rglob("*.parquet"))
    total = 0

    print(f"\nChecking {name}: {len(files)} files", flush=True)

    for i, file in enumerate(files, 1):
        total += pq.ParquetFile(file).metadata.num_rows

        if i % 100 == 0:
            print(f"{name}: {i}/{len(files)} files", flush=True)

    counts[name] = total
    print(f"{name}: {total:,} events", flush=True)

print("\n=== RECONCILIATION ===")
print("Bronze:", counts["Bronze"])
print("Silver:", counts["Silver"])
print("Quarantine:", counts["Quarantine"])
print("Difference:", counts["Bronze"] - counts["Silver"] - counts["Quarantine"])
