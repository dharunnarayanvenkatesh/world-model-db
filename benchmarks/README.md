# Benchmarks

Benchmarks must measure semantic operations without weakening correctness:

- observation append and durable commit;
- deterministic resolution with agreement and conflict;
- current-state lookup;
- valid-time, known-time, and combined bitemporal lookup;
- `WHY` provenance expansion;
- entity changes and world diff;
- bounded temporal path traversal;
- full reconstruction from immutable history.

Report dataset generator/seed, schema version, source-priority configuration,
record counts, conflict ratio, temporal-version count, hardware, OS, Rust
version, build profile, warm-up, sample count, and percentile method. Never mix
release and debug results.

The dependency-free harness is checked in as `wm-bench`. Run it after the
correctness suite:

```sh
cargo test --workspace
cargo build --release -p wm-bench
target/release/wm-bench \
  --entities 100 \
  --observations 1000 \
  --operations 10000 \
  --output benchmarks/results/local.md
```

It covers bulk transactional ingest, YCSB-C-like point reads, YCSB-B-like 95/5
read/write traffic, current and combined bitemporal state, columnar group-by,
world diff, temporal graph traversal, restart/open, and deterministic rebuild.
The names describe workload shapes only; these are not certified YCSB or TPC
implementations.

The harness warms read paths, records per-operation p50/p95/p99 using nearest-
rank indexing, runs in release mode, and emits a deterministic FNV-1a state
digest. Compare results only on the same machine and configuration.

The checked-in baseline, final optimized run, and comparison are in `results/`.
The optimized storage engine performs record-granular commits, reuses the open
redb handle, and maintains ID/entity/adjacency indexes. Intermediate database
files are intentionally ignored.
