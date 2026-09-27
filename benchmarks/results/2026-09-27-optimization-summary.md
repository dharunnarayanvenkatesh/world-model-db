# Optimization summary — 2026-09-27

Same-machine release runs with 100 entities, 1,000 initial observations, and a
10,000-operation budget. Both runs produced state digest
`554f7b28dd1f4c37`; semantics did not change. These are single-run workload
analogues, not certified YCSB/TPC results or statistical confidence intervals.

| Workload | Baseline ops/s | Optimized ops/s | Change |
|---|---:|---:|---:|
| Entity insert | 63.97 | 824.82 | 12.89x |
| Observation ingest | 31.03 | 121.16 | 3.90x |
| Entity point read | 1,276,682.67 | 4,262,756.30 | 3.34x |
| Observation point read | 235,584.02 | 3,730,508.10 | 15.84x |
| Current entity state | 63,417.57 | 2,284,148.01 | 36.02x |
| Combined bitemporal state | 72,485.83 | 1,796,299.62 | 24.78x |
| WHY provenance | 61,918.35 | 459,727.84 | 7.42x |
| Entity changes | 17,105.71 | 66,032.75 | 3.86x |
| OLAP group-by | 748.14 | 2,342.95 | 3.13x |
| World diff | 64,114.89 | 94,020.31 | 1.47x |
| Temporal graph path | 896.59 | 2,624.07 | 2.93x |
| Restart/open | 32.34 | 65.10 | 2.01x |
| Current-state rebuild | 37.60 | 89.66 | 2.38x |
| YCSB-B-like 95/5 | 420.78 | 1,261.58 | 3.00x |

Database size fell from 3.00 MiB to 1.50 MiB. The changes responsible are:

1. changed-record detection and record-granular ACID updates instead of a full
   snapshot rewrite on every mutation;
2. shared in-process redb handles, removing reopen/lock setup from every commit;
3. incrementally maintained ID, entity, provenance, and graph-adjacency indexes;
4. indexed observation selection during deterministic resolution/rebuild.

The next measured bottlenecks are full-history OLAP/diff scans and per-write
durable commit latency. Those require columnar segments and an explicit batch or
group-commit API; they should not be hidden by weakening default durability.
