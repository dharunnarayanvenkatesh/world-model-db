# World Model DB benchmark results

- Run timestamp (Unix UTC): `1790510713`
- Build: `--release`
- Platform: `windows/x86_64`
- Schema version: `1`
- Dataset: 100 entities, 1000 initial observations
- Final records: 1050 observations, 1050 fact versions, 99 relationships, 0 conflicts
- Source priority: `100`; generated conflict ratio: `0%`
- Timed operation budget: 10000
- Database size after mixed workload: 1576960 bytes (1.50 MiB)
- Deterministic state digest (FNV-1a): `554f7b28dd1f4c37`

> These are deterministic workload analogues, not certified YCSB or TPC results.

| Workload | Operations | Throughput ops/s | p50 µs | p95 µs | p99 µs | Detail |
|---|---:|---:|---:|---:|---:|---|
| Entity insert | 100 | 824.82 | 1164.70 | 1589.60 | 1915.50 | individual transactional inserts |
| Bulk observation ingest | 1000 | 121.16 | 8181.90 | 14373.70 | 15687.90 | resolve + provenance + redb commit per observation |
| YCSB-C point entity read | 10000 | 4262756.30 | 0.20 | 0.20 | 0.30 | 100% reads; deterministic key distribution |
| YCSB-C point observation read | 10000 | 3730508.10 | 0.20 | 0.30 | 0.40 | 100% reads over immutable observation IDs |
| Current entity state | 1000 | 2284148.01 | 0.40 | 0.50 | 0.50 | bitemporal current-view lookup |
| Combined bitemporal state | 1000 | 1796299.62 | 0.50 | 0.60 | 0.70 | VALID AT plus KNOWN AT |
| WHY provenance expansion | 1000 | 459727.84 | 2.10 | 2.80 | 3.10 | fact + supporting observations + sources + conflicts |
| Entity changes | 100 | 66032.75 | 14.90 | 17.40 | 18.70 | facts, relationships, and events in a known-time window |
| OLAP columnar GROUP BY | 100 | 2342.95 | 392.90 | 579.80 | 746.10 | SELECT predicate, COUNT(*) FROM wm_facts GROUP BY predicate |
| World diff | 100 | 94020.31 | 10.50 | 10.60 | 11.70 | full historical transition scan |
| Temporal graph path | 100 | 2624.07 | 375.50 | 412.60 | 471.50 | chain path from first to last entity |
| Database restart/open | 30 | 65.10 | 15360.20 | 16187.60 | 16281.40 | redb open + canonical snapshot decode |
| Current-state rebuild | 5 | 89.66 | 11075.20 | 11576.00 | 11576.00 | replay immutable observations and deterministic resolution |
| YCSB-B mixed 95/5 | 1000 | 1261.58 | 0.60 | 10.30 | 16001.50 | 95% point reads, 5% resolved transactional writes |
