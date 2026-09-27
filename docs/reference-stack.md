# External engine reference map

This document records the canonical external architecture map for World Model
DB. These projects are accelerators and design references; they do not define
the product's semantics or become dependencies merely by appearing here.

## Focused world-model stack

| Project | Intended boundary |
|---|---|
| Apache Arrow | Canonical in-memory columnar interchange between query, storage, graph, vector, and ML components |
| Apache DataFusion | Rust SQL planning, optimization, expressions, and analytical execution |
| RisingWave | Streaming SQL and incremental materialized views for continuously updated world state |
| Kùzu | Embedded entity/relationship graph traversal and temporal graph design reference |
| Apache Iceberg | Versioned historical tables, snapshots, and time travel |
| Apache OpenDAL | Replaceable object-store and filesystem access |
| FAISS | Optional semantic retrieval whose results must resolve to real World Model DB IDs |
| Apache Jena | Optional RDF, ontology, and explicit semantic-reasoning boundary |

The intended long-term flow is:

```text
events / documents / sensors / APIs / agents
                    |
                    v
               RisingWave
                    |
                    v
          canonical Apache Arrow
          /          |           \
         v           v            v
 Iceberg/Parquet    Kùzu          FAISS
 historical state   graph         semantic recall
          \          |           /
           \         |          /
              Apache DataFusion
              unified query layer
```

## Architecture donors and references

- Apache Calcite: relational algebra and optimizer architecture.
- YugabyteDB: distributed SQL, transaction, replication, and sharding design.
- Apache Doris: distributed analytical execution and OLAP storage layout.
- etcd/raft and TiKV `raft-rs`: consensus and replicated control-plane study.
- RocksDB: LSM and storage-engine design reference.
- Apache Parquet: columnar persistence under analytical and historical state.

These donors are not authorization to begin distributed V1 work. V0's
bitemporal, provenance, conflict, and reconstruction semantics must remain the
source of truth.

## License gate

Only MIT and Apache-2.0 code may be cloned, linked, copied, or studied into the
repository. Every selected release and its complete transitive build graph must
be audited before use. Projects or releases under GPL, LGPL, AGPL, SSPL, BSL,
PolyForm, Elastic License, Commons Clause, other source-available terms, or
commercial-only terms are rejected even when their architecture is attractive.

Names must be checked exactly. For example, `redb` is an MIT/Apache-2.0 embedded
Rust database, while similarly named `reddb`/`reddb-io` releases use prohibited
BSL/AGPL terms.
