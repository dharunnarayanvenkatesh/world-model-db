# SpyTime mapping

The original SpyTime relation is mapped to the immutable WQMDB observation log:

| SpyTime field | WQMDB field |
|---|---|
| Spy | `subject_entity_id` |
| City | string `object` |
| Valid begin/end | `claimed_valid_from` / `claimed_valid_to` |
| Transaction begin | `ingested_at` |
| Transaction end | `metadata["spytime.transaction_end"]` |
| Reporter | `source_id` and `metadata["spytime.reporter"]` |

Each input row has a unique physical predicate so WQMDB's fact resolver does
not collapse historical visits. The logical predicate for every row is
`LOCATED_IN`. Query timings scan the durable observations loaded into the
engine's materialized world state; dataset generation and loading are excluded.

The ten query meanings are those listed by the original SpyTime benchmark:
attribute lookup, bitemporal time slice, recording time, as-known history,
valid overlap, same-city lookup, simultaneous same-city lookup,
transaction-overlap join, inconsistent-location detection, and discovery lag.

Specification: <https://cs.nyu.edu/~shasha/spytime/spytime.html>

## Run SF1 in Google Colab

SpyTime is CPU- and storage-bound; an A100 is not used by this Rust workload.
Choose a Colab runtime with the strongest available CPU and enough local disk.
The commands below generate the 10,000-row dataset inside Colab, run the release
binary, and leave the repository checkout clean.

Cell 1 — clone the repository. The runner installs a minimal stable Rust
toolchain only when Colab does not already provide `cargo`:

```bash
!git clone https://github.com/dharunnarayanvenkatesh/world-model-db.git /content/world-model-db
```

Cell 2 — run the guarded end-to-end script. It validates and builds the exact
Git revision, generates the dataset, loads WQMDB, executes Q1-Q10, verifies the
outputs, and creates a download archive:

```bash
%cd /content/world-model-db
!bash benchmarks/spytime/run-colab.sh
```

Cell 3 — download the exact dataset, environment manifest, and both reports:

```python
from google.colab import files
files.download("/content/wqmdb-spytime-sf1.tar.gz")
```

Keep the runtime otherwise idle during Cell 2. For another database, import
`/content/wqmdb-output/dataset-sf1.csv`, implement Q1-Q10 with the same half-open
interval semantics, and compare its geometric mean only after matching WQMDB's
per-query row totals and checksums.

Optional environment variables let you change the run without editing the
script: `WQMDB_RECORDS`, `WQMDB_REPETITIONS`, `WQMDB_SEED`,
`WQMDB_OUTPUT_DIR`, and `WQMDB_ARCHIVE`. The defaults are the comparable SF1
configuration: 10,000 rows, ten repetitions, and the fixed published-project
seed.
