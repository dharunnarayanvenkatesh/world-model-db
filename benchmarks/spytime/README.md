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

Cell 1 — clone and install Rust only if Colab does not already provide it:

```bash
!git clone https://github.com/dharunnarayanvenkatesh/world-model-db.git /content/world-model-db
!command -v cargo >/dev/null || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

Cell 2 — validate and build the exact Git revision being measured:

```bash
%cd /content/world-model-db
!source "$HOME/.cargo/env" 2>/dev/null || true; cargo test -p wm-bench --bin wm-spytime
!source "$HOME/.cargo/env" 2>/dev/null || true; cargo build --release -p wm-bench --bin wm-spytime
!git rev-parse HEAD
```

Cell 3 — run SF1 outside the checkout so generated data is never committed:

```bash
!mkdir -p /content/wqmdb-output
!/content/world-model-db/target/release/wm-spytime \
  --records 10000 \
  --repetitions 10 \
  --database /content/wqmdb-output/spytime-sf1.redb \
  --dataset /content/wqmdb-output/dataset-sf1.csv \
  --json /content/wqmdb-output/spytime-wqmdb-sf1.json \
  --markdown /content/wqmdb-output/spytime-wqmdb-sf1.md
```

Cell 4 — bundle and download the exact dataset plus both reports:

```python
import shutil
from google.colab import files

archive = shutil.make_archive(
    "/content/wqmdb-spytime-sf1",
    "zip",
    "/content/wqmdb-output",
)
files.download(archive)
```

Keep the runtime otherwise idle during Cell 3. For another database, import
`/content/wqmdb-output/dataset-sf1.csv`, implement Q1-Q10 with the same half-open
interval semantics, and compare its geometric mean only after matching WQMDB's
per-query row totals and checksums.
