# Local storage format

V0 stores one durable, schema-versioned local database selected by `--db`.
Canonical records are keyed independently and changed records are committed
together through a redb ACID write transaction. A process-wide weak handle
registry reuses the open database safely across engine instances. The record
encoding is an implementation format, not the public API; clients must use the
Rust, CLI, REST, JSONL, or CSV surfaces.

## Invariants

- Immutable records are append-only in logical history.
- Every stored object has a stable typed ID.
- UTC timestamps are serialized without local-time dependence.
- Maps and sets use canonical ordering before checksumming or comparison.
- Derived ID, entity, provenance, and graph-adjacency indexes live in memory and
  can be rebuilt from durable records.
- Unknown schema versions fail closed; they are never guessed.
- A write commits atomically through redb's single-writer transaction.

## Logical sections

The local representation contains, at minimum:

1. header and schema version;
2. entities, aliases, typed identifiers, and sources;
3. immutable observations;
4. historical fact and relationship versions;
5. events, evidence links, conflicts, and correlations;
6. materialized current facts and relationships;
7. deterministic configuration/formula versions.

## Schema evolution

V0 uses schema version `1`. A future reader must explicitly migrate an older
version and retain the original until migration succeeds. Additive JSONL fields
may be ignored only when their meaning is optional; unknown `record_type` values,
invalid required fields, or incompatible versions are errors.

## Reconstruction

Reconstruction replays history using the persisted resolver configuration. It
must reproduce the same IDs, statuses, confidence components, conflicts, and
materialized state. This guarantee is why resolver rule versions and stable
tie-breakers are stored alongside derived output.

## Crash and backup behavior

redb supplies transactional commit and crash-consistency behavior. World Model
DB does not yet expose retained backup generations, online snapshots, or
application-level recovery diagnostics. V1 adds those operational controls and
columnar historical segments.

Stop writes or use an application-provided snapshot before copying the file.
Copying a temporary file is not a backup. V0 does not provide online replication,
point-in-time recovery tooling, encryption at rest, or remote object storage.
