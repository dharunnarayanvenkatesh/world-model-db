# Architecture

The external engine shortlist and donor boundaries are recorded in
[reference-stack.md](reference-stack.md). They are replaceable accelerators;
World Model DB owns the temporal, provenance, conflict, and query semantics.

## Design objective

World Model DB maintains a reproducible belief state over a changing world. It
separates immutable input from derived interpretations so a newer belief never
destroys the evidence or the history that preceded it.

V0 is a single process backed by one local database file. Crate boundaries are
semantic boundaries, not distributed services.

## Data flow

```text
JSON / JSONL / CSV / REST
            |
            v
     validation + normalization
            |
            v
    immutable observation log <--------------------+
            |                                       |
            v                                       |
 deterministic entity resolution                   |
            |                                       |
            v                                       |
      fact candidates                               |
            |                                       |
            +--> conflict detection                 |
            |                                       |
            v                                       |
 deterministic resolution                           |
            |                                       |
            +--> bitemporal fact history            |
            +--> evidence/provenance                 |
            +--> temporal relationships/events      |
            v                                       |
 materialized current facts/relationships           |
            |                                       |
            +--> state / why / changes / diff / path|
                                                    |
rebuild discards derived views and replays ----------+
```

Input acknowledgement happens only after the durable representation is written.
An invalid record fails visibly; it is never silently skipped.

## Components

### `wm-core`

Owns strongly typed IDs, entities, sources, observations, facts, relationships,
events, evidence, conflicts, status enums, values, and validation. It has no
transport or persistence knowledge.

### `wm-catalog`

Owns entity lookup, normalized names, aliases, typed identifiers, and sources.
Entity IDs are immutable. Deterministic normalization is intentionally simple;
future ML resolvers must implement the same boundary without changing stored IDs.

### `wm-temporal`

Owns interval validation, containment, overlap, valid-time and known-time
selection. Intervals are half-open and UTC. Open ends represent infinity.

### `wm-storage`

Owns record-granular redb transactions, schema versions, legacy snapshot reads,
materialized state, maintained lookup indexes, and reconstruction. The public
storage traits keep a future backend from leaking into domain semantics.

### `wm-resolution`

Groups comparable observations, creates candidates, detects conflicts, and
chooses the preferred current belief using deterministic rules. It records every
candidate, decision reason, input observation, and confidence component.

### `wm-graph`

Traverses the same relationship records and entity IDs used by the rest of the
system. Temporal and type filters apply before traversal. Stable neighbor order
makes bounded path selection deterministic.

### `wm-query`

Composes storage, temporal, resolution, and graph operations into state, as-of,
known-at, why, changes, world diff, and path results. Querying does not mutate
history.

### `wm-agent`

Maps agent tool calls onto the canonical world model. Agents are registered as
sources; memories are immutable observations carrying agent, session,
idempotency, importance, tag, and memory-kind metadata. Context retrieval ranks
resolved facts and applies explicit fact/count and character budgets. It does
not maintain a separate vector or chat-history truth store.

```text
researcher / planner / executor agents
                 |
                 v
       idempotent agent gateway
                 |
                 v
immutable observations + agent/session provenance
                 |
                 v
resolution / conflicts / bitemporal world state
                 |
                 v
      compact shared context bundles
```

### `wm-ingest`

Parses JSON, JSONL, and CSV into domain commands. Transport-specific field names
are normalized here, while validation remains in domain code.

### `wm-server` and `wm-cli`

Thin adapters. They parse requests, call the application/query layer, and
serialize structured results. Resolution or temporal rules do not live in HTTP
handlers or command dispatch.

## Core boundaries

The intended replaceable interfaces are equivalent to:

```rust,ignore
trait ObservationStore { /* append and read immutable observations */ }
trait FactStore { /* append versions and select temporal views */ }
trait GraphStore { /* store and scan temporal relationships */ }
trait EntityResolver { /* deterministic identity resolution */ }
trait ResolutionEngine { /* candidates, conflicts, decisions */ }
trait QueryEngine { /* state, why, changes, diff, path */ }
trait StorageBackend { /* durable load, commit, reconstruct */ }
```

Concrete signatures are documented by the Rust API; this diagram describes
responsibility, not a promise that every method resides in one trait.

## Consistency and reconstruction

V0 serializes writes inside one process. Derived current-state indexes are a
cache over immutable observations and historical versions. Reconstruction:

1. validates the stored schema version;
2. loads catalog records and immutable observations;
3. replays deterministic resolution in a stable order;
4. regenerates facts, conflicts, provenance, and current-state indexes;
5. compares deterministic identifiers/counts before replacing materialization.

This is local durable persistence, not a distributed transaction protocol.

## Security and trust boundary

All imported text and JSON values are untrusted data. Ingestion enforces size,
type, timestamp, ID, and confidence constraints before persistence. V0 server
deployments must be bound to a trusted interface or placed behind an authenticating
reverse proxy because V0 provides no authentication or tenant isolation.

## Dependency policy

The linked V0 dependency is the audited MIT/Apache-2.0 `redb` engine. Apache
Arrow, DataFusion, Iceberg, OpenDAL, Kùzu, and FAISS remain design candidates or
studied references, not linked product dependencies. A candidate cannot be
added until its exact version and full transitive license tree pass the policy
in `docs/licensing.md`.
