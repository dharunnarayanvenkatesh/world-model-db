# REST and Rust API

The REST adapter exposes JSON representations of the same application operations
as the CLI. Business rules reside below the transport layer.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/agent/tools` | get function-calling tool schemas |
| `POST` | `/agent/register` | register an agent as a provenance source |
| `POST` | `/agent/memory` | append an idempotent agent memory |
| `POST` | `/agent/context` | build a ranked, size-bounded context bundle |
| `GET` | `/agent/sessions/:id/memory?agent_id=...` | inspect an agent session's immutable writes |
| `POST` | `/entities` | create an entity |
| `GET` | `/entities/:id` | get an entity |
| `POST` | `/observations` | append an immutable observation |
| `GET` | `/observations/:id` | get an observation |
| `GET` | `/facts` | filter facts, including temporal filters |
| `GET` | `/facts/:id` | get a fact version |
| `GET` | `/facts/:id/why` | explain provenance and resolution |
| `GET` | `/entities/:id/state` | get resolved state |
| `GET` | `/entities/:id/changes` | list changes in a range |
| `GET` | `/conflicts` | list/filter conflicts |
| `GET` | `/conflicts/:id` | get a conflict and candidates |
| `GET` | `/graph/path` | find a bounded temporal path |
| `POST` | `/query` | submit a structured query operation |

Query timestamps are RFC 3339 UTC. JSON responses use stable IDs and include
schema/API version metadata. Creation returns `201`; successful reads return
`200`; validation errors return `400`; missing IDs return `404`; duplicate
immutable IDs return `409`; integrity failures return `500` and are logged.

## Append observation

```http
POST /observations
Content-Type: application/json

{
  "observation_id": "obs:official:bob-ceo",
  "source_id": "src:acme-official",
  "subject_entity_id": "company:acme",
  "predicate": "CEO",
  "object": { "entity_id": "person:bob" },
  "observed_at": "2026-09-01T12:00:00Z",
  "confidence": 0.99
}
```

The server assigns `ingested_at` if omitted. Reusing an observation ID with
different content is a conflict, not an update.

## Agent memory

```http
POST /agent/memory
Content-Type: application/json

{
  "agent_id": "researcher",
  "session_id": "run-42",
  "idempotency_key": "turn-1-call-1",
  "subject_entity_id": "company:acme",
  "predicate": "RISK",
  "object": "supply chain",
  "object_type": "string",
  "observed_at": "2026-09-27T10:00:00Z",
  "confidence": 0.9,
  "importance": 0.8,
  "tags": ["research", "planning"],
  "memory_kind": "fact"
}
```

The response returns the observation ID, derived fact IDs, and `replayed`.
Safe retries return `200` with `replayed: true`; key reuse with changed content
returns `409`.

## Agent context

```http
POST /agent/context
Content-Type: application/json

{
  "agent_id": "planner",
  "session_id": "plan-7",
  "entity_ids": ["company:acme"],
  "predicates": ["RISK", "CEO"],
  "max_facts": 24,
  "char_budget": 8000,
  "include_conflicts": true
}
```

Facts are ranked by memory importance, confidence, recency, and stable ID.
The bundle reports agent/source attribution, bitemporal bounds, conflicts,
estimated characters, and whether the result was truncated.

## State and bitemporal selection

```http
GET /entities/company%3Aacme/state?valid_at=2026-01-01T00%3A00%3A00Z&known_at=2026-02-01T00%3A00%3A00Z
```

## Path

```http
GET /graph/path?from=company%3Aacme&to=company%3Anova&max_depth=5&valid_at=2026-09-15T00%3A00%3A00Z
```

## Structured query

`POST /query` is an operation envelope, not arbitrary code:

```json
{
  "operation": "world_diff",
  "from": "2026-01-01T00:00:00Z",
  "to": "2026-09-30T23:59:59Z"
}
```

## Rust interface

Rust callers use application/domain types directly and receive typed results.
The stable conceptual surface is create/get entity, append/get observation,
state, temporal state, why, changes, diff, conflicts, and path. Refer to
generated Rust documentation for concrete types and signatures; adapters should
depend on these interfaces instead of reaching into storage internals.

## Operational warning

V0 has no built-in authentication, authorization, TLS termination, quotas, or
tenant isolation. Do not expose the server to an untrusted network. Bind it to a
trusted interface or place it behind an appropriately configured gateway.
