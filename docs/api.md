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
| `POST` | `/twins` | register a domain-neutral digital twin |
| `GET` | `/twins` | list digital twins |
| `GET` | `/twins/:id` | get twin identity and model metadata |
| `POST` | `/twins/:id/telemetry` | append reported telemetry |
| `POST` | `/twins/:id/desired` | append desired/control-plane state |
| `POST` | `/twins/:id/configuration` | append configuration state |
| `POST` | `/twins/:id/derived` | append calculated, predicted, or simulated state |
| `GET` | `/twins/:id/state` | get bitemporal twin state and drift |
| `POST` | `/twins/:id/relationships` | connect twins with a temporal typed link |
| `POST` | `/twins/:id/commands` | request an idempotent, expiring command |
| `GET` | `/twins/:id/commands` | inspect command lifecycle events |
| `POST` | `/twins/:id/commands/:command_id/ack` | acknowledge command progress or completion |
| `POST` | `/entities` | create an entity |
| `GET` | `/entities/:id` | get an entity |
| `POST` | `/observations` | append an immutable observation |
| `GET` | `/observations/:id` | get an observation |
| `POST` | `/relationships` | append a typed temporal relationship |
| `GET` | `/facts` | filter facts, including temporal filters |
| `GET` | `/facts/:id` | get a fact version |
| `GET` | `/facts/:id/why` | explain provenance and resolution |
| `GET` | `/entities/:id/state` | get resolved state |
| `GET` | `/entities/:id/changes` | list changes in a range |
| `GET` | `/conflicts` | list/filter conflicts |
| `GET` | `/conflicts/:id` | get a conflict and candidates |
| `GET` | `/graph/path` | find a bounded temporal path |
| `POST` | `/query` | submit a structured query operation |
| `GET` | `/ontology` | summarize the formal ontology/world state |
| `POST` | `/ontology/definitions` | register schema, module, type, rule, action, permission, or mapping |
| `GET` | `/ontology/consistency` | validate schema and world invariants |
| `POST` | `/ontology/materialize` | materialize computed facts and inferred links |
| `GET` | `/ontology/entities/:id/computed` | evaluate computed properties |
| `POST` | `/ontology/actions/:name/execute` | execute an atomic guarded action |
| `POST` | `/ontology/resolve` | score and optionally persist entity equivalence |
| `POST` | `/ontology/mappings/:id/apply` | map a source record into a typed semantic entity |
| `GET` | `/ontology/authorized-entities` | return an object-level permission-filtered view |
| `POST` | `/ontology/query` | semantic type/filter/traverse/temporal query |
| `GET` | `/ontology/graph/blast-radius` | dependency closure from an entity |
| `GET` | `/ontology/graph/shortest-path` | weighted semantic path |
| `GET` | `/ontology/graph/centrality` | degree, betweenness, PageRank, and business weight |

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
  "claimed_valid_from": "2026-09-01T00:00:00Z",
  "claimed_valid_to": null,
  "cardinality": "single",
  "confidence": 0.99
}
```

The server assigns `ingested_at` if omitted. `claimed_valid_from` defaults to
`observed_at`; `claimed_valid_to` is exclusive and optional. `cardinality` is
`single` (default) or `multi` and must remain consistent for one
subject/predicate pair. RFC 3339 offsets are normalized to UTC.

If the subject has a registered ontology type, the server parses the object as
the property's declared value type and rejects unknown properties, type
mismatches, and incompatible cardinality before appending the observation.

## Enterprise ontology

The ontology endpoints, compact definition syntax, action semantics,
permissions, mappings, inference, and graph analysis are documented in the
[ontology runtime guide](ontology.md).

## Digital twins

Twin endpoints use the same entity, observation, fact, relationship, event,
conflict, provenance, and bitemporal query semantics as the rest of the API.
They add reported/desired/configuration/derived channels, drift calculation,
adapter metadata, topology, and command lifecycle events. See the
[digital-twin guide](digital-twins.md) for complete requests and standards
mapping.

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
  "claimed_valid_from": "2026-09-27T00:00:00Z",
  "cardinality": "single",
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

V0 evaluates configured object/action permission rules, but has no transport
authentication, identity provider, TLS termination, quotas, or tenant
isolation. Do not expose the server to an untrusted network. Bind it to a
trusted interface or place it behind an appropriately configured gateway.
