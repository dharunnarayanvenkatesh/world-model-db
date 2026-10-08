# Enterprise ontology runtime

World Model DB implements the enterprise ontology as a durable, executable
control plane over the existing bitemporal world state. The catalog is not
documentation-only metadata: entity, observation, relationship, and action
writes are checked against it before commit.

The formal state represented by schema version 3 is:

```text
O(t) = (V(t), E(t), Sigma, C, R, F, A, Pi, H, P)
```

`V` and `E` are temporal entities and relationships; `Sigma` is the versioned
type system; `C` is the constraint set; `R` and `F` are inference and computed
functions; `A` is the action system; `Pi` is policy; `H` is immutable history;
and `P` is evidence/provenance.

## What is executable

| Document primitives | V0 implementation |
|---|---|
| 1–8: types, properties, cardinality, subtypes, interfaces, domain/range, identity | `ObjectTypeDefinition`, `InterfaceDefinition`, `PropertySchema`, `RelationshipTypeDefinition`; enforced on writes |
| 9–10: entity resolution and equivalence | weighted feature score passed through a sigmoid, threshold decision, durable `EntityEquivalence` |
| 11–16: computed properties and graph logic | copy/count/exists/sum expressions; path rules; transitive, symmetric, inverse, and composition materialization |
| 17–23: time, history, transitions, pre/postconditions, invariants | existing valid/known time and append-only history plus atomic ontology actions with rollback on failed postconditions |
| 24–28: authorization, filtered graphs, provenance, confidence, conflicts | priority-based allow/deny predicates, authorized entity views, evidence chains, explicit confidence and deterministic conflict resolution |
| 29–30: schema evolution | durable schema/type versions, supersession metadata, compatibility modes, and compatibility diagnostics |
| 31–35: graph constraints and analysis | acyclicity, required/maximum degree, reachability, weighted shortest path, degree/betweenness/PageRank/business weight, blast radius |
| 36–42: semantic algebra and consistency | typed select/filter/traverse/aggregate, temporal filtering, consistency reports, disjointness, required existence, derived types through conditions, materialized inference |
| 43–50: propagation, mapping, reconciliation, IDs, namespaces, modules, formal state | inherited constraints, source mappings/transforms, deterministic resolver, semantic ID templates, namespace modules/dependencies, and formal-state summary |

This is a practical V0 interpretation of every primitive. It is deliberately a
small deterministic language, not an OWL reasoner or a distributed policy
service. Production identity, tenant isolation, consensus, and horizontal
execution remain outside the current single-node POC.

## Register definitions

Definitions use one endpoint so an agent can discover and write the catalog
without knowing storage records:

```http
POST /ontology/definitions
Content-Type: application/json

{
  "kind": "object_type",
  "name": "company",
  "namespace": "enterprise",
  "version": 1,
  "properties": "registration_number:string:1:1;status:string:1:1;risk_score:float:0:1",
  "identity_properties": ["registration_number"],
  "identity_weights": [2.5],
  "identity_threshold": 0.9
}
```

Property specifications use `name:type:min:max`; `*` is an unbounded maximum.
Types are `entity`, `string`, `integer`, `float`, `boolean`, `timestamp`, or
`json`.

```http
POST /ontology/definitions

{
  "kind": "relationship_type",
  "name": "owns",
  "version": 1,
  "domain": ["company"],
  "range": ["company"],
  "max_outgoing": 100,
  "transitive": true,
  "inverse_of": "owned_by",
  "acyclic": true,
  "connected": false,
  "weight": 1.0
}
```

Composition specifications use `next_relationship>implied_relationship`.

## Computed properties and inference

Supported computed expressions are `copy:<property>`,
`count_out:<relationship>`, `count_in:<relationship>`,
`exists_out:<relationship>`, and `sum:<property>`.

An inference rule is an ordered relationship path and an implied relationship:

```json
{
  "kind": "inference_rule",
  "id": "rule:grandparent",
  "path": ["parent_of", "parent_of"],
  "implies": "grandparent_of",
  "materialized": true
}
```

`POST /ontology/materialize` closes prior materialized computed versions,
writes new bitemporal facts, and adds non-duplicate inferred relationships.
Derived records are explicitly labeled in `resolution_rule`.

Derived classes are conditional semantic views over a base type:

```json
{
  "kind": "derived_class",
  "name": "high_risk_company",
  "base_type": "company",
  "conditions": "risk_score:ge:float:0.8"
}
```

Semantic queries may select `high_risk_company` like a declared object type;
membership is evaluated from current world state.

## Guarded actions

Conditions use `property:operator:type:value`, separated by semicolons.
Operators are `eq`, `ne`, `gt`, `ge`, `lt`, `le`, and `exists`. Effects use
`set:property:type:value`, `remove:property`,
`relationship:type:target_entity_id`, or `event:event_type`.

```json
{
  "kind": "action",
  "id": "action:close-company",
  "name": "close-company",
  "target_type": "company",
  "preconditions": "status:eq:string:active",
  "effects": "set:status:string:closed;event:company_closed",
  "postconditions": "status:eq:string:closed",
  "allowed_roles": ["operator"]
}
```

Execute it with:

```http
POST /ontology/actions/close-company/execute

{
  "actor": "agent:operations",
  "roles": ["operator"],
  "target": "company:acme"
}
```

The runtime checks type, role, permission rules, preconditions, all relationship
targets and constraints, then postconditions. A failed postcondition restores
entity attributes and removes action-created links/events. A successful action
stores an immutable before/after execution record.

## Permissions

Rules match principal, role, action, object type/ID, and optional property
conditions. Higher priority wins; a deny wins an equal-priority tie. An empty
policy set is permissive for POC compatibility. Once policies exist, unmatched
requests are denied.

```json
{
  "kind": "permission",
  "id": "permission:operators-close",
  "role": "operator",
  "action": "close-company",
  "object_type": "company",
  "conditions": "status:eq:string:active",
  "effect": "allow",
  "priority": 100
}
```

`GET /ontology/authorized-entities?principal=agent%3Aops&roles=operator&action=read`
returns an object-filtered view. Transport authentication is still the
operator's responsibility.

## Schema mappings

A mapping binds a source namespace/type to an ontology type, creates a semantic
ID from a template, and applies deterministic field transforms. Field specs use
`source:target:transform[:argument]`; transforms are `identity`, `lowercase`,
`uppercase`, `trim`, and `prefix`.

```json
{
  "kind": "mapping",
  "id": "mapping:crm-customer",
  "source_namespace": "crm",
  "source_type": "customer",
  "target_type": "company",
  "semantic_id_template": "crm:customer:{id}",
  "fields": "legal_name:name:trim;country:country:uppercase"
}
```

Rust callers apply a mapping with `wm_ontology::map_record`, validate the
result, and write it through `Engine`. Agents can call
`POST /ontology/mappings/:id/apply` with the source record; the engine applies
transforms, coerces values to the target property types, validates the object,
and upserts the semantic ID.

## Semantic and graph queries

`POST /ontology/query` combines type selection, property conditions, directed
traversal, maximum depth, valid time, inferred-edge inclusion, and aggregation
by resulting object type.

Additional endpoints:

- `GET /ontology/entities/:id/computed`
- `POST /ontology/resolve`
- `GET /ontology/consistency`
- `GET /ontology/graph/blast-radius?root=...&max_depth=...`
- `GET /ontology/graph/shortest-path?from=...&to=...`
- `GET /ontology/graph/centrality`
- `GET /ontology` for the formal-state counts

The transport-independent implementation lives in `wm-ontology`. Register
catalog records and mutate durable state through `wm_resolution::Engine`; use
the pure functions in `wm_ontology` for compatibility checks, mappings,
computed views, authorization, consistency, semantic queries, and graph
analysis.
