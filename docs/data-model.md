# Data model

The model distinguishes source assertions from database interpretations. Fields
shown here are logical fields; the serialized format is defined separately.

## IDs and values

IDs are immutable, case-sensitive strings with a kind prefix, for example
`company:acme`, `src:acme-official`, and `obs:filing:alice-ceo`. Callers choose
stable IDs; changing an identity means creating a new object and an explicit
relationship or alias.

An observation object is one of: entity reference, string, signed integer,
floating point, boolean, timestamp, or JSON. Entity references are typed in JSON
as `{"entity_id":"person:alice"}` so they cannot be confused with strings.

Inputs accept RFC 3339 offsets and are normalized to UTC before persistence and
comparison. Temporal intervals are half-open: `[from, to)`. A null `to` is
open-ended, and empty or inverted intervals are rejected.

## Entity

An identifiable thing: person, company, location, product, organization,
document, event, software, device, or an application-defined type.

```text
entity_id, entity_type, canonical_name, attributes, created_at, retired_at
```

Aliases and optional typed identifiers participate in deterministic entity
resolution. Retirement closes availability; it does not delete the entity.
When an ontology type is registered, entity attributes are validated against
its inherited property and interface contracts on every write.

## Ontology catalog

The durable ontology plane contains schema versions, namespace modules,
interfaces, versioned object and relationship types, computed properties,
inference rules, actions, permission rules, source mappings, action executions,
and resolved equivalences. Runtime entities and links continue to use the same
world-state records; the catalog supplies their executable meaning.

Object types define parent types, capability interfaces, typed properties,
cardinality, identity features/weights, and disjoint types. Relationship types
define domain/range, outgoing cardinality, transitivity, symmetry, inverses,
composition, acyclicity, and semantic path weight.

Successful action executions store actor, roles, target, timestamp, and before
and after attributes. Computed and inferred records name their ontology rule in
`resolution_rule`, preserving the distinction between asserted and derived
state.

## Source

The origin of an assertion.

```text
source_id, source_type, uri, name, priority, metadata, created_at
```

Priority and confidence are explicit resolver inputs. A source is evidence
metadata, never an entity merely because it has a URI.

## Observation

An immutable raw assertion.

```text
observation_id, source_id, subject_entity_id, predicate, object,
observed_at, ingested_at, claimed_valid_from, claimed_valid_to, cardinality,
confidence, raw_payload, metadata, retracted
```

`observed_at` is when the source made or measured the assertion. `ingested_at`
is when World Model DB learned it. `claimed_valid_from` and `claimed_valid_to`
are the source's claim about when the assertion holds in the modeled world; the
start defaults to `observed_at`. `cardinality` is `single` or `multi`. Single
predicates choose one value per overlapping valid-time segment, while multi
predicates preserve all supported values. A correction or retraction is another
observation; stored input is never edited.

## Fact

A resolved interpretation of one or more observations.

```text
fact_id, subject_entity_id, predicate, object,
valid_from, valid_to, known_from, known_to,
confidence, confidence_explanation, status,
created_from_observations, resolution_rule, created_at
```

Statuses:

- `SUPPORTED`: preferred belief for its bitemporal interval.
- `CONTESTED`: supported by evidence but opposed by an active candidate.
- `SUPERSEDED`: once preferred, then replaced without erasing history.
- `UNRESOLVED`: no deterministic winner under configured rules.
- `RETRACTED`: explicitly withdrawn; retained historically.

A fact without at least one source observation is invalid. Resolution is
performed independently for each valid-time segment and knowledge snapshot, so
non-overlapping succession does not create a conflict.

## Relationship

A temporal directed edge between entities.

```text
relationship_id, source_entity_id, relationship_type, target_entity_id,
valid_from, valid_to, known_from, known_to,
confidence, status, evidence_ids, resolution_rule, created_at
```

Examples include `WORKS_AT`, `OWNS`, `SUPPLIES`, `CONTROLS`, `LOCATED_IN`,
`DEPENDS_ON`, `PART_OF`, `ACQUIRED`, and `COMPETES_WITH`. Direction is
meaningful. A query may choose to traverse both directions, but storage does not
invent an inverse relationship.

## Event

A bounded or instantaneous occurrence.

```text
event_id, event_type, timestamp, end_timestamp,
entities, attributes, source_observations, confidence
```

Events describe occurrences such as an acquisition or move. They do not replace
facts or relationships: an `ACQUISITION` event may support a temporal `ACQUIRED`
relationship and both retain the same provenance.

## Evidence

Evidence connects a derived fact or relationship to immutable observations and
their sources.

```text
evidence_id, derived_object_id, observation_id, source_id, role, created_at
```

The role records support, conflict, or retraction. `WHY` follows these links and
returns the complete explanation chain.

## Conflict

A preserved disagreement among comparable candidates.

```text
conflict_id, subject_entity_id, predicate, candidate_fact_ids,
detected_at, resolution_status, resolution_reason, resolved_at
```

Statuses are `OPEN`, `RESOLVED`, `MANUAL_REVIEW`, and `SUPERSEDED`. Resolution
selects a preferred belief but does not delete the losing evidence.

## Correlation

V0 correlations are lightweight deterministic signals, never causal claims.

```text
correlation_id, left_object_id, right_object_id,
correlation_type, score, evidence
```

Allowed signals include same entity/event, shared relationship/source,
co-occurrence, and configured time proximity.

## Confidence

Confidence is an explainable deterministic calculation. A deployment may tune
weights, but must persist the formula version and components with the result.
A typical composition uses source confidence, observation confidence, support
count, conflict penalty, and recency weight, clamps the output to `[0, 1]`, and
uses score only after higher-order resolver rules such as explicit retraction.

It must never be described as a probability that the fact is true.
