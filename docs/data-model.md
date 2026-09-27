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

All timestamps are RFC 3339 UTC instants. Temporal intervals are half-open:
`[from, to)`. A null `to` is open-ended.

## Entity

An identifiable thing: person, company, location, product, organization,
document, event, software, device, or an application-defined type.

```text
entity_id, entity_type, canonical_name, attributes, created_at, retired_at
```

Aliases and optional typed identifiers participate in deterministic entity
resolution. Retirement closes availability; it does not delete the entity.

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
observed_at, ingested_at, confidence, raw_payload, metadata, retracted
```

`observed_at` is when the source made or measured the assertion. `ingested_at`
is when World Model DB learned it. A correction or retraction is another
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

A fact without at least one source observation is invalid.

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
