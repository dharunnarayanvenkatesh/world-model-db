# Query semantics

V0 exposes structured CLI, Rust, and REST operations. SQL examples below define
the intended semantics and naming; full DataFusion SQL syntax is a V1 target.

## OLAP aggregation

The V0 query engine materializes current facts in a structure-of-arrays
columnar batch and supports deterministic single-dimension aggregation:

```sql
SELECT predicate, COUNT(*) FROM wm_facts GROUP BY predicate
SELECT subject_entity_id, COUNT(*) FROM wm_facts GROUP BY subject_entity_id
SELECT status, COUNT(*) FROM wm_facts GROUP BY status
```

This keeps analytical queries operational while the pinned DataFusion release
is reference-only due to the strict transitive-license gate.

## Time parameters

With neither time parameter, a query reads the current materialized belief.

- `valid_at`: filter to versions whose valid interval contains the instant.
- `known_at`: filter to versions whose known interval contains the instant.
- both: what the database believed at `known_at` about the world at `valid_at`.

These are independent. A filing ingested in February may report a change valid
since January; a January valid-time query can therefore differ depending on
whether `known_at` is January 31 or February 28.

## Current state

```sh
wm --db worldmodel.wmdb state company:acme
```

Equivalent relational intent:

```sql
SELECT * FROM wm_facts
WHERE subject_entity_id = 'company:acme'
  AND status = 'SUPPORTED'
  AND valid_from <= CURRENT_TIMESTAMP
  AND (valid_to IS NULL OR CURRENT_TIMESTAMP < valid_to)
  AND known_from <= CURRENT_TIMESTAMP
  AND (known_to IS NULL OR CURRENT_TIMESTAMP < known_to);
```

Historical state is the same operation with `valid_at` and/or `known_at` in the
Rust/REST request. Conceptual future syntax:

```sql
SELECT * FROM wm_facts
AS OF VALID_TIME '2026-01-01T00:00:00Z'
KNOWN AT '2026-02-01T00:00:00Z'
WHERE subject_entity_id = 'company:acme';
```

## Why

```sh
wm --db worldmodel.wmdb why fact:acme:ceo:bob
```

The result contains the fact version, supporting observations and sources,
conflicting observations, resolution status/reason/rule, confidence explanation,
and valid/known timestamps. Missing provenance is an integrity error, not an
empty explanation.

## Changes

```sh
wm --db worldmodel.wmdb changes company:acme \
  --from 2026-01-01T00:00:00Z --to 2026-09-30T23:59:59Z
```

The inclusive query bounds select transitions within the range. Results are
partitioned into facts added/removed/superseded, relationships added/removed,
and events involving the entity. Stable ordering is timestamp, object kind,
then ID.

## World diff

```sh
wm --db worldmodel.wmdb diff-world \
  --from 2026-01-01T00:00:00Z --to 2026-09-30T23:59:59Z
```

This compares two valid-time snapshots at the query's known time. It reports
deterministic counts and details for entities added/retired, facts added or
superseded, relationships added/removed, and conflicts opened/resolved.

World diff is a semantic diff, not a byte-level storage comparison.

## Conflicts

```sh
wm --db worldmodel.wmdb conflicts
```

Open disagreements are returned by default. Structured callers may filter by
status, subject, predicate, and known-time. A resolved conflict remains
historically queryable.

## Temporal paths

```sh
wm --db worldmodel.wmdb path company:acme company:nova \
  --max-depth 5
```

Structured callers may also supply a relationship-type set and `valid_at`.
Only edges valid at that time participate. V0 performs bounded traversal with a
stable edge order and returns one deterministic shortest path when one exists.
`max_depth` must be finite.

## Query safety

IDs, timestamps, predicates, and filters are data values, never concatenated
into executable expressions. Result pagination and input limits belong at the
transport boundary; the semantic query result remains deterministic.
