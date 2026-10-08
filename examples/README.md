# Demo dataset

`demo.jsonl` is the canonical V0 story in portable ingestion format. Every line
is an immutable observation with the six required fields: `source`, `subject`,
`predicate`, `object`, `observed_at`, and `confidence`. Optional
`claimed_valid_from`, `claimed_valid_to`, and `cardinality` fields control
valid-time resolution; they default to `observed_at`, open-ended, and `single`.
Other additional fields are preserved as metadata. `record_type` is included for forward-compatible tooling;
in V0 every line has the value `observation`.

Load it into an initialized database:

```sh
wm --db worldmodel.wmdb init
wm --db worldmodel.wmdb load examples/demo.jsonl
```

The dataset establishes:

- Acme and Nova Labs; Alice, Bob, and Carol; Chennai and Bengaluru.
- Two sources supporting Alice as Acme's early CEO.
- A low-priority incorrect claim that Carol is CEO, which remains visible as
  conflicting evidence.
- Two later sources supporting Bob as CEO, superseding Alice in current state.
- Nova Labs moving from Chennai to Bengaluru.
- Acme acquiring Nova Labs.

Suggested walkthrough:

```sh
wm --db worldmodel.wmdb state company:acme
wm --db worldmodel.wmdb conflicts
wm --db worldmodel.wmdb changes company:acme \
  --from 2026-01-01T00:00:00Z --to 2026-09-30T23:59:59Z
wm --db worldmodel.wmdb path company:acme company:nova --max-depth 5
wm --db worldmodel.wmdb diff-world \
  --from 2026-01-01T00:00:00Z --to 2026-09-30T23:59:59Z
```

Use the fact ID emitted by `state` or `changes` with `wm why <fact-id>`. IDs are
printed rather than hard-coded here because the resolver owns deterministic fact
ID construction.

`observation.json` and `observations.csv` demonstrate the other V0 input formats.
They overlap the main story and should be loaded into a fresh database when used
as format examples.
