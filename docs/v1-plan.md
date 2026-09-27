# V1 plan

V1 begins only after V0 semantic and reconstruction tests remain green. The
order below strengthens the single-node engine before considering distribution.

## 1. Freeze and migrate schema v1

- Add explicit forward migrations with fixtures from every released schema.
- Add corruption detection, recovery diagnostics, and offline integrity checks.
- Preserve deterministic reconstruction across version upgrades.

Exit criterion: every prior release fixture migrates without losing observations,
temporal intervals, conflicts, or evidence links.

## 2. Complete SQL integration

- Integrate Apache Arrow/DataFusion only after a full license audit.
- Expose facts, observations, relationships, events, and conflicts as tables.
- Implement valid-time and known-time planning without ambiguous timestamp rules.
- Keep `WHY`, `CHANGES`, world diff, and graph path as typed functions/operators
  when forcing them into SQL would reduce clarity.

Exit criterion: SQL and structured APIs return equivalent deterministic results
for the bitemporal conformance suite.

## 3. Storage scale and observability

- Add columnar historical segments and indexed temporal scans.
- Evaluate OpenDAL/Iceberg as replaceable storage accelerators, not product identity.
- Add compaction that never erases logical history.
- Add metrics, tracing, resource limits, pagination, and cancellation.

Exit criterion: reproducible benchmarks show improvement without changing
semantic output or violating the dependency policy.

## 4. Safer ingestion and operations

- Streaming/batched imports with record-level error reports and idempotency keys.
- Online snapshots, verified restore, and explicit migration tooling.
- Authentication/authorization and tenant boundaries for network deployments.

Exit criterion: documented failure injection demonstrates atomicity, restart,
backup, restore, and safe retry behavior.

## 5. Resolution extensions

- Pluggable entity-resolution implementations behind the existing interface.
- Versioned resolution policies, manual-review workflow, and what-if replay.
- Optional learned candidates may propose matches but must never bypass evidence,
  provenance, or deterministic policy application.

Exit criterion: alternative resolvers remain reproducible and every decision is
explainable from stored inputs and a versioned rule.

## 6. Optional semantic retrieval

Evaluate vector search only when it accelerates discovery. Every result must map
back to a real source, observation, entity, fact, relationship, or event ID.
Vectors remain rebuildable supporting infrastructure, never the source of truth.

## Explicitly deferred

Raft, multi-region consensus, a Kubernetes operator, managed cloud control plane,
custom distributed filesystem, autonomous agents, and GPU-native execution are
not V1 defaults. Distribution should be considered only after single-node
semantics, migration, operational safety, and workload evidence justify it.

