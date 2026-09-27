# Agent-native World Model DB

The agent API is a thin semantic gateway over the canonical database. It does
not create a second memory subsystem. Every agent memory is an immutable
observation, every agent is a source, and all derived beliefs retain the normal
provenance, conflict, bitemporal, and reconstruction guarantees.

## Why this is agent-native

- **Safe tool retries:** `(agent_id, idempotency_key)` identifies one write.
  An identical retry returns the original receipt; changed content returns a
  conflict instead of silently overwriting memory.
- **Multi-agent attribution:** facts expose the source IDs and agent IDs that
  contributed evidence. Competing agents remain visible in conflicts.
- **Session traceability:** every write carries a session ID and can be replayed
  as immutable session memory.
- **Prompt-sized retrieval:** context requests set both `max_facts` and
  `char_budget`; responses state whether they were truncated.
- **Temporal grounding:** context can be selected by valid time and known time.
- **Explainability:** agents can call the normal `WHY` endpoint for any returned
  fact and inspect supporting or conflicting evidence.
- **Framework-neutral tools:** `docs/agent-tools.json` is a portable JSON Schema
  function manifest. The same operations are available through REST, CLI, and
  the typed Rust API.

## Recommended multi-agent pattern

1. Register long-lived roles such as `researcher`, `planner`, and `executor`.
2. Give every run a session ID and every tool call a stable idempotency key.
3. Store externally grounded statements as `fact`, choices as `decision`, work
   items as `task`, and model self-analysis as `reflection`.
4. Before planning or acting, request context for the entities and predicates
   in scope with a prompt-appropriate character budget.
5. If a context fact is surprising, call `wm_explain` before acting on it.
6. Keep agent source priority explicit. Priority is a deterministic resolution
   input, not proof that an agent is correct.

## REST sequence

Register:

```json
{
  "agent_id": "researcher",
  "name": "Research Agent",
  "model": "model-x",
  "capabilities": ["research", "source-checking"],
  "priority": 20
}
```

Remember:

```json
{
  "agent_id": "researcher",
  "session_id": "run-42",
  "idempotency_key": "turn-1-call-1",
  "subject_entity_id": "company:acme",
  "predicate": "RISK",
  "object": "supply chain",
  "observed_at": "2026-09-27T10:00:00Z",
  "confidence": 0.9,
  "importance": 0.8,
  "tags": ["research"],
  "memory_kind": "fact"
}
```

Recall compact shared context:

```json
{
  "agent_id": "planner",
  "session_id": "plan-7",
  "entity_ids": ["company:acme"],
  "predicates": ["RISK"],
  "max_facts": 24,
  "char_budget": 8000,
  "include_conflicts": true
}
```

## Trust boundary

Agent memory is evidence, not truth. Agent-generated observations are resolved
using the same deterministic source-priority and conflict rules as documents,
sensors, APIs, and human input. The current server still assumes a trusted
network and does not provide authentication or tenant isolation.
