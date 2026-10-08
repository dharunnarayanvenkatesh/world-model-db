# Known limitations

V0 proves world-model semantics on one machine. It is suitable for evaluation,
development, and small trusted deployments, not a production control plane.

- One process and one local database file; no clustering, replication, sharding,
  consensus, distributed transactions, or multi-region behavior.
- redb provides transactional record-granular local commits and the hot point,
  entity-state, provenance, and adjacency paths are indexed. Full-world diffs,
  OLAP grouping, and snapshot decoding remain linear in retained history.
- The current query surface is structured Rust/CLI/REST operations. Full
  DataFusion SQL and custom temporal SQL grammar are not implemented in V0.
- Entity resolution supports deterministic weighted ontology identity features,
  token similarity, sigmoid scoring, thresholds, and durable equivalence. It
  does not perform learned, multilingual, or embedding-based matching.
- Resolution rules express preferred belief, not truth or causality. A source
  priority mistake can deterministically select the wrong candidate.
- Confidence is a configurable explanatory score, not a calibrated probability.
- Graph traversal and ontology analytics are local; there is no distributed
  graph optimizer or arbitrary Cypher/SPARQL pattern language.
- Correlation is limited to deterministic signals and is never labeled causation.
- No vector index, embeddings, semantic search, LLM extraction, or web crawler.
- JSON/JSONL/CSV and REST are ingestion adapters, not general ETL orchestration.
- The REST server evaluates configured object/action authorization predicates,
  but has no transport authentication, identity provider, TLS, quotas, tenant
  isolation, or hardened public-network deployment profile.
- There is no application-level online backup, retained restore generation,
  encryption at rest, or schema migration tooling beyond explicit version
  checks.
- No dashboard, Kubernetes operator, cloud management plane, billing, or GPU
  execution.

Most importantly, evidence-backed does not mean verified. World Model DB can
show what sources asserted and why a deterministic rule preferred one assertion;
it cannot guarantee that any source describes reality correctly.
