//! Agent-native memory and context gateway.
//!
//! Agent writes remain ordinary immutable observations. Agent/session identity,
//! idempotency, importance, and tags are stored as observation metadata, so
//! multi-agent memory inherits the database's temporal, provenance, conflict,
//! and reconstruction semantics instead of creating a second memory model.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use wm_core::*;
use wm_resolution::{Engine, NewObservation, ResolutionEngine};

const AGENT_ID: &str = "wm.agent_id";
const SESSION_ID: &str = "wm.session_id";
const IDEMPOTENCY_KEY: &str = "wm.idempotency_key";
const IMPORTANCE: &str = "wm.importance";
const TAGS: &str = "wm.tags";
const MEMORY_KIND: &str = "wm.memory_kind";

#[derive(Clone, Debug, PartialEq)]
pub struct AgentRegistration {
    pub agent_id: String,
    pub name: String,
    pub model: String,
    pub capabilities: Vec<String>,
    pub priority: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentMemoryWrite {
    pub agent_id: String,
    pub session_id: String,
    pub idempotency_key: String,
    pub subject_entity_id: EntityId,
    pub predicate: String,
    pub object: ObjectValue,
    pub observed_at: String,
    pub confidence: f64,
    pub importance: f64,
    pub tags: Vec<String>,
    pub memory_kind: String,
    pub raw_payload: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentMemoryReceipt {
    pub observation_id: ObservationId,
    pub fact_ids: Vec<FactId>,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentContextRequest {
    pub agent_id: String,
    pub session_id: Option<String>,
    pub entity_ids: Vec<EntityId>,
    pub predicates: Vec<String>,
    pub valid_at: Option<String>,
    pub known_at: Option<String>,
    pub max_facts: usize,
    pub char_budget: usize,
    pub include_conflicts: bool,
}

impl Default for AgentContextRequest {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            session_id: None,
            entity_ids: Vec::new(),
            predicates: Vec::new(),
            valid_at: None,
            known_at: None,
            max_facts: 32,
            char_budget: 12_000,
            include_conflicts: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextFact {
    pub fact: Fact,
    pub importance: f64,
    pub source_ids: Vec<SourceId>,
    pub agent_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentContextBundle {
    pub agent_id: String,
    pub session_id: Option<String>,
    pub facts: Vec<ContextFact>,
    pub conflicts: Vec<Conflict>,
    pub truncated: bool,
    pub estimated_chars: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentError {
    Invalid(String),
    NotFound(String),
    IdempotencyConflict(String),
    Storage(String),
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message)
            | Self::NotFound(message)
            | Self::IdempotencyConflict(message)
            | Self::Storage(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for AgentError {}

pub struct AgentGateway<'a> {
    engine: &'a mut Engine,
}

impl<'a> AgentGateway<'a> {
    pub fn new(engine: &'a mut Engine) -> Self {
        Self { engine }
    }

    pub fn register(&mut self, registration: AgentRegistration) -> Result<SourceId, AgentError> {
        validate_identifier("agent_id", &registration.agent_id)?;
        let uri = format!("agent://{}", registration.agent_id);
        if let Some(position) = self
            .engine
            .store
            .state
            .sources
            .iter()
            .position(|source| source.uri == uri)
        {
            let metadata = registration_metadata(&registration);
            let source = &mut self.engine.store.state.sources[position];
            source.name = registration.name;
            source.priority = registration.priority;
            source.metadata = metadata;
            let id = source.id.clone();
            self.engine
                .save()
                .map_err(|error| AgentError::Storage(error.to_string()))?;
            return Ok(id);
        }
        self.engine
            .create_source(
                "ai_agent",
                uri,
                registration.name.clone(),
                registration.priority,
                registration_metadata(&registration),
            )
            .map_err(map_engine_error)
    }

    pub fn remember(&mut self, memory: AgentMemoryWrite) -> Result<AgentMemoryReceipt, AgentError> {
        validate_memory(&memory)?;
        if let Some(existing) = find_idempotent_observation(&self.engine.store.state, &memory) {
            if existing.subject_entity_id != memory.subject_entity_id
                || existing.predicate != memory.predicate
                || existing.object != memory.object
                || existing.observed_at != memory.observed_at
            {
                return Err(AgentError::IdempotencyConflict(format!(
                    "idempotency key '{}' was already used with different memory content",
                    memory.idempotency_key
                )));
            }
            return Ok(receipt(&self.engine.store.state, &existing.id, true));
        }
        let source_id = self.ensure_agent_source(&memory.agent_id)?;
        self.ensure_subject(&memory.subject_entity_id)?;
        let mut metadata = BTreeMap::from([
            (
                AGENT_ID.into(),
                ObjectValue::String(memory.agent_id.clone()),
            ),
            (
                SESSION_ID.into(),
                ObjectValue::String(memory.session_id.clone()),
            ),
            (
                IDEMPOTENCY_KEY.into(),
                ObjectValue::String(memory.idempotency_key.clone()),
            ),
            (IMPORTANCE.into(), ObjectValue::Float(memory.importance)),
            (TAGS.into(), ObjectValue::String(memory.tags.join(","))),
            (
                MEMORY_KIND.into(),
                ObjectValue::String(memory.memory_kind.clone()),
            ),
        ]);
        metadata.insert(
            "wm.writer".into(),
            ObjectValue::String("agent_gateway_v1".into()),
        );
        let observation_id = self
            .engine
            .observe(NewObservation {
                source_id,
                subject_entity_id: memory.subject_entity_id,
                predicate: memory.predicate,
                object: memory.object,
                observed_at: memory.observed_at,
                ingested_at: None,
                confidence: memory.confidence,
                raw_payload: memory.raw_payload,
                metadata,
                retracted: false,
            })
            .map_err(map_engine_error)?;
        Ok(receipt(&self.engine.store.state, &observation_id, false))
    }

    pub fn context(&self, request: AgentContextRequest) -> Result<AgentContextBundle, AgentError> {
        validate_identifier("agent_id", &request.agent_id)?;
        if request.max_facts == 0 || request.max_facts > 1_000 {
            return Err(AgentError::Invalid(
                "max_facts must be between 1 and 1000".into(),
            ));
        }
        if request.char_budget < 256 || request.char_budget > 1_000_000 {
            return Err(AgentError::Invalid(
                "char_budget must be between 256 and 1000000".into(),
            ));
        }
        let predicate_filter = request
            .predicates
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let mut facts = if request.entity_ids.is_empty()
            && request.valid_at.is_none()
            && request.known_at.is_none()
        {
            self.engine
                .store
                .state
                .current_facts()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            let entities = if request.entity_ids.is_empty() {
                self.engine
                    .store
                    .state
                    .entities
                    .iter()
                    .map(|entity| entity.id.clone())
                    .collect::<Vec<_>>()
            } else {
                request.entity_ids.clone()
            };
            entities
                .iter()
                .flat_map(|entity| {
                    wm_query::entity_state(
                        &self.engine.store.state,
                        entity,
                        request.valid_at.as_deref(),
                        request.known_at.as_deref(),
                    )
                })
                .cloned()
                .collect()
        };
        let mut seen = BTreeSet::new();
        facts.retain(|fact| {
            seen.insert(fact.id.clone())
                && (predicate_filter.is_empty()
                    || predicate_filter.contains(&fact.predicate.to_ascii_lowercase()))
        });
        let mut context_facts = facts
            .into_iter()
            .map(|fact| enrich_fact(&self.engine.store.state, fact))
            .collect::<Vec<_>>();
        context_facts.sort_by(|left, right| {
            right
                .importance
                .total_cmp(&left.importance)
                .then_with(|| right.fact.confidence.total_cmp(&left.fact.confidence))
                .then_with(|| right.fact.known_from.cmp(&left.fact.known_from))
                .then_with(|| left.fact.id.cmp(&right.fact.id))
        });
        let available = context_facts.len();
        let mut estimated_chars = 0;
        let mut selected = Vec::new();
        for fact in context_facts {
            if selected.len() >= request.max_facts {
                break;
            }
            let size = estimate_fact_chars(&fact);
            if estimated_chars + size > request.char_budget {
                break;
            }
            estimated_chars += size;
            selected.push(fact);
        }
        let context_facts = selected;
        let selected_ids = context_facts
            .iter()
            .map(|fact| fact.fact.id.clone())
            .collect::<BTreeSet<_>>();
        let conflicts = if request.include_conflicts {
            self.engine
                .store
                .state
                .conflicts
                .iter()
                .filter(|conflict| {
                    conflict
                        .candidate_fact_ids
                        .iter()
                        .any(|id| selected_ids.contains(id))
                })
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let truncated = context_facts.len() < available;
        Ok(AgentContextBundle {
            agent_id: request.agent_id,
            session_id: request.session_id,
            facts: context_facts,
            conflicts,
            truncated,
            estimated_chars,
        })
    }

    pub fn session_memory(&self, agent_id: &str, session_id: &str) -> Vec<Observation> {
        let mut memories = self
            .engine
            .store
            .state
            .observations
            .iter()
            .filter(|observation| metadata_string(observation, AGENT_ID) == Some(agent_id))
            .filter(|observation| metadata_string(observation, SESSION_ID) == Some(session_id))
            .cloned()
            .collect::<Vec<_>>();
        memories.sort_by(|left, right| {
            left.ingested_at
                .cmp(&right.ingested_at)
                .then(left.id.cmp(&right.id))
        });
        memories
    }

    fn ensure_agent_source(&mut self, agent_id: &str) -> Result<SourceId, AgentError> {
        if let Some(source) = self.engine.store.state.sources.iter().find(|source| {
            source.metadata.get(AGENT_ID) == Some(&ObjectValue::String(agent_id.into()))
        }) {
            return Ok(source.id.clone());
        }
        self.register(AgentRegistration {
            agent_id: agent_id.into(),
            name: agent_id.into(),
            model: "unspecified".into(),
            capabilities: Vec::new(),
            priority: 0,
        })
    }

    fn ensure_subject(&mut self, entity_id: &EntityId) -> Result<(), AgentError> {
        if self.engine.store.state.entity(entity_id).is_some() {
            return Ok(());
        }
        let (entity_type, name) = entity_id
            .as_str()
            .split_once(':')
            .unwrap_or(("entity", entity_id.as_str()));
        self.engine.store.state.entities.push(Entity {
            id: entity_id.clone(),
            entity_type: entity_type.into(),
            canonical_name: name.into(),
            aliases: Vec::new(),
            attributes: BTreeMap::from([(
                "wm.auto_materialized".into(),
                ObjectValue::Boolean(true),
            )]),
            created_at: wm_resolution::now_utc(),
            retired_at: None,
        });
        self.engine
            .save()
            .map_err(|error| AgentError::Storage(error.to_string()))
    }
}

pub fn context_json(bundle: &AgentContextBundle) -> String {
    format!(
        "{{\"agent_id\":\"{}\",\"session_id\":{},\"facts\":[{}],\"conflicts\":[{}],\"truncated\":{},\"estimated_chars\":{}}}",
        escape_json(&bundle.agent_id),
        json_option(bundle.session_id.as_deref()),
        bundle
            .facts
            .iter()
            .map(context_fact_json)
            .collect::<Vec<_>>()
            .join(","),
        bundle
            .conflicts
            .iter()
            .map(conflict_json)
            .collect::<Vec<_>>()
            .join(","),
        bundle.truncated,
        bundle.estimated_chars
    )
}

pub fn memory_receipt_json(receipt: &AgentMemoryReceipt) -> String {
    format!(
        "{{\"observation_id\":\"{}\",\"fact_ids\":[{}],\"replayed\":{}}}",
        escape_json(receipt.observation_id.as_str()),
        receipt
            .fact_ids
            .iter()
            .map(|id| format!("\"{}\"", escape_json(id.as_str())))
            .collect::<Vec<_>>()
            .join(","),
        receipt.replayed
    )
}

pub fn session_memory_json(memories: &[Observation]) -> String {
    format!(
        "[{}]",
        memories
            .iter()
            .map(|observation| format!(
                "{{\"observation_id\":\"{}\",\"subject_entity_id\":\"{}\",\"predicate\":\"{}\",\"object\":{},\"observed_at\":\"{}\",\"ingested_at\":\"{}\",\"confidence\":{}}}",
                escape_json(observation.id.as_str()),
                escape_json(observation.subject_entity_id.as_str()),
                escape_json(&observation.predicate),
                wm_query::object_json(&observation.object),
                escape_json(&observation.observed_at),
                escape_json(&observation.ingested_at),
                observation.confidence
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub fn tool_manifest_json() -> &'static str {
    include_str!("../../../docs/agent-tools.json")
}

fn registration_metadata(registration: &AgentRegistration) -> BTreeMap<String, ObjectValue> {
    BTreeMap::from([
        (
            AGENT_ID.into(),
            ObjectValue::String(registration.agent_id.clone()),
        ),
        (
            "wm.model".into(),
            ObjectValue::String(registration.model.clone()),
        ),
        (
            "wm.capabilities".into(),
            ObjectValue::String(registration.capabilities.join(",")),
        ),
        ("wm.agent_native".into(), ObjectValue::Boolean(true)),
    ])
}

fn validate_memory(memory: &AgentMemoryWrite) -> Result<(), AgentError> {
    validate_identifier("agent_id", &memory.agent_id)?;
    validate_identifier("session_id", &memory.session_id)?;
    validate_identifier("idempotency_key", &memory.idempotency_key)?;
    if memory.predicate.trim().is_empty() {
        return Err(AgentError::Invalid("predicate is required".into()));
    }
    if !(0.0..=1.0).contains(&memory.confidence) || !memory.confidence.is_finite() {
        return Err(AgentError::Invalid(
            "confidence must be between 0 and 1".into(),
        ));
    }
    if !(0.0..=1.0).contains(&memory.importance) || !memory.importance.is_finite() {
        return Err(AgentError::Invalid(
            "importance must be between 0 and 1".into(),
        ));
    }
    Ok(())
}

fn validate_identifier(label: &str, value: &str) -> Result<(), AgentError> {
    if value.trim().is_empty() || value.len() > 256 {
        Err(AgentError::Invalid(format!(
            "{label} must contain 1 to 256 characters"
        )))
    } else {
        Ok(())
    }
}

fn find_idempotent_observation<'a>(
    state: &'a wm_storage::WorldState,
    memory: &AgentMemoryWrite,
) -> Option<&'a Observation> {
    state.observations.iter().find(|observation| {
        metadata_string(observation, AGENT_ID) == Some(memory.agent_id.as_str())
            && metadata_string(observation, IDEMPOTENCY_KEY)
                == Some(memory.idempotency_key.as_str())
    })
}

fn receipt(
    state: &wm_storage::WorldState,
    observation_id: &ObservationId,
    replayed: bool,
) -> AgentMemoryReceipt {
    let fact_ids = state
        .facts
        .iter()
        .filter(|fact| fact.created_from_observations.contains(observation_id))
        .map(|fact| fact.id.clone())
        .collect();
    AgentMemoryReceipt {
        observation_id: observation_id.clone(),
        fact_ids,
        replayed,
    }
}

fn enrich_fact(state: &wm_storage::WorldState, fact: Fact) -> ContextFact {
    let observations = fact
        .created_from_observations
        .iter()
        .filter_map(|id| state.observation(id))
        .collect::<Vec<_>>();
    let importance = observations
        .iter()
        .filter_map(|observation| metadata_number(observation, IMPORTANCE))
        .fold(0.5_f64, f64::max);
    let source_ids = observations
        .iter()
        .map(|observation| observation.source_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let agent_ids = observations
        .iter()
        .filter_map(|observation| metadata_string(observation, AGENT_ID).map(str::to_owned))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    ContextFact {
        fact,
        importance,
        source_ids,
        agent_ids,
    }
}

fn metadata_string<'a>(observation: &'a Observation, key: &str) -> Option<&'a str> {
    match observation.metadata.get(key) {
        Some(ObjectValue::String(value)) => Some(value),
        _ => None,
    }
}

fn metadata_number(observation: &Observation, key: &str) -> Option<f64> {
    match observation.metadata.get(key) {
        Some(ObjectValue::Float(value)) => Some(*value),
        Some(ObjectValue::Integer(value)) => Some(*value as f64),
        Some(ObjectValue::String(value)) => value.parse().ok(),
        _ => None,
    }
}

fn estimate_fact_chars(fact: &ContextFact) -> usize {
    128 + fact.fact.id.as_str().len()
        + fact.fact.subject_entity_id.as_str().len()
        + fact.fact.predicate.len()
        + object_text(&fact.fact.object).len()
        + fact.agent_ids.iter().map(String::len).sum::<usize>()
}

fn context_fact_json(value: &ContextFact) -> String {
    format!(
        "{{\"fact_id\":\"{}\",\"subject_entity_id\":\"{}\",\"predicate\":\"{}\",\"object\":{},\"confidence\":{},\"importance\":{},\"valid_from\":\"{}\",\"valid_to\":{},\"known_from\":\"{}\",\"known_to\":{},\"status\":\"{:?}\",\"source_ids\":[{}],\"agent_ids\":[{}]}}",
        escape_json(value.fact.id.as_str()),
        escape_json(value.fact.subject_entity_id.as_str()),
        escape_json(&value.fact.predicate),
        wm_query::object_json(&value.fact.object),
        value.fact.confidence,
        value.importance,
        escape_json(&value.fact.valid_from),
        json_option(value.fact.valid_to.as_deref()),
        escape_json(&value.fact.known_from),
        json_option(value.fact.known_to.as_deref()),
        value.fact.status,
        value
            .source_ids
            .iter()
            .map(|id| format!("\"{}\"", escape_json(id.as_str())))
            .collect::<Vec<_>>()
            .join(","),
        value
            .agent_ids
            .iter()
            .map(|id| format!("\"{}\"", escape_json(id)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn conflict_json(conflict: &Conflict) -> String {
    format!(
        "{{\"conflict_id\":\"{}\",\"subject\":\"{}\",\"predicate\":\"{}\",\"candidate_fact_ids\":[{}],\"resolution_status\":\"{:?}\"}}",
        escape_json(conflict.id.as_str()),
        escape_json(conflict.subject.as_str()),
        escape_json(&conflict.predicate),
        conflict
            .candidate_fact_ids
            .iter()
            .map(|id| format!("\"{}\"", escape_json(id.as_str())))
            .collect::<Vec<_>>()
            .join(","),
        conflict.resolution_status
    )
}

fn json_option(value: Option<&str>) -> String {
    value
        .map(|value| format!("\"{}\"", escape_json(value)))
        .unwrap_or_else(|| "null".into())
}

fn object_text(value: &ObjectValue) -> String {
    match value {
        ObjectValue::Entity(value) => value.0.clone(),
        ObjectValue::String(value) | ObjectValue::Timestamp(value) | ObjectValue::Json(value) => {
            value.clone()
        }
        ObjectValue::Integer(value) => value.to_string(),
        ObjectValue::Float(value) => value.to_string(),
        ObjectValue::Boolean(value) => value.to_string(),
    }
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn map_engine_error(error: wm_resolution::ResolutionError) -> AgentError {
    match error {
        wm_resolution::ResolutionError::Invalid(message) => AgentError::Invalid(message),
        wm_resolution::ResolutionError::NotFound(message) => AgentError::NotFound(message),
        wm_resolution::ResolutionError::Io(error) => AgentError::Storage(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wm-agent-{name}-{nonce}.redb"))
    }

    fn memory(object: &str) -> AgentMemoryWrite {
        AgentMemoryWrite {
            agent_id: "researcher".into(),
            session_id: "session-1".into(),
            idempotency_key: "turn-1-call-1".into(),
            subject_entity_id: "company:acme".into(),
            predicate: "RISK".into(),
            object: ObjectValue::String(object.into()),
            observed_at: "2026-09-27T10:00:00Z".into(),
            confidence: 0.9,
            importance: 0.8,
            tags: vec!["research".into()],
            memory_kind: "fact".into(),
            raw_payload: "agent tool call".into(),
        }
    }

    #[test]
    fn memory_is_idempotent_and_context_is_agent_ready() {
        let path = test_path("idempotency");
        {
            let mut engine = Engine::init(&path).unwrap();
            let mut gateway = AgentGateway::new(&mut engine);
            gateway
                .register(AgentRegistration {
                    agent_id: "researcher".into(),
                    name: "Research Agent".into(),
                    model: "model-x".into(),
                    capabilities: vec!["research".into()],
                    priority: 10,
                })
                .unwrap();
            let first = gateway.remember(memory("supply chain")).unwrap();
            let replay = gateway.remember(memory("supply chain")).unwrap();
            assert_eq!(first.observation_id, replay.observation_id);
            assert!(!first.replayed);
            assert!(replay.replayed);
            assert_eq!(gateway.engine.store.state.observations.len(), 1);

            let context = gateway
                .context(AgentContextRequest {
                    agent_id: "planner".into(),
                    entity_ids: vec!["company:acme".into()],
                    ..AgentContextRequest::default()
                })
                .unwrap();
            assert_eq!(context.facts.len(), 1);
            assert_eq!(context.facts[0].agent_ids, vec!["researcher"]);
            assert_eq!(context.facts[0].importance, 0.8);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn changed_content_rejects_reused_idempotency_key() {
        let path = test_path("collision");
        {
            let mut engine = Engine::init(&path).unwrap();
            let mut gateway = AgentGateway::new(&mut engine);
            gateway.remember(memory("first")).unwrap();
            let error = gateway.remember(memory("changed")).unwrap_err();
            assert!(matches!(error, AgentError::IdempotencyConflict(_)));
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn competing_agents_keep_conflict_visible_in_shared_context() {
        let path = test_path("multi-agent-conflict");
        {
            let mut engine = Engine::init(&path).unwrap();
            let mut gateway = AgentGateway::new(&mut engine);
            gateway
                .register(AgentRegistration {
                    agent_id: "researcher".into(),
                    name: "Researcher".into(),
                    model: "model-r".into(),
                    capabilities: Vec::new(),
                    priority: 10,
                })
                .unwrap();
            gateway.remember(memory("medium")).unwrap();
            gateway
                .register(AgentRegistration {
                    agent_id: "reviewer".into(),
                    name: "Reviewer".into(),
                    model: "model-v".into(),
                    capabilities: Vec::new(),
                    priority: 20,
                })
                .unwrap();
            let mut review = memory("high");
            review.agent_id = "reviewer".into();
            review.idempotency_key = "review-1".into();
            review.observed_at = "2026-09-27T10:01:00Z".into();
            gateway.remember(review).unwrap();

            let context = gateway
                .context(AgentContextRequest {
                    agent_id: "planner".into(),
                    entity_ids: vec!["company:acme".into()],
                    ..AgentContextRequest::default()
                })
                .unwrap();
            assert_eq!(context.conflicts.len(), 1);
            assert_eq!(context.facts[0].agent_ids, vec!["reviewer"]);
        }
        std::fs::remove_file(path).unwrap();
    }
}
