//! Bitemporal state, provenance, change, diff, and small SQL-compatible queries.

use wm_core::*;
use wm_graph::{GraphPath, find_path};
use wm_storage::WorldState;
use wm_temporal::interval_contains;

/// A dependency-free columnar projection of the fact table. Each vector is a
/// column with identical row cardinality, making the V0 OLAP boundary explicit
/// without allowing a rejected dependency graph to bypass the license policy.
#[derive(Clone, Debug, Default)]
pub struct ColumnarFactBatch {
    pub fact_id: Vec<String>,
    pub subject_entity_id: Vec<String>,
    pub predicate: Vec<String>,
    pub object: Vec<String>,
    pub valid_from: Vec<String>,
    pub known_from: Vec<String>,
    pub confidence: Vec<f64>,
    pub status: Vec<String>,
}

impl ColumnarFactBatch {
    pub fn from_facts<'a>(facts: impl IntoIterator<Item = &'a Fact>) -> Self {
        let mut batch = Self::default();
        for fact in facts {
            batch.fact_id.push(fact.id.0.clone());
            batch
                .subject_entity_id
                .push(fact.subject_entity_id.0.clone());
            batch.predicate.push(fact.predicate.clone());
            batch.object.push(object_text(&fact.object));
            batch.valid_from.push(fact.valid_from.clone());
            batch.known_from.push(fact.known_from.clone());
            batch.confidence.push(fact.confidence);
            batch.status.push(format!("{:?}", fact.status));
        }
        batch
    }

    pub fn len(&self) -> usize {
        self.fact_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fact_id.is_empty()
    }
}

/// Deterministic group-by/count over the materialized current fact view.
pub fn aggregate_fact_counts(
    state: &WorldState,
    dimension: &str,
) -> Result<Vec<(String, usize)>, String> {
    let batch = ColumnarFactBatch::from_facts(state.current_facts());
    let column = match dimension.to_ascii_lowercase().as_str() {
        "predicate" => &batch.predicate,
        "status" => &batch.status,
        "subject_entity_id" | "subject" => &batch.subject_entity_id,
        _ => return Err(format!("unsupported OLAP dimension '{dimension}'")),
    };
    let mut counts = std::collections::BTreeMap::new();
    for value in column {
        *counts.entry(value.clone()).or_insert(0usize) += 1;
    }
    Ok(counts.into_iter().collect())
}

#[derive(Clone, Debug)]
pub struct WhyResult {
    pub fact: Fact,
    pub observations: Vec<Observation>,
    pub sources: Vec<Source>,
    pub conflicting_observations: Vec<Observation>,
    pub conflicts: Vec<Conflict>,
}

#[derive(Clone, Debug, Default)]
pub struct ChangeSet {
    pub facts_added: Vec<Fact>,
    pub facts_removed_or_superseded: Vec<Fact>,
    pub relationships_added: Vec<Relationship>,
    pub relationships_removed: Vec<Relationship>,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorldDiff {
    pub entities_added: usize,
    pub entities_retired: usize,
    pub facts_added: usize,
    pub facts_superseded: usize,
    pub relationships_added: usize,
    pub relationships_removed: usize,
    pub conflicts_opened: usize,
    pub conflicts_resolved: usize,
}

pub trait QueryEngine {
    fn execute_query(&self, query: &str) -> Result<String, String>;
}
impl QueryEngine for WorldState {
    fn execute_query(&self, query: &str) -> Result<String, String> {
        execute(self, query)
    }
}

pub fn entity_state<'a>(
    state: &'a WorldState,
    entity: &EntityId,
    valid_at: Option<&str>,
    known_at: Option<&str>,
) -> Vec<&'a Fact> {
    let candidates = state
        .fact_positions_for_entity(entity)
        .map(|positions| {
            positions
                .iter()
                .filter_map(|position| state.facts.get(*position))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| {
            state
                .facts
                .iter()
                .filter(|fact| &fact.subject_entity_id == entity)
                .collect()
        });
    candidates
        .into_iter()
        .filter(|fact| {
            (match known_at {
                Some(at) => {
                    interval_contains(&fact.known_from, fact.known_to.as_deref(), at)
                        && matches!(fact.status, FactStatus::Supported | FactStatus::Superseded)
                }
                None => fact.known_to.is_none() && fact.status == FactStatus::Supported,
            }) && valid_at
                .is_none_or(|at| interval_contains(&fact.valid_from, fact.valid_to.as_deref(), at))
        })
        .collect()
}

pub fn why(state: &WorldState, id: &FactId) -> Option<WhyResult> {
    let fact = state.fact(id)?.clone();
    let observations = fact
        .created_from_observations
        .iter()
        .filter_map(|observation_id| state.observation(observation_id))
        .cloned()
        .collect::<Vec<_>>();
    let sources = state
        .sources
        .iter()
        .filter(|s| observations.iter().any(|o| o.source_id == s.id))
        .cloned()
        .collect();
    let conflicts = state
        .conflicts
        .iter()
        .filter(|c| c.candidate_fact_ids.contains(id))
        .cloned()
        .collect::<Vec<_>>();
    let conflicting_fact_ids = conflicts
        .iter()
        .flat_map(|c| c.candidate_fact_ids.iter())
        .filter(|candidate| *candidate != id)
        .collect::<Vec<_>>();
    let conflicting_observation_ids = conflicting_fact_ids
        .iter()
        .filter_map(|fact_id| state.fact(fact_id))
        .flat_map(|f| f.created_from_observations.iter())
        .collect::<Vec<_>>();
    let conflicting_observations = conflicting_observation_ids
        .iter()
        .filter_map(|observation_id| state.observation(observation_id))
        .cloned()
        .collect();
    Some(WhyResult {
        fact,
        observations,
        sources,
        conflicting_observations,
        conflicts,
    })
}

pub fn changes(state: &WorldState, entity: &EntityId, from: &str, to: &str) -> ChangeSet {
    let in_window = |value: &str| value > from && value <= to;
    let facts = state
        .fact_positions_for_entity(entity)
        .map(|positions| {
            positions
                .iter()
                .filter_map(|position| state.facts.get(*position))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| {
            state
                .facts
                .iter()
                .filter(|fact| &fact.subject_entity_id == entity)
                .collect()
        });
    let relationships = state
        .relationship_positions_for_entity(entity)
        .map(|positions| {
            positions
                .iter()
                .filter_map(|position| state.relationships.get(*position))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| {
            state
                .relationships
                .iter()
                .filter(|relationship| {
                    &relationship.source_entity_id == entity
                        || &relationship.target_entity_id == entity
                })
                .collect()
        });
    let events = state
        .event_positions_for_entity(entity)
        .map(|positions| {
            positions
                .iter()
                .filter_map(|position| state.events.get(*position))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| {
            state
                .events
                .iter()
                .filter(|event| event.entities.contains(entity))
                .collect()
        });
    ChangeSet {
        facts_added: facts
            .iter()
            .filter(|f| in_window(&f.known_from) && f.status == FactStatus::Supported)
            .map(|fact| (*fact).clone())
            .collect(),
        facts_removed_or_superseded: facts
            .iter()
            .filter(|f| f.known_to.as_deref().is_some_and(in_window))
            .map(|fact| (*fact).clone())
            .collect(),
        relationships_added: relationships
            .iter()
            .filter(|r| in_window(&r.known_from))
            .map(|relationship| (*relationship).clone())
            .collect(),
        relationships_removed: relationships
            .iter()
            .filter(|r| r.known_to.as_deref().is_some_and(in_window))
            .map(|relationship| (*relationship).clone())
            .collect(),
        events: events
            .iter()
            .filter(|e| in_window(&e.timestamp))
            .map(|event| (*event).clone())
            .collect(),
    }
}

pub fn diff_world(state: &WorldState, from: &str, to: &str) -> WorldDiff {
    let in_window = |v: &str| v > from && v <= to;
    WorldDiff {
        entities_added: state
            .entities
            .iter()
            .filter(|e| in_window(&e.created_at))
            .count(),
        entities_retired: state
            .entities
            .iter()
            .filter(|e| e.retired_at.as_deref().is_some_and(in_window))
            .count(),
        facts_added: state
            .facts
            .iter()
            .filter(|f| in_window(&f.known_from) && f.status == FactStatus::Supported)
            .count(),
        facts_superseded: state
            .facts
            .iter()
            .filter(|f| f.known_to.as_deref().is_some_and(in_window))
            .count(),
        relationships_added: state
            .relationships
            .iter()
            .filter(|r| in_window(&r.known_from))
            .count(),
        relationships_removed: state
            .relationships
            .iter()
            .filter(|r| r.known_to.as_deref().is_some_and(in_window))
            .count(),
        conflicts_opened: state
            .conflicts
            .iter()
            .filter(|c| in_window(&c.detected_at))
            .count(),
        conflicts_resolved: state
            .conflicts
            .iter()
            .filter(|c| c.resolved_at.as_deref().is_some_and(in_window))
            .count(),
    }
}

pub fn execute(state: &WorldState, query: &str) -> Result<String, String> {
    let tokens = query.split_whitespace().collect::<Vec<_>>();
    if tokens.len() >= 3
        && tokens[0].eq_ignore_ascii_case("STATE")
        && tokens[1].eq_ignore_ascii_case("OF")
    {
        let id = EntityId(tokens[2].trim_matches('\'').to_owned());
        let valid = find_clause(&tokens, "VALID", "AT");
        let known = find_clause(&tokens, "KNOWN", "AT");
        return Ok(facts_json(&entity_state(state, &id, valid, known)));
    }
    if tokens.len() >= 3
        && tokens[0].eq_ignore_ascii_case("WHY")
        && tokens[1].eq_ignore_ascii_case("FACT")
    {
        return why(state, &FactId(tokens[2].trim_matches('\'').to_owned()))
            .map(|v| why_json(&v))
            .ok_or_else(|| "fact not found".into());
    }
    if tokens.len() >= 5
        && tokens[0].eq_ignore_ascii_case("DIFF")
        && tokens[1].eq_ignore_ascii_case("WORLD")
    {
        let between = tokens
            .iter()
            .position(|t| t.eq_ignore_ascii_case("BETWEEN"))
            .ok_or("BETWEEN required")?;
        let from = *tokens.get(between + 1).ok_or("from time required")?;
        let to = *tokens.get(between + 3).ok_or("to time required")?;
        return Ok(diff_json(&diff_world(
            state,
            from.trim_matches('\''),
            to.trim_matches('\''),
        )));
    }
    if tokens
        .first()
        .is_some_and(|t| t.eq_ignore_ascii_case("SELECT"))
        && query.to_ascii_lowercase().contains("from wm_facts")
    {
        if let Some(group_index) = tokens
            .iter()
            .position(|token| token.eq_ignore_ascii_case("GROUP"))
            && tokens
                .get(group_index + 1)
                .is_some_and(|token| token.eq_ignore_ascii_case("BY"))
        {
            let dimension = tokens
                .get(group_index + 2)
                .ok_or("GROUP BY dimension required")?;
            return aggregate_fact_counts(state, dimension)
                .map(|rows| aggregate_json(dimension, &rows));
        }
        let facts = state.current_facts().collect::<Vec<_>>();
        return Ok(facts_json(&facts));
    }
    Err(
        "supported queries: STATE OF, WHY FACT, DIFF WORLD BETWEEN, SELECT ... FROM wm_facts"
            .into(),
    )
}

fn aggregate_json(dimension: &str, rows: &[(String, usize)]) -> String {
    format!(
        "[{}]",
        rows.iter()
            .map(|(value, count)| format!(
                "{{\"{}\":\"{}\",\"count\":{}}}",
                esc(dimension),
                esc(value),
                count
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
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

fn find_clause<'a>(tokens: &[&'a str], a: &str, b: &str) -> Option<&'a str> {
    tokens
        .windows(3)
        .find(|w| w[0].eq_ignore_ascii_case(a) && w[1].eq_ignore_ascii_case(b))
        .map(|w| w[2].trim_matches('\''))
}
fn esc(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
pub fn object_json(v: &ObjectValue) -> String {
    match v {
        ObjectValue::Entity(v) => format!("{{\"entity_id\":\"{}\"}}", esc(&v.0)),
        ObjectValue::String(v) | ObjectValue::Timestamp(v) | ObjectValue::Json(v) => {
            format!("\"{}\"", esc(v))
        }
        ObjectValue::Integer(v) => v.to_string(),
        ObjectValue::Float(v) => v.to_string(),
        ObjectValue::Boolean(v) => v.to_string(),
    }
}
pub fn fact_json(f: &Fact) -> String {
    format!(
        "{{\"fact_id\":\"{}\",\"subject_entity_id\":\"{}\",\"predicate\":\"{}\",\"object\":{},\"valid_from\":\"{}\",\"valid_to\":{},\"known_from\":\"{}\",\"known_to\":{},\"confidence\":{},\"status\":\"{:?}\",\"resolution_rule\":\"{}\"}}",
        esc(&f.id.0),
        esc(&f.subject_entity_id.0),
        esc(&f.predicate),
        object_json(&f.object),
        f.valid_from,
        f.valid_to
            .as_ref()
            .map(|v| format!("\"{v}\""))
            .unwrap_or("null".into()),
        f.known_from,
        f.known_to
            .as_ref()
            .map(|v| format!("\"{v}\""))
            .unwrap_or("null".into()),
        f.confidence,
        f.status,
        esc(&f.resolution_rule)
    )
}
pub fn facts_json(facts: &[&Fact]) -> String {
    format!(
        "[{}]",
        facts
            .iter()
            .map(|f| fact_json(f))
            .collect::<Vec<_>>()
            .join(",")
    )
}
pub fn why_json(v: &WhyResult) -> String {
    format!(
        "{{\"fact\":{},\"supporting_observations\":[{}],\"sources\":[{}],\"conflicting_observations\":[{}],\"resolution_status\":\"{:?}\",\"resolution_reason\":\"{}\"}}",
        fact_json(&v.fact),
        v.observations
            .iter()
            .map(|o| format!("\"{}\"", o.id.0))
            .collect::<Vec<_>>()
            .join(","),
        v.sources
            .iter()
            .map(|s| format!("\"{}\"", s.id.0))
            .collect::<Vec<_>>()
            .join(","),
        v.conflicting_observations
            .iter()
            .map(|o| format!("\"{}\"", o.id.0))
            .collect::<Vec<_>>()
            .join(","),
        v.fact.status,
        esc(&v.fact.resolution_rule)
    )
}
pub fn changes_json(v: &ChangeSet) -> String {
    format!(
        "{{\"facts_added\":{},\"facts_removed_or_superseded\":{},\"relationships_added\":{},\"relationships_removed\":{},\"events\":{}}}",
        v.facts_added.len(),
        v.facts_removed_or_superseded.len(),
        v.relationships_added.len(),
        v.relationships_removed.len(),
        v.events.len()
    )
}
pub fn diff_json(v: &WorldDiff) -> String {
    format!(
        "{{\"entities_added\":{},\"entities_retired\":{},\"facts_added\":{},\"facts_superseded\":{},\"relationships_added\":{},\"relationships_removed\":{},\"conflicts_opened\":{},\"conflicts_resolved\":{}}}",
        v.entities_added,
        v.entities_retired,
        v.facts_added,
        v.facts_superseded,
        v.relationships_added,
        v.relationships_removed,
        v.conflicts_opened,
        v.conflicts_resolved
    )
}
pub fn path_json(v: &GraphPath) -> String {
    format!(
        "{{\"entities\":[{}],\"relationships\":[{}]}}",
        v.entities
            .iter()
            .map(|e| format!("\"{}\"", e.0))
            .collect::<Vec<_>>()
            .join(","),
        v.relationships
            .iter()
            .map(|e| format!("\"{}\"", e.0))
            .collect::<Vec<_>>()
            .join(",")
    )
}
pub fn query_path(
    state: &WorldState,
    from: &EntityId,
    to: &EntityId,
    max_depth: usize,
    kind: Option<&str>,
    valid_at: Option<&str>,
) -> Option<GraphPath> {
    find_path(state, from, to, max_depth, kind, valid_at)
}
