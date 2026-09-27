//! Deterministic fact resolution and mutation-free observation ingestion.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use wm_core::*;
use wm_storage::{FileStore, WorldState};
use wm_temporal::validate_rfc3339;

#[derive(Clone, Debug)]
pub struct ResolutionConfig {
    pub support_bonus: f64,
    pub conflict_penalty: f64,
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            support_bonus: 0.02,
            conflict_penalty: 0.05,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NewObservation {
    pub source_id: SourceId,
    pub subject_entity_id: EntityId,
    pub predicate: String,
    pub object: ObjectValue,
    pub observed_at: String,
    pub ingested_at: Option<String>,
    pub confidence: f64,
    pub raw_payload: String,
    pub metadata: BTreeMap<String, ObjectValue>,
    pub retracted: bool,
}

#[derive(Debug)]
pub enum ResolutionError {
    Io(io::Error),
    Invalid(String),
    NotFound(String),
}

impl fmt::Display for ResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Invalid(e) | Self::NotFound(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for ResolutionError {}
impl From<io::Error> for ResolutionError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub trait ResolutionEngine {
    fn observe(&mut self, input: NewObservation) -> Result<ObservationId, ResolutionError>;
    fn rebuild_current(&mut self) -> Result<(), ResolutionError>;
}

#[derive(Clone, Debug)]
pub struct Engine {
    pub store: FileStore,
    pub config: ResolutionConfig,
}

impl Engine {
    pub fn init(path: impl AsRef<Path>) -> Result<Self, ResolutionError> {
        Ok(Self {
            store: FileStore::init(path)?,
            config: ResolutionConfig::default(),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, ResolutionError> {
        let mut engine = Self {
            store: FileStore::open(path)?,
            config: ResolutionConfig::default(),
        };
        if engine.store.state.facts.is_empty() && !engine.store.state.observations.is_empty() {
            engine.rebuild_current()?;
        }
        Ok(engine)
    }

    pub fn save(&mut self) -> Result<(), ResolutionError> {
        self.store.save().map_err(Into::into)
    }

    pub fn create_entity(
        &mut self,
        entity_type: impl Into<String>,
        canonical_name: impl Into<String>,
        aliases: Vec<String>,
        attributes: BTreeMap<String, ObjectValue>,
    ) -> Result<EntityId, ResolutionError> {
        let entity_type = entity_type.into();
        let canonical_name = canonical_name.into();
        if let Some(existing) = self.store.state.entities.iter().find(|e| {
            e.entity_type.eq_ignore_ascii_case(&entity_type)
                && normalize(&e.canonical_name) == normalize(&canonical_name)
        }) {
            return Ok(existing.id.clone());
        }
        let base = format!("{}:{}", slug(&entity_type), slug(&canonical_name));
        let id = unique_entity_id(&self.store.state, &base);
        self.store.state.entities.push(Entity {
            id: id.clone(),
            entity_type,
            canonical_name,
            aliases,
            attributes,
            created_at: now_utc(),
            retired_at: None,
        });
        self.save()?;
        Ok(id)
    }

    pub fn create_source(
        &mut self,
        source_type: impl Into<String>,
        uri: impl Into<String>,
        name: impl Into<String>,
        priority: i32,
        metadata: BTreeMap<String, ObjectValue>,
    ) -> Result<SourceId, ResolutionError> {
        let uri = uri.into();
        if let Some(existing) = self.store.state.sources.iter().find(|s| s.uri == uri) {
            return Ok(existing.id.clone());
        }
        let name = name.into();
        let base = format!("src:{}", slug(&name));
        let id = unique_source_id(&self.store.state, &base);
        self.store.state.sources.push(Source {
            id: id.clone(),
            source_type: source_type.into(),
            uri,
            name,
            metadata,
            priority,
            created_at: now_utc(),
        });
        self.save()?;
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_relationship(
        &mut self,
        source: EntityId,
        relationship_type: impl Into<String>,
        target: EntityId,
        valid_from: impl Into<String>,
        valid_to: Option<String>,
        confidence: f64,
        observation_ids: Vec<ObservationId>,
    ) -> Result<RelationshipId, ResolutionError> {
        ensure_entity(&self.store.state, &source)?;
        ensure_entity(&self.store.state, &target)?;
        let valid_from = valid_from.into();
        validate_time(&valid_from)?;
        if let Some(value) = &valid_to {
            validate_time(value)?;
        }
        validate_confidence(confidence)?;
        let now = now_utc();
        let id = RelationshipId(self.store.state.next_id("rel"));
        let mut evidence_ids = Vec::new();
        for observation_id in observation_ids {
            let source_id = self
                .store
                .state
                .observation(&observation_id)
                .map(|o| o.source_id.clone())
                .ok_or_else(|| {
                    ResolutionError::NotFound(format!("observation {observation_id} not found"))
                })?;
            let evidence_id = EvidenceId(self.store.state.next_id("evidence"));
            self.store.state.evidence.push(Evidence {
                id: evidence_id.clone(),
                derived_object_id: id.0.clone(),
                observation_id: observation_id.clone(),
                source_id,
                role: EvidenceRole::Support,
                created_at: now.clone(),
            });
            evidence_ids.push(evidence_id);
        }
        self.store.state.relationships.push(Relationship {
            id: id.clone(),
            source_entity_id: source,
            relationship_type: relationship_type.into(),
            target_entity_id: target,
            valid_from,
            valid_to,
            known_from: now.clone(),
            known_to: None,
            confidence,
            status: FactStatus::Supported,
            evidence_ids,
            resolution_rule: "direct temporal relationship assertion".into(),
            created_at: now,
        });
        self.save()?;
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_event(
        &mut self,
        event_type: impl Into<String>,
        timestamp: impl Into<String>,
        end_timestamp: Option<String>,
        entities: Vec<EntityId>,
        attributes: BTreeMap<String, ObjectValue>,
        source_observations: Vec<ObservationId>,
        confidence: f64,
    ) -> Result<EventId, ResolutionError> {
        let timestamp = timestamp.into();
        validate_time(&timestamp)?;
        validate_confidence(confidence)?;
        for entity in &entities {
            ensure_entity(&self.store.state, entity)?;
        }
        let id = EventId(self.store.state.next_id("event"));
        self.store.state.events.push(Event {
            id: id.clone(),
            event_type: event_type.into(),
            timestamp,
            end_timestamp,
            entities,
            attributes,
            source_observations,
            confidence,
        });
        self.save()?;
        Ok(id)
    }

    fn apply_observation(&mut self, observation: &Observation) -> Result<(), ResolutionError> {
        self.store.state.sync_indexes();
        resolve_key(
            &mut self.store.state,
            &self.config,
            &observation.subject_entity_id,
            &observation.predicate,
            &observation.ingested_at,
        )
    }
}

impl ResolutionEngine for Engine {
    fn observe(&mut self, input: NewObservation) -> Result<ObservationId, ResolutionError> {
        ensure_entity(&self.store.state, &input.subject_entity_id)?;
        if self.store.state.source(&input.source_id).is_none() {
            return Err(ResolutionError::NotFound(format!(
                "source {} not found",
                input.source_id
            )));
        }
        validate_time(&input.observed_at)?;
        validate_confidence(input.confidence)?;
        let ingested_at = input.ingested_at.unwrap_or_else(now_utc);
        validate_time(&ingested_at)?;
        let id = ObservationId(self.store.state.next_id("observation"));
        let observation = Observation {
            id: id.clone(),
            source_id: input.source_id,
            subject_entity_id: input.subject_entity_id,
            predicate: input.predicate,
            object: input.object,
            observed_at: input.observed_at,
            ingested_at,
            confidence: input.confidence,
            raw_payload: input.raw_payload,
            metadata: input.metadata,
            retracted: input.retracted,
        };
        self.store.state.observations.push(observation.clone());
        self.apply_observation(&observation)?;
        self.save()?;
        Ok(id)
    }

    fn rebuild_current(&mut self) -> Result<(), ResolutionError> {
        self.store.state.facts.clear();
        self.store.state.conflicts.clear();
        self.store
            .state
            .evidence
            .retain(|e| e.derived_object_id.starts_with("rel:"));
        self.store
            .state
            .counters
            .retain(|kind, _| !matches!(kind.as_str(), "fact" | "conflict"));
        let mut observations = self.store.state.observations.clone();
        observations.sort_by(|a, b| a.ingested_at.cmp(&b.ingested_at).then(a.id.cmp(&b.id)));
        for observation in &observations {
            self.apply_observation(observation)?;
        }
        Ok(())
    }
}

fn resolve_key(
    state: &mut WorldState,
    config: &ResolutionConfig,
    subject: &EntityId,
    predicate: &str,
    known_at: &str,
) -> Result<(), ResolutionError> {
    let relevant = state
        .observation_positions_for_entity(subject)
        .map(|positions| {
            positions
                .iter()
                .filter_map(|position| state.observations.get(*position))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| {
            state
                .observations
                .iter()
                .filter(|observation| &observation.subject_entity_id == subject)
                .collect()
        })
        .into_iter()
        .filter(|o| o.predicate == predicate && o.ingested_at.as_str() <= known_at)
        .cloned()
        .collect::<Vec<_>>();
    let mut retracted_after: BTreeMap<String, String> = BTreeMap::new();
    for o in relevant.iter().filter(|o| o.retracted) {
        retracted_after.insert(object_key(&o.object), o.ingested_at.clone());
    }
    let active = relevant
        .iter()
        .filter(|o| {
            !o.retracted
                && retracted_after
                    .get(&object_key(&o.object))
                    .is_none_or(|time| &o.ingested_at > time)
        })
        .cloned()
        .collect::<Vec<_>>();
    let previous_winner = state
        .facts
        .iter()
        .find(|f| {
            &f.subject_entity_id == subject
                && f.predicate == predicate
                && f.known_to.is_none()
                && f.status == FactStatus::Supported
        })
        .map(|f| object_key(&f.object));
    for fact in state.facts.iter_mut().filter(|f| {
        &f.subject_entity_id == subject && f.predicate == predicate && f.known_to.is_none()
    }) {
        fact.known_to = Some(known_at.to_owned());
        if fact.status == FactStatus::Supported {
            fact.status = FactStatus::Superseded;
        }
    }
    let mut groups: BTreeMap<String, Vec<Observation>> = BTreeMap::new();
    for observation in active {
        groups
            .entry(object_key(&observation.object))
            .or_default()
            .push(observation);
    }
    if groups.is_empty() {
        for (key, time) in retracted_after {
            if let Some(obs) = relevant.iter().rev().find(|o| object_key(&o.object) == key) {
                let id = FactId(state.next_id("fact"));
                state.facts.push(Fact {
                    id,
                    subject_entity_id: subject.clone(),
                    predicate: predicate.to_owned(),
                    object: obs.object.clone(),
                    valid_from: obs.observed_at.clone(),
                    valid_to: Some(obs.observed_at.clone()),
                    known_from: time.clone(),
                    known_to: None,
                    confidence: 0.0,
                    status: FactStatus::Retracted,
                    created_from_observations: vec![obs.id.clone()],
                    confidence_explanation: "explicit retraction preserved".into(),
                    resolution_rule: "explicit retraction".into(),
                    created_at: time,
                });
            }
        }
        close_conflict(
            state,
            subject,
            predicate,
            known_at,
            "all candidates retracted",
        );
        return Ok(());
    }
    let winner = groups
        .iter()
        .max_by(|(ak, av), (bk, bv)| compare_candidates(state, av, bv).then_with(|| bk.cmp(ak)))
        .map(|(k, _)| k.clone())
        .unwrap();
    let conflict = groups.len() > 1;
    let mut candidate_ids = Vec::new();
    let winner_valid_from = groups[&winner]
        .iter()
        .map(|o| o.observed_at.as_str())
        .max()
        .unwrap_or(known_at)
        .to_owned();
    if previous_winner.as_ref().is_some_and(|old| old != &winner) {
        for fact in state.facts.iter_mut().filter(|f| {
            &f.subject_entity_id == subject
                && f.predicate == predicate
                && f.status == FactStatus::Superseded
                && f.valid_to.is_none()
        }) {
            fact.valid_to = Some(winner_valid_from.clone());
        }
    }
    for (key, observations) in groups {
        let supported = key == winner;
        let avg =
            observations.iter().map(|o| o.confidence).sum::<f64>() / observations.len() as f64;
        let confidence = (avg + config.support_bonus * observations.len().saturating_sub(1) as f64
            - if conflict {
                config.conflict_penalty
            } else {
                0.0
            })
        .clamp(0.0, 1.0);
        let latest = observations
            .iter()
            .max_by_key(|o| (&o.observed_at, &o.id))
            .unwrap();
        let fact_id = FactId(state.next_id("fact"));
        let support_ids = observations
            .iter()
            .map(|o| o.id.clone())
            .collect::<Vec<_>>();
        state.facts.push(Fact { id: fact_id.clone(), subject_entity_id: subject.clone(), predicate: predicate.to_owned(), object: latest.object.clone(),
            valid_from: observations.iter().map(|o| o.observed_at.clone()).min().unwrap(), valid_to: None,
            known_from: known_at.to_owned(), known_to: None, confidence,
            status: if supported { FactStatus::Supported } else { FactStatus::Contested }, created_from_observations: support_ids.clone(),
            confidence_explanation: format!("{} supporting observation(s); mean confidence {:.3}; conflict penalty {:.3}", observations.len(), avg, if conflict { config.conflict_penalty } else { 0.0 }),
            resolution_rule: "source priority, recency, observation confidence, support count, deterministic object key".into(), created_at: known_at.to_owned() });
        for observation in &observations {
            let evidence_id = EvidenceId(state.next_id("evidence"));
            state.evidence.push(Evidence {
                id: evidence_id,
                derived_object_id: fact_id.0.clone(),
                observation_id: observation.id.clone(),
                source_id: observation.source_id.clone(),
                role: if supported {
                    EvidenceRole::Support
                } else {
                    EvidenceRole::Conflict
                },
                created_at: known_at.to_owned(),
            });
        }
        candidate_ids.push(fact_id);
    }
    if conflict {
        open_conflict(state, subject, predicate, candidate_ids, known_at);
    } else {
        close_conflict(state, subject, predicate, known_at, "candidates converged");
    }
    Ok(())
}

fn compare_candidates(state: &WorldState, left: &[Observation], right: &[Observation]) -> Ordering {
    candidate_rank(state, left).cmp(&candidate_rank(state, right))
}
fn candidate_rank(state: &WorldState, values: &[Observation]) -> (i32, String, i64, usize) {
    let priority = values
        .iter()
        .filter_map(|o| state.source(&o.source_id).map(|s| s.priority))
        .max()
        .unwrap_or_default();
    let latest = values
        .iter()
        .map(|o| o.observed_at.clone())
        .max()
        .unwrap_or_default();
    let confidence = (values.iter().map(|o| o.confidence).sum::<f64>() * 1_000_000.0) as i64;
    (priority, latest, confidence, values.len())
}
fn open_conflict(
    state: &mut WorldState,
    subject: &EntityId,
    predicate: &str,
    ids: Vec<FactId>,
    now: &str,
) {
    if let Some(conflict) = state.conflicts.iter_mut().rev().find(|c| {
        &c.subject == subject
            && c.predicate == predicate
            && c.resolution_status == ConflictResolutionStatus::Open
    }) {
        conflict.candidate_fact_ids = ids;
        return;
    }
    let id = ConflictId(state.next_id("conflict"));
    state.conflicts.push(Conflict {
        id,
        subject: subject.clone(),
        predicate: predicate.to_owned(),
        candidate_fact_ids: ids,
        detected_at: now.to_owned(),
        resolution_status: ConflictResolutionStatus::Open,
        resolution_reason: None,
        resolved_at: None,
    });
}
fn close_conflict(
    state: &mut WorldState,
    subject: &EntityId,
    predicate: &str,
    now: &str,
    reason: &str,
) {
    for conflict in state.conflicts.iter_mut().filter(|c| {
        &c.subject == subject
            && c.predicate == predicate
            && c.resolution_status == ConflictResolutionStatus::Open
    }) {
        conflict.resolution_status = ConflictResolutionStatus::Resolved;
        conflict.resolution_reason = Some(reason.into());
        conflict.resolved_at = Some(now.into());
    }
}
fn object_key(value: &ObjectValue) -> String {
    match value {
        ObjectValue::Entity(v) => format!("e:{}", v.0),
        ObjectValue::String(v) => format!("s:{v}"),
        ObjectValue::Integer(v) => format!("i:{v}"),
        ObjectValue::Float(v) => format!("f:{:016x}", v.to_bits()),
        ObjectValue::Boolean(v) => format!("b:{v}"),
        ObjectValue::Timestamp(v) => format!("t:{v}"),
        ObjectValue::Json(v) => format!("j:{v}"),
    }
}
fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}
fn slug(value: &str) -> String {
    let out = value
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();
    out.split('-')
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}
fn unique_entity_id(state: &WorldState, base: &str) -> EntityId {
    let used = state
        .entities
        .iter()
        .map(|e| e.id.0.as_str())
        .collect::<BTreeSet<_>>();
    if !used.contains(base) {
        return EntityId(base.into());
    }
    let mut n = 2;
    loop {
        let v = format!("{base}-{n}");
        if !used.contains(v.as_str()) {
            return EntityId(v);
        }
        n += 1;
    }
}
fn unique_source_id(state: &WorldState, base: &str) -> SourceId {
    let used = state
        .sources
        .iter()
        .map(|e| e.id.0.as_str())
        .collect::<BTreeSet<_>>();
    if !used.contains(base) {
        return SourceId(base.into());
    }
    let mut n = 2;
    loop {
        let v = format!("{base}-{n}");
        if !used.contains(v.as_str()) {
            return SourceId(v);
        }
        n += 1;
    }
}
fn ensure_entity(state: &WorldState, id: &EntityId) -> Result<(), ResolutionError> {
    state
        .entity(id)
        .is_some()
        .then_some(())
        .ok_or_else(|| ResolutionError::NotFound(format!("entity {id} not found")))
}
fn validate_confidence(v: f64) -> Result<(), ResolutionError> {
    if v.is_finite() && (0.0..=1.0).contains(&v) {
        Ok(())
    } else {
        Err(ResolutionError::Invalid(
            "confidence must be between 0 and 1".into(),
        ))
    }
}
fn validate_time(v: &str) -> Result<(), ResolutionError> {
    validate_rfc3339(v).map_err(|e| ResolutionError::Invalid(e.to_string()))
}

pub fn now_utc() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let rem = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}
fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    y += if m <= 2 { 1 } else { 0 };
    (y, m, d)
}
