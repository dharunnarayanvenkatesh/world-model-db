//! Durable, single-node storage for World Model DB.
//!
//! The V0 format is an append-friendly, versioned text snapshot. Raw observations
//! and other input records are persisted; resolved facts are rebuilt by the
//! resolution crate, keeping immutable input separate from materialized belief.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use wm_core::*;

pub const SCHEMA_VERSION: u32 = 1;
const STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("wm_state");
const STATE_KEY: &str = "canonical_snapshot";
const RECORDS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("wm_records");

#[derive(Clone, Debug)]
pub struct WorldState {
    pub schema_version: u32,
    pub entities: Vec<Entity>,
    pub sources: Vec<Source>,
    pub observations: Vec<Observation>,
    pub facts: Vec<Fact>,
    pub relationships: Vec<Relationship>,
    pub events: Vec<Event>,
    pub evidence: Vec<Evidence>,
    pub conflicts: Vec<Conflict>,
    pub correlations: Vec<Correlation>,
    pub counters: BTreeMap<String, u64>,
    indexes: WorldIndexes,
}

#[derive(Clone, Debug, Default)]
struct WorldIndexes {
    entities: BTreeMap<EntityId, usize>,
    sources: BTreeMap<SourceId, usize>,
    observations: BTreeMap<ObservationId, usize>,
    observations_by_entity: BTreeMap<EntityId, Vec<usize>>,
    facts: BTreeMap<FactId, usize>,
    facts_by_entity: BTreeMap<EntityId, Vec<usize>>,
    relationships_by_entity: BTreeMap<EntityId, Vec<usize>>,
    events_by_entity: BTreeMap<EntityId, Vec<usize>>,
    entity_count: usize,
    source_count: usize,
    observation_count: usize,
    fact_count: usize,
    relationship_count: usize,
    event_count: usize,
}

impl Default for WorldState {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            entities: Vec::new(),
            sources: Vec::new(),
            observations: Vec::new(),
            facts: Vec::new(),
            relationships: Vec::new(),
            events: Vec::new(),
            evidence: Vec::new(),
            conflicts: Vec::new(),
            correlations: Vec::new(),
            counters: BTreeMap::new(),
            indexes: WorldIndexes::default(),
        }
    }
}

impl WorldState {
    pub fn next_id(&mut self, kind: &str) -> String {
        let value = self.counters.entry(kind.to_owned()).or_insert(0);
        *value += 1;
        format!("{kind}:{}", *value)
    }

    pub fn current_facts(&self) -> impl Iterator<Item = &Fact> {
        self.facts
            .iter()
            .filter(|fact| fact.known_to.is_none() && fact.status == FactStatus::Supported)
    }

    pub fn current_relationships(&self) -> impl Iterator<Item = &Relationship> {
        self.relationships
            .iter()
            .filter(|rel| rel.known_to.is_none() && rel.status == FactStatus::Supported)
    }

    pub fn sync_indexes(&mut self) {
        if self.entities.len() < self.indexes.entity_count
            || self.sources.len() < self.indexes.source_count
            || self.observations.len() < self.indexes.observation_count
            || self.facts.len() < self.indexes.fact_count
            || self.relationships.len() < self.indexes.relationship_count
            || self.events.len() < self.indexes.event_count
        {
            self.indexes = WorldIndexes::default();
        }
        for (position, entity) in self
            .entities
            .iter()
            .enumerate()
            .skip(self.indexes.entity_count)
        {
            self.indexes.entities.insert(entity.id.clone(), position);
        }
        for (position, source) in self
            .sources
            .iter()
            .enumerate()
            .skip(self.indexes.source_count)
        {
            self.indexes.sources.insert(source.id.clone(), position);
        }
        for (position, observation) in self
            .observations
            .iter()
            .enumerate()
            .skip(self.indexes.observation_count)
        {
            self.indexes
                .observations
                .insert(observation.id.clone(), position);
            self.indexes
                .observations_by_entity
                .entry(observation.subject_entity_id.clone())
                .or_default()
                .push(position);
        }
        for (position, fact) in self.facts.iter().enumerate().skip(self.indexes.fact_count) {
            self.indexes.facts.insert(fact.id.clone(), position);
            self.indexes
                .facts_by_entity
                .entry(fact.subject_entity_id.clone())
                .or_default()
                .push(position);
        }
        for (position, relationship) in self
            .relationships
            .iter()
            .enumerate()
            .skip(self.indexes.relationship_count)
        {
            for entity in [
                &relationship.source_entity_id,
                &relationship.target_entity_id,
            ] {
                self.indexes
                    .relationships_by_entity
                    .entry(entity.clone())
                    .or_default()
                    .push(position);
            }
        }
        for (position, event) in self
            .events
            .iter()
            .enumerate()
            .skip(self.indexes.event_count)
        {
            for entity in &event.entities {
                self.indexes
                    .events_by_entity
                    .entry(entity.clone())
                    .or_default()
                    .push(position);
            }
        }
        self.indexes.entity_count = self.entities.len();
        self.indexes.source_count = self.sources.len();
        self.indexes.observation_count = self.observations.len();
        self.indexes.fact_count = self.facts.len();
        self.indexes.relationship_count = self.relationships.len();
        self.indexes.event_count = self.events.len();
    }

    pub fn entity(&self, id: &EntityId) -> Option<&Entity> {
        self.indexes_current()
            .then(|| self.indexes.entities.get(id))
            .flatten()
            .and_then(|position| self.entities.get(*position))
            .or_else(|| self.entities.iter().find(|entity| &entity.id == id))
    }

    pub fn source(&self, id: &SourceId) -> Option<&Source> {
        self.indexes_current()
            .then(|| self.indexes.sources.get(id))
            .flatten()
            .and_then(|position| self.sources.get(*position))
            .or_else(|| self.sources.iter().find(|source| &source.id == id))
    }

    pub fn observation(&self, id: &ObservationId) -> Option<&Observation> {
        self.indexes_current()
            .then(|| self.indexes.observations.get(id))
            .flatten()
            .and_then(|position| self.observations.get(*position))
            .or_else(|| {
                self.observations
                    .iter()
                    .find(|observation| &observation.id == id)
            })
    }

    pub fn fact(&self, id: &FactId) -> Option<&Fact> {
        self.indexes_current()
            .then(|| self.indexes.facts.get(id))
            .flatten()
            .and_then(|position| self.facts.get(*position))
            .or_else(|| self.facts.iter().find(|fact| &fact.id == id))
    }

    pub fn fact_positions_for_entity(&self, id: &EntityId) -> Option<&[usize]> {
        self.indexes_current()
            .then(|| self.indexes.facts_by_entity.get(id).map(Vec::as_slice))
            .flatten()
    }

    pub fn observation_positions_for_entity(&self, id: &EntityId) -> Option<&[usize]> {
        self.indexes_current()
            .then(|| {
                self.indexes
                    .observations_by_entity
                    .get(id)
                    .map(Vec::as_slice)
            })
            .flatten()
    }

    pub fn relationship_positions_for_entity(&self, id: &EntityId) -> Option<&[usize]> {
        self.indexes_current()
            .then(|| {
                self.indexes
                    .relationships_by_entity
                    .get(id)
                    .map(Vec::as_slice)
            })
            .flatten()
    }

    pub fn event_positions_for_entity(&self, id: &EntityId) -> Option<&[usize]> {
        self.indexes_current()
            .then(|| self.indexes.events_by_entity.get(id).map(Vec::as_slice))
            .flatten()
    }

    fn indexes_current(&self) -> bool {
        self.indexes.entity_count == self.entities.len()
            && self.indexes.source_count == self.sources.len()
            && self.indexes.observation_count == self.observations.len()
            && self.indexes.fact_count == self.facts.len()
            && self.indexes.relationship_count == self.relationships.len()
            && self.indexes.event_count == self.events.len()
    }
}

pub trait StorageBackend {
    fn load(&self) -> io::Result<WorldState>;
    fn store(&self, state: &WorldState) -> io::Result<()>;
}

#[derive(Clone)]
pub struct FileStore {
    pub path: PathBuf,
    pub state: WorldState,
    persisted_hashes: BTreeMap<String, u64>,
    database: Arc<Database>,
}

impl std::fmt::Debug for FileStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileStore")
            .field("path", &self.path)
            .field("state", &self.state)
            .field("persisted_records", &self.persisted_hashes.len())
            .finish_non_exhaustive()
    }
}

impl FileStore {
    pub fn init(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let database = shared_database(&path, true)?;
        let mut store = Self {
            path,
            state: WorldState::default(),
            persisted_hashes: BTreeMap::new(),
            database,
        };
        store.save()?;
        Ok(store)
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Self::init(path);
        }
        let database = shared_database(&path, false)?;
        let (state, persisted_hashes) = SnapshotBackend {
            database: Arc::clone(&database),
        }
        .load_records()?;
        Ok(Self {
            path,
            state,
            persisted_hashes,
            database,
        })
    }

    pub fn save(&mut self) -> io::Result<()> {
        self.state.sync_indexes();
        self.persisted_hashes = SnapshotBackend {
            database: Arc::clone(&self.database),
        }
        .store_records(&self.state, &self.persisted_hashes)?;
        Ok(())
    }
}

#[derive(Clone)]
struct SnapshotBackend {
    database: Arc<Database>,
}

impl StorageBackend for SnapshotBackend {
    fn load(&self) -> io::Result<WorldState> {
        self.load_records().map(|(state, _)| state)
    }

    fn store(&self, state: &WorldState) -> io::Result<()> {
        let hashes = self.load_records().map(|(_, hashes)| hashes)?;
        self.store_records(state, &hashes).map(|_| ())
    }
}

impl SnapshotBackend {
    fn load_records(&self) -> io::Result<(WorldState, BTreeMap<String, u64>)> {
        let transaction = self.database.begin_read().map_err(storage_error)?;
        match transaction.open_table(RECORDS_TABLE) {
            Ok(table) => {
                let mut lines = Vec::new();
                let mut hashes = BTreeMap::new();
                for entry in table.iter().map_err(storage_error)? {
                    let (key, value) = entry.map_err(storage_error)?;
                    let key = key.value().to_owned();
                    let value = value.value();
                    hashes.insert(key, stable_hash(value.as_bytes()));
                    lines.push(value.to_owned());
                }
                lines.sort_by(|left, right| compare_record_lines(left, right));
                let state = decode_snapshot(&lines.join("\n"))?;
                Ok((state, hashes))
            }
            Err(TableError::TableDoesNotExist(_)) => self.load_legacy_snapshot(&transaction),
            Err(error) => Err(storage_error(error)),
        }
    }

    fn load_legacy_snapshot(
        &self,
        transaction: &redb::ReadTransaction,
    ) -> io::Result<(WorldState, BTreeMap<String, u64>)> {
        let table = transaction.open_table(STATE_TABLE).map_err(storage_error)?;
        let value = table
            .get_owned(STATE_KEY)
            .map_err(storage_error)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "world state is missing"))?;
        let input = std::str::from_utf8(value.value())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "world state is not UTF-8"))?;
        Ok((decode_snapshot(input)?, BTreeMap::new()))
    }

    fn store_records(
        &self,
        state: &WorldState,
        previous_hashes: &BTreeMap<String, u64>,
    ) -> io::Result<BTreeMap<String, u64>> {
        let records = encode_records(state)?;
        let hashes = records
            .iter()
            .map(|(key, value)| (key.clone(), stable_hash(value.as_bytes())))
            .collect::<BTreeMap<_, _>>();
        let transaction = self.database.begin_write().map_err(storage_error)?;
        {
            let mut table = transaction
                .open_table(RECORDS_TABLE)
                .map_err(storage_error)?;
            for (key, value) in &records {
                if previous_hashes.get(key) != hashes.get(key) {
                    table
                        .insert(key.as_str(), value.as_str())
                        .map_err(storage_error)?;
                }
            }
            for key in previous_hashes.keys() {
                if !records.contains_key(key) {
                    table.remove(key.as_str()).map_err(storage_error)?;
                }
            }
        }
        transaction.commit().map_err(storage_error)?;
        Ok(hashes)
    }
}

fn shared_database(path: &Path, create: bool) -> io::Result<Arc<Database>> {
    static DATABASES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Database>>>> = OnceLock::new();
    let key = absolute_path(path)?;
    let databases = DATABASES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut databases = databases
        .lock()
        .map_err(|_| io::Error::other("database registry lock is poisoned"))?;
    if let Some(database) = databases.get(&key).and_then(Weak::upgrade) {
        return Ok(database);
    }
    databases.retain(|_, database| database.strong_count() > 0);
    let database = Arc::new(if create {
        Database::create(path).map_err(storage_error)?
    } else {
        Database::open(path).map_err(storage_error)?
    });
    databases.insert(key, Arc::downgrade(&database));
    Ok(database)
}

fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn encode_records(state: &WorldState) -> io::Result<BTreeMap<String, String>> {
    let mut records = BTreeMap::new();
    for line in encode_snapshot(state)
        .lines()
        .filter(|line| !line.is_empty())
    {
        let mut fields = line.split('\t');
        let kind = fields.next().unwrap_or_default();
        let identifier = fields.next().unwrap_or_default();
        let key = if kind == "WMDB" {
            "META".to_owned()
        } else {
            format!("{kind}:{identifier}")
        };
        if records.insert(key, line.to_owned()).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate durable record key",
            ));
        }
    }
    Ok(records)
}

fn stable_hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x1000_0000_01b3)
    })
}

fn compare_record_lines(left: &str, right: &str) -> Ordering {
    let (left_kind, left_id) = record_identity(left);
    let (right_kind, right_id) = record_identity(right);
    record_kind_rank(left_kind)
        .cmp(&record_kind_rank(right_kind))
        .then_with(|| natural_id_cmp(left_id, right_id))
}

fn record_identity(line: &str) -> (&str, &str) {
    let mut fields = line.split('\t');
    (
        fields.next().unwrap_or_default(),
        fields.next().unwrap_or_default(),
    )
}

fn record_kind_rank(kind: &str) -> u8 {
    match kind {
        "WMDB" => 0,
        "COUNTER" => 1,
        "ENTITY" => 2,
        "SOURCE" => 3,
        "OBSERVATION" => 4,
        "FACT" => 5,
        "RELATIONSHIP" => 6,
        "EVENT" => 7,
        "EVIDENCE" => 8,
        "CONFLICT" => 9,
        "CORRELATION" => 10,
        _ => u8::MAX,
    }
}

fn natural_id_cmp(left: &str, right: &str) -> Ordering {
    let left_parts = left.rsplit_once(':');
    let right_parts = right.rsplit_once(':');
    match (left_parts, right_parts) {
        (Some((left_prefix, left_number)), Some((right_prefix, right_number)))
            if left_prefix == right_prefix =>
        {
            match (left_number.parse::<u64>(), right_number.parse::<u64>()) {
                (Ok(left_number), Ok(right_number)) => left_number.cmp(&right_number),
                _ => left.cmp(right),
            }
        }
        _ => left.cmp(right),
    }
}

fn storage_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn encode_snapshot(state: &WorldState) -> String {
    let mut lines = vec![format!("WMDB\t{}", state.schema_version)];
    for (kind, count) in &state.counters {
        lines.push(record("COUNTER", &[kind, &count.to_string()]));
    }
    for e in &state.entities {
        lines.push(record(
            "ENTITY",
            &[
                &e.id.0,
                &e.entity_type,
                &e.canonical_name,
                &join(&e.aliases),
                &encode_map(&e.attributes),
                &e.created_at,
                e.retired_at.as_deref().unwrap_or(""),
            ],
        ));
    }
    for s in &state.sources {
        lines.push(record(
            "SOURCE",
            &[
                &s.id.0,
                &s.source_type,
                &s.uri,
                &s.name,
                &encode_map(&s.metadata),
                &s.priority.to_string(),
                &s.created_at,
            ],
        ));
    }
    for o in &state.observations {
        let (object_kind, object_value) = encode_object(&o.object);
        lines.push(record(
            "OBSERVATION",
            &[
                &o.id.0,
                &o.source_id.0,
                &o.subject_entity_id.0,
                &o.predicate,
                object_kind,
                &object_value,
                &o.observed_at,
                &o.ingested_at,
                &o.confidence.to_string(),
                &o.raw_payload,
                &encode_map(&o.metadata),
                if o.retracted { "1" } else { "0" },
            ],
        ));
    }
    for f in &state.facts {
        let (object_kind, object_value) = encode_object(&f.object);
        lines.push(record(
            "FACT",
            &[
                &f.id.0,
                &f.subject_entity_id.0,
                &f.predicate,
                object_kind,
                &object_value,
                &f.valid_from,
                f.valid_to.as_deref().unwrap_or(""),
                &f.known_from,
                f.known_to.as_deref().unwrap_or(""),
                &f.confidence.to_string(),
                fact_status(&f.status),
                &join_ids(&f.created_from_observations),
                &f.confidence_explanation,
                &f.resolution_rule,
                &f.created_at,
            ],
        ));
    }
    for r in &state.relationships {
        lines.push(record(
            "RELATIONSHIP",
            &[
                &r.id.0,
                &r.source_entity_id.0,
                &r.relationship_type,
                &r.target_entity_id.0,
                &r.valid_from,
                r.valid_to.as_deref().unwrap_or(""),
                &r.known_from,
                r.known_to.as_deref().unwrap_or(""),
                &r.confidence.to_string(),
                fact_status(&r.status),
                &join_evidence_ids(&r.evidence_ids),
                &r.resolution_rule,
                &r.created_at,
            ],
        ));
    }
    for e in &state.events {
        lines.push(record(
            "EVENT",
            &[
                &e.id.0,
                &e.event_type,
                &e.timestamp,
                e.end_timestamp.as_deref().unwrap_or(""),
                &join_entity_ids(&e.entities),
                &encode_map(&e.attributes),
                &join_ids(&e.source_observations),
                &e.confidence.to_string(),
            ],
        ));
    }
    for e in &state.evidence {
        lines.push(record(
            "EVIDENCE",
            &[
                &e.id.0,
                &e.derived_object_id,
                &e.observation_id.0,
                &e.source_id.0,
                evidence_role(&e.role),
                &e.created_at,
            ],
        ));
    }
    for c in &state.conflicts {
        lines.push(record(
            "CONFLICT",
            &[
                &c.id.0,
                &c.subject.0,
                &c.predicate,
                &join_fact_ids(&c.candidate_fact_ids),
                &c.detected_at,
                conflict_status(&c.resolution_status),
                c.resolution_reason.as_deref().unwrap_or(""),
                c.resolved_at.as_deref().unwrap_or(""),
            ],
        ));
    }
    for c in &state.correlations {
        lines.push(record(
            "CORRELATION",
            &[
                &c.id.0,
                &c.left_object_id,
                &c.right_object_id,
                &c.correlation_type,
                &c.score.to_string(),
                &join_ids(&c.evidence),
                &c.created_at,
            ],
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

fn decode_snapshot(input: &str) -> io::Result<WorldState> {
    let mut state = WorldState::default();
    for (line_number, line) in input.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let fields = line
            .split('\t')
            .map(unescape)
            .collect::<io::Result<Vec<_>>>()?;
        let bad = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid record at line {}", line_number + 1),
            )
        };
        match fields.first().map(String::as_str) {
            Some("WMDB") => {
                state.schema_version = field(&fields, 1)?.parse().map_err(|_| bad())?;
                if state.schema_version != SCHEMA_VERSION {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported schema version",
                    ));
                }
            }
            Some("COUNTER") => {
                state.counters.insert(
                    field(&fields, 1)?.to_owned(),
                    field(&fields, 2)?.parse().map_err(|_| bad())?,
                );
            }
            Some("ENTITY") => state.entities.push(Entity {
                id: EntityId(field(&fields, 1)?.to_owned()),
                entity_type: field(&fields, 2)?.to_owned(),
                canonical_name: field(&fields, 3)?.to_owned(),
                aliases: split(field(&fields, 4)?),
                attributes: decode_map(field(&fields, 5)?)?,
                created_at: field(&fields, 6)?.to_owned(),
                retired_at: optional(field(&fields, 7)?),
            }),
            Some("SOURCE") => state.sources.push(Source {
                id: SourceId(field(&fields, 1)?.to_owned()),
                source_type: field(&fields, 2)?.to_owned(),
                uri: field(&fields, 3)?.to_owned(),
                name: field(&fields, 4)?.to_owned(),
                metadata: decode_map(field(&fields, 5)?)?,
                priority: field(&fields, 6)?.parse().map_err(|_| bad())?,
                created_at: field(&fields, 7)?.to_owned(),
            }),
            Some("OBSERVATION") => state.observations.push(Observation {
                id: ObservationId(field(&fields, 1)?.to_owned()),
                source_id: SourceId(field(&fields, 2)?.to_owned()),
                subject_entity_id: EntityId(field(&fields, 3)?.to_owned()),
                predicate: field(&fields, 4)?.to_owned(),
                object: decode_object(field(&fields, 5)?, field(&fields, 6)?)?,
                observed_at: field(&fields, 7)?.to_owned(),
                ingested_at: field(&fields, 8)?.to_owned(),
                confidence: field(&fields, 9)?.parse().map_err(|_| bad())?,
                raw_payload: field(&fields, 10)?.to_owned(),
                metadata: decode_map(field(&fields, 11)?)?,
                retracted: field(&fields, 12)? == "1",
            }),
            Some("FACT") => state.facts.push(Fact {
                id: FactId(field(&fields, 1)?.to_owned()),
                subject_entity_id: EntityId(field(&fields, 2)?.to_owned()),
                predicate: field(&fields, 3)?.to_owned(),
                object: decode_object(field(&fields, 4)?, field(&fields, 5)?)?,
                valid_from: field(&fields, 6)?.to_owned(),
                valid_to: optional(field(&fields, 7)?),
                known_from: field(&fields, 8)?.to_owned(),
                known_to: optional(field(&fields, 9)?),
                confidence: field(&fields, 10)?.parse().map_err(|_| bad())?,
                status: parse_fact_status(field(&fields, 11)?)?,
                created_from_observations: split(field(&fields, 12)?)
                    .into_iter()
                    .map(ObservationId)
                    .collect(),
                confidence_explanation: field(&fields, 13)?.to_owned(),
                resolution_rule: field(&fields, 14)?.to_owned(),
                created_at: field(&fields, 15)?.to_owned(),
            }),
            Some("RELATIONSHIP") => state.relationships.push(Relationship {
                id: RelationshipId(field(&fields, 1)?.to_owned()),
                source_entity_id: EntityId(field(&fields, 2)?.to_owned()),
                relationship_type: field(&fields, 3)?.to_owned(),
                target_entity_id: EntityId(field(&fields, 4)?.to_owned()),
                valid_from: field(&fields, 5)?.to_owned(),
                valid_to: optional(field(&fields, 6)?),
                known_from: field(&fields, 7)?.to_owned(),
                known_to: optional(field(&fields, 8)?),
                confidence: field(&fields, 9)?.parse().map_err(|_| bad())?,
                status: parse_fact_status(field(&fields, 10)?)?,
                evidence_ids: split(field(&fields, 11)?)
                    .into_iter()
                    .map(EvidenceId)
                    .collect(),
                resolution_rule: field(&fields, 12)?.to_owned(),
                created_at: field(&fields, 13)?.to_owned(),
            }),
            Some("EVENT") => state.events.push(Event {
                id: EventId(field(&fields, 1)?.to_owned()),
                event_type: field(&fields, 2)?.to_owned(),
                timestamp: field(&fields, 3)?.to_owned(),
                end_timestamp: optional(field(&fields, 4)?),
                entities: split(field(&fields, 5)?)
                    .into_iter()
                    .map(EntityId)
                    .collect(),
                attributes: decode_map(field(&fields, 6)?)?,
                source_observations: split(field(&fields, 7)?)
                    .into_iter()
                    .map(ObservationId)
                    .collect(),
                confidence: field(&fields, 8)?.parse().map_err(|_| bad())?,
            }),
            Some("EVIDENCE") => state.evidence.push(Evidence {
                id: EvidenceId(field(&fields, 1)?.to_owned()),
                derived_object_id: field(&fields, 2)?.to_owned(),
                observation_id: ObservationId(field(&fields, 3)?.to_owned()),
                source_id: SourceId(field(&fields, 4)?.to_owned()),
                role: parse_evidence_role(field(&fields, 5)?)?,
                created_at: field(&fields, 6)?.to_owned(),
            }),
            Some("CONFLICT") => state.conflicts.push(Conflict {
                id: ConflictId(field(&fields, 1)?.to_owned()),
                subject: EntityId(field(&fields, 2)?.to_owned()),
                predicate: field(&fields, 3)?.to_owned(),
                candidate_fact_ids: split(field(&fields, 4)?).into_iter().map(FactId).collect(),
                detected_at: field(&fields, 5)?.to_owned(),
                resolution_status: parse_conflict_status(field(&fields, 6)?)?,
                resolution_reason: optional(field(&fields, 7)?),
                resolved_at: optional(field(&fields, 8)?),
            }),
            Some("CORRELATION") => state.correlations.push(Correlation {
                id: CorrelationId(field(&fields, 1)?.to_owned()),
                left_object_id: field(&fields, 2)?.to_owned(),
                right_object_id: field(&fields, 3)?.to_owned(),
                correlation_type: field(&fields, 4)?.to_owned(),
                score: field(&fields, 5)?.parse().map_err(|_| bad())?,
                evidence: split(field(&fields, 6)?)
                    .into_iter()
                    .map(ObservationId)
                    .collect(),
                created_at: field(&fields, 7)?.to_owned(),
            }),
            _ => return Err(bad()),
        }
    }
    state.sync_indexes();
    Ok(state)
}

fn record(kind: &str, fields: &[&str]) -> String {
    std::iter::once(kind.to_owned())
        .chain(fields.iter().map(|v| escape(v)))
        .collect::<Vec<_>>()
        .join("\t")
}

fn escape(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b" -_.:/{}[]\"".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn unescape(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad escape"));
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .map_err(|_| io::ErrorKind::InvalidData)?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| io::ErrorKind::InvalidData)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8"))
}

fn field(fields: &[String], index: usize) -> io::Result<&str> {
    fields
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing field"))
}
fn optional(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}
fn join(values: &[String]) -> String {
    values
        .iter()
        .map(|v| escape(v))
        .collect::<Vec<_>>()
        .join("|")
}
fn split(value: &str) -> Vec<String> {
    if value.is_empty() {
        Vec::new()
    } else {
        value
            .split('|')
            .map(unescape)
            .collect::<Result<_, _>>()
            .unwrap_or_default()
    }
}
fn join_ids(values: &[ObservationId]) -> String {
    join(&values.iter().map(|v| v.0.clone()).collect::<Vec<_>>())
}
fn join_evidence_ids(values: &[EvidenceId]) -> String {
    join(&values.iter().map(|v| v.0.clone()).collect::<Vec<_>>())
}
fn join_fact_ids(values: &[FactId]) -> String {
    join(&values.iter().map(|v| v.0.clone()).collect::<Vec<_>>())
}
fn join_entity_ids(values: &[EntityId]) -> String {
    join(&values.iter().map(|v| v.0.clone()).collect::<Vec<_>>())
}

fn encode_map(values: &BTreeMap<String, ObjectValue>) -> String {
    values
        .iter()
        .map(|(key, value)| {
            let (kind, raw) = encode_object(value);
            format!("{}~{}~{}", escape(key), kind, escape(&raw))
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn decode_map(value: &str) -> io::Result<BTreeMap<String, ObjectValue>> {
    let mut result = BTreeMap::new();
    if value.is_empty() {
        return Ok(result);
    }
    for item in value.split('|') {
        let mut parts = item.splitn(3, '~');
        let key = unescape(parts.next().unwrap_or_default())?;
        let kind = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "map kind missing"))?;
        let raw = unescape(
            parts
                .next()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "map value missing"))?,
        )?;
        result.insert(key, decode_object(kind, &raw)?);
    }
    Ok(result)
}

fn encode_object(value: &ObjectValue) -> (&'static str, String) {
    match value {
        ObjectValue::Entity(v) => ("entity", v.0.clone()),
        ObjectValue::String(v) => ("string", v.clone()),
        ObjectValue::Integer(v) => ("integer", v.to_string()),
        ObjectValue::Float(v) => ("float", v.to_string()),
        ObjectValue::Boolean(v) => ("boolean", v.to_string()),
        ObjectValue::Timestamp(v) => ("timestamp", v.clone()),
        ObjectValue::Json(v) => ("json", v.clone()),
    }
}
fn decode_object(kind: &str, value: &str) -> io::Result<ObjectValue> {
    Ok(match kind {
        "entity" => ObjectValue::Entity(EntityId(value.to_owned())),
        "string" => ObjectValue::String(value.to_owned()),
        "integer" => ObjectValue::Integer(value.parse().map_err(|_| io::ErrorKind::InvalidData)?),
        "float" => ObjectValue::Float(value.parse().map_err(|_| io::ErrorKind::InvalidData)?),
        "boolean" => ObjectValue::Boolean(value.parse().map_err(|_| io::ErrorKind::InvalidData)?),
        "timestamp" => ObjectValue::Timestamp(value.to_owned()),
        "json" => ObjectValue::Json(value.to_owned()),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown object kind",
            ));
        }
    })
}
fn fact_status(status: &FactStatus) -> &'static str {
    match status {
        FactStatus::Supported => "SUPPORTED",
        FactStatus::Contested => "CONTESTED",
        FactStatus::Superseded => "SUPERSEDED",
        FactStatus::Unresolved => "UNRESOLVED",
        FactStatus::Retracted => "RETRACTED",
    }
}
fn parse_fact_status(value: &str) -> io::Result<FactStatus> {
    match value {
        "SUPPORTED" => Ok(FactStatus::Supported),
        "CONTESTED" => Ok(FactStatus::Contested),
        "SUPERSEDED" => Ok(FactStatus::Superseded),
        "UNRESOLVED" => Ok(FactStatus::Unresolved),
        "RETRACTED" => Ok(FactStatus::Retracted),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown fact status",
        )),
    }
}
fn evidence_role(value: &EvidenceRole) -> &'static str {
    match value {
        EvidenceRole::Support => "SUPPORT",
        EvidenceRole::Conflict => "CONFLICT",
        EvidenceRole::Retraction => "RETRACTION",
    }
}
fn parse_evidence_role(value: &str) -> io::Result<EvidenceRole> {
    match value {
        "SUPPORT" => Ok(EvidenceRole::Support),
        "CONFLICT" => Ok(EvidenceRole::Conflict),
        "RETRACTION" => Ok(EvidenceRole::Retraction),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown evidence role",
        )),
    }
}
fn conflict_status(value: &ConflictResolutionStatus) -> &'static str {
    match value {
        ConflictResolutionStatus::Open => "OPEN",
        ConflictResolutionStatus::Resolved => "RESOLVED",
        ConflictResolutionStatus::ManualReview => "MANUAL_REVIEW",
        ConflictResolutionStatus::Superseded => "SUPERSEDED",
    }
}
fn parse_conflict_status(value: &str) -> io::Result<ConflictResolutionStatus> {
    match value {
        "OPEN" => Ok(ConflictResolutionStatus::Open),
        "RESOLVED" => Ok(ConflictResolutionStatus::Resolved),
        "MANUAL_REVIEW" => Ok(ConflictResolutionStatus::ManualReview),
        "SUPERSEDED" => Ok(ConflictResolutionStatus::Superseded),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown conflict status",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wmdb-{name}-{}-{nonce}.redb", std::process::id()))
    }

    #[test]
    fn escaping_round_trips_unicode_and_delimiters() {
        let value = "Acme\tNova|Chennai\n雪";
        assert_eq!(unescape(&escape(value)).unwrap(), value);
    }

    #[test]
    fn ids_are_deterministic() {
        let mut state = WorldState::default();
        assert_eq!(state.next_id("fact"), "fact:1");
        assert_eq!(state.next_id("fact"), "fact:2");
    }

    #[test]
    fn durable_records_restore_semantic_and_natural_id_order() {
        assert_eq!(
            compare_record_lines("FACT\tfact:2", "FACT\tfact:10"),
            Ordering::Less
        );
        assert_eq!(
            compare_record_lines("ENTITY\torg:z", "FACT\tfact:1"),
            Ordering::Less
        );
    }

    #[test]
    fn indexes_support_point_and_entity_lookups() {
        let mut state = WorldState::default();
        let entity_id = EntityId("org:acme".into());
        state.entities.push(Entity {
            id: entity_id.clone(),
            entity_type: "organization".into(),
            canonical_name: "Acme".into(),
            aliases: Vec::new(),
            attributes: BTreeMap::new(),
            created_at: "2026-01-01T00:00:00Z".into(),
            retired_at: None,
        });
        assert_eq!(state.entity(&entity_id).unwrap().canonical_name, "Acme");
        state.sync_indexes();
        assert_eq!(state.entity(&entity_id).unwrap().canonical_name, "Acme");
        assert_eq!(state.fact_positions_for_entity(&entity_id), None);
    }

    #[test]
    fn legacy_snapshot_is_read_and_migrated_on_save() {
        let path = test_path("legacy-migration");
        let mut state = WorldState::default();
        state.counters.insert("fact".into(), 7);
        {
            let database = Database::create(&path).unwrap();
            let transaction = database.begin_write().unwrap();
            {
                let mut table = transaction.open_table(STATE_TABLE).unwrap();
                let snapshot = encode_snapshot(&state);
                table.insert(STATE_KEY, snapshot.as_bytes()).unwrap();
            }
            transaction.commit().unwrap();
        }
        {
            let mut store = FileStore::open(&path).unwrap();
            assert_eq!(store.state.counters.get("fact"), Some(&7));
            store.save().unwrap();
        }
        {
            let store = FileStore::open(&path).unwrap();
            assert_eq!(store.state.counters.get("fact"), Some(&7));
            assert!(!store.persisted_hashes.is_empty());
        }
        fs::remove_file(path).unwrap();
    }
}
