//! Domain-neutral digital-twin operations built on World Model DB primitives.
//!
//! A twin is an entity whose reported, desired, configured, and derived state
//! is represented by immutable observations and resolved bitemporal facts.
//! This keeps telemetry, commands, topology, provenance, and historical state
//! in one evidence model without assuming an industrial sector or protocol.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use wm_core::{
    Entity, EntityId, Event, EventId, ObjectValue, ObservationId, PredicateCardinality,
    RelationshipId, SourceId,
};
use wm_resolution::{Engine, NewObservation, ResolutionEngine, ResolutionError};

pub const TWIN_ENTITY_TYPE: &str = "digital_twin";
pub const REPORTED_PREFIX: &str = "twin.reported.";
pub const DESIRED_PREFIX: &str = "twin.desired.";
pub const CONFIGURATION_PREFIX: &str = "twin.configuration.";
pub const DERIVED_PREFIX: &str = "twin.derived.";
const COMMAND_REQUESTED: &str = "twin.command.requested";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TwinChannel {
    Reported,
    Desired,
    Configuration,
    Derived,
}

impl TwinChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reported => "reported",
            Self::Desired => "desired",
            Self::Configuration => "configuration",
            Self::Derived => "derived",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Reported => REPORTED_PREFIX,
            Self::Desired => DESIRED_PREFIX,
            Self::Configuration => CONFIGURATION_PREFIX,
            Self::Derived => DERIVED_PREFIX,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwinDefinition {
    /// Domain classification such as `manufacturing.machine`, `building.room`,
    /// `vehicle`, `patient`, `network.service`, or an application-defined URI.
    pub twin_kind: String,
    pub name: String,
    pub aliases: Vec<String>,
    /// Identifier for an external DTDL, AAS, NGSI-LD, or application model.
    pub model_id: Option<String>,
    pub schema_version: Option<String>,
    pub capabilities: Vec<String>,
    pub external_ids: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, ObjectValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwinSignalWrite {
    pub signal: String,
    pub value: ObjectValue,
    pub observed_at: String,
    pub ingested_at: Option<String>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub confidence: f64,
    pub cardinality: PredicateCardinality,
    pub unit: Option<String>,
    pub quality: Option<String>,
    pub sequence: Option<String>,
    pub raw_payload: String,
}

impl TwinSignalWrite {
    pub fn reported(
        signal: impl Into<String>,
        value: ObjectValue,
        observed_at: impl Into<String>,
    ) -> Self {
        Self {
            signal: signal.into(),
            value,
            observed_at: observed_at.into(),
            ingested_at: None,
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            cardinality: PredicateCardinality::SingleExclusive,
            unit: None,
            quality: None,
            sequence: None,
            raw_payload: String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwinLink {
    pub relationship_type: String,
    pub target_twin_id: EntityId,
    pub valid_from: String,
    pub valid_to: Option<String>,
    pub confidence: f64,
    pub evidence: Vec<ObservationId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwinCommandRequest {
    pub command_type: String,
    pub requested_by: String,
    pub requested_at: String,
    pub expires_at: Option<String>,
    pub idempotency_key: String,
    pub parameters: BTreeMap<String, ObjectValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TwinCommandReceipt {
    pub command_id: EventId,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandStatus {
    Accepted,
    Running,
    Succeeded,
    Failed,
    Rejected,
    Cancelled,
}

impl CommandStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwinDrift {
    pub signal: String,
    pub desired: Vec<ObjectValue>,
    pub reported: Vec<ObjectValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TwinSnapshot {
    pub twin_id: EntityId,
    pub valid_at: Option<String>,
    pub known_at: Option<String>,
    pub reported: BTreeMap<String, Vec<ObjectValue>>,
    pub desired: BTreeMap<String, Vec<ObjectValue>>,
    pub configuration: BTreeMap<String, Vec<ObjectValue>>,
    pub derived: BTreeMap<String, Vec<ObjectValue>>,
    pub drift: Vec<TwinDrift>,
    pub open_conflicts: usize,
}

#[derive(Debug)]
pub enum TwinError {
    Invalid(String),
    NotFound(String),
    Engine(ResolutionError),
}

impl fmt::Display for TwinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::NotFound(message) => formatter.write_str(message),
            Self::Engine(error) => error.fmt(formatter),
        }
    }
}

impl Error for TwinError {}

impl From<ResolutionError> for TwinError {
    fn from(value: ResolutionError) -> Self {
        match value {
            ResolutionError::Invalid(message) => Self::Invalid(message),
            ResolutionError::NotFound(message) => Self::NotFound(message),
            other => Self::Engine(other),
        }
    }
}

pub struct TwinGateway<'a> {
    engine: &'a mut Engine,
}

impl<'a> TwinGateway<'a> {
    pub fn new(engine: &'a mut Engine) -> Self {
        Self { engine }
    }

    pub fn register(&mut self, definition: TwinDefinition) -> Result<EntityId, TwinError> {
        validate_name("twin_kind", &definition.twin_kind)?;
        validate_name("name", &definition.name)?;
        let expected_kind = definition.twin_kind.clone();
        let expected_model = definition.model_id.clone();
        let mut attributes = definition.metadata;
        attributes.insert(
            "twin.kind".into(),
            ObjectValue::String(definition.twin_kind),
        );
        if let Some(model_id) = definition.model_id {
            attributes.insert("twin.model_id".into(), ObjectValue::String(model_id));
        }
        if let Some(version) = definition.schema_version {
            attributes.insert("twin.schema_version".into(), ObjectValue::String(version));
        }
        attributes.insert(
            "twin.capabilities".into(),
            ObjectValue::Json(json_string_array(&definition.capabilities)),
        );
        for (namespace, value) in definition.external_ids {
            validate_name("external ID namespace", &namespace)?;
            attributes.insert(
                format!("twin.external.{namespace}"),
                ObjectValue::String(value),
            );
        }
        let id = self
            .engine
            .create_entity(
                TWIN_ENTITY_TYPE,
                definition.name,
                definition.aliases,
                attributes,
            )
            .map_err(TwinError::from)?;
        let entity = self
            .engine
            .store
            .state
            .entity(&id)
            .expect("created twin must exist");
        let kind_matches = matches!(
            entity.attributes.get("twin.kind"),
            Some(ObjectValue::String(kind)) if kind == &expected_kind
        );
        let model_matches = match (&expected_model, entity.attributes.get("twin.model_id")) {
            (None, None) => true,
            (Some(expected), Some(ObjectValue::String(actual))) => expected == actual,
            _ => false,
        };
        if !kind_matches || !model_matches {
            return Err(TwinError::Invalid(format!(
                "digital twin '{}' already exists with a different kind or model",
                entity.canonical_name
            )));
        }
        Ok(id)
    }

    pub fn ensure_adapter_source(
        &mut self,
        adapter_id: &str,
        protocol: &str,
        priority: i32,
    ) -> Result<SourceId, TwinError> {
        validate_name("adapter_id", adapter_id)?;
        validate_name("protocol", protocol)?;
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "twin.adapter_id".into(),
            ObjectValue::String(adapter_id.into()),
        );
        metadata.insert("twin.protocol".into(), ObjectValue::String(protocol.into()));
        self.engine
            .create_source(
                "digital_twin_adapter",
                format!("twin-adapter://{adapter_id}"),
                format!("Twin adapter {adapter_id}"),
                priority,
                metadata,
            )
            .map_err(Into::into)
    }

    pub fn write_signal(
        &mut self,
        twin_id: &EntityId,
        source_id: SourceId,
        channel: TwinChannel,
        write: TwinSignalWrite,
    ) -> Result<ObservationId, TwinError> {
        self.ensure_twin(twin_id)?;
        validate_signal(&write.signal)?;
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "twin.channel".into(),
            ObjectValue::String(channel.as_str().into()),
        );
        if let Some(unit) = write.unit {
            metadata.insert("twin.unit".into(), ObjectValue::String(unit));
        }
        if let Some(quality) = write.quality {
            metadata.insert("twin.quality".into(), ObjectValue::String(quality));
        }
        if let Some(sequence) = write.sequence {
            metadata.insert("twin.sequence".into(), ObjectValue::String(sequence));
        }
        self.engine
            .observe(NewObservation {
                source_id,
                subject_entity_id: twin_id.clone(),
                predicate: format!("{}{}", channel.prefix(), write.signal),
                object: write.value,
                observed_at: write.observed_at,
                ingested_at: write.ingested_at,
                claimed_valid_from: write.valid_from,
                claimed_valid_to: write.valid_to,
                cardinality: write.cardinality,
                confidence: write.confidence,
                raw_payload: write.raw_payload,
                metadata,
                retracted: false,
            })
            .map_err(Into::into)
    }

    pub fn report(
        &mut self,
        twin_id: &EntityId,
        source_id: SourceId,
        write: TwinSignalWrite,
    ) -> Result<ObservationId, TwinError> {
        self.write_signal(twin_id, source_id, TwinChannel::Reported, write)
    }

    pub fn set_desired(
        &mut self,
        twin_id: &EntityId,
        source_id: SourceId,
        write: TwinSignalWrite,
    ) -> Result<ObservationId, TwinError> {
        self.write_signal(twin_id, source_id, TwinChannel::Desired, write)
    }

    pub fn configure(
        &mut self,
        twin_id: &EntityId,
        source_id: SourceId,
        write: TwinSignalWrite,
    ) -> Result<ObservationId, TwinError> {
        self.write_signal(twin_id, source_id, TwinChannel::Configuration, write)
    }

    pub fn connect(
        &mut self,
        twin_id: &EntityId,
        link: TwinLink,
    ) -> Result<RelationshipId, TwinError> {
        self.ensure_twin(twin_id)?;
        self.ensure_twin(&link.target_twin_id)?;
        validate_name("relationship_type", &link.relationship_type)?;
        let relationship_type = if link.relationship_type.starts_with("twin.") {
            link.relationship_type
        } else {
            format!("twin.{}", link.relationship_type)
        };
        self.engine
            .add_relationship(
                twin_id.clone(),
                relationship_type,
                link.target_twin_id,
                link.valid_from,
                link.valid_to,
                link.confidence,
                link.evidence,
            )
            .map_err(Into::into)
    }

    pub fn snapshot(
        &self,
        twin_id: &EntityId,
        valid_at: Option<&str>,
        known_at: Option<&str>,
    ) -> Result<TwinSnapshot, TwinError> {
        self.ensure_twin(twin_id)?;
        let mut snapshot = TwinSnapshot {
            twin_id: twin_id.clone(),
            valid_at: valid_at.map(str::to_owned),
            known_at: known_at.map(str::to_owned),
            reported: BTreeMap::new(),
            desired: BTreeMap::new(),
            configuration: BTreeMap::new(),
            derived: BTreeMap::new(),
            drift: Vec::new(),
            open_conflicts: self
                .engine
                .store
                .state
                .conflicts
                .iter()
                .filter(|conflict| &conflict.subject == twin_id && conflict.resolved_at.is_none())
                .count(),
        };
        for fact in wm_query::entity_state(&self.engine.store.state, twin_id, valid_at, known_at) {
            if let Some(signal) = fact.predicate.strip_prefix(REPORTED_PREFIX) {
                snapshot
                    .reported
                    .entry(signal.into())
                    .or_default()
                    .push(fact.object.clone());
            } else if let Some(signal) = fact.predicate.strip_prefix(DESIRED_PREFIX) {
                snapshot
                    .desired
                    .entry(signal.into())
                    .or_default()
                    .push(fact.object.clone());
            } else if let Some(signal) = fact.predicate.strip_prefix(CONFIGURATION_PREFIX) {
                snapshot
                    .configuration
                    .entry(signal.into())
                    .or_default()
                    .push(fact.object.clone());
            } else if let Some(signal) = fact.predicate.strip_prefix(DERIVED_PREFIX) {
                snapshot
                    .derived
                    .entry(signal.into())
                    .or_default()
                    .push(fact.object.clone());
            }
        }
        for (signal, desired) in &snapshot.desired {
            let reported = snapshot.reported.get(signal).cloned().unwrap_or_default();
            if &reported != desired {
                snapshot.drift.push(TwinDrift {
                    signal: signal.clone(),
                    desired: desired.clone(),
                    reported,
                });
            }
        }
        Ok(snapshot)
    }

    pub fn request_command(
        &mut self,
        twin_id: &EntityId,
        request: TwinCommandRequest,
    ) -> Result<TwinCommandReceipt, TwinError> {
        self.ensure_twin(twin_id)?;
        validate_name("command_type", &request.command_type)?;
        validate_name("requested_by", &request.requested_by)?;
        validate_name("idempotency_key", &request.idempotency_key)?;
        if let Some(existing) = self.engine.store.state.events.iter().find(|event| {
            event.event_type == COMMAND_REQUESTED
                && event.entities.contains(twin_id)
                && string_attribute(event, "twin.idempotency_key")
                    == Some(request.idempotency_key.as_str())
        }) {
            let same_parameters = request
                .parameters
                .iter()
                .all(|(key, value)| existing.attributes.get(key) == Some(value))
                && existing.attributes.len() == request.parameters.len() + 3;
            let same = string_attribute(existing, "twin.command_type")
                == Some(request.command_type.as_str())
                && string_attribute(existing, "twin.requested_by")
                    == Some(request.requested_by.as_str())
                && same_parameters;
            if !same {
                return Err(TwinError::Invalid(format!(
                    "idempotency key '{}' was already used for a different command",
                    request.idempotency_key
                )));
            }
            return Ok(TwinCommandReceipt {
                command_id: existing.id.clone(),
                replayed: true,
            });
        }
        let mut attributes = request.parameters;
        attributes.insert(
            "twin.command_type".into(),
            ObjectValue::String(request.command_type),
        );
        attributes.insert(
            "twin.requested_by".into(),
            ObjectValue::String(request.requested_by),
        );
        attributes.insert(
            "twin.idempotency_key".into(),
            ObjectValue::String(request.idempotency_key),
        );
        let command_id = self.engine.add_event(
            COMMAND_REQUESTED,
            request.requested_at,
            request.expires_at,
            vec![twin_id.clone()],
            attributes,
            Vec::new(),
            1.0,
        )?;
        Ok(TwinCommandReceipt {
            command_id,
            replayed: false,
        })
    }

    pub fn acknowledge_command(
        &mut self,
        twin_id: &EntityId,
        command_id: &EventId,
        status: CommandStatus,
        at: impl Into<String>,
        message: Option<String>,
    ) -> Result<EventId, TwinError> {
        self.ensure_twin(twin_id)?;
        let command_exists = self.engine.store.state.events.iter().any(|event| {
            &event.id == command_id
                && event.event_type == COMMAND_REQUESTED
                && event.entities.contains(twin_id)
        });
        if !command_exists {
            return Err(TwinError::NotFound(format!(
                "command {command_id} not found for twin {twin_id}"
            )));
        }
        let event_type = format!("twin.command.{}", status.as_str());
        if let Some(existing) = self.engine.store.state.events.iter().find(|event| {
            event.event_type == event_type
                && event.entities.contains(twin_id)
                && string_attribute(event, "twin.command_id") == Some(command_id.as_str())
        }) {
            return Ok(existing.id.clone());
        }
        let mut attributes = BTreeMap::new();
        attributes.insert(
            "twin.command_id".into(),
            ObjectValue::String(command_id.to_string()),
        );
        attributes.insert(
            "twin.status".into(),
            ObjectValue::String(status.as_str().into()),
        );
        if let Some(message) = message {
            attributes.insert("twin.message".into(), ObjectValue::String(message));
        }
        self.engine
            .add_event(
                event_type,
                at,
                None,
                vec![twin_id.clone()],
                attributes,
                Vec::new(),
                1.0,
            )
            .map_err(Into::into)
    }

    pub fn commands(&self, twin_id: &EntityId) -> Result<Vec<Event>, TwinError> {
        self.ensure_twin(twin_id)?;
        Ok(self
            .engine
            .store
            .state
            .events
            .iter()
            .filter(|event| {
                event.entities.contains(twin_id) && event.event_type.starts_with("twin.command.")
            })
            .cloned()
            .collect())
    }

    pub fn twins(&self) -> Vec<&Entity> {
        self.engine
            .store
            .state
            .entities
            .iter()
            .filter(|entity| entity.entity_type == TWIN_ENTITY_TYPE)
            .collect()
    }

    fn ensure_twin(&self, twin_id: &EntityId) -> Result<&Entity, TwinError> {
        self.engine
            .store
            .state
            .entities
            .iter()
            .find(|entity| &entity.id == twin_id && entity.entity_type == TWIN_ENTITY_TYPE)
            .ok_or_else(|| TwinError::NotFound(format!("digital twin {twin_id} not found")))
    }
}

fn validate_name(label: &str, value: &str) -> Result<(), TwinError> {
    if value.trim().is_empty() {
        return Err(TwinError::Invalid(format!("{label} must not be empty")));
    }
    if value.len() > 512 || value.chars().any(char::is_control) {
        return Err(TwinError::Invalid(format!(
            "{label} is not a valid identifier"
        )));
    }
    Ok(())
}

fn validate_signal(signal: &str) -> Result<(), TwinError> {
    validate_name("signal", signal)?;
    if signal.starts_with("twin.") {
        return Err(TwinError::Invalid(
            "signal must not include the reserved 'twin.' prefix".into(),
        ));
    }
    Ok(())
}

fn string_attribute<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
    match event.attributes.get(key) {
        Some(ObjectValue::String(value)) => Some(value),
        _ => None,
    }
}

fn json_string_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", escape_json(value)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

pub fn snapshot_json(snapshot: &TwinSnapshot) -> String {
    format!(
        "{{\"twin_id\":\"{}\",\"valid_at\":{},\"known_at\":{},\"reported\":{},\"desired\":{},\"configuration\":{},\"derived\":{},\"drift\":[{}],\"open_conflicts\":{}}}",
        escape_json(snapshot.twin_id.as_str()),
        json_option(snapshot.valid_at.as_deref()),
        json_option(snapshot.known_at.as_deref()),
        state_map_json(&snapshot.reported),
        state_map_json(&snapshot.desired),
        state_map_json(&snapshot.configuration),
        state_map_json(&snapshot.derived),
        snapshot
            .drift
            .iter()
            .map(|drift| format!(
                "{{\"signal\":\"{}\",\"desired\":{},\"reported\":{}}}",
                escape_json(&drift.signal),
                values_json(&drift.desired),
                values_json(&drift.reported)
            ))
            .collect::<Vec<_>>()
            .join(","),
        snapshot.open_conflicts
    )
}

pub fn events_json(events: &[Event]) -> String {
    format!(
        "[{}]",
        events
            .iter()
            .map(|event| format!(
                "{{\"event_id\":\"{}\",\"event_type\":\"{}\",\"timestamp\":\"{}\",\"end_timestamp\":{},\"attributes\":{}}}",
                escape_json(event.id.as_str()),
                escape_json(&event.event_type),
                escape_json(&event.timestamp),
                json_option(event.end_timestamp.as_deref()),
                object_map_json(&event.attributes)
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub fn twins_json(twins: &[&Entity]) -> String {
    format!(
        "[{}]",
        twins
            .iter()
            .map(|entity| twin_json(entity))
            .collect::<Vec<_>>()
            .join(",")
    )
}

pub fn twin_json(entity: &Entity) -> String {
    format!(
        "{{\"twin_id\":\"{}\",\"name\":\"{}\",\"kind\":{},\"model_id\":{},\"schema_version\":{},\"capabilities\":{},\"created_at\":\"{}\",\"retired_at\":{}}}",
        escape_json(entity.id.as_str()),
        escape_json(&entity.canonical_name),
        entity_attribute_json(entity, "twin.kind"),
        entity_attribute_json(entity, "twin.model_id"),
        entity_attribute_json(entity, "twin.schema_version"),
        entity_attribute_json(entity, "twin.capabilities"),
        escape_json(&entity.created_at),
        json_option(entity.retired_at.as_deref())
    )
}

fn entity_attribute_json(entity: &Entity, key: &str) -> String {
    match entity.attributes.get(key) {
        Some(ObjectValue::Json(value)) => value.clone(),
        Some(value) => wm_query::object_json(value),
        None => "null".into(),
    }
}

fn state_map_json(values: &BTreeMap<String, Vec<ObjectValue>>) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(|(key, values)| format!("\"{}\":{}", escape_json(key), values_json(values)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn values_json(values: &[ObjectValue]) -> String {
    if values.len() == 1 {
        wm_query::object_json(&values[0])
    } else {
        format!(
            "[{}]",
            values
                .iter()
                .map(wm_query::object_json)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

fn object_map_json(values: &BTreeMap<String, ObjectValue>) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(|(key, value)| format!(
                "\"{}\":{}",
                escape_json(key),
                wm_query::object_json(value)
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn json_option(value: Option<&str>) -> String {
    value
        .map(|value| format!("\"{}\"", escape_json(value)))
        .unwrap_or_else(|| "null".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wm-twin-{name}-{nonce}.redb"))
    }

    fn definition(name: &str, kind: &str) -> TwinDefinition {
        TwinDefinition {
            twin_kind: kind.into(),
            name: name.into(),
            aliases: Vec::new(),
            model_id: Some(format!("model:{kind}")),
            schema_version: Some("1".into()),
            capabilities: vec!["telemetry".into(), "commands".into()],
            external_ids: BTreeMap::new(),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn models_different_domains_with_shared_twin_semantics() {
        let database = path("domains");
        {
            let mut engine = Engine::init(&database).unwrap();
            let mut twins = TwinGateway::new(&mut engine);
            let machine = twins
                .register(definition("Press 7", "manufacturing.machine"))
                .unwrap();
            let service = twins
                .register(definition("Payments API", "software.service"))
                .unwrap();
            let source = twins
                .ensure_adapter_source("opcua-line-1", "opcua", 10)
                .unwrap();
            twins
                .report(
                    &machine,
                    source.clone(),
                    TwinSignalWrite::reported(
                        "temperature",
                        ObjectValue::Float(81.5),
                        "2026-10-01T10:00:00Z",
                    ),
                )
                .unwrap();
            let mut machine_mode = TwinSignalWrite::reported(
                "mode",
                ObjectValue::String("automatic".into()),
                "2026-10-01T10:00:00Z",
            );
            machine_mode.cardinality = PredicateCardinality::SingleExclusive;
            twins
                .report(&machine, source.clone(), machine_mode)
                .unwrap();
            let mut service_modes = TwinSignalWrite::reported(
                "mode",
                ObjectValue::String("active".into()),
                "2026-10-01T10:00:00Z",
            );
            service_modes.cardinality = PredicateCardinality::MultiValue;
            twins
                .report(&service, source.clone(), service_modes)
                .unwrap();
            twins
                .set_desired(
                    &machine,
                    source,
                    TwinSignalWrite::reported(
                        "temperature",
                        ObjectValue::Float(75.0),
                        "2026-10-01T10:00:01Z",
                    ),
                )
                .unwrap();
            twins
                .connect(
                    &machine,
                    TwinLink {
                        relationship_type: "depends_on".into(),
                        target_twin_id: service,
                        valid_from: "2026-10-01T00:00:00Z".into(),
                        valid_to: None,
                        confidence: 1.0,
                        evidence: Vec::new(),
                    },
                )
                .unwrap();
            let snapshot = twins.snapshot(&machine, None, None).unwrap();
            assert_eq!(
                snapshot.reported["temperature"],
                vec![ObjectValue::Float(81.5)]
            );
            assert_eq!(snapshot.drift.len(), 1);
            assert_eq!(twins.twins().len(), 2);
            let mismatch = twins
                .register(definition("Press 7", "building.elevator"))
                .unwrap_err();
            assert!(matches!(mismatch, TwinError::Invalid(_)));
        }
        std::fs::remove_file(database).unwrap();
    }

    #[test]
    fn preserves_late_telemetry_in_two_time_dimensions() {
        let database = path("bitemporal");
        {
            let mut engine = Engine::init(&database).unwrap();
            let mut twins = TwinGateway::new(&mut engine);
            let twin = twins
                .register(definition("Room 101", "building.room"))
                .unwrap();
            let source = twins.ensure_adapter_source("bacnet", "bacnet", 10).unwrap();
            let mut first = TwinSignalWrite::reported(
                "occupancy",
                ObjectValue::Integer(3),
                "2026-10-01T09:00:00Z",
            );
            first.ingested_at = Some("2026-10-01T09:01:00Z".into());
            twins.report(&twin, source.clone(), first).unwrap();
            let mut correction = TwinSignalWrite::reported(
                "occupancy",
                ObjectValue::Integer(2),
                "2026-10-01T09:00:00Z",
            );
            correction.ingested_at = Some("2026-10-01T10:00:00Z".into());
            correction.valid_from = Some("2026-10-01T09:00:00Z".into());
            twins.report(&twin, source, correction).unwrap();

            let earlier = twins
                .snapshot(
                    &twin,
                    Some("2026-10-01T09:30:00Z"),
                    Some("2026-10-01T09:30:00Z"),
                )
                .unwrap();
            let later = twins
                .snapshot(
                    &twin,
                    Some("2026-10-01T09:30:00Z"),
                    Some("2026-10-01T10:30:00Z"),
                )
                .unwrap();
            assert_eq!(earlier.reported["occupancy"], vec![ObjectValue::Integer(3)]);
            assert_eq!(later.reported["occupancy"], vec![ObjectValue::Integer(2)]);
        }
        std::fs::remove_file(database).unwrap();
    }

    #[test]
    fn command_requests_are_idempotent_and_auditable() {
        let database = path("commands");
        {
            let mut engine = Engine::init(&database).unwrap();
            let mut twins = TwinGateway::new(&mut engine);
            let twin = twins
                .register(definition("Drone 4", "vehicle.drone"))
                .unwrap();
            let request = TwinCommandRequest {
                command_type: "return_home".into(),
                requested_by: "planner-agent".into(),
                requested_at: "2026-10-01T11:00:00Z".into(),
                expires_at: Some("2026-10-01T11:05:00Z".into()),
                idempotency_key: "mission-9-step-3".into(),
                parameters: BTreeMap::new(),
            };
            let first = twins.request_command(&twin, request.clone()).unwrap();
            let replay = twins.request_command(&twin, request).unwrap();
            assert_eq!(first.command_id, replay.command_id);
            assert!(!first.replayed);
            assert!(replay.replayed);
            let changed = TwinCommandRequest {
                command_type: "return_home".into(),
                requested_by: "planner-agent".into(),
                requested_at: "2026-10-01T11:00:00Z".into(),
                expires_at: Some("2026-10-01T11:05:00Z".into()),
                idempotency_key: "mission-9-step-3".into(),
                parameters: BTreeMap::from([("altitude".into(), ObjectValue::Integer(100))]),
            };
            assert!(matches!(
                twins.request_command(&twin, changed),
                Err(TwinError::Invalid(_))
            ));
            let acknowledgement = twins
                .acknowledge_command(
                    &twin,
                    &first.command_id,
                    CommandStatus::Succeeded,
                    "2026-10-01T11:01:00Z",
                    None,
                )
                .unwrap();
            let replayed_acknowledgement = twins
                .acknowledge_command(
                    &twin,
                    &first.command_id,
                    CommandStatus::Succeeded,
                    "2026-10-01T11:01:00Z",
                    None,
                )
                .unwrap();
            assert_eq!(acknowledgement, replayed_acknowledgement);
            assert_eq!(twins.commands(&twin).unwrap().len(), 2);
        }
        std::fs::remove_file(database).unwrap();
    }
}
