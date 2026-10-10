//! Deterministic fact resolution and mutation-free observation ingestion.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use wm_core::*;
use wm_ontology::{
    ConsistencyReport, ResolutionMatch, check_consistency, effective_properties, execute_action,
    map_record, materialize_computed, materialize_inference, record_equivalence, resolve_entities,
    schema_compatibility, validate_entity, validate_observation, validate_relationship,
};
use wm_storage::{FileStore, WorldState};
use wm_temporal::{canonicalize_rfc3339, compare_rfc3339, interval_contains};

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
    pub claimed_valid_from: Option<String>,
    pub claimed_valid_to: Option<String>,
    pub cardinality: PredicateCardinality,
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
        let entity = Entity {
            id: id.clone(),
            entity_type,
            canonical_name,
            aliases,
            attributes,
            created_at: now_utc(),
            retired_at: None,
        };
        reject_violations(validate_entity(&self.store.state.ontology, &entity))?;
        self.store.state.entities.push(entity);
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
        let relationship_type = relationship_type.into();
        let source_entity = self
            .store
            .state
            .entity(&source)
            .cloned()
            .ok_or_else(|| ResolutionError::NotFound(format!("entity {source} not found")))?;
        let target_entity = self
            .store
            .state
            .entity(&target)
            .cloned()
            .ok_or_else(|| ResolutionError::NotFound(format!("entity {target} not found")))?;
        reject_violations(validate_relationship(
            &self.store.state,
            &source_entity,
            &relationship_type,
            &target_entity,
        ))?;
        let valid_from = normalize_time(&valid_from.into())?;
        let valid_to = valid_to.map(|value| normalize_time(&value)).transpose()?;
        validate_interval(&valid_from, valid_to.as_deref())?;
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
            relationship_type,
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
        let timestamp = normalize_time(&timestamp.into())?;
        let end_timestamp = end_timestamp
            .map(|value| normalize_time(&value))
            .transpose()?;
        validate_interval(&timestamp, end_timestamp.as_deref())?;
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

    pub fn register_schema(
        &mut self,
        schema: OntologySchemaVersion,
    ) -> Result<(), ResolutionError> {
        if schema.version == 0 || schema.id.as_str().is_empty() {
            return Err(ResolutionError::Invalid(
                "ontology schema requires a non-empty ID and version > 0".into(),
            ));
        }
        if let Some(parent) = &schema.supersedes
            && !self
                .store
                .state
                .ontology
                .schemas
                .iter()
                .any(|candidate| &candidate.id == parent)
        {
            return Err(ResolutionError::Invalid(format!(
                "superseded schema {parent} does not exist"
            )));
        }
        upsert_by(
            &mut self.store.state.ontology.schemas,
            schema,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn register_module(&mut self, module: OntologyModule) -> Result<(), ResolutionError> {
        if module.id.as_str().is_empty() || module.namespace.is_empty() || module.version == 0 {
            return Err(ResolutionError::Invalid(
                "ontology module requires ID, namespace, and version > 0".into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.modules,
            module,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn register_interface(
        &mut self,
        interface: InterfaceDefinition,
    ) -> Result<(), ResolutionError> {
        validate_property_schemas(&interface.required_properties)?;
        upsert_by(
            &mut self.store.state.ontology.interfaces,
            interface,
            |left, right| left.name.eq_ignore_ascii_case(&right.name),
        );
        self.save()
    }

    pub fn register_object_type(
        &mut self,
        definition: ObjectTypeDefinition,
    ) -> Result<(), ResolutionError> {
        if definition.name.is_empty() || definition.namespace.is_empty() || definition.version == 0
        {
            return Err(ResolutionError::Invalid(
                "object type requires name, namespace, and version > 0".into(),
            ));
        }
        validate_property_schemas(&definition.properties)?;
        if let Some(identity) = &definition.identity
            && (identity.properties.len() != identity.weights.len()
                || !(0.0..=1.0).contains(&identity.threshold)
                || identity.weights.iter().any(|weight| !weight.is_finite()))
        {
            return Err(ResolutionError::Invalid(
                "identity rule requires equal property/weight counts, finite weights, and threshold in [0,1]"
                    .into(),
            ));
        }
        if let Some(existing) = self
            .store
            .state
            .ontology
            .object_types
            .iter()
            .find(|existing| {
                existing.name.eq_ignore_ascii_case(&definition.name)
                    && existing.version == definition.version
            })
        {
            return if existing == &definition {
                Ok(())
            } else {
                Err(ResolutionError::Invalid(format!(
                    "object type '{}@{}' is immutable; publish a new version",
                    definition.name, definition.version
                )))
            };
        }
        if let Some(previous) = self
            .store
            .state
            .ontology
            .object_types
            .iter()
            .filter(|existing| {
                existing.name.eq_ignore_ascii_case(&definition.name)
                    && existing.version < definition.version
            })
            .max_by_key(|existing| existing.version)
        {
            let issues = schema_compatibility(&self.store.state.ontology, previous, &definition);
            let breaking_allowed = self
                .store
                .state
                .ontology
                .schemas
                .iter()
                .max_by_key(|schema| schema.version)
                .is_some_and(|schema| schema.compatibility == CompatibilityMode::Breaking);
            if !issues.is_empty() && !breaking_allowed {
                return Err(ResolutionError::Invalid(format!(
                    "incompatible object type version: {}",
                    issues.join("; ")
                )));
            }
        }
        upsert_by(
            &mut self.store.state.ontology.object_types,
            definition,
            |left, right| {
                left.name.eq_ignore_ascii_case(&right.name) && left.version == right.version
            },
        );
        self.save()
    }

    pub fn register_relationship_type(
        &mut self,
        definition: RelationshipTypeDefinition,
    ) -> Result<(), ResolutionError> {
        if definition.name.is_empty()
            || definition.version == 0
            || !definition.weight.is_finite()
            || definition.weight <= 0.0
            || definition
                .max_outgoing
                .is_some_and(|maximum| definition.min_outgoing > maximum)
        {
            return Err(ResolutionError::Invalid(
                "relationship type requires a name, version > 0, positive weight, and valid cardinality"
                    .into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.relationship_types,
            definition,
            |left, right| {
                left.name.eq_ignore_ascii_case(&right.name) && left.version == right.version
            },
        );
        self.save()
    }

    pub fn register_computed_property(
        &mut self,
        definition: ComputedPropertyDefinition,
    ) -> Result<(), ResolutionError> {
        let function = definition.expression.split(':').next().unwrap_or_default();
        if !matches!(
            function,
            "copy" | "count_out" | "count_in" | "exists_out" | "sum"
        ) || !definition.expression.contains(':')
        {
            return Err(ResolutionError::Invalid(
                "unsupported computed property expression".into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.computed_properties,
            definition,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn register_inference_rule(&mut self, rule: InferenceRule) -> Result<(), ResolutionError> {
        if rule.relationship_path.is_empty() || rule.implies_relationship.is_empty() {
            return Err(ResolutionError::Invalid(
                "inference rule requires a non-empty path and conclusion".into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.inference_rules,
            rule,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn register_derived_class(
        &mut self,
        definition: DerivedClassDefinition,
    ) -> Result<(), ResolutionError> {
        if definition.name.is_empty()
            || definition.base_type.is_empty()
            || definition.conditions.is_empty()
        {
            return Err(ResolutionError::Invalid(
                "derived class requires a name, base type, and at least one condition".into(),
            ));
        }
        if wm_ontology::active_object_type(&self.store.state.ontology, &definition.base_type)
            .is_none()
        {
            return Err(ResolutionError::Invalid(format!(
                "derived class base type '{}' is not defined",
                definition.base_type
            )));
        }
        upsert_by(
            &mut self.store.state.ontology.derived_classes,
            definition,
            |left, right| left.name.eq_ignore_ascii_case(&right.name),
        );
        self.save()
    }

    pub fn register_action(&mut self, action: ActionDefinition) -> Result<(), ResolutionError> {
        if action.name.is_empty() || action.target_type.is_empty() {
            return Err(ResolutionError::Invalid(
                "action requires a name and target type".into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.actions,
            action,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn register_permission(&mut self, rule: PermissionRule) -> Result<(), ResolutionError> {
        if rule.action.is_empty() {
            return Err(ResolutionError::Invalid(
                "permission rule requires an action".into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.permissions,
            rule,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn register_mapping(&mut self, mapping: SchemaMapping) -> Result<(), ResolutionError> {
        if mapping.source_namespace.is_empty()
            || mapping.source_type.is_empty()
            || mapping.target_type.is_empty()
            || mapping.semantic_id_template.is_empty()
        {
            return Err(ResolutionError::Invalid(
                "schema mapping requires source namespace/type, target type, and semantic ID template"
                    .into(),
            ));
        }
        upsert_by(
            &mut self.store.state.ontology.mappings,
            mapping,
            |left, right| left.id == right.id,
        );
        self.save()
    }

    pub fn apply_mapping(
        &mut self,
        mapping_id: &SchemaMappingId,
        record: &BTreeMap<String, String>,
    ) -> Result<EntityId, ResolutionError> {
        let mapping = self
            .store
            .state
            .ontology
            .mappings
            .iter()
            .find(|mapping| &mapping.id == mapping_id)
            .cloned()
            .ok_or_else(|| {
                ResolutionError::NotFound(format!("schema mapping {mapping_id} not found"))
            })?;
        let mut mapped = map_record(&mapping, record).map_err(ResolutionError::Invalid)?;
        if !self.store.state.ontology.object_types.is_empty() {
            let properties = effective_properties(&self.store.state.ontology, &mapped.entity_type)
                .map_err(ResolutionError::Invalid)?;
            for (property, value) in &mut mapped.attributes {
                let schema = properties.get(property).ok_or_else(|| {
                    ResolutionError::Invalid(format!(
                        "mapped property '{property}' is not declared for type '{}'",
                        mapped.entity_type
                    ))
                })?;
                *value = coerce_mapped_value(value.clone(), &schema.value_type)?;
            }
        }
        let canonical_name = mapped
            .attributes
            .get("name")
            .and_then(|value| match value {
                ObjectValue::String(value) => Some(value.clone()),
                _ => None,
            })
            .unwrap_or_else(|| mapped.id.to_string());
        if let Some(position) = self
            .store
            .state
            .entities
            .iter()
            .position(|entity| entity.id == mapped.id)
        {
            let mut updated = self.store.state.entities[position].clone();
            if !updated
                .entity_type
                .eq_ignore_ascii_case(&mapped.entity_type)
            {
                return Err(ResolutionError::Invalid(format!(
                    "semantic ID {} already belongs to type '{}'",
                    mapped.id, updated.entity_type
                )));
            }
            updated.attributes.extend(mapped.attributes);
            reject_violations(validate_entity(&self.store.state.ontology, &updated))?;
            self.store.state.entities[position] = updated;
        } else {
            let entity = Entity {
                id: mapped.id.clone(),
                entity_type: mapped.entity_type,
                canonical_name,
                aliases: Vec::new(),
                attributes: mapped.attributes,
                created_at: now_utc(),
                retired_at: None,
            };
            reject_violations(validate_entity(&self.store.state.ontology, &entity))?;
            self.store.state.entities.push(entity);
        }
        self.save()?;
        Ok(mapped.id)
    }

    pub fn materialize_ontology(&mut self) -> Result<(usize, usize), ResolutionError> {
        let now = now_utc();
        let entities = self
            .store
            .state
            .entities
            .iter()
            .map(|entity| entity.id.clone())
            .collect::<Vec<_>>();
        let mut computed = 0;
        for entity in entities {
            computed += materialize_computed(&mut self.store.state, &entity, &now)
                .map_err(ResolutionError::Invalid)?
                .len();
        }
        let inferred = materialize_inference(&mut self.store.state, &now).len();
        self.save()?;
        Ok((computed, inferred))
    }

    pub fn execute_ontology_action(
        &mut self,
        action: &str,
        actor: &str,
        roles: &[String],
        target: &EntityId,
    ) -> Result<ActionExecutionId, ResolutionError> {
        let id = execute_action(
            &mut self.store.state,
            action,
            actor,
            roles,
            target,
            &now_utc(),
        )
        .map_err(ResolutionError::Invalid)?;
        self.save()?;
        Ok(id)
    }

    pub fn resolve_entity_pair(
        &mut self,
        left: &EntityId,
        right: &EntityId,
        persist_equivalence: bool,
    ) -> Result<ResolutionMatch, ResolutionError> {
        let left_entity = self
            .store
            .state
            .entity(left)
            .cloned()
            .ok_or_else(|| ResolutionError::NotFound(format!("entity {left} not found")))?;
        let right_entity = self
            .store
            .state
            .entity(right)
            .cloned()
            .ok_or_else(|| ResolutionError::NotFound(format!("entity {right} not found")))?;
        let resolution = resolve_entities(&self.store.state.ontology, &left_entity, &right_entity)
            .map_err(ResolutionError::Invalid)?;
        if persist_equivalence && record_equivalence(&mut self.store.state, &resolution, &now_utc())
        {
            self.save()?;
        }
        Ok(resolution)
    }

    pub fn ontology_consistency(&self) -> ConsistencyReport {
        check_consistency(&self.store.state)
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
        let subject = self
            .store
            .state
            .entity(&input.subject_entity_id)
            .expect("entity existence was checked");
        reject_violations(validate_observation(
            &self.store.state,
            subject,
            &input.predicate,
            &input.object,
            &input.cardinality,
        ))?;
        validate_confidence(input.confidence)?;
        if let Some(existing) = self.store.state.observations.iter().find(|observation| {
            observation.subject_entity_id == input.subject_entity_id
                && observation.predicate == input.predicate
                && observation.cardinality != input.cardinality
        }) {
            return Err(ResolutionError::Invalid(format!(
                "predicate {} already uses {:?} cardinality (observation {})",
                input.predicate, existing.cardinality, existing.id
            )));
        }
        let observed_at = normalize_time(&input.observed_at)?;
        let ingested_at = normalize_time(&input.ingested_at.unwrap_or_else(now_utc))?;
        let claimed_valid_from =
            normalize_time(input.claimed_valid_from.as_deref().unwrap_or(&observed_at))?;
        let claimed_valid_to = input
            .claimed_valid_to
            .map(|value| normalize_time(&value))
            .transpose()?;
        validate_interval(&claimed_valid_from, claimed_valid_to.as_deref())?;
        let requires_rebuild = self
            .store
            .state
            .observations
            .iter()
            .map(|observation| observation.ingested_at.as_str())
            .max_by(|left, right| time_cmp(left, right))
            .is_some_and(|latest| time_cmp(&ingested_at, latest) != Ordering::Greater);
        let id = ObservationId(self.store.state.next_id("observation"));
        let observation = Observation {
            id: id.clone(),
            source_id: input.source_id,
            subject_entity_id: input.subject_entity_id,
            predicate: input.predicate,
            object: input.object,
            observed_at,
            ingested_at,
            claimed_valid_from,
            claimed_valid_to,
            cardinality: input.cardinality,
            confidence: input.confidence,
            raw_payload: input.raw_payload,
            metadata: input.metadata,
            retracted: input.retracted,
        };
        let subject = observation.subject_entity_id.clone();
        let predicate = observation.predicate.clone();
        let known_at = observation.ingested_at.clone();
        self.store.state.observations.push(observation);
        if requires_rebuild {
            self.rebuild_current()?;
        } else {
            self.store.state.sync_indexes();
            resolve_key(
                &mut self.store.state,
                &self.config,
                &subject,
                &predicate,
                &known_at,
            )?;
        }
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
        self.store.state.sync_indexes();
        let mut snapshots = self
            .store
            .state
            .observations
            .iter()
            .map(|observation| {
                (
                    observation.ingested_at.clone(),
                    observation.subject_entity_id.clone(),
                    observation.predicate.clone(),
                )
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| {
            time_cmp(&left.0, &right.0)
                .then(left.1.cmp(&right.1))
                .then(left.2.cmp(&right.2))
        });
        snapshots.dedup();
        for (known_at, subject, predicate) in snapshots {
            resolve_key(
                &mut self.store.state,
                &self.config,
                &subject,
                &predicate,
                &known_at,
            )?;
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
    let mut relevant = state
        .observations
        .iter()
        .filter(|observation| {
            &observation.subject_entity_id == subject
                && observation.predicate == predicate
                && time_cmp(&observation.ingested_at, known_at) != Ordering::Greater
        })
        .cloned()
        .collect::<Vec<_>>();
    relevant.sort_by(observation_order);
    let cardinality = relevant
        .first()
        .map(|observation| observation.cardinality.clone())
        .unwrap_or(PredicateCardinality::SingleExclusive);
    if relevant
        .iter()
        .any(|observation| observation.cardinality != cardinality)
    {
        return Err(ResolutionError::Invalid(format!(
            "predicate {predicate} mixes cardinality declarations"
        )));
    }

    for fact in state.facts.iter_mut().filter(|f| {
        &f.subject_entity_id == subject && f.predicate == predicate && f.known_to.is_none()
    }) {
        fact.known_to = Some(known_at.to_owned());
        if fact.status == FactStatus::Supported {
            fact.status = FactStatus::Superseded;
        }
    }
    close_conflict(
        state,
        subject,
        predicate,
        known_at,
        "knowledge snapshot superseded",
    );

    let effective_ends = relevant
        .iter()
        .map(|observation| {
            (
                observation.id.clone(),
                effective_valid_to(observation, &relevant, &cardinality),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut boundaries = Vec::new();
    for observation in &relevant {
        boundaries.push(observation.claimed_valid_from.clone());
        if let Some(end) = effective_ends.get(&observation.id).and_then(Clone::clone) {
            boundaries.push(end);
        }
    }
    boundaries.sort_by(|left, right| time_cmp(left, right));
    boundaries.dedup_by(|left, right| time_cmp(left, right) == Ordering::Equal);
    let mut conflict_ids = Vec::new();
    for (index, valid_from) in boundaries.iter().enumerate() {
        let valid_to = boundaries.get(index + 1).cloned();
        let mut groups: BTreeMap<String, Vec<Observation>> = BTreeMap::new();
        for observation in relevant.iter().filter(|observation| !observation.retracted) {
            let end = effective_ends
                .get(&observation.id)
                .and_then(|end| end.as_deref());
            if interval_contains(&observation.claimed_valid_from, end, valid_from)
                && !is_retracted_at(observation, &relevant, valid_from)
            {
                groups
                    .entry(object_key(&observation.object))
                    .or_default()
                    .push(observation.clone());
            }
        }
        for retraction in retractions_at(&relevant, valid_from) {
            create_retraction_fact(
                state,
                subject,
                predicate,
                retraction,
                valid_from,
                valid_to.clone(),
                known_at,
            );
        }
        if groups.is_empty() {
            continue;
        }
        let is_conflict = cardinality == PredicateCardinality::SingleExclusive && groups.len() > 1;
        let winner = if cardinality == PredicateCardinality::SingleExclusive {
            groups
                .iter()
                .max_by(|(left_key, left), (right_key, right)| {
                    compare_candidates(state, left, right).then_with(|| right_key.cmp(left_key))
                })
                .map(|(key, _)| key.clone())
        } else {
            None
        };
        for (key, mut observations) in groups {
            observations.sort_by(observation_order);
            let supported = winner.as_ref().is_none_or(|winner| winner == &key);
            let average =
                observations.iter().map(|o| o.confidence).sum::<f64>() / observations.len() as f64;
            let confidence = (average
                + config.support_bonus * observations.len().saturating_sub(1) as f64
                - if is_conflict {
                    config.conflict_penalty
                } else {
                    0.0
                })
            .clamp(0.0, 1.0);
            let latest = observations.last().unwrap();
            let fact_id = FactId(state.next_id("fact"));
            state.facts.push(Fact {
                id: fact_id.clone(),
                subject_entity_id: subject.clone(),
                predicate: predicate.to_owned(),
                object: latest.object.clone(),
                valid_from: valid_from.clone(),
                valid_to: valid_to.clone(),
                known_from: known_at.to_owned(),
                known_to: None,
                confidence,
                status: if supported {
                    FactStatus::Supported
                } else {
                    FactStatus::Contested
                },
                created_from_observations: observations.iter().map(|o| o.id.clone()).collect(),
                confidence_explanation: format!(
                    "{} supporting observation(s); mean confidence {:.3}; conflict penalty {:.3}",
                    observations.len(),
                    average,
                    if is_conflict { config.conflict_penalty } else { 0.0 }
                ),
                resolution_rule: match cardinality {
                    PredicateCardinality::SingleExclusive => "valid-time segment; source priority, recency, confidence, support count, deterministic object key",
                    PredicateCardinality::MultiValue => "valid-time segment; multi-value union",
                }
                .into(),
                created_at: known_at.to_owned(),
            });
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
            if is_conflict {
                conflict_ids.push(fact_id);
            }
        }
    }
    if !conflict_ids.is_empty() {
        open_conflict(state, subject, predicate, conflict_ids, known_at);
    }
    Ok(())
}

fn observation_order(left: &Observation, right: &Observation) -> Ordering {
    time_cmp(&left.ingested_at, &right.ingested_at)
        .then_with(|| time_cmp(&left.claimed_valid_from, &right.claimed_valid_from))
        .then_with(|| time_cmp(&left.observed_at, &right.observed_at))
        .then(left.source_id.cmp(&right.source_id))
        .then(object_key(&left.object).cmp(&object_key(&right.object)))
        .then(left.id.cmp(&right.id))
}

fn effective_valid_to(
    observation: &Observation,
    relevant: &[Observation],
    cardinality: &PredicateCardinality,
) -> Option<String> {
    let inferred = (cardinality == &PredicateCardinality::SingleExclusive)
        .then(|| {
            relevant
                .iter()
                .filter(|candidate| {
                    !candidate.retracted
                        && candidate.source_id == observation.source_id
                        && object_key(&candidate.object) != object_key(&observation.object)
                        && time_cmp(
                            &candidate.claimed_valid_from,
                            &observation.claimed_valid_from,
                        ) == Ordering::Greater
                })
                .map(|candidate| candidate.claimed_valid_from.clone())
                .min_by(|left, right| time_cmp(left, right))
        })
        .flatten();
    match (&observation.claimed_valid_to, inferred) {
        (Some(explicit), Some(inferred)) => {
            Some(if time_cmp(explicit, &inferred) != Ordering::Greater {
                explicit.clone()
            } else {
                inferred
            })
        }
        (Some(explicit), None) => Some(explicit.clone()),
        (None, inferred) => inferred,
    }
}

fn is_retracted_at(observation: &Observation, relevant: &[Observation], valid_at: &str) -> bool {
    relevant.iter().any(|candidate| {
        candidate.retracted
            && time_cmp(&candidate.ingested_at, &observation.ingested_at) != Ordering::Less
            && object_key(&candidate.object) == object_key(&observation.object)
            && interval_contains(
                &candidate.claimed_valid_from,
                candidate.claimed_valid_to.as_deref(),
                valid_at,
            )
    })
}

fn retractions_at<'a>(relevant: &'a [Observation], valid_at: &str) -> Vec<&'a Observation> {
    let mut by_object: BTreeMap<String, &'a Observation> = BTreeMap::new();
    for observation in relevant.iter().filter(|observation| {
        observation.retracted
            && interval_contains(
                &observation.claimed_valid_from,
                observation.claimed_valid_to.as_deref(),
                valid_at,
            )
    }) {
        let key = object_key(&observation.object);
        if by_object
            .get(&key)
            .is_none_or(|current| observation_order(current, observation) == Ordering::Less)
        {
            by_object.insert(key, observation);
        }
    }
    by_object.into_values().collect()
}

fn create_retraction_fact(
    state: &mut WorldState,
    subject: &EntityId,
    predicate: &str,
    observation: &Observation,
    valid_from: &str,
    valid_to: Option<String>,
    known_at: &str,
) {
    let fact_id = FactId(state.next_id("fact"));
    state.facts.push(Fact {
        id: fact_id.clone(),
        subject_entity_id: subject.clone(),
        predicate: predicate.to_owned(),
        object: observation.object.clone(),
        valid_from: valid_from.to_owned(),
        valid_to,
        known_from: known_at.to_owned(),
        known_to: None,
        confidence: 0.0,
        status: FactStatus::Retracted,
        created_from_observations: vec![observation.id.clone()],
        confidence_explanation: "explicit retraction preserved as a valid-time interval".into(),
        resolution_rule: "explicit retraction".into(),
        created_at: known_at.to_owned(),
    });
    let evidence_id = EvidenceId(state.next_id("evidence"));
    state.evidence.push(Evidence {
        id: evidence_id,
        derived_object_id: fact_id.0,
        observation_id: observation.id.clone(),
        source_id: observation.source_id.clone(),
        role: EvidenceRole::Retraction,
        created_at: known_at.to_owned(),
    });
}

fn compare_candidates(state: &WorldState, left: &[Observation], right: &[Observation]) -> Ordering {
    let priority = |values: &[Observation]| {
        values
            .iter()
            .filter_map(|o| state.source(&o.source_id).map(|s| s.priority))
            .max()
            .unwrap_or_default()
    };
    let left_latest = left
        .iter()
        .map(|o| o.observed_at.as_str())
        .max_by(|left, right| time_cmp(left, right))
        .unwrap_or_default();
    let right_latest = right
        .iter()
        .map(|o| o.observed_at.as_str())
        .max_by(|left, right| time_cmp(left, right))
        .unwrap_or_default();
    let confidence = |values: &[Observation]| {
        (values.iter().map(|o| o.confidence).sum::<f64>() * 1_000_000.0) as i64
    };
    priority(left)
        .cmp(&priority(right))
        .then_with(|| time_cmp(left_latest, right_latest))
        .then_with(|| confidence(left).cmp(&confidence(right)))
        .then(left.len().cmp(&right.len()))
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

fn reject_violations(violations: Vec<wm_ontology::Violation>) -> Result<(), ResolutionError> {
    if violations.is_empty() {
        Ok(())
    } else {
        Err(ResolutionError::Invalid(
            violations
                .into_iter()
                .map(|violation| format!("{}: {}", violation.code, violation.message))
                .collect::<Vec<_>>()
                .join("; "),
        ))
    }
}

fn validate_property_schemas(properties: &[PropertySchema]) -> Result<(), ResolutionError> {
    let mut names = BTreeSet::new();
    for property in properties {
        if property.name.is_empty()
            || !names.insert(property.name.to_ascii_lowercase())
            || property
                .max_count
                .is_some_and(|maximum| property.min_count > maximum)
        {
            return Err(ResolutionError::Invalid(
                "property schemas require unique non-empty names and valid cardinality".into(),
            ));
        }
        if property
            .allowed_values
            .iter()
            .any(|value| value.value_type() != property.value_type)
        {
            return Err(ResolutionError::Invalid(format!(
                "allowed value for '{}' has the wrong type",
                property.name
            )));
        }
    }
    Ok(())
}

fn coerce_mapped_value(
    value: ObjectValue,
    expected: &ValueType,
) -> Result<ObjectValue, ResolutionError> {
    if value.value_type() == *expected {
        return Ok(value);
    }
    let ObjectValue::String(raw) = value else {
        return Err(ResolutionError::Invalid(format!(
            "cannot coerce mapped value to {expected:?}"
        )));
    };
    match expected {
        ValueType::Entity => Ok(ObjectValue::Entity(raw.into())),
        ValueType::String => Ok(ObjectValue::String(raw)),
        ValueType::Integer => raw
            .parse()
            .map(ObjectValue::Integer)
            .map_err(|_| ResolutionError::Invalid("mapped value is not an integer".into())),
        ValueType::Float => raw
            .parse()
            .map(ObjectValue::Float)
            .map_err(|_| ResolutionError::Invalid("mapped value is not a number".into())),
        ValueType::Boolean => raw
            .parse()
            .map(ObjectValue::Boolean)
            .map_err(|_| ResolutionError::Invalid("mapped value is not a boolean".into())),
        ValueType::Timestamp => Ok(ObjectValue::Timestamp(raw)),
        ValueType::Json => Ok(ObjectValue::Json(raw)),
    }
}

fn upsert_by<T>(values: &mut Vec<T>, value: T, same: impl Fn(&T, &T) -> bool) {
    if let Some(position) = values.iter().position(|candidate| same(candidate, &value)) {
        values[position] = value;
    } else {
        values.push(value);
    }
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
fn normalize_time(value: &str) -> Result<String, ResolutionError> {
    canonicalize_rfc3339(value).map_err(|error| ResolutionError::Invalid(error.to_string()))
}
fn validate_interval(from: &str, to: Option<&str>) -> Result<(), ResolutionError> {
    if to.is_some_and(|to| time_cmp(from, to) != Ordering::Less) {
        Err(ResolutionError::Invalid(
            "valid-time interval must have valid_from < valid_to".into(),
        ))
    } else {
        Ok(())
    }
}
fn time_cmp(left: &str, right: &str) -> Ordering {
    compare_rfc3339(left, right).unwrap_or_else(|| left.cmp(right))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("wm-resolution-{name}-{nonce}.redb"))
    }

    fn setup(name: &str) -> (Engine, std::path::PathBuf, EntityId, SourceId) {
        let path = path(name);
        let mut engine = Engine::init(&path).unwrap();
        let entity = engine
            .create_entity("company", "Acme", Vec::new(), BTreeMap::new())
            .unwrap();
        let source = engine
            .create_source(
                "filing",
                "source://official",
                "Official",
                100,
                BTreeMap::new(),
            )
            .unwrap();
        (engine, path, entity, source)
    }

    fn input(
        source: &SourceId,
        entity: &EntityId,
        object: &str,
        valid_from: &str,
        known_at: &str,
    ) -> NewObservation {
        NewObservation {
            source_id: source.clone(),
            subject_entity_id: entity.clone(),
            predicate: "CEO".into(),
            object: ObjectValue::String(object.into()),
            observed_at: valid_from.into(),
            ingested_at: Some(known_at.into()),
            claimed_valid_from: Some(valid_from.into()),
            claimed_valid_to: None,
            cardinality: PredicateCardinality::SingleExclusive,
            confidence: 0.95,
            raw_payload: String::new(),
            metadata: BTreeMap::new(),
            retracted: false,
        }
    }

    fn semantic_facts(state: &WorldState) -> Vec<String> {
        state
            .facts
            .iter()
            .map(|fact| {
                format!(
                    "{}|{}|{}|{}|{}|{:?}",
                    fact.predicate,
                    object_key(&fact.object),
                    fact.valid_from,
                    fact.valid_to.as_deref().unwrap_or(""),
                    fact.known_from,
                    fact.status
                )
            })
            .collect()
    }

    #[test]
    fn succession_is_segmented_and_late_arrival_rebuild_is_equivalent() {
        let (mut first, first_path, entity, source) = setup("ordered");
        first
            .observe(input(
                &source,
                &entity,
                "Alice",
                "2026-01-01T00:00:00Z",
                "2026-01-02T00:00:00Z",
            ))
            .unwrap();
        first
            .observe(input(
                &source,
                &entity,
                "Bob",
                "2026-02-01T00:00:00Z",
                "2026-02-02T00:00:00Z",
            ))
            .unwrap();
        assert!(first.store.state.conflicts.is_empty());
        let current = first.store.state.current_facts().collect::<Vec<_>>();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].object, ObjectValue::String("Bob".into()));
        assert!(first.store.state.facts.iter().any(|fact| {
            fact.object == ObjectValue::String("Alice".into())
                && fact.valid_to.as_deref() == Some("2026-02-01T00:00:00Z")
        }));

        let (mut late, late_path, late_entity, late_source) = setup("late");
        late.observe(input(
            &late_source,
            &late_entity,
            "Bob",
            "2026-02-01T00:00:00Z",
            "2026-02-02T00:00:00Z",
        ))
        .unwrap();
        late.observe(input(
            &late_source,
            &late_entity,
            "Alice",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        ))
        .unwrap();
        assert_eq!(
            semantic_facts(&first.store.state),
            semantic_facts(&late.store.state)
        );
        drop(first);
        drop(late);
        std::fs::remove_file(first_path).unwrap();
        std::fs::remove_file(late_path).unwrap();
    }

    #[test]
    fn cardinality_controls_conflicts_and_offsets_are_canonical() {
        let (mut engine, path, entity, source) = setup("cardinality");
        let other = engine
            .create_source("agent", "source://other", "Other", 10, BTreeMap::new())
            .unwrap();
        let mut first = input(
            &source,
            &entity,
            "red",
            "2026-01-01T10:00:00+05:30",
            "2026-01-02T10:00:00+05:30",
        );
        first.predicate = "COLOR".into();
        engine.observe(first).unwrap();
        let mut second = input(
            &other,
            &entity,
            "blue",
            "2026-01-01T04:30:00Z",
            "2026-01-02T04:31:00Z",
        );
        second.predicate = "COLOR".into();
        engine.observe(second).unwrap();
        assert_eq!(
            engine.store.state.observations[0].claimed_valid_from,
            "2026-01-01T04:30:00Z"
        );
        assert!(
            engine
                .store
                .state
                .conflicts
                .iter()
                .any(|conflict| { conflict.resolution_status == ConflictResolutionStatus::Open })
        );

        let mut multi = input(
            &source,
            &entity,
            "rust",
            "2026-03-01T00:00:00Z",
            "2026-03-02T00:00:00Z",
        );
        multi.predicate = "SKILL".into();
        multi.cardinality = PredicateCardinality::MultiValue;
        engine.observe(multi).unwrap();
        let mut multi_two = input(
            &other,
            &entity,
            "sql",
            "2026-03-01T00:00:00Z",
            "2026-03-02T00:01:00Z",
        );
        multi_two.predicate = "SKILL".into();
        multi_two.cardinality = PredicateCardinality::MultiValue;
        engine.observe(multi_two).unwrap();
        assert!(!engine.store.state.conflicts.iter().any(|conflict| {
            conflict.predicate == "SKILL"
                && conflict.resolution_status == ConflictResolutionStatus::Open
        }));
        assert_eq!(
            engine
                .store
                .state
                .current_facts()
                .filter(|fact| fact.predicate == "SKILL")
                .count(),
            2
        );
        drop(engine);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_inverted_intervals_and_preserves_nonempty_retractions() {
        let (mut engine, path, entity, source) = setup("intervals");
        let mut invalid = input(
            &source,
            &entity,
            "Alice",
            "2026-02-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        invalid.claimed_valid_to = Some("2026-01-01T00:00:00Z".into());
        assert!(matches!(
            engine.observe(invalid),
            Err(ResolutionError::Invalid(_))
        ));

        engine
            .observe(input(
                &source,
                &entity,
                "Alice",
                "2026-01-01T00:00:00Z",
                "2026-01-02T00:00:00Z",
            ))
            .unwrap();
        let mut retraction = input(
            &source,
            &entity,
            "Alice",
            "2026-02-01T00:00:00Z",
            "2026-02-02T00:00:00Z",
        );
        retraction.retracted = true;
        retraction.claimed_valid_to = Some("2026-03-01T00:00:00Z".into());
        engine.observe(retraction).unwrap();
        assert!(engine.store.state.facts.iter().any(|fact| {
            fact.status == FactStatus::Retracted
                && fact.valid_from == "2026-02-01T00:00:00Z"
                && fact.valid_to.as_deref() == Some("2026-03-01T00:00:00Z")
        }));
        assert!(engine.store.state.facts.iter().all(|fact| {
            fact.valid_to
                .as_ref()
                .is_none_or(|end| fact.valid_from < *end)
        }));
        drop(engine);
        std::fs::remove_file(path).unwrap();
    }
}
