//! Domain types shared by all World Model Database crates.
//!
//! The core intentionally has no dependencies. Timestamps and JSON payloads are
//! stored as strings so storage and transport layers can choose their own
//! implementations without leaking them into the domain model.

use std::collections::BTreeMap;
use std::fmt;

macro_rules! id_type {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct $name(pub String);

            impl $name {
                pub fn new(value: impl Into<String>) -> Self {
                    Self(value.into())
                }

                pub fn as_str(&self) -> &str {
                    &self.0
                }

                pub fn into_inner(self) -> String {
                    self.0
                }
            }

            impl From<String> for $name {
                fn from(value: String) -> Self {
                    Self(value)
                }
            }

            impl From<&str> for $name {
                fn from(value: &str) -> Self {
                    Self(value.to_owned())
                }
            }

            impl AsRef<str> for $name {
                fn as_ref(&self) -> &str {
                    self.as_str()
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(self.as_str())
                }
            }
        )+
    };
}

id_type!(
    EntityId,
    SourceId,
    ObservationId,
    FactId,
    RelationshipId,
    EventId,
    EvidenceId,
    ConflictId,
    CorrelationId,
    OntologySchemaId,
    OntologyModuleId,
    OntologyRuleId,
    OntologyActionId,
    PermissionRuleId,
    SchemaMappingId,
    ActionExecutionId,
);

/// A typed value asserted by an observation or fact.
#[derive(Clone, Debug, PartialEq)]
pub enum ObjectValue {
    Entity(EntityId),
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Timestamp(String),
    /// Raw JSON, retained as text to keep `wm-core` dependency-free.
    Json(String),
}

/// Resolution state of a fact or relationship.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FactStatus {
    Supported,
    Contested,
    Superseded,
    Unresolved,
    Retracted,
}

/// Whether facts for a predicate compete for a single slot or may coexist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateCardinality {
    SingleExclusive,
    MultiValue,
}

/// The part played by a source observation in a derived object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvidenceRole {
    Support,
    Conflict,
    Retraction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConflictResolutionStatus {
    Open,
    Resolved,
    ManualReview,
    Superseded,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entity {
    pub id: EntityId,
    pub entity_type: String,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub attributes: BTreeMap<String, ObjectValue>,
    pub created_at: String,
    pub retired_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Source {
    pub id: SourceId,
    pub source_type: String,
    pub uri: String,
    pub name: String,
    pub metadata: BTreeMap<String, ObjectValue>,
    pub priority: i32,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Observation {
    pub id: ObservationId,
    pub source_id: SourceId,
    pub subject_entity_id: EntityId,
    pub predicate: String,
    pub object: ObjectValue,
    pub observed_at: String,
    pub ingested_at: String,
    /// The interval in the modeled world claimed by this observation.
    pub claimed_valid_from: String,
    pub claimed_valid_to: Option<String>,
    pub cardinality: PredicateCardinality,
    pub confidence: f64,
    pub raw_payload: String,
    pub metadata: BTreeMap<String, ObjectValue>,
    pub retracted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fact {
    pub id: FactId,
    pub subject_entity_id: EntityId,
    pub predicate: String,
    pub object: ObjectValue,
    pub valid_from: String,
    pub valid_to: Option<String>,
    pub known_from: String,
    pub known_to: Option<String>,
    pub confidence: f64,
    pub status: FactStatus,
    pub created_from_observations: Vec<ObservationId>,
    pub confidence_explanation: String,
    pub resolution_rule: String,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Relationship {
    pub id: RelationshipId,
    pub source_entity_id: EntityId,
    pub relationship_type: String,
    pub target_entity_id: EntityId,
    pub valid_from: String,
    pub valid_to: Option<String>,
    pub known_from: String,
    pub known_to: Option<String>,
    pub confidence: f64,
    pub status: FactStatus,
    pub evidence_ids: Vec<EvidenceId>,
    pub resolution_rule: String,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub id: EventId,
    pub event_type: String,
    pub timestamp: String,
    pub end_timestamp: Option<String>,
    pub entities: Vec<EntityId>,
    pub attributes: BTreeMap<String, ObjectValue>,
    pub source_observations: Vec<ObservationId>,
    pub confidence: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Evidence {
    pub id: EvidenceId,
    /// ID of the fact, relationship, event, or correlation this evidence backs.
    pub derived_object_id: String,
    pub observation_id: ObservationId,
    pub source_id: SourceId,
    pub role: EvidenceRole,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Conflict {
    pub id: ConflictId,
    pub subject: EntityId,
    pub predicate: String,
    pub candidate_fact_ids: Vec<FactId>,
    pub detected_at: String,
    pub resolution_status: ConflictResolutionStatus,
    pub resolution_reason: Option<String>,
    pub resolved_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Correlation {
    pub id: CorrelationId,
    pub left_object_id: String,
    pub right_object_id: String,
    pub correlation_type: String,
    pub score: f64,
    pub evidence: Vec<ObservationId>,
    pub created_at: String,
}

/// Primitive value types understood by ontology property schemas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueType {
    Entity,
    String,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Json,
}

impl ObjectValue {
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Entity(_) => ValueType::Entity,
            Self::String(_) => ValueType::String,
            Self::Integer(_) => ValueType::Integer,
            Self::Float(_) => ValueType::Float,
            Self::Boolean(_) => ValueType::Boolean,
            Self::Timestamp(_) => ValueType::Timestamp,
            Self::Json(_) => ValueType::Json,
        }
    }
}

/// A property contract inherited through object types and capability interfaces.
#[derive(Clone, Debug, PartialEq)]
pub struct PropertySchema {
    pub name: String,
    pub value_type: ValueType,
    pub min_count: usize,
    pub max_count: Option<usize>,
    pub allowed_values: Vec<ObjectValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IdentityRule {
    pub properties: Vec<String>,
    /// Weight for each property in the same order as `properties`.
    pub weights: Vec<f64>,
    pub threshold: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InterfaceDefinition {
    pub name: String,
    pub required_properties: Vec<PropertySchema>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObjectTypeDefinition {
    pub name: String,
    pub namespace: String,
    pub version: u32,
    pub parent_types: Vec<String>,
    pub interfaces: Vec<String>,
    pub properties: Vec<PropertySchema>,
    pub identity: Option<IdentityRule>,
    pub disjoint_with: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipComposition {
    pub then_relationship: String,
    pub implies_relationship: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipTypeDefinition {
    pub name: String,
    pub version: u32,
    pub domain_types: Vec<String>,
    pub range_types: Vec<String>,
    pub min_outgoing: usize,
    pub max_outgoing: Option<usize>,
    pub transitive: bool,
    pub symmetric: bool,
    pub inverse_of: Option<String>,
    pub compositions: Vec<RelationshipComposition>,
    pub acyclic: bool,
    /// Require all participating domain/range entities to be in one component.
    pub connected: bool,
    /// Business/semantic edge cost used by weighted path search.
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DerivedClassDefinition {
    pub name: String,
    pub base_type: String,
    pub conditions: Vec<Condition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ComputedPropertyDefinition {
    pub id: OntologyRuleId,
    pub target_type: String,
    pub property: String,
    /// V0 expressions: `copy:<property>`, `count_out:<relationship>`,
    /// `count_in:<relationship>`, `exists_out:<relationship>`, `sum:<property>`.
    pub expression: String,
    pub materialized: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InferenceRule {
    pub id: OntologyRuleId,
    /// Ordered relationship path to match, for example `[parent_of, parent_of]`.
    pub relationship_path: Vec<String>,
    pub implies_relationship: String,
    pub materialized: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComparisonOperator {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    Exists,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Condition {
    pub property: String,
    pub operator: ComparisonOperator,
    pub value: Option<ObjectValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActionEffect {
    SetProperty {
        property: String,
        value: ObjectValue,
    },
    RemoveProperty {
        property: String,
    },
    AddRelationship {
        relationship_type: String,
        target_entity_id: EntityId,
    },
    EmitEvent {
        event_type: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActionDefinition {
    pub id: OntologyActionId,
    pub name: String,
    pub target_type: String,
    pub preconditions: Vec<Condition>,
    pub effects: Vec<ActionEffect>,
    pub postconditions: Vec<Condition>,
    pub allowed_roles: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PermissionEffect {
    Allow,
    Deny,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PermissionRule {
    pub id: PermissionRuleId,
    pub principal: Option<String>,
    pub role: Option<String>,
    pub action: String,
    pub object_type: Option<String>,
    pub object_id: Option<EntityId>,
    pub conditions: Vec<Condition>,
    pub effect: PermissionEffect,
    pub priority: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldTransform {
    Identity,
    Lowercase,
    Uppercase,
    Trim,
    Prefix(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldMapping {
    pub source_field: String,
    pub target_property: String,
    pub transform: FieldTransform,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SchemaMapping {
    pub id: SchemaMappingId,
    pub source_namespace: String,
    pub source_type: String,
    pub target_type: String,
    /// Template such as `crm:customer:{id}`.
    pub semantic_id_template: String,
    pub fields: Vec<FieldMapping>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OntologyModule {
    pub id: OntologyModuleId,
    pub namespace: String,
    pub version: u32,
    pub dependencies: Vec<OntologyModuleId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompatibilityMode {
    Backward,
    Forward,
    Full,
    Breaking,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OntologySchemaVersion {
    pub id: OntologySchemaId,
    pub version: u32,
    pub supersedes: Option<OntologySchemaId>,
    pub compatibility: CompatibilityMode,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActionExecution {
    pub id: ActionExecutionId,
    pub action_id: OntologyActionId,
    pub actor: String,
    pub roles: Vec<String>,
    pub target_entity_id: EntityId,
    pub occurred_at: String,
    pub succeeded: bool,
    pub message: String,
    pub before: BTreeMap<String, ObjectValue>,
    pub after: BTreeMap<String, ObjectValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityEquivalence {
    pub left: EntityId,
    pub right: EntityId,
    pub score: f64,
    pub evidence: Vec<String>,
    pub resolved_at: String,
}

/// Durable ontology plane. Runtime world state remains in the existing entity,
/// fact, relationship, event, history, and provenance collections.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OntologyCatalog {
    pub schemas: Vec<OntologySchemaVersion>,
    pub modules: Vec<OntologyModule>,
    pub interfaces: Vec<InterfaceDefinition>,
    pub object_types: Vec<ObjectTypeDefinition>,
    pub relationship_types: Vec<RelationshipTypeDefinition>,
    pub computed_properties: Vec<ComputedPropertyDefinition>,
    pub inference_rules: Vec<InferenceRule>,
    pub derived_classes: Vec<DerivedClassDefinition>,
    pub actions: Vec<ActionDefinition>,
    pub permissions: Vec<PermissionRule>,
    pub mappings: Vec<SchemaMapping>,
    pub action_executions: Vec<ActionExecution>,
    pub equivalences: Vec<EntityEquivalence>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_distinct_and_ergonomic() {
        let id = EntityId::new("entity-1");
        assert_eq!(id.as_str(), "entity-1");
        assert_eq!(id.to_string(), "entity-1");
        assert_eq!(EntityId::from("entity-1"), id);
    }
}
