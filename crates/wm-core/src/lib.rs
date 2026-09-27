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
