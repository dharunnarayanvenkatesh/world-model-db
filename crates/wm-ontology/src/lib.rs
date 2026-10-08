//! Executable enterprise ontology runtime for World Model DB.
//!
//! The crate turns the durable ontology catalog into write-time constraints,
//! computed state, graph inference, guarded actions, permissions, mappings,
//! identity resolution, consistency checks, and semantic graph analytics.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use wm_core::*;
use wm_storage::WorldState;
use wm_temporal::interval_contains;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub code: String,
    pub object_id: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConsistencyReport {
    pub checked_entities: usize,
    pub checked_relationships: usize,
    pub violations: Vec<Violation>,
}

impl ConsistencyReport {
    pub fn is_consistent(&self) -> bool {
        self.violations.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolutionMatch {
    pub left: EntityId,
    pub right: EntityId,
    pub score: f64,
    pub equivalent: bool,
    pub evidence: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MappedEntity {
    pub id: EntityId,
    pub entity_type: String,
    pub attributes: BTreeMap<String, ObjectValue>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SemanticQuery {
    pub object_type: Option<String>,
    pub conditions: Vec<Condition>,
    pub traverse_relationship: Option<String>,
    pub from_entity: Option<EntityId>,
    pub max_depth: usize,
    pub valid_at: Option<String>,
    pub include_inferred: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SemanticQueryResult {
    pub entities: Vec<Entity>,
    pub relationships: Vec<Relationship>,
    pub aggregates: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CentralityScore {
    pub entity_id: EntityId,
    pub degree: f64,
    pub betweenness: f64,
    pub pagerank: f64,
    pub business_weight: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FormalStateSummary {
    pub entities: usize,
    pub relationships: usize,
    pub schema_types: usize,
    pub constraints: usize,
    pub rules: usize,
    pub functions: usize,
    pub actions: usize,
    pub permissions: usize,
    pub history_records: usize,
    pub provenance_records: usize,
}

pub fn active_object_type<'a>(
    catalog: &'a OntologyCatalog,
    name: &str,
) -> Option<&'a ObjectTypeDefinition> {
    catalog
        .object_types
        .iter()
        .filter(|definition| definition.name.eq_ignore_ascii_case(name))
        .max_by_key(|definition| definition.version)
}

pub fn active_relationship_type<'a>(
    catalog: &'a OntologyCatalog,
    name: &str,
) -> Option<&'a RelationshipTypeDefinition> {
    catalog
        .relationship_types
        .iter()
        .filter(|definition| definition.name.eq_ignore_ascii_case(name))
        .max_by_key(|definition| definition.version)
}

pub fn derived_classes(state: &WorldState, entity: &Entity) -> Vec<String> {
    state
        .ontology
        .derived_classes
        .iter()
        .filter(|definition| {
            is_a(&state.ontology, &entity.entity_type, &definition.base_type)
                && conditions_match(state, entity, &definition.conditions)
        })
        .map(|definition| definition.name.clone())
        .collect()
}

pub fn has_semantic_type(state: &WorldState, entity: &Entity, object_type: &str) -> bool {
    is_a(&state.ontology, &entity.entity_type, object_type)
        || derived_classes(state, entity)
            .iter()
            .any(|derived| derived.eq_ignore_ascii_case(object_type))
}

pub fn is_a(catalog: &OntologyCatalog, child: &str, parent: &str) -> bool {
    if child.eq_ignore_ascii_case(parent) {
        return true;
    }
    let mut queue = VecDeque::from([child.to_owned()]);
    let mut visited = BTreeSet::new();
    while let Some(current) = queue.pop_front() {
        if !visited.insert(current.to_ascii_lowercase()) {
            continue;
        }
        if let Some(definition) = active_object_type(catalog, &current) {
            for candidate in &definition.parent_types {
                if candidate.eq_ignore_ascii_case(parent) {
                    return true;
                }
                queue.push_back(candidate.clone());
            }
        }
    }
    false
}

pub fn effective_properties(
    catalog: &OntologyCatalog,
    object_type: &str,
) -> Result<BTreeMap<String, PropertySchema>, String> {
    let mut properties = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    collect_properties(catalog, object_type, &mut visiting, &mut properties)?;
    Ok(properties)
}

fn collect_properties(
    catalog: &OntologyCatalog,
    object_type: &str,
    visiting: &mut BTreeSet<String>,
    output: &mut BTreeMap<String, PropertySchema>,
) -> Result<(), String> {
    let key = object_type.to_ascii_lowercase();
    if !visiting.insert(key.clone()) {
        return Err(format!("subtype cycle includes '{object_type}'"));
    }
    let definition = active_object_type(catalog, object_type)
        .ok_or_else(|| format!("ontology object type '{object_type}' is not defined"))?;
    for parent in &definition.parent_types {
        collect_properties(catalog, parent, visiting, output)?;
    }
    for interface_name in &definition.interfaces {
        let interface = catalog
            .interfaces
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(interface_name))
            .ok_or_else(|| format!("interface '{interface_name}' is not defined"))?;
        for property in &interface.required_properties {
            output.insert(property.name.clone(), property.clone());
        }
    }
    for property in &definition.properties {
        output.insert(property.name.clone(), property.clone());
    }
    visiting.remove(&key);
    Ok(())
}

pub fn validate_entity(catalog: &OntologyCatalog, entity: &Entity) -> Vec<Violation> {
    if catalog.object_types.is_empty() {
        return Vec::new();
    }
    let properties = match effective_properties(catalog, &entity.entity_type) {
        Ok(properties) => properties,
        Err(message) => {
            return vec![Violation {
                code: "unknown_type".into(),
                object_id: entity.id.to_string(),
                message,
            }];
        }
    };
    let mut violations = Vec::new();
    for schema in properties.values() {
        match entity.attributes.get(&schema.name) {
            None if schema.min_count > 0 => violations.push(Violation {
                code: "required_property".into(),
                object_id: entity.id.to_string(),
                message: format!(
                    "type '{}' requires property '{}'",
                    entity.entity_type, schema.name
                ),
            }),
            Some(value) => validate_value(entity.id.as_ref(), schema, value, &mut violations),
            None => {}
        }
    }
    for property in entity.attributes.keys() {
        if !properties.contains_key(property) {
            violations.push(Violation {
                code: "unknown_property".into(),
                object_id: entity.id.to_string(),
                message: format!(
                    "property '{property}' is not declared for type '{}'",
                    entity.entity_type
                ),
            });
        }
    }
    violations
}

fn validate_value(
    object_id: &str,
    schema: &PropertySchema,
    value: &ObjectValue,
    violations: &mut Vec<Violation>,
) {
    if value.value_type() != schema.value_type {
        violations.push(Violation {
            code: "property_type".into(),
            object_id: object_id.into(),
            message: format!(
                "property '{}' expects {:?}, received {:?}",
                schema.name,
                schema.value_type,
                value.value_type()
            ),
        });
    }
    if !schema.allowed_values.is_empty() && !schema.allowed_values.contains(value) {
        violations.push(Violation {
            code: "allowed_values".into(),
            object_id: object_id.into(),
            message: format!("property '{}' has a disallowed value", schema.name),
        });
    }
}

pub fn validate_observation(
    state: &WorldState,
    entity: &Entity,
    predicate: &str,
    object: &ObjectValue,
    cardinality: &PredicateCardinality,
) -> Vec<Violation> {
    if state.ontology.object_types.is_empty() {
        return Vec::new();
    }
    let Ok(properties) = effective_properties(&state.ontology, &entity.entity_type) else {
        return validate_entity(&state.ontology, entity);
    };
    let Some(schema) = properties.get(predicate) else {
        return vec![Violation {
            code: "unknown_property".into(),
            object_id: entity.id.to_string(),
            message: format!(
                "property '{predicate}' is not declared for type '{}'",
                entity.entity_type
            ),
        }];
    };
    let mut violations = Vec::new();
    validate_value(entity.id.as_ref(), schema, object, &mut violations);
    if schema.max_count == Some(1) && cardinality != &PredicateCardinality::SingleExclusive {
        violations.push(Violation {
            code: "cardinality".into(),
            object_id: entity.id.to_string(),
            message: format!("property '{predicate}' has maximum cardinality 1"),
        });
    }
    violations
}

pub fn validate_relationship(
    state: &WorldState,
    source: &Entity,
    relationship_type: &str,
    target: &Entity,
) -> Vec<Violation> {
    if state.ontology.relationship_types.is_empty() {
        return Vec::new();
    }
    let Some(definition) = active_relationship_type(&state.ontology, relationship_type) else {
        return vec![Violation {
            code: "unknown_relationship_type".into(),
            object_id: format!("{}->{}", source.id, target.id),
            message: format!("relationship type '{relationship_type}' is not defined"),
        }];
    };
    let mut violations = Vec::new();
    if !definition.domain_types.is_empty()
        && !definition
            .domain_types
            .iter()
            .any(|domain| is_a(&state.ontology, &source.entity_type, domain))
    {
        violations.push(Violation {
            code: "relationship_domain".into(),
            object_id: source.id.to_string(),
            message: format!(
                "type '{}' is outside the domain of '{relationship_type}'",
                source.entity_type
            ),
        });
    }
    if !definition.range_types.is_empty()
        && !definition
            .range_types
            .iter()
            .any(|range| is_a(&state.ontology, &target.entity_type, range))
    {
        violations.push(Violation {
            code: "relationship_range".into(),
            object_id: target.id.to_string(),
            message: format!(
                "type '{}' is outside the range of '{relationship_type}'",
                target.entity_type
            ),
        });
    }
    let outgoing = state
        .current_relationships()
        .filter(|relationship| {
            relationship.source_entity_id == source.id
                && relationship
                    .relationship_type
                    .eq_ignore_ascii_case(relationship_type)
        })
        .count();
    if definition
        .max_outgoing
        .is_some_and(|maximum| outgoing >= maximum)
    {
        violations.push(Violation {
            code: "relationship_cardinality".into(),
            object_id: source.id.to_string(),
            message: format!("relationship '{relationship_type}' exceeds its outgoing maximum"),
        });
    }
    if definition.acyclic && reachable(state, &target.id, &source.id, Some(relationship_type)) {
        violations.push(Violation {
            code: "relationship_cycle".into(),
            object_id: source.id.to_string(),
            message: format!("relationship '{relationship_type}' would create a cycle"),
        });
    }
    violations
}

pub fn schema_compatibility(
    catalog: &OntologyCatalog,
    older: &ObjectTypeDefinition,
    newer: &ObjectTypeDefinition,
) -> Vec<String> {
    let mut issues = Vec::new();
    let old = older
        .properties
        .iter()
        .map(|property| (property.name.to_ascii_lowercase(), property))
        .collect::<BTreeMap<_, _>>();
    let new = newer
        .properties
        .iter()
        .map(|property| (property.name.to_ascii_lowercase(), property))
        .collect::<BTreeMap<_, _>>();
    for (name, property) in &old {
        match new.get(name) {
            None => issues.push(format!("removed property '{}'", property.name)),
            Some(next) if next.value_type != property.value_type => {
                issues.push(format!("changed type of property '{}'", property.name))
            }
            Some(next)
                if next.max_count.is_some()
                    && property
                        .max_count
                        .is_none_or(|old_max| next.max_count.unwrap_or(old_max) < old_max) =>
            {
                issues.push(format!(
                    "narrowed cardinality of property '{}'",
                    property.name
                ))
            }
            _ => {}
        }
    }
    for (name, property) in &new {
        if !old.contains_key(name) && property.min_count > 0 {
            issues.push(format!("added required property '{}'", property.name));
        }
    }
    if !is_a(catalog, &newer.name, &older.name) && older.name != newer.name {
        issues.push("new type is not compatible with the prior type hierarchy".into());
    }
    issues
}

pub fn computed_properties(
    state: &WorldState,
    entity_id: &EntityId,
) -> Result<BTreeMap<String, ObjectValue>, String> {
    let entity = state
        .entity(entity_id)
        .ok_or_else(|| format!("entity {entity_id} not found"))?;
    let mut output = BTreeMap::new();
    for definition in state
        .ontology
        .computed_properties
        .iter()
        .filter(|definition| {
            is_a(
                &state.ontology,
                &entity.entity_type,
                &definition.target_type,
            )
        })
    {
        let (function, argument) = definition
            .expression
            .split_once(':')
            .ok_or_else(|| format!("invalid computed expression '{}'", definition.expression))?;
        let value = match function {
            "copy" => entity_value(state, entity, argument).cloned(),
            "count_out" => Some(ObjectValue::Integer(
                state
                    .current_relationships()
                    .filter(|relationship| {
                        relationship.source_entity_id == *entity_id
                            && relationship
                                .relationship_type
                                .eq_ignore_ascii_case(argument)
                    })
                    .count() as i64,
            )),
            "count_in" => Some(ObjectValue::Integer(
                state
                    .current_relationships()
                    .filter(|relationship| {
                        relationship.target_entity_id == *entity_id
                            && relationship
                                .relationship_type
                                .eq_ignore_ascii_case(argument)
                    })
                    .count() as i64,
            )),
            "exists_out" => Some(ObjectValue::Boolean(state.current_relationships().any(
                |relationship| {
                    relationship.source_entity_id == *entity_id
                        && relationship
                            .relationship_type
                            .eq_ignore_ascii_case(argument)
                },
            ))),
            "sum" => {
                let total = values_for_property(state, entity, argument)
                    .into_iter()
                    .filter_map(|value| match value {
                        ObjectValue::Integer(value) => Some(*value as f64),
                        ObjectValue::Float(value) => Some(*value),
                        _ => None,
                    })
                    .sum::<f64>();
                Some(ObjectValue::Float(total))
            }
            _ => return Err(format!("unsupported computed function '{function}'")),
        };
        if let Some(value) = value {
            output.insert(definition.property.clone(), value);
        }
    }
    Ok(output)
}

pub fn materialize_computed(
    state: &mut WorldState,
    entity_id: &EntityId,
    now: &str,
) -> Result<Vec<FactId>, String> {
    let values = computed_properties(state, entity_id)?;
    let materialized = state
        .ontology
        .computed_properties
        .iter()
        .filter(|definition| definition.materialized)
        .map(|definition| definition.property.clone())
        .collect::<BTreeSet<_>>();
    let mut ids = Vec::new();
    for (property, value) in values {
        if !materialized.contains(&property) {
            continue;
        }
        for fact in state.facts.iter_mut().filter(|fact| {
            fact.subject_entity_id == *entity_id
                && fact.predicate == property
                && fact.known_to.is_none()
                && fact.resolution_rule == "ontology computed property"
        }) {
            fact.known_to = Some(now.to_owned());
            fact.status = FactStatus::Superseded;
        }
        let id = FactId(state.next_id("fact"));
        state.facts.push(Fact {
            id: id.clone(),
            subject_entity_id: entity_id.clone(),
            predicate: property,
            object: value,
            valid_from: now.into(),
            valid_to: None,
            known_from: now.into(),
            known_to: None,
            confidence: 1.0,
            status: FactStatus::Supported,
            created_from_observations: Vec::new(),
            confidence_explanation: "deterministic ontology computation".into(),
            resolution_rule: "ontology computed property".into(),
            created_at: now.into(),
        });
        ids.push(id);
    }
    Ok(ids)
}

pub fn materialize_inference(state: &mut WorldState, now: &str) -> Vec<RelationshipId> {
    let mut created = Vec::new();
    let mut known = state
        .current_relationships()
        .map(|relationship| {
            (
                relationship.source_entity_id.clone(),
                relationship.relationship_type.to_ascii_lowercase(),
                relationship.target_entity_id.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut pending = BTreeSet::new();
    let active = state.current_relationships().cloned().collect::<Vec<_>>();
    for relationship in &active {
        if let Some(definition) =
            active_relationship_type(&state.ontology, &relationship.relationship_type)
        {
            if definition.symmetric {
                pending.insert((
                    relationship.target_entity_id.clone(),
                    definition.name.clone(),
                    relationship.source_entity_id.clone(),
                ));
            }
            if let Some(inverse) = &definition.inverse_of {
                pending.insert((
                    relationship.target_entity_id.clone(),
                    inverse.clone(),
                    relationship.source_entity_id.clone(),
                ));
            }
            if definition.transitive {
                for next in &active {
                    if next.source_entity_id == relationship.target_entity_id
                        && next
                            .relationship_type
                            .eq_ignore_ascii_case(&relationship.relationship_type)
                    {
                        pending.insert((
                            relationship.source_entity_id.clone(),
                            definition.name.clone(),
                            next.target_entity_id.clone(),
                        ));
                    }
                }
            }
            for composition in &definition.compositions {
                for next in &active {
                    if next.source_entity_id == relationship.target_entity_id
                        && next
                            .relationship_type
                            .eq_ignore_ascii_case(&composition.then_relationship)
                    {
                        pending.insert((
                            relationship.source_entity_id.clone(),
                            composition.implies_relationship.clone(),
                            next.target_entity_id.clone(),
                        ));
                    }
                }
            }
        }
    }
    for rule in state
        .ontology
        .inference_rules
        .iter()
        .filter(|rule| rule.materialized && !rule.relationship_path.is_empty())
    {
        for start in &state.entities {
            let mut frontier = BTreeSet::from([start.id.clone()]);
            for relationship_type in &rule.relationship_path {
                frontier = frontier
                    .iter()
                    .flat_map(|entity_id| {
                        active
                            .iter()
                            .filter(move |relationship| {
                                relationship.source_entity_id == *entity_id
                                    && relationship
                                        .relationship_type
                                        .eq_ignore_ascii_case(relationship_type)
                            })
                            .map(|relationship| relationship.target_entity_id.clone())
                    })
                    .collect();
            }
            for target in frontier {
                pending.insert((start.id.clone(), rule.implies_relationship.clone(), target));
            }
        }
    }
    for (source, relationship_type, target) in pending {
        let key = (
            source.clone(),
            relationship_type.to_ascii_lowercase(),
            target.clone(),
        );
        if source == target || !known.insert(key) {
            continue;
        }
        let id = RelationshipId(state.next_id("rel"));
        state.relationships.push(Relationship {
            id: id.clone(),
            source_entity_id: source,
            relationship_type,
            target_entity_id: target,
            valid_from: now.into(),
            valid_to: None,
            known_from: now.into(),
            known_to: None,
            confidence: 1.0,
            status: FactStatus::Supported,
            evidence_ids: Vec::new(),
            resolution_rule: "ontology materialized inference".into(),
            created_at: now.into(),
        });
        created.push(id);
    }
    created
}

pub fn authorize(
    state: &WorldState,
    principal: &str,
    roles: &[String],
    action: &str,
    entity: &Entity,
) -> bool {
    let mut matching = state
        .ontology
        .permissions
        .iter()
        .filter(|rule| rule.action == "*" || rule.action.eq_ignore_ascii_case(action))
        .filter(|rule| {
            rule.principal
                .as_deref()
                .is_none_or(|candidate| candidate == "*" || candidate == principal)
        })
        .filter(|rule| {
            rule.role
                .as_ref()
                .is_none_or(|role| role == "*" || roles.iter().any(|candidate| candidate == role))
        })
        .filter(|rule| {
            rule.object_type
                .as_deref()
                .is_none_or(|object_type| is_a(&state.ontology, &entity.entity_type, object_type))
                && rule.object_id.as_ref().is_none_or(|id| id == &entity.id)
                && conditions_match(state, entity, &rule.conditions)
        })
        .collect::<Vec<_>>();
    if matching.is_empty() {
        return state.ontology.permissions.is_empty();
    }
    matching.sort_by(|left, right| {
        right.priority.cmp(&left.priority).then_with(|| {
            matches!(right.effect, PermissionEffect::Deny)
                .cmp(&matches!(left.effect, PermissionEffect::Deny))
        })
    });
    matching
        .first()
        .is_some_and(|rule| rule.effect == PermissionEffect::Allow)
}

pub fn authorized_entities(
    state: &WorldState,
    principal: &str,
    roles: &[String],
    action: &str,
) -> Vec<Entity> {
    state
        .entities
        .iter()
        .filter(|entity| authorize(state, principal, roles, action, entity))
        .cloned()
        .collect()
}

pub fn execute_action(
    state: &mut WorldState,
    action_name: &str,
    actor: &str,
    roles: &[String],
    target: &EntityId,
    now: &str,
) -> Result<ActionExecutionId, String> {
    let action = state
        .ontology
        .actions
        .iter()
        .find(|action| action.name.eq_ignore_ascii_case(action_name))
        .cloned()
        .ok_or_else(|| format!("action '{action_name}' is not defined"))?;
    let entity = state
        .entity(target)
        .cloned()
        .ok_or_else(|| format!("entity {target} not found"))?;
    if !is_a(&state.ontology, &entity.entity_type, &action.target_type) {
        return Err(format!(
            "action '{}' does not apply to type '{}'",
            action.name, entity.entity_type
        ));
    }
    if !action.allowed_roles.is_empty()
        && !roles
            .iter()
            .any(|role| action.allowed_roles.iter().any(|allowed| allowed == role))
    {
        return Err(format!("actor '{actor}' lacks an action role"));
    }
    if !authorize(state, actor, roles, &action.name, &entity) {
        return Err(format!(
            "actor '{actor}' is not authorized for action '{}'",
            action.name
        ));
    }
    if !conditions_match(state, &entity, &action.preconditions) {
        return Err(format!("preconditions failed for action '{}'", action.name));
    }
    for effect in &action.effects {
        if let ActionEffect::AddRelationship {
            relationship_type,
            target_entity_id,
        } = effect
        {
            let target_entity = state
                .entity(target_entity_id)
                .ok_or_else(|| format!("relationship target {target_entity_id} not found"))?;
            let violations =
                validate_relationship(state, &entity, relationship_type, target_entity);
            if let Some(violation) = violations.first() {
                return Err(violation.message.clone());
            }
        }
    }
    let before = entity.attributes.clone();
    let original_relationship_count = state.relationships.len();
    let original_event_count = state.events.len();
    let entity_position = state
        .entities
        .iter()
        .position(|candidate| candidate.id == *target)
        .expect("entity was found");
    for effect in &action.effects {
        match effect {
            ActionEffect::SetProperty { property, value } => {
                state.entities[entity_position]
                    .attributes
                    .insert(property.clone(), value.clone());
            }
            ActionEffect::RemoveProperty { property } => {
                state.entities[entity_position].attributes.remove(property);
            }
            ActionEffect::AddRelationship {
                relationship_type,
                target_entity_id,
            } => {
                let id = RelationshipId(state.next_id("rel"));
                state.relationships.push(Relationship {
                    id,
                    source_entity_id: target.clone(),
                    relationship_type: relationship_type.clone(),
                    target_entity_id: target_entity_id.clone(),
                    valid_from: now.into(),
                    valid_to: None,
                    known_from: now.into(),
                    known_to: None,
                    confidence: 1.0,
                    status: FactStatus::Supported,
                    evidence_ids: Vec::new(),
                    resolution_rule: format!("ontology action {}", action.name),
                    created_at: now.into(),
                });
            }
            ActionEffect::EmitEvent { event_type } => {
                let id = EventId(state.next_id("event"));
                state.events.push(Event {
                    id,
                    event_type: event_type.clone(),
                    timestamp: now.into(),
                    end_timestamp: None,
                    entities: vec![target.clone()],
                    attributes: BTreeMap::from([(
                        "action".into(),
                        ObjectValue::String(action.name.clone()),
                    )]),
                    source_observations: Vec::new(),
                    confidence: 1.0,
                });
            }
        }
    }
    let after_entity = state.entities[entity_position].clone();
    let violations = validate_entity(&state.ontology, &after_entity);
    if !violations.is_empty() || !conditions_match(state, &after_entity, &action.postconditions) {
        state.entities[entity_position].attributes = before;
        state.relationships.truncate(original_relationship_count);
        state.events.truncate(original_event_count);
        return Err(format!(
            "postconditions failed for action '{}'",
            action.name
        ));
    }
    let id = ActionExecutionId(state.next_id("action_execution"));
    state.ontology.action_executions.push(ActionExecution {
        id: id.clone(),
        action_id: action.id,
        actor: actor.into(),
        roles: roles.to_vec(),
        target_entity_id: target.clone(),
        occurred_at: now.into(),
        succeeded: true,
        message: "action committed".into(),
        before,
        after: after_entity.attributes,
    });
    Ok(id)
}

pub fn map_record(
    mapping: &SchemaMapping,
    record: &BTreeMap<String, String>,
) -> Result<MappedEntity, String> {
    let mut semantic_id = mapping.semantic_id_template.clone();
    for (field, value) in record {
        semantic_id = semantic_id.replace(&format!("{{{field}}}"), value);
    }
    if semantic_id.contains('{') {
        return Err("semantic ID template references a missing field".into());
    }
    let mut attributes = BTreeMap::new();
    for field in &mapping.fields {
        let raw = record
            .get(&field.source_field)
            .ok_or_else(|| format!("source field '{}' is missing", field.source_field))?;
        let transformed = match &field.transform {
            FieldTransform::Identity => raw.clone(),
            FieldTransform::Lowercase => raw.to_lowercase(),
            FieldTransform::Uppercase => raw.to_uppercase(),
            FieldTransform::Trim => raw.trim().into(),
            FieldTransform::Prefix(prefix) => format!("{prefix}{raw}"),
        };
        attributes.insert(
            field.target_property.clone(),
            ObjectValue::String(transformed),
        );
    }
    Ok(MappedEntity {
        id: semantic_id.into(),
        entity_type: mapping.target_type.clone(),
        attributes,
    })
}

pub fn resolve_entities(
    catalog: &OntologyCatalog,
    left: &Entity,
    right: &Entity,
) -> Result<ResolutionMatch, String> {
    if !left.entity_type.eq_ignore_ascii_case(&right.entity_type) {
        return Ok(ResolutionMatch {
            left: left.id.clone(),
            right: right.id.clone(),
            score: 0.0,
            equivalent: false,
            evidence: vec!["different ontology types".into()],
        });
    }
    let identity = active_object_type(catalog, &left.entity_type)
        .and_then(|definition| definition.identity.as_ref())
        .ok_or_else(|| format!("type '{}' has no identity rule", left.entity_type))?;
    if identity.properties.len() != identity.weights.len() {
        return Err("identity properties and weights must have equal length".into());
    }
    let mut weighted = 0.0;
    let mut evidence = Vec::new();
    for (property, weight) in identity.properties.iter().zip(&identity.weights) {
        let left_value = entity_value_direct(left, property);
        let right_value = entity_value_direct(right, property);
        let feature = similarity(left_value, right_value);
        weighted += feature * weight;
        evidence.push(format!("{property}={feature:.3}*{weight:.3}"));
    }
    let score = 1.0 / (1.0 + (-weighted).exp());
    Ok(ResolutionMatch {
        left: left.id.clone(),
        right: right.id.clone(),
        score,
        equivalent: score >= identity.threshold,
        evidence,
    })
}

pub fn record_equivalence(state: &mut WorldState, resolution: &ResolutionMatch, now: &str) -> bool {
    if !resolution.equivalent
        || state.ontology.equivalences.iter().any(|existing| {
            (existing.left == resolution.left && existing.right == resolution.right)
                || (existing.left == resolution.right && existing.right == resolution.left)
        })
    {
        return false;
    }
    state.ontology.equivalences.push(EntityEquivalence {
        left: resolution.left.clone(),
        right: resolution.right.clone(),
        score: resolution.score,
        evidence: resolution.evidence.clone(),
        resolved_at: now.into(),
    });
    true
}

pub fn reachable(
    state: &WorldState,
    from: &EntityId,
    to: &EntityId,
    relationship_type: Option<&str>,
) -> bool {
    if from == to {
        return true;
    }
    let mut queue = VecDeque::from([from.clone()]);
    let mut visited = BTreeSet::from([from.clone()]);
    while let Some(current) = queue.pop_front() {
        for relationship in state.current_relationships().filter(|relationship| {
            relationship.source_entity_id == current
                && relationship_type.is_none_or(|candidate| {
                    relationship
                        .relationship_type
                        .eq_ignore_ascii_case(candidate)
                })
        }) {
            if &relationship.target_entity_id == to {
                return true;
            }
            if visited.insert(relationship.target_entity_id.clone()) {
                queue.push_back(relationship.target_entity_id.clone());
            }
        }
    }
    false
}

pub fn shortest_semantic_path(
    state: &WorldState,
    from: &EntityId,
    to: &EntityId,
) -> Option<(f64, Vec<EntityId>)> {
    let mut distances = BTreeMap::from([(from.clone(), 0.0_f64)]);
    let mut parent = BTreeMap::<EntityId, EntityId>::new();
    let mut unvisited = state
        .entities
        .iter()
        .map(|entity| entity.id.clone())
        .collect::<BTreeSet<_>>();
    while !unvisited.is_empty() {
        let current = unvisited
            .iter()
            .filter_map(|entity| {
                distances
                    .get(entity)
                    .map(|distance| (entity.clone(), *distance))
            })
            .min_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal))?;
        unvisited.remove(&current.0);
        if &current.0 == to {
            let mut path = vec![to.clone()];
            let mut cursor = to;
            while cursor != from {
                cursor = parent.get(cursor)?;
                path.push(cursor.clone());
            }
            path.reverse();
            return Some((current.1, path));
        }
        for relationship in state.current_relationships().filter(|relationship| {
            relationship.source_entity_id == current.0 || relationship.target_entity_id == current.0
        }) {
            let next = if relationship.source_entity_id == current.0 {
                &relationship.target_entity_id
            } else {
                &relationship.source_entity_id
            };
            if !unvisited.contains(next) {
                continue;
            }
            let weight = active_relationship_type(&state.ontology, &relationship.relationship_type)
                .map(|definition| definition.weight.max(0.000_001))
                .unwrap_or(1.0);
            let candidate = current.1 + weight;
            if distances
                .get(next)
                .is_none_or(|distance| candidate < *distance)
            {
                distances.insert(next.clone(), candidate);
                parent.insert(next.clone(), current.0.clone());
            }
        }
    }
    None
}

pub fn blast_radius(state: &WorldState, root: &EntityId, max_depth: usize) -> Vec<EntityId> {
    let mut output = Vec::new();
    let mut visited = BTreeSet::from([root.clone()]);
    let mut queue = VecDeque::from([(root.clone(), 0_usize)]);
    while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for relationship in state
            .current_relationships()
            .filter(|relationship| relationship.source_entity_id == current)
        {
            if visited.insert(relationship.target_entity_id.clone()) {
                output.push(relationship.target_entity_id.clone());
                queue.push_back((relationship.target_entity_id.clone(), depth + 1));
            }
        }
    }
    output
}

pub fn centrality(state: &WorldState) -> Vec<CentralityScore> {
    let count = state.entities.len().max(1) as f64;
    let mut pagerank = state
        .entities
        .iter()
        .map(|entity| (entity.id.clone(), 1.0 / count))
        .collect::<BTreeMap<_, _>>();
    for _ in 0..20 {
        let mut next = state
            .entities
            .iter()
            .map(|entity| (entity.id.clone(), 0.15 / count))
            .collect::<BTreeMap<_, _>>();
        for entity in &state.entities {
            let outgoing = state
                .current_relationships()
                .filter(|relationship| relationship.source_entity_id == entity.id)
                .collect::<Vec<_>>();
            if outgoing.is_empty() {
                continue;
            }
            let share = pagerank.get(&entity.id).copied().unwrap_or_default() * 0.85
                / outgoing.len() as f64;
            for relationship in outgoing {
                *next
                    .entry(relationship.target_entity_id.clone())
                    .or_default() += share;
            }
        }
        pagerank = next;
    }
    let mut betweenness = BTreeMap::<EntityId, f64>::new();
    for source in &state.entities {
        for target in &state.entities {
            if source.id >= target.id {
                continue;
            }
            if let Some((_, path)) = shortest_semantic_path(state, &source.id, &target.id) {
                for entity in path.iter().skip(1).take(path.len().saturating_sub(2)) {
                    *betweenness.entry(entity.clone()).or_default() += 1.0;
                }
            }
        }
    }
    state
        .entities
        .iter()
        .map(|entity| {
            let related = state
                .current_relationships()
                .filter(|relationship| {
                    relationship.source_entity_id == entity.id
                        || relationship.target_entity_id == entity.id
                })
                .collect::<Vec<_>>();
            let business_weight = related
                .iter()
                .map(|relationship| {
                    active_relationship_type(&state.ontology, &relationship.relationship_type)
                        .map(|definition| definition.weight)
                        .unwrap_or(1.0)
                })
                .sum();
            CentralityScore {
                entity_id: entity.id.clone(),
                degree: related.len() as f64,
                betweenness: betweenness.get(&entity.id).copied().unwrap_or_default(),
                pagerank: pagerank.get(&entity.id).copied().unwrap_or_default(),
                business_weight,
            }
        })
        .collect()
}

pub fn semantic_query(state: &WorldState, query: &SemanticQuery) -> SemanticQueryResult {
    let mut selected = if let Some(root) = &query.from_entity {
        let mut ids = BTreeSet::from([root.clone()]);
        let mut queue = VecDeque::from([(root.clone(), 0_usize)]);
        while let Some((current, depth)) = queue.pop_front() {
            if depth >= query.max_depth.max(1) {
                continue;
            }
            for relationship in state.current_relationships().filter(|relationship| {
                relationship.source_entity_id == current
                    && query.traverse_relationship.as_deref().is_none_or(|kind| {
                        relationship.relationship_type.eq_ignore_ascii_case(kind)
                    })
                    && query.valid_at.as_deref().is_none_or(|at| {
                        interval_contains(
                            &relationship.valid_from,
                            relationship.valid_to.as_deref(),
                            at,
                        )
                    })
                    && (query.include_inferred
                        || relationship.resolution_rule != "ontology materialized inference")
            }) {
                if ids.insert(relationship.target_entity_id.clone()) {
                    queue.push_back((relationship.target_entity_id.clone(), depth + 1));
                }
            }
        }
        state
            .entities
            .iter()
            .filter(|entity| ids.contains(&entity.id))
            .cloned()
            .collect::<Vec<_>>()
    } else {
        state.entities.clone()
    };
    selected.retain(|entity| {
        query
            .object_type
            .as_deref()
            .is_none_or(|object_type| has_semantic_type(state, entity, object_type))
            && conditions_match(state, entity, &query.conditions)
    });
    let selected_ids = selected
        .iter()
        .map(|entity| entity.id.clone())
        .collect::<BTreeSet<_>>();
    let relationships = state
        .current_relationships()
        .filter(|relationship| {
            selected_ids.contains(&relationship.source_entity_id)
                && selected_ids.contains(&relationship.target_entity_id)
                && query.valid_at.as_deref().is_none_or(|at| {
                    interval_contains(
                        &relationship.valid_from,
                        relationship.valid_to.as_deref(),
                        at,
                    )
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut aggregates = BTreeMap::new();
    for entity in &selected {
        *aggregates.entry(entity.entity_type.clone()).or_insert(0) += 1;
    }
    SemanticQueryResult {
        entities: selected,
        relationships,
        aggregates,
    }
}

pub fn check_consistency(state: &WorldState) -> ConsistencyReport {
    let mut report = ConsistencyReport {
        checked_entities: state.entities.len(),
        checked_relationships: state.relationships.len(),
        violations: Vec::new(),
    };
    for entity in &state.entities {
        report
            .violations
            .extend(validate_entity(&state.ontology, entity));
    }
    for relationship in state.current_relationships() {
        let Some(source) = state.entity(&relationship.source_entity_id) else {
            report.violations.push(Violation {
                code: "missing_source".into(),
                object_id: relationship.id.to_string(),
                message: "relationship source entity is missing".into(),
            });
            continue;
        };
        let Some(target) = state.entity(&relationship.target_entity_id) else {
            report.violations.push(Violation {
                code: "missing_target".into(),
                object_id: relationship.id.to_string(),
                message: "relationship target entity is missing".into(),
            });
            continue;
        };
        report.violations.extend(validate_relationship_existing(
            state,
            source,
            &relationship.relationship_type,
            target,
        ));
    }
    for definition in &state.ontology.object_types {
        for disjoint in &definition.disjoint_with {
            if is_a(&state.ontology, &definition.name, disjoint) {
                report.violations.push(Violation {
                    code: "disjointness".into(),
                    object_id: definition.name.clone(),
                    message: format!(
                        "type '{}' is disjoint with ancestor '{disjoint}'",
                        definition.name
                    ),
                });
            }
        }
        if let Err(message) = effective_properties(&state.ontology, &definition.name) {
            report.violations.push(Violation {
                code: "schema_hierarchy".into(),
                object_id: definition.name.clone(),
                message,
            });
        }
    }
    for definition in &state.ontology.derived_classes {
        match effective_properties(&state.ontology, &definition.base_type) {
            Ok(properties) => {
                for condition in &definition.conditions {
                    if !properties.contains_key(&condition.property) {
                        report.violations.push(Violation {
                            code: "derived_class_property".into(),
                            object_id: definition.name.clone(),
                            message: format!(
                                "derived class '{}' references unknown property '{}'",
                                definition.name, condition.property
                            ),
                        });
                    }
                }
            }
            Err(message) => report.violations.push(Violation {
                code: "derived_class_base".into(),
                object_id: definition.name.clone(),
                message,
            }),
        }
    }
    for definition in &state.ontology.computed_properties {
        match effective_properties(&state.ontology, &definition.target_type) {
            Ok(properties) if !properties.contains_key(&definition.property) => {
                report.violations.push(Violation {
                    code: "computed_property_schema".into(),
                    object_id: definition.id.to_string(),
                    message: format!(
                        "computed property '{}' is not declared on type '{}'",
                        definition.property, definition.target_type
                    ),
                });
            }
            Err(message) => report.violations.push(Violation {
                code: "computed_property_type".into(),
                object_id: definition.id.to_string(),
                message,
            }),
            _ => {}
        }
    }
    for rule in &state.ontology.inference_rules {
        for relationship_type in rule
            .relationship_path
            .iter()
            .chain(std::iter::once(&rule.implies_relationship))
        {
            if active_relationship_type(&state.ontology, relationship_type).is_none() {
                report.violations.push(Violation {
                    code: "inference_relationship".into(),
                    object_id: rule.id.to_string(),
                    message: format!(
                        "inference rule references undefined relationship '{relationship_type}'"
                    ),
                });
            }
        }
    }
    for action in &state.ontology.actions {
        if active_object_type(&state.ontology, &action.target_type).is_none() {
            report.violations.push(Violation {
                code: "action_target_type".into(),
                object_id: action.id.to_string(),
                message: format!("action target type '{}' is not defined", action.target_type),
            });
        }
        for effect in &action.effects {
            if let ActionEffect::AddRelationship {
                relationship_type, ..
            } = effect
                && active_relationship_type(&state.ontology, relationship_type).is_none()
            {
                report.violations.push(Violation {
                    code: "action_relationship".into(),
                    object_id: action.id.to_string(),
                    message: format!(
                        "action references undefined relationship '{relationship_type}'"
                    ),
                });
            }
        }
    }
    for mapping in &state.ontology.mappings {
        match effective_properties(&state.ontology, &mapping.target_type) {
            Ok(properties) => {
                for field in &mapping.fields {
                    if !properties.contains_key(&field.target_property) {
                        report.violations.push(Violation {
                            code: "mapping_property".into(),
                            object_id: mapping.id.to_string(),
                            message: format!(
                                "mapping targets unknown property '{}'",
                                field.target_property
                            ),
                        });
                    }
                }
            }
            Err(message) => report.violations.push(Violation {
                code: "mapping_target_type".into(),
                object_id: mapping.id.to_string(),
                message,
            }),
        }
    }
    for definition in &state.ontology.relationship_types {
        for object_type in definition
            .domain_types
            .iter()
            .chain(&definition.range_types)
        {
            if active_object_type(&state.ontology, object_type).is_none() {
                report.violations.push(Violation {
                    code: "relationship_endpoint_type".into(),
                    object_id: definition.name.clone(),
                    message: format!(
                        "relationship '{}' references undefined type '{object_type}'",
                        definition.name
                    ),
                });
            }
        }
        if definition.min_outgoing > 0 {
            for entity in state.entities.iter().filter(|entity| {
                definition
                    .domain_types
                    .iter()
                    .any(|domain| is_a(&state.ontology, &entity.entity_type, domain))
            }) {
                let count = state
                    .current_relationships()
                    .filter(|relationship| {
                        relationship.source_entity_id == entity.id
                            && relationship
                                .relationship_type
                                .eq_ignore_ascii_case(&definition.name)
                    })
                    .count();
                if count < definition.min_outgoing {
                    report.violations.push(Violation {
                        code: "required_existence".into(),
                        object_id: entity.id.to_string(),
                        message: format!(
                            "relationship '{}' requires at least {} outgoing link(s)",
                            definition.name, definition.min_outgoing
                        ),
                    });
                }
            }
        }
        if definition.connected {
            let participating = state
                .entities
                .iter()
                .filter(|entity| {
                    definition
                        .domain_types
                        .iter()
                        .chain(&definition.range_types)
                        .any(|object_type| is_a(&state.ontology, &entity.entity_type, object_type))
                })
                .map(|entity| entity.id.clone())
                .collect::<Vec<_>>();
            if let Some(root) = participating.first() {
                for entity in participating.iter().skip(1) {
                    if !reachable_undirected(state, root, entity, &definition.name) {
                        report.violations.push(Violation {
                            code: "relationship_connectedness".into(),
                            object_id: definition.name.clone(),
                            message: format!(
                                "relationship '{}' does not connect participating entity '{}'",
                                definition.name, entity
                            ),
                        });
                    }
                }
            }
        }
    }
    for module in &state.ontology.modules {
        for dependency in &module.dependencies {
            if !state
                .ontology
                .modules
                .iter()
                .any(|candidate| &candidate.id == dependency)
            {
                report.violations.push(Violation {
                    code: "module_dependency".into(),
                    object_id: module.id.to_string(),
                    message: format!("module dependency '{dependency}' is missing"),
                });
            }
        }
    }
    report
}

fn validate_relationship_existing(
    state: &WorldState,
    source: &Entity,
    relationship_type: &str,
    target: &Entity,
) -> Vec<Violation> {
    let mut clone = state.clone();
    clone.relationships.retain(|relationship| {
        !(relationship.source_entity_id == source.id
            && relationship.target_entity_id == target.id
            && relationship
                .relationship_type
                .eq_ignore_ascii_case(relationship_type))
    });
    validate_relationship(&clone, source, relationship_type, target)
}

fn reachable_undirected(
    state: &WorldState,
    from: &EntityId,
    to: &EntityId,
    relationship_type: &str,
) -> bool {
    let mut queue = VecDeque::from([from.clone()]);
    let mut visited = BTreeSet::from([from.clone()]);
    while let Some(current) = queue.pop_front() {
        for relationship in state.current_relationships().filter(|relationship| {
            relationship
                .relationship_type
                .eq_ignore_ascii_case(relationship_type)
                && (relationship.source_entity_id == current
                    || relationship.target_entity_id == current)
        }) {
            let next = if relationship.source_entity_id == current {
                &relationship.target_entity_id
            } else {
                &relationship.source_entity_id
            };
            if next == to {
                return true;
            }
            if visited.insert(next.clone()) {
                queue.push_back(next.clone());
            }
        }
    }
    false
}

pub fn formal_state_summary(state: &WorldState) -> FormalStateSummary {
    FormalStateSummary {
        entities: state.entities.len(),
        relationships: state.relationships.len(),
        schema_types: state.ontology.object_types.len()
            + state.ontology.relationship_types.len()
            + state.ontology.interfaces.len()
            + state.ontology.derived_classes.len(),
        constraints: state
            .ontology
            .object_types
            .iter()
            .map(|definition| definition.properties.len() + definition.disjoint_with.len())
            .sum::<usize>()
            + state.ontology.relationship_types.len(),
        rules: state.ontology.inference_rules.len(),
        functions: state.ontology.computed_properties.len(),
        actions: state.ontology.actions.len(),
        permissions: state.ontology.permissions.len(),
        history_records: state.observations.len()
            + state.facts.len()
            + state.relationships.len()
            + state.events.len()
            + state.ontology.action_executions.len(),
        provenance_records: state.evidence.len(),
    }
}

fn entity_value<'a>(
    state: &'a WorldState,
    entity: &'a Entity,
    property: &str,
) -> Option<&'a ObjectValue> {
    entity_value_direct(entity, property).or_else(|| {
        state
            .current_facts()
            .find(|fact| {
                fact.subject_entity_id == entity.id && fact.predicate.eq_ignore_ascii_case(property)
            })
            .map(|fact| &fact.object)
    })
}

fn entity_value_direct<'a>(entity: &'a Entity, property: &str) -> Option<&'a ObjectValue> {
    entity
        .attributes
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(property))
        .map(|(_, value)| value)
}

fn values_for_property<'a>(
    state: &'a WorldState,
    entity: &'a Entity,
    property: &str,
) -> Vec<&'a ObjectValue> {
    entity_value_direct(entity, property)
        .into_iter()
        .chain(
            state
                .current_facts()
                .filter(|fact| {
                    fact.subject_entity_id == entity.id
                        && fact.predicate.eq_ignore_ascii_case(property)
                })
                .map(|fact| &fact.object),
        )
        .collect()
}

fn conditions_match(state: &WorldState, entity: &Entity, conditions: &[Condition]) -> bool {
    conditions.iter().all(|condition| {
        let actual = entity_value(state, entity, &condition.property);
        match condition.operator {
            ComparisonOperator::Exists => actual.is_some(),
            ComparisonOperator::Equals => actual == condition.value.as_ref(),
            ComparisonOperator::NotEquals => actual != condition.value.as_ref(),
            ComparisonOperator::GreaterThan => {
                compare_values(actual, condition.value.as_ref()) == Some(Ordering::Greater)
            }
            ComparisonOperator::GreaterOrEqual => compare_values(actual, condition.value.as_ref())
                .is_some_and(|order| order != Ordering::Less),
            ComparisonOperator::LessThan => {
                compare_values(actual, condition.value.as_ref()) == Some(Ordering::Less)
            }
            ComparisonOperator::LessOrEqual => compare_values(actual, condition.value.as_ref())
                .is_some_and(|order| order != Ordering::Greater),
        }
    })
}

fn compare_values(left: Option<&ObjectValue>, right: Option<&ObjectValue>) -> Option<Ordering> {
    match (left?, right?) {
        (ObjectValue::Integer(left), ObjectValue::Integer(right)) => Some(left.cmp(right)),
        (ObjectValue::Float(left), ObjectValue::Float(right)) => left.partial_cmp(right),
        (ObjectValue::Integer(left), ObjectValue::Float(right)) => {
            (*left as f64).partial_cmp(right)
        }
        (ObjectValue::Float(left), ObjectValue::Integer(right)) => {
            left.partial_cmp(&(*right as f64))
        }
        (ObjectValue::String(left), ObjectValue::String(right))
        | (ObjectValue::Timestamp(left), ObjectValue::Timestamp(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn similarity(left: Option<&ObjectValue>, right: Option<&ObjectValue>) -> f64 {
    match (left, right) {
        (Some(left), Some(right)) if left == right => 1.0,
        (Some(ObjectValue::String(left)), Some(ObjectValue::String(right))) => {
            let left = tokens(left);
            let right = tokens(right);
            let union = left.union(&right).count();
            if union == 0 {
                0.0
            } else {
                left.intersection(&right).count() as f64 / union as f64
            }
        }
        _ => 0.0,
    }
}

fn tokens(value: &str) -> BTreeSet<String> {
    value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn property(name: &str, value_type: ValueType, required: bool) -> PropertySchema {
        PropertySchema {
            name: name.into(),
            value_type,
            min_count: usize::from(required),
            max_count: Some(1),
            allowed_values: Vec::new(),
        }
    }

    fn state() -> WorldState {
        let mut state = WorldState::default();
        state.ontology.object_types.push(ObjectTypeDefinition {
            name: "company".into(),
            namespace: "world".into(),
            version: 1,
            parent_types: Vec::new(),
            interfaces: Vec::new(),
            properties: vec![property("status", ValueType::String, true)],
            identity: Some(IdentityRule {
                properties: vec!["status".into()],
                weights: vec![2.0],
                threshold: 0.8,
            }),
            disjoint_with: Vec::new(),
        });
        state
            .ontology
            .relationship_types
            .push(RelationshipTypeDefinition {
                name: "owns".into(),
                version: 1,
                domain_types: vec!["company".into()],
                range_types: vec!["company".into()],
                min_outgoing: 0,
                max_outgoing: None,
                transitive: true,
                symmetric: false,
                inverse_of: Some("owned_by".into()),
                compositions: Vec::new(),
                acyclic: true,
                connected: false,
                weight: 1.0,
            });
        for name in ["a", "b", "c"] {
            state.entities.push(Entity {
                id: format!("company:{name}").into(),
                entity_type: "company".into(),
                canonical_name: name.into(),
                aliases: Vec::new(),
                attributes: BTreeMap::from([(
                    "status".into(),
                    ObjectValue::String("active".into()),
                )]),
                created_at: "2026-01-01T00:00:00Z".into(),
                retired_at: None,
            });
        }
        state.sync_indexes();
        state
    }

    #[test]
    fn validates_schema_and_materializes_graph_inference() {
        let mut state = state();
        assert!(validate_entity(&state.ontology, &state.entities[0]).is_empty());
        for (source, target) in [("company:a", "company:b"), ("company:b", "company:c")] {
            let id = RelationshipId(state.next_id("rel"));
            state.relationships.push(Relationship {
                id,
                source_entity_id: source.into(),
                relationship_type: "owns".into(),
                target_entity_id: target.into(),
                valid_from: "2026-01-01T00:00:00Z".into(),
                valid_to: None,
                known_from: "2026-01-01T00:00:00Z".into(),
                known_to: None,
                confidence: 1.0,
                status: FactStatus::Supported,
                evidence_ids: Vec::new(),
                resolution_rule: "test".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
            });
        }
        let created = materialize_inference(&mut state, "2026-01-02T00:00:00Z");
        assert!(created.len() >= 3);
        assert!(state.relationships.iter().any(|relationship| {
            relationship.source_entity_id.as_str() == "company:a"
                && relationship.target_entity_id.as_str() == "company:c"
                && relationship.relationship_type == "owns"
        }));
    }

    #[test]
    fn guarded_action_is_atomic_and_audited() {
        let mut state = state();
        state.ontology.actions.push(ActionDefinition {
            id: "action:close".into(),
            name: "close".into(),
            target_type: "company".into(),
            preconditions: vec![Condition {
                property: "status".into(),
                operator: ComparisonOperator::Equals,
                value: Some(ObjectValue::String("active".into())),
            }],
            effects: vec![ActionEffect::SetProperty {
                property: "status".into(),
                value: ObjectValue::String("closed".into()),
            }],
            postconditions: vec![Condition {
                property: "status".into(),
                operator: ComparisonOperator::Equals,
                value: Some(ObjectValue::String("closed".into())),
            }],
            allowed_roles: vec!["operator".into()],
        });
        let id = execute_action(
            &mut state,
            "close",
            "agent:ops",
            &["operator".into()],
            &EntityId::from("company:a"),
            "2026-01-02T00:00:00Z",
        )
        .unwrap();
        assert_eq!(id.as_str(), "action_execution:1");
        assert_eq!(state.ontology.action_executions.len(), 1);
    }

    #[test]
    fn deny_wins_equal_priority_and_object_filtering_is_fail_closed() {
        let mut state = state();
        state.ontology.permissions.extend([
            PermissionRule {
                id: "permission:allow".into(),
                principal: None,
                role: Some("reader".into()),
                action: "read".into(),
                object_type: Some("company".into()),
                object_id: None,
                conditions: Vec::new(),
                effect: PermissionEffect::Allow,
                priority: 10,
            },
            PermissionRule {
                id: "permission:deny-a".into(),
                principal: None,
                role: Some("reader".into()),
                action: "read".into(),
                object_type: None,
                object_id: Some("company:a".into()),
                conditions: Vec::new(),
                effect: PermissionEffect::Deny,
                priority: 10,
            },
        ]);
        assert!(!authorize(
            &state,
            "agent:test",
            &["reader".into()],
            "read",
            &state.entities[0]
        ));
        assert!(authorize(
            &state,
            "agent:test",
            &["reader".into()],
            "read",
            &state.entities[1]
        ));
        assert!(!authorize(
            &state,
            "agent:test",
            &["guest".into()],
            "read",
            &state.entities[1]
        ));
    }

    #[test]
    fn derived_classes_are_queryable_semantic_types() {
        let mut state = state();
        state.ontology.derived_classes.push(DerivedClassDefinition {
            name: "active_company".into(),
            base_type: "company".into(),
            conditions: vec![Condition {
                property: "status".into(),
                operator: ComparisonOperator::Equals,
                value: Some(ObjectValue::String("active".into())),
            }],
        });
        let result = semantic_query(
            &state,
            &SemanticQuery {
                object_type: Some("active_company".into()),
                ..SemanticQuery::default()
            },
        );
        assert_eq!(result.entities.len(), 3);
    }
}
