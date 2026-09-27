//! Temporal graph traversal over the same relationship IDs used by world state.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use wm_core::{EntityId, FactStatus, Relationship, RelationshipId};
use wm_storage::WorldState;
use wm_temporal::interval_contains;

#[derive(Clone, Debug, PartialEq)]
pub struct GraphPath {
    pub entities: Vec<EntityId>,
    pub relationships: Vec<RelationshipId>,
}

pub trait GraphStore {
    fn relationships(&self) -> &[Relationship];
}
impl GraphStore for WorldState {
    fn relationships(&self) -> &[Relationship] {
        &self.relationships
    }
}

pub fn find_path(
    state: &WorldState,
    from: &EntityId,
    to: &EntityId,
    max_depth: usize,
    relationship_type: Option<&str>,
    valid_at: Option<&str>,
) -> Option<GraphPath> {
    if from == to {
        return Some(GraphPath {
            entities: vec![from.clone()],
            relationships: vec![],
        });
    }
    let mut queue = VecDeque::from([from.clone()]);
    let mut visited = BTreeSet::from([from.clone()]);
    let mut parent: BTreeMap<EntityId, (EntityId, RelationshipId)> = BTreeMap::new();
    while let Some(current) = queue.pop_front() {
        let depth = path_depth(&parent, &current);
        if depth >= max_depth {
            continue;
        }
        let candidates = state
            .relationship_positions_for_entity(&current)
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
                        relationship.source_entity_id == current
                            || relationship.target_entity_id == current
                    })
                    .collect()
            });
        for relationship in candidates.into_iter().filter(|r| {
            r.status == FactStatus::Supported
                && r.known_to.is_none()
                && relationship_type
                    .is_none_or(|kind| r.relationship_type.eq_ignore_ascii_case(kind))
                && valid_at
                    .is_none_or(|at| interval_contains(&r.valid_from, r.valid_to.as_deref(), at))
        }) {
            let next = if relationship.source_entity_id == current {
                &relationship.target_entity_id
            } else {
                &relationship.source_entity_id
            };
            if visited.insert(next.clone()) {
                parent.insert(next.clone(), (current.clone(), relationship.id.clone()));
                if next == to {
                    return Some(reconstruct(&parent, from, to));
                }
                queue.push_back(next.clone());
            }
        }
    }
    None
}

fn path_depth(parent: &BTreeMap<EntityId, (EntityId, RelationshipId)>, node: &EntityId) -> usize {
    let mut depth = 0;
    let mut cursor = node;
    while let Some((p, _)) = parent.get(cursor) {
        depth += 1;
        cursor = p;
    }
    depth
}
fn reconstruct(
    parent: &BTreeMap<EntityId, (EntityId, RelationshipId)>,
    from: &EntityId,
    to: &EntityId,
) -> GraphPath {
    let mut entities = vec![to.clone()];
    let mut relationships = Vec::new();
    let mut cursor = to;
    while cursor != from {
        let (p, r) = &parent[cursor];
        relationships.push(r.clone());
        entities.push(p.clone());
        cursor = p;
    }
    entities.reverse();
    relationships.reverse();
    GraphPath {
        entities,
        relationships,
    }
}
