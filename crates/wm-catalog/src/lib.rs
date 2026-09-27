//! An in-memory entity catalog with deterministic alias resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use wm_core::{Entity, EntityId};

/// Normalize a human-readable name for matching.
///
/// Matching is case-insensitive and treats runs of punctuation or whitespace as
/// a single separator. The original names remain unchanged on [`Entity`].
pub fn normalize_alias(alias: &str) -> String {
    let mut normalized = String::new();
    let mut pending_separator = false;

    for character in alias.trim().chars() {
        if character.is_alphanumeric() {
            if pending_separator && !normalized.is_empty() {
                normalized.push(' ');
            }
            normalized.extend(character.to_lowercase());
            pending_separator = false;
        } else if !normalized.is_empty() {
            pending_separator = true;
        }
    }

    normalized
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogError {
    DuplicateEntity(EntityId),
    EmptyAlias,
    AmbiguousAlias {
        alias: String,
        candidates: Vec<EntityId>,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateEntity(id) => write!(formatter, "entity `{id}` is already registered"),
            Self::EmptyAlias => formatter.write_str("alias is empty after normalization"),
            Self::AmbiguousAlias { alias, candidates } => write!(
                formatter,
                "alias `{alias}` resolves to {} entities",
                candidates.len()
            ),
        }
    }
}

impl Error for CatalogError {}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Catalog {
    entities: BTreeMap<EntityId, Entity>,
    alias_index: BTreeMap<String, BTreeSet<EntityId>>,
}

/// A descriptive alias retained for callers that prefer the longer name.
pub type EntityCatalog = Catalog;
pub type AliasResolver = Catalog;

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entities.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }

    pub fn insert(&mut self, entity: Entity) -> Result<(), CatalogError> {
        if self.entities.contains_key(&entity.id) {
            return Err(CatalogError::DuplicateEntity(entity.id));
        }

        let entity_id = entity.id.clone();
        let names = std::iter::once(&entity.canonical_name).chain(entity.aliases.iter());
        let normalized_names: BTreeSet<String> = names
            .map(|name| normalize_alias(name))
            .filter(|name| !name.is_empty())
            .collect();

        for name in normalized_names {
            self.alias_index
                .entry(name)
                .or_default()
                .insert(entity_id.clone());
        }
        self.entities.insert(entity_id, entity);
        Ok(())
    }

    pub fn register(&mut self, entity: Entity) -> Result<(), CatalogError> {
        self.insert(entity)
    }

    pub fn add_entity(&mut self, entity: Entity) -> Result<(), CatalogError> {
        self.insert(entity)
    }

    pub fn get(&self, id: &EntityId) -> Option<&Entity> {
        self.entities.get(id)
    }

    pub fn contains(&self, id: &EntityId) -> bool {
        self.entities.contains_key(id)
    }

    pub fn entities(&self) -> impl Iterator<Item = &Entity> {
        self.entities.values()
    }

    pub fn resolve(&self, alias: &str) -> Result<Option<&Entity>, CatalogError> {
        let normalized = normalize_alias(alias);
        if normalized.is_empty() {
            return Err(CatalogError::EmptyAlias);
        }

        let Some(candidates) = self.alias_index.get(&normalized) else {
            return Ok(None);
        };
        if candidates.len() > 1 {
            return Err(CatalogError::AmbiguousAlias {
                alias: alias.to_owned(),
                candidates: candidates.iter().cloned().collect(),
            });
        }

        Ok(candidates
            .first()
            .and_then(|entity_id| self.entities.get(entity_id)))
    }

    pub fn resolve_alias(&self, alias: &str) -> Result<Option<&Entity>, CatalogError> {
        self.resolve(alias)
    }

    pub fn resolve_id(&self, alias: &str) -> Result<Option<&EntityId>, CatalogError> {
        self.resolve(alias)
            .map(|entity| entity.map(|entity| &entity.id))
    }

    pub fn resolve_all(&self, alias: &str) -> Vec<&Entity> {
        let normalized = normalize_alias(alias);
        self.alias_index
            .get(&normalized)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.entities.get(id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn entity(id: &str, name: &str, aliases: &[&str]) -> Entity {
        Entity {
            id: id.into(),
            entity_type: "organization".into(),
            canonical_name: name.into(),
            aliases: aliases.iter().map(|alias| (*alias).into()).collect(),
            attributes: BTreeMap::new(),
            created_at: "2026-01-01T00:00:00Z".into(),
            retired_at: None,
        }
    }

    #[test]
    fn normalization_is_stable() {
        assert_eq!(normalize_alias("  ACME,   Inc. "), "acme inc");
    }

    #[test]
    fn resolves_canonical_names_and_aliases() {
        let mut catalog = Catalog::new();
        catalog
            .insert(entity("one", "Acme Incorporated", &["ACME, Inc."]))
            .unwrap();

        assert_eq!(catalog.resolve_id("acme inc").unwrap(), Some(&"one".into()));
        assert_eq!(
            catalog.resolve_id("ACME INCORPORATED").unwrap(),
            Some(&"one".into())
        );
    }

    #[test]
    fn reports_ambiguous_aliases() {
        let mut catalog = Catalog::new();
        catalog.insert(entity("one", "First", &["shared"])).unwrap();
        catalog
            .insert(entity("two", "Second", &["shared"]))
            .unwrap();

        assert!(matches!(
            catalog.resolve("SHARED"),
            Err(CatalogError::AmbiguousAlias { candidates, .. }) if candidates.len() == 2
        ));
    }
}
