use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use wm_core::*;
use wm_resolution::{Engine, NewObservation, ResolutionEngine};

fn temp_db(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("wmdb-{name}-{}-{nonce}.wmdb", std::process::id()))
}

struct Fixture {
    engine: Engine,
    path: PathBuf,
    acme: EntityId,
    nova: EntityId,
    alice: EntityId,
    bob: EntityId,
    carol: EntityId,
    official: SourceId,
    blog: SourceId,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let path = temp_db(name);
        let mut engine = Engine::init(&path).unwrap();
        let mut entity = |kind: &str, name: &str| {
            engine
                .create_entity(kind, name, vec![], BTreeMap::new())
                .unwrap()
        };
        let acme = entity("company", "acme");
        let nova = entity("company", "nova");
        let alice = entity("person", "alice");
        let bob = entity("person", "bob");
        let carol = entity("person", "carol");
        let official = engine
            .create_source(
                "official",
                "https://acme.test",
                "official",
                100,
                BTreeMap::new(),
            )
            .unwrap();
        let blog = engine
            .create_source("blog", "https://blog.test", "blog", 10, BTreeMap::new())
            .unwrap();
        Self {
            engine,
            path,
            acme,
            nova,
            alice,
            bob,
            carol,
            official,
            blog,
        }
    }
    fn observe(
        &mut self,
        object: EntityId,
        observed: &str,
        known: &str,
        source: SourceId,
        confidence: f64,
        retracted: bool,
    ) -> ObservationId {
        self.engine
            .observe(NewObservation {
                source_id: source,
                subject_entity_id: self.acme.clone(),
                predicate: "CEO".into(),
                object: ObjectValue::Entity(object),
                observed_at: observed.into(),
                ingested_at: Some(known.into()),
                claimed_valid_from: None,
                claimed_valid_to: None,
                cardinality: PredicateCardinality::SingleExclusive,
                confidence,
                raw_payload: "test evidence".into(),
                metadata: BTreeMap::new(),
                retracted,
            })
            .unwrap()
    }
    fn seed_history(&mut self) {
        self.observe(
            self.alice.clone(),
            "2026-01-10T00:00:00Z",
            "2026-01-11T00:00:00Z",
            self.official.clone(),
            0.9,
            false,
        );
        self.observe(
            self.alice.clone(),
            "2026-01-12T00:00:00Z",
            "2026-01-13T00:00:00Z",
            self.official.clone(),
            0.95,
            false,
        );
        self.observe(
            self.carol.clone(),
            "2026-04-01T00:00:00Z",
            "2026-04-02T00:00:00Z",
            self.blog.clone(),
            0.3,
            false,
        );
        self.observe(
            self.bob.clone(),
            "2026-09-01T00:00:00Z",
            "2026-09-02T00:00:00Z",
            self.official.clone(),
            0.99,
            false,
        );
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn basic_observation_creates_fact_and_agreement_adds_support() {
    let mut f = Fixture::new("basic");
    f.observe(
        f.alice.clone(),
        "2026-01-10T00:00:00Z",
        "2026-01-11T00:00:00Z",
        f.official.clone(),
        0.9,
        false,
    );
    let first = f.engine.store.state.current_facts().next().unwrap();
    assert_eq!(first.status, FactStatus::Supported);
    let first_confidence = first.confidence;
    f.observe(
        f.alice.clone(),
        "2026-01-12T00:00:00Z",
        "2026-01-13T00:00:00Z",
        f.official.clone(),
        0.95,
        false,
    );
    let current = f.engine.store.state.current_facts().next().unwrap();
    assert_eq!(current.created_from_observations.len(), 2);
    assert!(current.confidence > first_confidence);
}

#[test]
fn conflicts_are_visible_and_newer_authoritative_source_wins() {
    let mut f = Fixture::new("conflict");
    f.seed_history();
    let current = f
        .engine
        .store
        .state
        .current_facts()
        .find(|x| x.predicate == "CEO")
        .unwrap();
    assert_eq!(current.object, ObjectValue::Entity(f.bob.clone()));
    assert!(
        f.engine
            .store
            .state
            .conflicts
            .iter()
            .any(|c| c.resolution_status == ConflictResolutionStatus::Open)
    );
    assert!(
        f.engine
            .store
            .state
            .facts
            .iter()
            .any(|x| x.object == ObjectValue::Entity(f.alice.clone())
                && x.status == FactStatus::Superseded)
    );
    assert!(
        f.engine
            .store
            .state
            .facts
            .iter()
            .any(|x| x.object == ObjectValue::Entity(f.carol.clone())
                && x.status == FactStatus::Contested)
    );
}

#[test]
fn valid_known_and_combined_bitemporal_queries_work() {
    let mut f = Fixture::new("bitemporal");
    f.seed_history();
    let feb = wm_query::entity_state(
        &f.engine.store.state,
        &f.acme,
        Some("2026-01-11T12:00:00Z"),
        Some("2026-02-01T00:00:00Z"),
    );
    assert_eq!(feb.len(), 1);
    assert_eq!(feb[0].object, ObjectValue::Entity(f.alice.clone()));
    let sep = wm_query::entity_state(
        &f.engine.store.state,
        &f.acme,
        Some("2026-09-03T00:00:00Z"),
        Some("2026-09-03T00:00:00Z"),
    );
    assert_eq!(sep.len(), 1);
    assert_eq!(sep[0].object, ObjectValue::Entity(f.bob.clone()));
}

#[test]
fn why_returns_full_provenance_and_conflicting_evidence() {
    let mut f = Fixture::new("why");
    f.seed_history();
    let id = f
        .engine
        .store
        .state
        .current_facts()
        .next()
        .unwrap()
        .id
        .clone();
    let why = wm_query::why(&f.engine.store.state, &id).unwrap();
    assert!(!why.observations.is_empty());
    assert!(!why.sources.is_empty());
    assert!(!why.conflicting_observations.is_empty());
    assert!(!why.fact.resolution_rule.is_empty());
}

#[test]
fn current_entity_state_and_changes_report_supersession() {
    let mut f = Fixture::new("changes");
    f.seed_history();
    let state = wm_query::entity_state(&f.engine.store.state, &f.acme, None, None);
    assert_eq!(state.len(), 1);
    let changes = wm_query::changes(
        &f.engine.store.state,
        &f.acme,
        "2026-01-01T00:00:00Z",
        "2026-10-01T00:00:00Z",
    );
    assert!(!changes.facts_added.is_empty());
    assert!(!changes.facts_removed_or_superseded.is_empty());
}

#[test]
fn relationship_and_temporal_traversal_share_entity_universe() {
    let mut f = Fixture::new("graph");
    f.engine
        .add_relationship(
            f.acme.clone(),
            "ACQUIRED",
            f.nova.clone(),
            "2026-09-15T00:00:00Z",
            None,
            1.0,
            vec![],
        )
        .unwrap();
    assert!(
        wm_graph::find_path(
            &f.engine.store.state,
            &f.acme,
            &f.nova,
            2,
            None,
            Some("2026-09-16T00:00:00Z")
        )
        .is_some()
    );
    assert!(
        wm_graph::find_path(
            &f.engine.store.state,
            &f.acme,
            &f.nova,
            2,
            None,
            Some("2026-09-14T00:00:00Z")
        )
        .is_none()
    );
}

#[test]
fn restart_preserves_state_and_rebuild_is_deterministic() {
    let mut f = Fixture::new("restart");
    f.seed_history();
    let current_before = f
        .engine
        .store
        .state
        .current_facts()
        .map(|x| (x.predicate.clone(), format!("{:?}", x.object)))
        .collect::<Vec<_>>();
    let reopened = Engine::open(&f.path).unwrap();
    assert_eq!(reopened.store.state.observations.len(), 4);
    assert_eq!(reopened.store.state.current_facts().count(), 1);
    f.engine.rebuild_current().unwrap();
    let current_after = f
        .engine
        .store
        .state
        .current_facts()
        .map(|x| (x.predicate.clone(), format!("{:?}", x.object)))
        .collect::<Vec<_>>();
    assert_eq!(current_before, current_after);
}

#[test]
fn retractions_remain_in_history_and_open_conflicts_remain_visible() {
    let mut f = Fixture::new("retract");
    f.seed_history();
    f.observe(
        f.bob.clone(),
        "2026-10-01T00:00:00Z",
        "2026-10-02T00:00:00Z",
        f.official.clone(),
        1.0,
        true,
    );
    assert!(
        f.engine
            .store
            .state
            .observations
            .iter()
            .any(|o| o.retracted)
    );
    assert!(
        f.engine
            .store
            .state
            .facts
            .iter()
            .any(|x| x.object == ObjectValue::Entity(f.bob.clone()) && x.known_to.is_some())
    );
    assert!(
        f.engine
            .store
            .state
            .conflicts
            .iter()
            .any(|c| c.resolution_status == ConflictResolutionStatus::Open)
    );
}

#[test]
fn world_diff_is_deterministic_and_query_extensions_execute() {
    let mut f = Fixture::new("diff");
    f.seed_history();
    let a = wm_query::diff_world(
        &f.engine.store.state,
        "2026-01-01T00:00:00Z",
        "2026-10-01T00:00:00Z",
    );
    let b = wm_query::diff_world(
        &f.engine.store.state,
        "2026-01-01T00:00:00Z",
        "2026-10-01T00:00:00Z",
    );
    assert_eq!(a, b);
    assert!(a.facts_added > 0);
    assert!(a.facts_superseded > 0);
    assert!(a.conflicts_opened > 0);
    let output = wm_query::execute(
        &f.engine.store.state,
        "STATE OF company:acme KNOWN AT '2026-02-01T00:00:00Z'",
    )
    .unwrap();
    assert!(output.contains("person:alice"));
    let aggregate = wm_query::execute(
        &f.engine.store.state,
        "SELECT predicate, COUNT(*) FROM wm_facts GROUP BY predicate",
    )
    .unwrap();
    assert!(aggregate.contains("\"predicate\":\"CEO\""));
    assert!(aggregate.contains("\"count\":1"));
}
