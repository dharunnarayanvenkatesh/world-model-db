use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::path::PathBuf;
use std::process::ExitCode;

use wm_agent::{AgentContextRequest, AgentGateway, AgentMemoryWrite, AgentRegistration};
use wm_core::*;
use wm_ingest::{Format, InputRecord};
use wm_resolution::{Engine, NewObservation, ResolutionEngine};

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wm: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(mut words: Vec<String>) -> Result<(), String> {
    if words.is_empty() || words[0] == "help" || words[0] == "--help" {
        print_help();
        return Ok(());
    }
    if words[0] == "--version" {
        println!("wm {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let db = take_option(&mut words, "--db")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".wmdb/world.wmdb"));
    let command = words.remove(0);
    if command == "init" {
        Engine::init(&db).map_err(|e| e.to_string())?;
        println!("initialized {}", db.display());
        return Ok(());
    }
    let mut engine = Engine::open(&db).map_err(|e| format!("open {}: {e}", db.display()))?;
    match command.as_str() {
        "entity" => entity_command(&mut engine, words),
        "source" => source_command(&mut engine, words),
        "observe" => observe_command(&mut engine, words),
        "ingest" | "load" => ingest_command(&mut engine, words),
        "demo" => ingest_command(&mut engine, vec!["examples/demo.jsonl".into()]),
        "state" => state_command(&engine, words),
        "why" => why_command(&engine, words),
        "conflicts" => conflicts_command(&engine),
        "changes" => changes_command(&engine, words),
        "diff-world" => diff_command(&engine, words),
        "path" => path_command(&engine, words),
        "query" => {
            println!(
                "{}",
                wm_query::execute(&engine.store.state, &words.join(" "))?
            );
            Ok(())
        }
        "agent" => agent_command(&mut engine, words),
        other => Err(format!("unknown command '{other}'; run `wm help`")),
    }
}

fn agent_command(engine: &mut Engine, mut words: Vec<String>) -> Result<(), String> {
    let action = shift(
        &mut words,
        "agent action (register|remember|context|session|tools)",
    )?;
    match action.as_str() {
        "tools" => {
            no_extra(&words)?;
            println!("{}", wm_agent::tool_manifest_json());
            Ok(())
        }
        "register" => {
            let agent_id = required_option(&mut words, "--id")?;
            let name = take_option(&mut words, "--name").unwrap_or_else(|| agent_id.clone());
            let model = take_option(&mut words, "--model").unwrap_or_else(|| "unspecified".into());
            let capabilities = take_all_options(&mut words, "--capability");
            let priority = take_option(&mut words, "--priority")
                .unwrap_or_else(|| "0".into())
                .parse::<i32>()
                .map_err(|_| "--priority must be an integer".to_owned())?;
            no_extra(&words)?;
            let source_id = AgentGateway::new(engine)
                .register(AgentRegistration {
                    agent_id: agent_id.clone(),
                    name,
                    model,
                    capabilities,
                    priority,
                })
                .map_err(|error| error.to_string())?;
            println!("agent_id\t{agent_id}\nsource_id\t{source_id}");
            Ok(())
        }
        "remember" => {
            let agent_id = required_option(&mut words, "--agent")?;
            let session_id = required_option(&mut words, "--session")?;
            let idempotency_key = required_option(&mut words, "--key")?;
            let subject_entity_id = required_option(&mut words, "--subject")?;
            let predicate = required_option(&mut words, "--predicate")?;
            let raw_object = required_option(&mut words, "--object")?;
            let object_type = take_option(&mut words, "--object-type");
            let observed_at = required_option(&mut words, "--observed-at")?;
            let confidence = parse_f64_option(&mut words, "--confidence", 1.0)?;
            let importance = parse_f64_option(&mut words, "--importance", 0.5)?;
            let tags = take_all_options(&mut words, "--tag");
            let memory_kind = take_option(&mut words, "--kind").unwrap_or_else(|| "fact".into());
            let raw_payload = take_option(&mut words, "--raw-payload").unwrap_or_default();
            no_extra(&words)?;
            let receipt = AgentGateway::new(engine)
                .remember(AgentMemoryWrite {
                    agent_id,
                    session_id,
                    idempotency_key,
                    subject_entity_id: subject_entity_id.into(),
                    predicate,
                    object: parse_object(&raw_object, object_type.as_deref()),
                    observed_at,
                    confidence,
                    importance,
                    tags,
                    memory_kind,
                    raw_payload,
                })
                .map_err(|error| error.to_string())?;
            println!("{}", wm_agent::memory_receipt_json(&receipt));
            Ok(())
        }
        "context" => {
            let request = AgentContextRequest {
                agent_id: required_option(&mut words, "--agent")?,
                session_id: take_option(&mut words, "--session"),
                entity_ids: take_all_options(&mut words, "--entity")
                    .into_iter()
                    .map(EntityId::from)
                    .collect(),
                predicates: take_all_options(&mut words, "--predicate"),
                valid_at: take_option(&mut words, "--valid-at"),
                known_at: take_option(&mut words, "--known-at"),
                max_facts: parse_usize_option(&mut words, "--max-facts", 32)?,
                char_budget: parse_usize_option(&mut words, "--char-budget", 12_000)?,
                include_conflicts: !take_flag(&mut words, "--no-conflicts"),
            };
            no_extra(&words)?;
            let bundle = AgentGateway::new(engine)
                .context(request)
                .map_err(|error| error.to_string())?;
            println!("{}", wm_agent::context_json(&bundle));
            Ok(())
        }
        "session" => {
            let agent_id = required_option(&mut words, "--agent")?;
            let session_id = required_option(&mut words, "--session")?;
            no_extra(&words)?;
            let memories = AgentGateway::new(engine).session_memory(&agent_id, &session_id);
            println!("{}", wm_agent::session_memory_json(&memories));
            Ok(())
        }
        _ => Err(format!("unknown agent action '{action}'")),
    }
}

fn entity_command(engine: &mut Engine, mut words: Vec<String>) -> Result<(), String> {
    let action = shift(&mut words, "entity action (create|get)")?;
    match action.as_str() {
        "create" => {
            let entity_type = required_option(&mut words, "--type")?;
            let name = required_option(&mut words, "--name")?;
            let aliases = take_all_options(&mut words, "--alias");
            no_extra(&words)?;
            let id = engine
                .create_entity(entity_type, name, aliases, BTreeMap::new())
                .map_err(|e| e.to_string())?;
            println!("{id}");
            Ok(())
        }
        "get" => {
            let id = shift(&mut words, "entity id")?;
            no_extra(&words)?;
            let entity = engine
                .store
                .state
                .entities
                .iter()
                .find(|e| e.id.as_str() == id)
                .ok_or_else(|| format!("entity {id} not found"))?;
            println!(
                "{}\t{}\t{}",
                entity.id, entity.entity_type, entity.canonical_name
            );
            Ok(())
        }
        _ => Err(format!("unknown entity action '{action}'")),
    }
}

fn source_command(engine: &mut Engine, mut words: Vec<String>) -> Result<(), String> {
    let action = shift(&mut words, "source action (create|get)")?;
    match action.as_str() {
        "create" => {
            let source_type =
                take_option(&mut words, "--type").unwrap_or_else(|| "document".into());
            let name = required_option(&mut words, "--name")?;
            let uri =
                take_option(&mut words, "--uri").unwrap_or_else(|| format!("urn:wm-source:{name}"));
            let priority = take_option(&mut words, "--priority")
                .unwrap_or_else(|| "0".into())
                .parse::<i32>()
                .map_err(|_| "--priority must be an integer".to_owned())?;
            no_extra(&words)?;
            let id = engine
                .create_source(source_type, uri, name, priority, BTreeMap::new())
                .map_err(|e| e.to_string())?;
            println!("{id}");
            Ok(())
        }
        "get" => {
            let id = shift(&mut words, "source id")?;
            no_extra(&words)?;
            let source = engine
                .store
                .state
                .sources
                .iter()
                .find(|e| e.id.as_str() == id)
                .ok_or_else(|| format!("source {id} not found"))?;
            println!(
                "{}\t{}\t{}\tpriority={}",
                source.id, source.source_type, source.name, source.priority
            );
            Ok(())
        }
        _ => Err(format!("unknown source action '{action}'")),
    }
}

fn observe_command(engine: &mut Engine, mut words: Vec<String>) -> Result<(), String> {
    let record = InputRecord {
        subject: required_option(&mut words, "--subject")?,
        predicate: required_option(&mut words, "--predicate")?,
        object: required_option(&mut words, "--object")?,
        source: required_option(&mut words, "--source")?,
        observed_at: required_option(&mut words, "--observed-at")?,
        confidence: take_option(&mut words, "--confidence")
            .unwrap_or_else(|| "1.0".into())
            .parse()
            .map_err(|_| "--confidence must be a number".to_owned())?,
        raw_payload: take_option(&mut words, "--raw-payload"),
        metadata: BTreeMap::new(),
    };
    let retracted = take_flag(&mut words, "--retracted");
    let object_type = take_option(&mut words, "--object-type");
    no_extra(&words)?;
    let id = append_record(engine, record, object_type.as_deref(), retracted, None)?;
    println!("{id}");
    Ok(())
}

fn ingest_command(engine: &mut Engine, mut words: Vec<String>) -> Result<(), String> {
    let path = PathBuf::from(shift(&mut words, "input file")?);
    let format = match take_option(&mut words, "--format").as_deref() {
        None => Format::from_path(&path).map_err(|e| e.to_string())?,
        Some("json") => Format::Json,
        Some("jsonl") | Some("ndjson") => Format::JsonLines,
        Some("csv") => Format::Csv,
        Some(other) => return Err(format!("unsupported format '{other}'")),
    };
    no_extra(&words)?;
    let records = wm_ingest::read(
        File::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?,
        format,
    )
    .map_err(|e| e.to_string())?;
    let mut ids = Vec::new();
    for record in records {
        ensure_input_dependencies(engine, &record)?;
        let ingested_at = Some(record.observed_at.clone());
        let relation_type = relation_predicate(&record.predicate).map(str::to_owned);
        let target = EntityId::from(record.object.as_str());
        let event_type = record.metadata.get("event_type").cloned();
        let observed_at = record.observed_at.clone();
        let subject = EntityId::from(record.subject.as_str());
        let id = append_record(engine, record, None, false, ingested_at)?;
        if let Some(kind) = relation_type {
            for old in engine.store.state.relationships.iter_mut().filter(|r| {
                r.source_entity_id == subject
                    && r.relationship_type == kind
                    && r.target_entity_id != target
                    && r.known_to.is_none()
            }) {
                old.valid_to = Some(observed_at.clone());
                old.known_to = Some(observed_at.clone());
                old.status = FactStatus::Superseded;
            }
            engine
                .add_relationship(
                    subject.clone(),
                    kind,
                    target.clone(),
                    observed_at.clone(),
                    None,
                    1.0,
                    vec![id.clone()],
                )
                .map_err(|e| e.to_string())?;
        }
        if let Some(kind) = event_type {
            engine
                .add_event(
                    kind,
                    observed_at,
                    None,
                    vec![subject, target],
                    BTreeMap::new(),
                    vec![id.clone()],
                    1.0,
                )
                .map_err(|e| e.to_string())?;
        }
        ids.push(id);
    }
    for id in &ids {
        println!("{id}");
    }
    eprintln!("ingested {} observation(s)", ids.len());
    Ok(())
}

fn append_record(
    engine: &mut Engine,
    record: InputRecord,
    kind: Option<&str>,
    retracted: bool,
    ingested_at: Option<String>,
) -> Result<ObservationId, String> {
    let inferred = kind.or_else(|| {
        engine
            .store
            .state
            .entities
            .iter()
            .any(|e| e.id.as_str() == record.object)
            .then_some("entity")
    });
    let object = parse_object(&record.object, inferred);
    let metadata = record
        .metadata
        .into_iter()
        .map(|(k, v)| (k, ObjectValue::String(v)))
        .collect();
    engine
        .observe(NewObservation {
            source_id: record.source.into(),
            subject_entity_id: record.subject.into(),
            predicate: record.predicate,
            object,
            observed_at: record.observed_at,
            ingested_at,
            confidence: record.confidence,
            raw_payload: record.raw_payload.unwrap_or_default(),
            metadata,
            retracted,
        })
        .map_err(|e| e.to_string())
}

fn ensure_input_dependencies(engine: &mut Engine, record: &InputRecord) -> Result<(), String> {
    if !engine
        .store
        .state
        .entities
        .iter()
        .any(|e| e.id.as_str() == record.subject)
    {
        let (kind, name) = record
            .subject
            .split_once(':')
            .unwrap_or(("entity", record.subject.as_str()));
        let id = engine
            .create_entity(kind, name, vec![], BTreeMap::new())
            .map_err(|e| e.to_string())?;
        if id.as_str() != record.subject {
            return Err(format!(
                "cannot materialize requested entity id {}; generated {id}",
                record.subject
            ));
        }
    }
    if relation_predicate(&record.predicate).is_some()
        && record.object.contains(':')
        && !engine
            .store
            .state
            .entities
            .iter()
            .any(|e| e.id.as_str() == record.object)
    {
        let (kind, name) = record
            .object
            .split_once(':')
            .unwrap_or(("entity", record.object.as_str()));
        let id = engine
            .create_entity(kind, name, vec![], BTreeMap::new())
            .map_err(|e| e.to_string())?;
        if id.as_str() != record.object {
            return Err(format!(
                "cannot materialize requested entity id {}; generated {id}",
                record.object
            ));
        }
    }
    if !engine
        .store
        .state
        .sources
        .iter()
        .any(|s| s.id.as_str() == record.source)
    {
        let name = record.source.strip_prefix("src:").unwrap_or(&record.source);
        let priority = record
            .metadata
            .get("source_priority")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let id = engine
            .create_source(
                "document",
                format!("urn:wm-source:{name}"),
                name,
                priority,
                BTreeMap::new(),
            )
            .map_err(|e| e.to_string())?;
        if id.as_str() != record.source {
            return Err(format!(
                "cannot materialize requested source id {}; generated {id}",
                record.source
            ));
        }
    }
    Ok(())
}

fn relation_predicate(predicate: &str) -> Option<&str> {
    const TYPES: &[&str] = &[
        "WORKS_AT",
        "OWNS",
        "SUPPLIES",
        "CONTROLS",
        "LOCATED_IN",
        "DEPENDS_ON",
        "PART_OF",
        "ACQUIRED",
        "COMPETES_WITH",
    ];
    TYPES
        .iter()
        .copied()
        .find(|kind| predicate.eq_ignore_ascii_case(kind))
}

fn state_command(engine: &Engine, mut words: Vec<String>) -> Result<(), String> {
    let id = shift(&mut words, "entity id")?;
    let valid_at = take_option(&mut words, "--valid-at");
    let known_at = take_option(&mut words, "--known-at");
    no_extra(&words)?;
    let facts = wm_query::entity_state(
        &engine.store.state,
        &EntityId::from(id),
        valid_at.as_deref(),
        known_at.as_deref(),
    );
    for fact in facts {
        print_fact(fact);
    }
    Ok(())
}

fn why_command(engine: &Engine, mut words: Vec<String>) -> Result<(), String> {
    let id = shift(&mut words, "fact id")?;
    no_extra(&words)?;
    let why = wm_query::why(&engine.store.state, &FactId::from(id.as_str()))
        .ok_or_else(|| format!("fact {id} not found"))?;
    print_fact(&why.fact);
    println!("rule\t{}", why.fact.resolution_rule);
    println!(
        "confidence\t{}\t{}",
        why.fact.confidence, why.fact.confidence_explanation
    );
    for obs in &why.observations {
        println!(
            "evidence\t{}\tsource={}\tobserved_at={}",
            obs.id, obs.source_id, obs.observed_at
        );
    }
    for obs in &why.conflicting_observations {
        println!("conflicting-evidence\t{}\tsource={}", obs.id, obs.source_id);
    }
    for conflict in &why.conflicts {
        println!(
            "conflict\t{}\t{:?}",
            conflict.id, conflict.resolution_status
        );
    }
    Ok(())
}

fn conflicts_command(engine: &Engine) -> Result<(), String> {
    for conflict in &engine.store.state.conflicts {
        println!(
            "{}\t{}\t{}\t{:?}\t{}",
            conflict.id,
            conflict.subject,
            conflict.predicate,
            conflict.resolution_status,
            conflict
                .candidate_fact_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    Ok(())
}

fn changes_command(engine: &Engine, mut words: Vec<String>) -> Result<(), String> {
    let id = shift(&mut words, "entity id")?;
    let from = required_option(&mut words, "--from")?;
    let to = required_option(&mut words, "--to")?;
    no_extra(&words)?;
    let changes = wm_query::changes(&engine.store.state, &EntityId::from(id), &from, &to);
    for fact in changes
        .facts_added
        .iter()
        .chain(&changes.facts_removed_or_superseded)
    {
        print_fact(fact);
    }
    for rel in changes
        .relationships_added
        .iter()
        .chain(&changes.relationships_removed)
    {
        println!(
            "relationship\t{}\t{}\t{}\t{}",
            rel.id, rel.source_entity_id, rel.relationship_type, rel.target_entity_id
        );
    }
    for event in &changes.events {
        println!(
            "event\t{}\t{}\t{}",
            event.id, event.event_type, event.timestamp
        );
    }
    Ok(())
}

fn diff_command(engine: &Engine, mut words: Vec<String>) -> Result<(), String> {
    let from = required_option(&mut words, "--from")?;
    let to = required_option(&mut words, "--to")?;
    no_extra(&words)?;
    let diff = wm_query::diff_world(&engine.store.state, &from, &to);
    println!(
        "entities_added\t{}\nentities_retired\t{}\nfacts_added\t{}\nfacts_superseded\t{}\nrelationships_added\t{}\nrelationships_removed\t{}\nconflicts_opened\t{}\nconflicts_resolved\t{}",
        diff.entities_added,
        diff.entities_retired,
        diff.facts_added,
        diff.facts_superseded,
        diff.relationships_added,
        diff.relationships_removed,
        diff.conflicts_opened,
        diff.conflicts_resolved
    );
    Ok(())
}

fn path_command(engine: &Engine, mut words: Vec<String>) -> Result<(), String> {
    let from = shift(&mut words, "source entity")?;
    let to = shift(&mut words, "target entity")?;
    let max_depth = take_option(&mut words, "--max-depth")
        .unwrap_or_else(|| "5".into())
        .parse::<usize>()
        .map_err(|_| "--max-depth must be an integer".to_owned())?;
    let rel_type = take_option(&mut words, "--type");
    let valid_at = take_option(&mut words, "--valid-at");
    no_extra(&words)?;
    let path = wm_graph::find_path(
        &engine.store.state,
        &EntityId::from(from.as_str()),
        &EntityId::from(to.as_str()),
        max_depth,
        rel_type.as_deref(),
        valid_at.as_deref(),
    )
    .ok_or_else(|| format!("no path from {from} to {to}"))?;
    println!(
        "{}",
        path.entities
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" -> ")
    );
    Ok(())
}

fn parse_object(value: &str, kind: Option<&str>) -> ObjectValue {
    match kind {
        Some("entity") => ObjectValue::Entity(value.into()),
        Some("integer") => value
            .parse()
            .map(ObjectValue::Integer)
            .unwrap_or_else(|_| ObjectValue::String(value.into())),
        Some("float") => value
            .parse()
            .map(ObjectValue::Float)
            .unwrap_or_else(|_| ObjectValue::String(value.into())),
        Some("boolean") => value
            .parse()
            .map(ObjectValue::Boolean)
            .unwrap_or_else(|_| ObjectValue::String(value.into())),
        Some("timestamp") => ObjectValue::Timestamp(value.into()),
        Some("json") => ObjectValue::Json(value.into()),
        _ => ObjectValue::String(value.into()),
    }
}
fn object_text(value: &ObjectValue) -> String {
    match value {
        ObjectValue::Entity(v) => v.to_string(),
        ObjectValue::String(v) | ObjectValue::Timestamp(v) | ObjectValue::Json(v) => v.clone(),
        ObjectValue::Integer(v) => v.to_string(),
        ObjectValue::Float(v) => v.to_string(),
        ObjectValue::Boolean(v) => v.to_string(),
    }
}
fn print_fact(fact: &Fact) {
    println!(
        "{}\t{}\t{}\t{}\t{:?}\tvalid={}..{}\tknown={}..{}",
        fact.id,
        fact.subject_entity_id,
        fact.predicate,
        object_text(&fact.object),
        fact.status,
        fact.valid_from,
        fact.valid_to.as_deref().unwrap_or("open"),
        fact.known_from,
        fact.known_to.as_deref().unwrap_or("open")
    );
}
fn shift(words: &mut Vec<String>, description: &str) -> Result<String, String> {
    if words.is_empty() {
        Err(format!("missing {description}"))
    } else {
        Ok(words.remove(0))
    }
}
fn take_option(words: &mut Vec<String>, name: &str) -> Option<String> {
    let index = words.iter().position(|v| v == name)?;
    words.remove(index);
    (index < words.len()).then(|| words.remove(index))
}
fn required_option(words: &mut Vec<String>, name: &str) -> Result<String, String> {
    take_option(words, name).ok_or_else(|| format!("missing {name}"))
}
fn parse_f64_option(words: &mut Vec<String>, name: &str, default: f64) -> Result<f64, String> {
    take_option(words, name)
        .map(|value| value.parse::<f64>())
        .transpose()
        .map_err(|_| format!("{name} must be a number"))
        .map(|value| value.unwrap_or(default))
}
fn parse_usize_option(
    words: &mut Vec<String>,
    name: &str,
    default: usize,
) -> Result<usize, String> {
    take_option(words, name)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| format!("{name} must be a positive integer"))
        .map(|value| value.unwrap_or(default))
}
fn take_all_options(words: &mut Vec<String>, name: &str) -> Vec<String> {
    let mut values = Vec::new();
    while let Some(v) = take_option(words, name) {
        values.push(v)
    }
    values
}
fn take_flag(words: &mut Vec<String>, name: &str) -> bool {
    words.iter().position(|v| v == name).is_some_and(|i| {
        words.remove(i);
        true
    })
}
fn no_extra(words: &[String]) -> Result<(), String> {
    if words.is_empty() {
        Ok(())
    } else {
        Err(format!("unexpected argument(s): {}", words.join(" ")))
    }
}
fn print_help() {
    println!(
        "World Model DB CLI\n\nUsage: wm [--db PATH] COMMAND\n\nCommands:\n  init\n  demo\n  load|ingest FILE\n  entity create|get\n  source create|get\n  observe\n  state ENTITY [--valid-at TIME] [--known-at TIME]\n  why FACT\n  conflicts\n  changes ENTITY --from TIME --to TIME\n  diff-world --from TIME --to TIME\n  path FROM TO [--max-depth N] [--type TYPE] [--valid-at TIME]\n  query QUERY\n  agent register|remember|context|session|tools"
    );
}
