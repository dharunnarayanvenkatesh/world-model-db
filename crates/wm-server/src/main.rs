mod http;

use std::collections::BTreeMap;
use std::env;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use http::{Request, Response, json_escape};
use wm_agent::{
    AgentContextRequest, AgentGateway, AgentMemoryWrite, AgentRegistration, context_json,
    memory_receipt_json, session_memory_json,
};
use wm_core::*;
use wm_resolution::{Engine, NewObservation, ResolutionEngine};

fn main() {
    if let Err(error) = run() {
        eprintln!("wm-server: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|v| v == "--help") {
        println!(
            "Usage: wm-server [--db PATH] [--bind ADDRESS]\n\nWarning: V0 has no authentication or TLS; bind only to a trusted interface."
        );
        return Ok(());
    }
    let database = take_option(&mut args, "--db")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".wmdb/world.wmdb"));
    let bind = take_option(&mut args, "--bind").unwrap_or_else(|| "127.0.0.1:8787".into());
    if !args.is_empty() {
        return Err(format!("unexpected argument(s): {}", args.join(" ")));
    }
    let engine = Arc::new(Mutex::new(
        Engine::open(&database).map_err(|e| format!("open {}: {e}", database.display()))?,
    ));
    let listener = TcpListener::bind(&bind).map_err(|e| format!("bind {bind}: {e}"))?;
    eprintln!(
        "World Model DB listening on http://{bind} (database {})",
        database.display()
    );
    for connection in listener.incoming() {
        let Ok(mut stream) = connection else { continue };
        let engine = Arc::clone(&engine);
        thread::spawn(move || {
            let response = match Request::read(&mut stream) {
                Ok(request) => match engine.lock() {
                    Ok(mut engine) => route(&mut engine, request),
                    Err(_) => Response::error(500, "database lock poisoned"),
                },
                Err(error) => Response::error(400, &error.to_string()),
            };
            let _ = response.write(&mut stream);
        });
    }
    Ok(())
}

fn route(engine: &mut Engine, request: Request) -> Response {
    let segments = request
        .path
        .trim_matches('/')
        .split('/')
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    let result: Result<(u16, String), (u16, String)> =
        match (request.method.as_str(), segments.as_slice()) {
            ("GET", []) => Ok((
                200,
                "{\"name\":\"World Model DB\",\"api_version\":\"v1\",\"agent_native\":true,\"capabilities\":[\"shared_memory\",\"bitemporal_context\",\"provenance\",\"conflicts\",\"multi_agent\"]}".into(),
            )),
            ("GET", ["agent", "tools"]) => Ok((200, wm_agent::tool_manifest_json().into())),
            ("POST", ["agent", "register"]) => post_agent_register(engine, &request.body),
            ("POST", ["agent", "memory"]) => post_agent_memory(engine, &request.body),
            ("POST", ["agent", "context"]) => post_agent_context(engine, &request.body),
            ("GET", ["agent", "sessions", session_id, "memory"]) => {
                get_agent_session_memory(engine, session_id, &request.query)
            }
            ("POST", ["entities"]) => post_entity(engine, &request.body),
            ("GET", ["entities", id]) => get_entity(engine, id),
            ("GET", ["entities", id, "state"]) => state(engine, id, &request.query),
            ("GET", ["entities", id, "changes"]) => changes(engine, id, &request.query),
            ("POST", ["observations"]) => post_observation(engine, &request.body),
            ("GET", ["observations", id]) => get_observation(engine, id),
            ("GET", ["facts"]) => list_facts(engine, &request.query),
            ("GET", ["facts", id, "why"]) => get_why(engine, id),
            ("GET", ["facts", id]) => get_fact(engine, id),
            ("GET", ["conflicts"]) => Ok((200, conflicts_json(&engine.store.state.conflicts))),
            ("GET", ["conflicts", id]) => engine
                .store
                .state
                .conflicts
                .iter()
                .find(|c| c.id.as_str() == *id)
                .map(|c| (200, conflict_json(c)))
                .ok_or((404, format!("conflict {id} not found"))),
            ("GET", ["graph", "path"]) => graph_path(engine, &request.query),
            ("POST", ["query"]) => query(engine, &request.body),
            (method, _) if !matches!(method, "GET" | "POST") => {
                Err((405, "method not allowed".into()))
            }
            _ => Err((404, "endpoint not found".into())),
        };
    match result {
        Ok((status, body)) => Response::json(status, body),
        Err((status, message)) => Response::error(status, &message),
    }
}

fn post_agent_register(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let agent_id = required_any(&mut values, &["agent_id"])?;
    let name = values.remove("name").unwrap_or_else(|| agent_id.clone());
    let model = values
        .remove("model")
        .unwrap_or_else(|| "unspecified".into());
    let capabilities = values
        .remove("capabilities")
        .map(|value| string_list(&value))
        .unwrap_or_default();
    let priority = values
        .remove("priority")
        .unwrap_or_else(|| "0".into())
        .parse::<i32>()
        .map_err(|_| (400, "priority must be an integer".into()))?;
    let source_id = AgentGateway::new(engine)
        .register(AgentRegistration {
            agent_id: agent_id.clone(),
            name,
            model,
            capabilities,
            priority,
        })
        .map_err(agent_error)?;
    Ok((
        201,
        format!(
            "{{\"agent_id\":\"{}\",\"source_id\":\"{}\"}}",
            json_escape(&agent_id),
            json_escape(source_id.as_str())
        ),
    ))
}

fn post_agent_memory(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let agent_id = required_any(&mut values, &["agent_id"])?;
    let session_id = required_any(&mut values, &["session_id"])?;
    let idempotency_key = required_any(&mut values, &["idempotency_key"])?;
    let subject_entity_id = required_any(&mut values, &["subject_entity_id", "subject"])?;
    let predicate = required_any(&mut values, &["predicate"])?;
    let raw_object = required_any(&mut values, &["object"])?;
    let object_type = values.remove("object_type");
    let observed_at = required_any(&mut values, &["observed_at"])?;
    let confidence = number(&mut values, "confidence", 1.0)?;
    let importance = number(&mut values, "importance", 0.5)?;
    let tags = values
        .remove("tags")
        .map(|value| string_list(&value))
        .unwrap_or_default();
    let memory_kind = values
        .remove("memory_kind")
        .unwrap_or_else(|| "fact".into());
    let receipt = AgentGateway::new(engine)
        .remember(AgentMemoryWrite {
            agent_id,
            session_id,
            idempotency_key,
            subject_entity_id: subject_entity_id.into(),
            predicate,
            object: agent_object(&raw_object, object_type.as_deref())?,
            observed_at,
            confidence,
            importance,
            tags,
            memory_kind,
            raw_payload: values
                .remove("raw_payload")
                .unwrap_or_else(|| body.to_owned()),
        })
        .map_err(agent_error)?;
    Ok((
        if receipt.replayed { 200 } else { 201 },
        memory_receipt_json(&receipt),
    ))
}

fn post_agent_context(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let request = AgentContextRequest {
        agent_id: required_any(&mut values, &["agent_id"])?,
        session_id: values
            .remove("session_id")
            .filter(|value| !value.is_empty()),
        entity_ids: values
            .remove("entity_ids")
            .map(|value| {
                string_list(&value)
                    .into_iter()
                    .map(EntityId::from)
                    .collect()
            })
            .unwrap_or_default(),
        predicates: values
            .remove("predicates")
            .map(|value| string_list(&value))
            .unwrap_or_default(),
        valid_at: values.remove("valid_at").filter(|value| !value.is_empty()),
        known_at: values.remove("known_at").filter(|value| !value.is_empty()),
        max_facts: integer(&mut values, "max_facts", 32)?,
        char_budget: integer(&mut values, "char_budget", 12_000)?,
        include_conflicts: boolean(&mut values, "include_conflicts", true)?,
    };
    let bundle = AgentGateway::new(engine)
        .context(request)
        .map_err(agent_error)?;
    Ok((200, context_json(&bundle)))
}

fn get_agent_session_memory(
    engine: &mut Engine,
    session_id: &str,
    query: &BTreeMap<String, String>,
) -> ApiResult {
    let agent_id = query
        .get("agent_id")
        .ok_or((400, "agent_id query parameter is required".into()))?;
    let memories = AgentGateway::new(engine).session_memory(agent_id, session_id);
    Ok((200, session_memory_json(&memories)))
}

fn post_entity(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let entity_type = required_any(&mut values, &["entity_type", "type"])?;
    let name = required_any(&mut values, &["canonical_name", "name"])?;
    let aliases = values
        .remove("aliases")
        .map(|v| {
            v.trim_matches(['[', ']'])
                .split(',')
                .map(|x| x.trim().trim_matches('"').to_owned())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let id = engine
        .create_entity(entity_type, name, aliases, BTreeMap::new())
        .map_err(engine_error)?;
    let entity = engine
        .store
        .state
        .entities
        .iter()
        .find(|e| e.id == id)
        .expect("created entity");
    Ok((201, entity_json(entity)))
}

fn post_observation(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let source = required_any(&mut values, &["source_id", "source"])?;
    let subject = required_any(&mut values, &["subject_entity_id", "subject"])?;
    let predicate = required_any(&mut values, &["predicate"])?;
    let raw_object = required_any(&mut values, &["object"])?;
    let observed_at = required_any(&mut values, &["observed_at"])?;
    let confidence = values
        .remove("confidence")
        .unwrap_or_else(|| "1.0".into())
        .parse::<f64>()
        .map_err(|_| (400, "confidence must be a number".into()))?;
    let object = if raw_object.starts_with('{') {
        let mut nested = wm_ingest::parse_object(&raw_object).map_err(|e| (400, e.to_string()))?;
        nested
            .remove("entity_id")
            .map(|id| ObjectValue::Entity(id.into()))
            .unwrap_or(ObjectValue::Json(raw_object))
    } else {
        ObjectValue::String(raw_object)
    };
    let id = engine
        .observe(NewObservation {
            source_id: source.into(),
            subject_entity_id: subject.into(),
            predicate,
            object,
            observed_at,
            ingested_at: values.remove("ingested_at"),
            confidence,
            raw_payload: values
                .remove("raw_payload")
                .unwrap_or_else(|| body.to_owned()),
            metadata: BTreeMap::new(),
            retracted: values
                .remove("retracted")
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        })
        .map_err(engine_error)?;
    let observation = engine
        .store
        .state
        .observations
        .iter()
        .find(|o| o.id == id)
        .expect("created observation");
    Ok((201, observation_json(observation)))
}

fn get_entity(engine: &Engine, id: &str) -> ApiResult {
    engine
        .store
        .state
        .entities
        .iter()
        .find(|e| e.id.as_str() == id)
        .map(|v| (200, entity_json(v)))
        .ok_or((404, format!("entity {id} not found")))
}
fn get_observation(engine: &Engine, id: &str) -> ApiResult {
    engine
        .store
        .state
        .observations
        .iter()
        .find(|e| e.id.as_str() == id)
        .map(|v| (200, observation_json(v)))
        .ok_or((404, format!("observation {id} not found")))
}
fn get_fact(engine: &Engine, id: &str) -> ApiResult {
    engine
        .store
        .state
        .facts
        .iter()
        .find(|e| e.id.as_str() == id)
        .map(|v| (200, wm_query::fact_json(v)))
        .ok_or((404, format!("fact {id} not found")))
}
fn get_why(engine: &Engine, id: &str) -> ApiResult {
    wm_query::why(&engine.store.state, &FactId::from(id))
        .map(|v| (200, wm_query::why_json(&v)))
        .ok_or((404, format!("fact {id} not found")))
}

fn list_facts(engine: &Engine, query: &BTreeMap<String, String>) -> ApiResult {
    let facts = engine
        .store
        .state
        .facts
        .iter()
        .filter(|fact| {
            query
                .get("subject")
                .or_else(|| query.get("subject_entity_id"))
                .is_none_or(|id| fact.subject_entity_id.as_str() == id)
        })
        .filter(|fact| {
            query
                .get("status")
                .is_none_or(|status| format!("{:?}", fact.status).eq_ignore_ascii_case(status))
        })
        .collect::<Vec<_>>();
    Ok((200, wm_query::facts_json(&facts)))
}

fn state(engine: &Engine, id: &str, query: &BTreeMap<String, String>) -> ApiResult {
    if !engine
        .store
        .state
        .entities
        .iter()
        .any(|e| e.id.as_str() == id)
    {
        return Err((404, format!("entity {id} not found")));
    }
    let facts = wm_query::entity_state(
        &engine.store.state,
        &EntityId::from(id),
        query.get("valid_at").map(String::as_str),
        query.get("known_at").map(String::as_str),
    );
    Ok((200, wm_query::facts_json(&facts)))
}

fn changes(engine: &Engine, id: &str, query: &BTreeMap<String, String>) -> ApiResult {
    let from = query
        .get("from")
        .ok_or((400, "from query parameter is required".into()))?;
    let to = query
        .get("to")
        .ok_or((400, "to query parameter is required".into()))?;
    Ok((
        200,
        wm_query::changes_json(&wm_query::changes(
            &engine.store.state,
            &EntityId::from(id),
            from,
            to,
        )),
    ))
}

fn graph_path(engine: &Engine, query: &BTreeMap<String, String>) -> ApiResult {
    let from = query
        .get("from")
        .ok_or((400, "from query parameter is required".into()))?;
    let to = query
        .get("to")
        .ok_or((400, "to query parameter is required".into()))?;
    let max_depth = query
        .get("max_depth")
        .map(|v| v.parse::<usize>())
        .transpose()
        .map_err(|_| (400, "max_depth must be an integer".into()))?
        .unwrap_or(5);
    let path = wm_graph::find_path(
        &engine.store.state,
        &EntityId::from(from.as_str()),
        &EntityId::from(to.as_str()),
        max_depth,
        query.get("type").map(String::as_str),
        query.get("valid_at").map(String::as_str),
    )
    .ok_or((404, "path not found".into()))?;
    Ok((200, wm_query::path_json(&path)))
}

fn query(engine: &Engine, body: &str) -> ApiResult {
    let trimmed = body.trim();
    let statement = if trimmed.starts_with('{') {
        let values = fields(trimmed)?;
        if values.get("operation").is_some_and(|v| v == "world_diff") {
            let from = values.get("from").ok_or((400, "from is required".into()))?;
            let to = values.get("to").ok_or((400, "to is required".into()))?;
            return Ok((
                200,
                wm_query::diff_json(&wm_query::diff_world(&engine.store.state, from, to)),
            ));
        }
        values
            .get("query")
            .or_else(|| values.get("sql"))
            .cloned()
            .ok_or((400, "query or operation is required".into()))?
    } else {
        trimmed.to_owned()
    };
    wm_query::execute(&engine.store.state, &statement)
        .map(|body| (200, body))
        .map_err(|e| (400, e))
}

type ApiResult = Result<(u16, String), (u16, String)>;
fn fields(body: &str) -> Result<BTreeMap<String, String>, (u16, String)> {
    wm_ingest::parse_object(body).map_err(|e| (400, e.to_string()))
}
fn required_any(
    values: &mut BTreeMap<String, String>,
    keys: &[&str],
) -> Result<String, (u16, String)> {
    for key in keys {
        if let Some(value) = values.remove(*key).filter(|v| !v.is_empty()) {
            return Ok(value);
        }
    }
    Err((400, format!("{} is required", keys.join(" or "))))
}
fn engine_error(error: wm_resolution::ResolutionError) -> (u16, String) {
    match error {
        wm_resolution::ResolutionError::NotFound(v) => (404, v),
        wm_resolution::ResolutionError::Invalid(v) => (400, v),
        wm_resolution::ResolutionError::Io(v) => (500, v.to_string()),
    }
}
fn agent_error(error: wm_agent::AgentError) -> (u16, String) {
    match error {
        wm_agent::AgentError::Invalid(message) => (400, message),
        wm_agent::AgentError::NotFound(message) => (404, message),
        wm_agent::AgentError::IdempotencyConflict(message) => (409, message),
        wm_agent::AgentError::Storage(message) => (500, message),
    }
}
fn string_list(raw: &str) -> Vec<String> {
    raw.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|value| value.trim().trim_matches('"').to_owned())
        .filter(|value| !value.is_empty())
        .collect()
}
fn number(
    values: &mut BTreeMap<String, String>,
    key: &str,
    default: f64,
) -> Result<f64, (u16, String)> {
    values
        .remove(key)
        .map(|value| value.parse::<f64>())
        .transpose()
        .map_err(|_| (400, format!("{key} must be a number")))
        .map(|value| value.unwrap_or(default))
}
fn integer(
    values: &mut BTreeMap<String, String>,
    key: &str,
    default: usize,
) -> Result<usize, (u16, String)> {
    values
        .remove(key)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| (400, format!("{key} must be a positive integer")))
        .map(|value| value.unwrap_or(default))
}
fn boolean(
    values: &mut BTreeMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool, (u16, String)> {
    values
        .remove(key)
        .map(|value| value.parse::<bool>())
        .transpose()
        .map_err(|_| (400, format!("{key} must be true or false")))
        .map(|value| value.unwrap_or(default))
}
fn agent_object(raw: &str, object_type: Option<&str>) -> Result<ObjectValue, (u16, String)> {
    Ok(match object_type {
        Some("entity") => ObjectValue::Entity(raw.trim_matches('"').into()),
        Some("integer") => ObjectValue::Integer(
            raw.parse()
                .map_err(|_| (400, "object must be an integer".into()))?,
        ),
        Some("float") => ObjectValue::Float(
            raw.parse()
                .map_err(|_| (400, "object must be a number".into()))?,
        ),
        Some("boolean") => ObjectValue::Boolean(
            raw.parse()
                .map_err(|_| (400, "object must be true or false".into()))?,
        ),
        Some("timestamp") => ObjectValue::Timestamp(raw.into()),
        Some("json") => ObjectValue::Json(raw.into()),
        Some("string") => ObjectValue::String(raw.into()),
        Some(other) => return Err((400, format!("unsupported object_type '{other}'"))),
        None if raw.starts_with('{') => {
            let mut nested =
                wm_ingest::parse_object(raw).map_err(|error| (400, error.to_string()))?;
            nested
                .remove("entity_id")
                .map(|id| ObjectValue::Entity(id.into()))
                .unwrap_or_else(|| ObjectValue::Json(raw.into()))
        }
        None => ObjectValue::String(raw.into()),
    })
}
fn take_option(args: &mut Vec<String>, name: &str) -> Option<String> {
    let i = args.iter().position(|v| v == name)?;
    args.remove(i);
    (i < args.len()).then(|| args.remove(i))
}

fn entity_json(v: &Entity) -> String {
    format!(
        "{{\"entity_id\":\"{}\",\"entity_type\":\"{}\",\"canonical_name\":\"{}\",\"aliases\":[{}],\"created_at\":\"{}\",\"retired_at\":{}}}",
        json_escape(v.id.as_str()),
        json_escape(&v.entity_type),
        json_escape(&v.canonical_name),
        v.aliases
            .iter()
            .map(|a| format!("\"{}\"", json_escape(a)))
            .collect::<Vec<_>>()
            .join(","),
        v.created_at,
        json_opt(v.retired_at.as_deref())
    )
}
fn observation_json(v: &Observation) -> String {
    format!(
        "{{\"observation_id\":\"{}\",\"source_id\":\"{}\",\"subject_entity_id\":\"{}\",\"predicate\":\"{}\",\"object\":{},\"observed_at\":\"{}\",\"ingested_at\":\"{}\",\"confidence\":{},\"retracted\":{}}}",
        json_escape(v.id.as_str()),
        json_escape(v.source_id.as_str()),
        json_escape(v.subject_entity_id.as_str()),
        json_escape(&v.predicate),
        wm_query::object_json(&v.object),
        v.observed_at,
        v.ingested_at,
        v.confidence,
        v.retracted
    )
}
fn conflict_json(v: &Conflict) -> String {
    format!(
        "{{\"conflict_id\":\"{}\",\"subject\":\"{}\",\"predicate\":\"{}\",\"candidate_fact_ids\":[{}],\"detected_at\":\"{}\",\"resolution_status\":\"{:?}\",\"resolution_reason\":{}}}",
        json_escape(v.id.as_str()),
        json_escape(v.subject.as_str()),
        json_escape(&v.predicate),
        v.candidate_fact_ids
            .iter()
            .map(|id| format!("\"{}\"", json_escape(id.as_str())))
            .collect::<Vec<_>>()
            .join(","),
        v.detected_at,
        v.resolution_status,
        json_opt(v.resolution_reason.as_deref())
    )
}
fn conflicts_json(values: &[Conflict]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(conflict_json)
            .collect::<Vec<_>>()
            .join(",")
    )
}
fn json_opt(value: Option<&str>) -> String {
    value
        .map(|v| format!("\"{}\"", json_escape(v)))
        .unwrap_or_else(|| "null".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(method: &str, path: &str, body: &str) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            query: BTreeMap::new(),
            body: body.into(),
        }
    }

    #[test]
    fn serializes_entity_as_json() {
        let entity = Entity {
            id: "company:acme".into(),
            entity_type: "company".into(),
            canonical_name: "Acme \"Corp\"".into(),
            aliases: vec![],
            attributes: BTreeMap::new(),
            created_at: "2026-01-01T00:00:00Z".into(),
            retired_at: None,
        };
        assert!(entity_json(&entity).contains("Acme \\\"Corp\\\""));
    }

    #[test]
    fn agent_routes_support_safe_retry_and_context() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wm-server-agent-{nonce}.redb"));
        {
            let mut engine = Engine::init(&path).unwrap();
            let body = r#"{"agent_id":"researcher","session_id":"run-1","idempotency_key":"call-1","subject_entity_id":"company:acme","predicate":"RISK","object":"supply chain","observed_at":"2026-09-27T10:00:00Z","importance":0.8}"#;
            let created = route(&mut engine, request("POST", "/agent/memory", body));
            assert_eq!(created.status, 201);
            assert!(created.body.contains("\"replayed\":false"));
            let replayed = route(&mut engine, request("POST", "/agent/memory", body));
            assert_eq!(replayed.status, 200);
            assert!(replayed.body.contains("\"replayed\":true"));
            let context = route(
                &mut engine,
                request(
                    "POST",
                    "/agent/context",
                    r#"{"agent_id":"planner","entity_ids":["company:acme"],"char_budget":8000}"#,
                ),
            );
            assert_eq!(context.status, 200);
            assert!(context.body.contains("\"agent_ids\":[\"researcher\"]"));
        }
        std::fs::remove_file(path).unwrap();
    }
}
