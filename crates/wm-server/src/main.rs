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
use wm_ontology::{
    SemanticQuery, authorized_entities, blast_radius, centrality, check_consistency,
    computed_properties, effective_properties, formal_state_summary, semantic_query,
    shortest_semantic_path,
};
use wm_resolution::{Engine, NewObservation, ResolutionEngine};
use wm_twin::{
    CommandStatus, TwinChannel, TwinCommandRequest, TwinDefinition, TwinGateway, TwinLink,
    TwinSignalWrite,
};

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
                "{\"name\":\"World Model DB\",\"api_version\":\"v1\",\"agent_native\":true,\"capabilities\":[\"shared_memory\",\"bitemporal_context\",\"provenance\",\"conflicts\",\"multi_agent\",\"typed_ontology\",\"inference\",\"guarded_actions\",\"object_security\",\"semantic_graph\",\"digital_twins\",\"reported_desired_state\",\"twin_commands\"]}".into(),
            )),
            ("GET", ["agent", "tools"]) => Ok((200, wm_agent::tool_manifest_json().into())),
            ("POST", ["agent", "register"]) => post_agent_register(engine, &request.body),
            ("POST", ["agent", "memory"]) => post_agent_memory(engine, &request.body),
            ("POST", ["agent", "context"]) => post_agent_context(engine, &request.body),
            ("GET", ["agent", "sessions", session_id, "memory"]) => {
                get_agent_session_memory(engine, session_id, &request.query)
            }
            ("POST", ["twins"]) => post_twin(engine, &request.body),
            ("GET", ["twins"]) => list_twins(engine),
            ("GET", ["twins", id]) => get_twin(engine, id),
            ("POST", ["twins", id, "telemetry"]) => {
                post_twin_signal(engine, id, TwinChannel::Reported, &request.body)
            }
            ("POST", ["twins", id, "desired"]) => {
                post_twin_signal(engine, id, TwinChannel::Desired, &request.body)
            }
            ("POST", ["twins", id, "configuration"]) => {
                post_twin_signal(engine, id, TwinChannel::Configuration, &request.body)
            }
            ("POST", ["twins", id, "derived"]) => {
                post_twin_signal(engine, id, TwinChannel::Derived, &request.body)
            }
            ("GET", ["twins", id, "state"]) => twin_state(engine, id, &request.query),
            ("POST", ["twins", id, "relationships"]) => {
                post_twin_relationship(engine, id, &request.body)
            }
            ("POST", ["twins", id, "commands"]) => {
                post_twin_command(engine, id, &request.body)
            }
            ("GET", ["twins", id, "commands"]) => list_twin_commands(engine, id),
            ("POST", ["twins", id, "commands", command_id, "ack"]) => {
                acknowledge_twin_command(engine, id, command_id, &request.body)
            }
            ("POST", ["entities"]) => post_entity(engine, &request.body),
            ("GET", ["entities", id]) => get_entity(engine, id),
            ("GET", ["entities", id, "state"]) => state(engine, id, &request.query),
            ("GET", ["entities", id, "changes"]) => changes(engine, id, &request.query),
            ("POST", ["observations"]) => post_observation(engine, &request.body),
            ("GET", ["observations", id]) => get_observation(engine, id),
            ("POST", ["relationships"]) => post_relationship(engine, &request.body),
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
            ("GET", ["ontology"]) => ontology_summary(engine),
            ("POST", ["ontology", "definitions"]) => {
                post_ontology_definition(engine, &request.body)
            }
            ("GET", ["ontology", "consistency"]) => ontology_consistency(engine),
            ("POST", ["ontology", "materialize"]) => ontology_materialize(engine),
            ("GET", ["ontology", "entities", id, "computed"]) => {
                ontology_computed(engine, id)
            }
            ("POST", ["ontology", "actions", action, "execute"]) => {
                ontology_execute_action(engine, action, &request.body)
            }
            ("POST", ["ontology", "resolve"]) => ontology_resolve(engine, &request.body),
            ("POST", ["ontology", "mappings", id, "apply"]) => {
                ontology_apply_mapping(engine, id, &request.body)
            }
            ("GET", ["ontology", "authorized-entities"]) => {
                ontology_authorized_entities(engine, &request.query)
            }
            ("GET", ["ontology", "graph", "blast-radius"]) => {
                ontology_blast_radius(engine, &request.query)
            }
            ("GET", ["ontology", "graph", "centrality"]) => ontology_centrality(engine),
            ("GET", ["ontology", "graph", "shortest-path"]) => {
                ontology_shortest_path(engine, &request.query)
            }
            ("POST", ["ontology", "query"]) => ontology_query(engine, &request.body),
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
    let claimed_valid_from = values
        .remove("claimed_valid_from")
        .filter(|v| !v.is_empty());
    let claimed_valid_to = values.remove("claimed_valid_to").filter(|v| !v.is_empty());
    let cardinality =
        parse_cardinality(values.remove("cardinality").as_deref().unwrap_or("single"))?;
    let receipt = AgentGateway::new(engine)
        .remember(AgentMemoryWrite {
            agent_id,
            session_id,
            idempotency_key,
            subject_entity_id: subject_entity_id.into(),
            predicate,
            object: agent_object(&raw_object, object_type.as_deref())?,
            observed_at,
            claimed_valid_from,
            claimed_valid_to,
            cardinality,
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

fn post_twin(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let twin_kind = required_any(&mut values, &["twin_kind", "kind"])?;
    let name = required_any(&mut values, &["name", "canonical_name"])?;
    let aliases = values
        .remove("aliases")
        .map(|raw| string_list(&raw))
        .unwrap_or_default();
    let capabilities = values
        .remove("capabilities")
        .map(|raw| string_list(&raw))
        .unwrap_or_default();
    let external_ids = values
        .remove("external_ids")
        .map(|raw| wm_ingest::parse_object(&raw).map_err(|error| (400, error.to_string())))
        .transpose()?
        .unwrap_or_default();
    let mut metadata = BTreeMap::new();
    if let Some(raw) = values.remove("metadata") {
        metadata.insert("twin.metadata".into(), ObjectValue::Json(raw));
    }
    let id = TwinGateway::new(engine)
        .register(TwinDefinition {
            twin_kind,
            name,
            aliases,
            model_id: values.remove("model_id"),
            schema_version: values.remove("schema_version"),
            capabilities,
            external_ids,
            metadata,
        })
        .map_err(twin_error)?;
    get_twin(engine, id.as_str()).map(|(_, body)| (201, body))
}

fn list_twins(engine: &mut Engine) -> ApiResult {
    let gateway = TwinGateway::new(engine);
    Ok((200, wm_twin::twins_json(&gateway.twins())))
}

fn get_twin(engine: &Engine, id: &str) -> ApiResult {
    engine
        .store
        .state
        .entities
        .iter()
        .find(|entity| entity.id.as_str() == id && entity.entity_type == wm_twin::TWIN_ENTITY_TYPE)
        .map(|entity| (200, wm_twin::twin_json(entity)))
        .ok_or((404, format!("digital twin {id} not found")))
}

fn post_twin_signal(engine: &mut Engine, id: &str, channel: TwinChannel, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let signal = required_any(&mut values, &["signal", "property", "name"])?;
    let raw_value = required_any(&mut values, &["value", "object"])?;
    let observed_at = required_any(&mut values, &["observed_at", "timestamp"])?;
    let value = agent_object(&raw_value, values.remove("value_type").as_deref())?;
    let confidence = number(&mut values, "confidence", 1.0)?;
    let cardinality =
        parse_cardinality(values.remove("cardinality").as_deref().unwrap_or("single"))?;
    let supplied_source = values.remove("source_id");
    let adapter_id = values.remove("adapter_id").unwrap_or_else(|| "rest".into());
    let protocol = values
        .remove("protocol")
        .unwrap_or_else(|| "http-json".into());
    let priority = values
        .remove("source_priority")
        .map(|raw| raw.parse::<i32>())
        .transpose()
        .map_err(|_| (400, "source_priority must be an integer".into()))?
        .unwrap_or(0);
    let mut gateway = TwinGateway::new(engine);
    let source_id = match supplied_source {
        Some(source_id) => SourceId::from(source_id),
        None => gateway
            .ensure_adapter_source(&adapter_id, &protocol, priority)
            .map_err(twin_error)?,
    };
    let observation_id = gateway
        .write_signal(
            &EntityId::from(id),
            source_id,
            channel,
            TwinSignalWrite {
                signal,
                value,
                observed_at,
                ingested_at: values.remove("ingested_at"),
                valid_from: values
                    .remove("valid_from")
                    .or_else(|| values.remove("claimed_valid_from")),
                valid_to: values
                    .remove("valid_to")
                    .or_else(|| values.remove("claimed_valid_to")),
                confidence,
                cardinality,
                unit: values.remove("unit"),
                quality: values.remove("quality"),
                sequence: values.remove("sequence"),
                raw_payload: values
                    .remove("raw_payload")
                    .unwrap_or_else(|| body.to_owned()),
            },
        )
        .map_err(twin_error)?;
    Ok((
        201,
        format!(
            "{{\"observation_id\":\"{}\",\"channel\":\"{}\"}}",
            json_escape(observation_id.as_str()),
            channel.as_str()
        ),
    ))
}

fn twin_state(engine: &mut Engine, id: &str, query: &BTreeMap<String, String>) -> ApiResult {
    let gateway = TwinGateway::new(engine);
    let snapshot = gateway
        .snapshot(
            &EntityId::from(id),
            query.get("valid_at").map(String::as_str),
            query.get("known_at").map(String::as_str),
        )
        .map_err(twin_error)?;
    Ok((200, wm_twin::snapshot_json(&snapshot)))
}

fn post_twin_relationship(engine: &mut Engine, id: &str, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let relationship_type = required_any(&mut values, &["relationship_type", "type"])?;
    let target_twin_id = required_any(&mut values, &["target_twin_id", "target"])?;
    let valid_from = values
        .remove("valid_from")
        .unwrap_or_else(wm_resolution::now_utc);
    let confidence = number(&mut values, "confidence", 1.0)?;
    let evidence = values
        .remove("observation_ids")
        .map(|raw| string_list(&raw).into_iter().map(Into::into).collect())
        .unwrap_or_default();
    let relationship_id = TwinGateway::new(engine)
        .connect(
            &EntityId::from(id),
            TwinLink {
                relationship_type,
                target_twin_id: target_twin_id.into(),
                valid_from,
                valid_to: values.remove("valid_to"),
                confidence,
                evidence,
            },
        )
        .map_err(twin_error)?;
    Ok((
        201,
        format!(
            "{{\"relationship_id\":\"{}\"}}",
            json_escape(relationship_id.as_str())
        ),
    ))
}

fn post_twin_command(engine: &mut Engine, id: &str, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let command_type = required_any(&mut values, &["command_type", "type"])?;
    let requested_by = required_any(&mut values, &["requested_by", "actor"])?;
    let idempotency_key = required_any(&mut values, &["idempotency_key", "key"])?;
    let requested_at = values
        .remove("requested_at")
        .unwrap_or_else(wm_resolution::now_utc);
    let mut parameters = BTreeMap::new();
    if let Some(raw) = values.remove("parameters") {
        parameters.insert("twin.parameters".into(), ObjectValue::Json(raw));
    }
    let receipt = TwinGateway::new(engine)
        .request_command(
            &EntityId::from(id),
            TwinCommandRequest {
                command_type,
                requested_by,
                requested_at,
                expires_at: values.remove("expires_at"),
                idempotency_key,
                parameters,
            },
        )
        .map_err(twin_error)?;
    Ok((
        if receipt.replayed { 200 } else { 201 },
        format!(
            "{{\"command_id\":\"{}\",\"replayed\":{}}}",
            json_escape(receipt.command_id.as_str()),
            receipt.replayed
        ),
    ))
}

fn acknowledge_twin_command(
    engine: &mut Engine,
    id: &str,
    command_id: &str,
    body: &str,
) -> ApiResult {
    let mut values = fields(body)?;
    let status = match required_any(&mut values, &["status"])?
        .to_ascii_lowercase()
        .as_str()
    {
        "accepted" => CommandStatus::Accepted,
        "running" => CommandStatus::Running,
        "succeeded" | "success" => CommandStatus::Succeeded,
        "failed" | "failure" => CommandStatus::Failed,
        "rejected" => CommandStatus::Rejected,
        "cancelled" | "canceled" => CommandStatus::Cancelled,
        _ => {
            return Err((
                400,
                "status must be accepted, running, succeeded, failed, rejected, or cancelled"
                    .into(),
            ));
        }
    };
    let event_id = TwinGateway::new(engine)
        .acknowledge_command(
            &EntityId::from(id),
            &EventId::from(command_id),
            status,
            values.remove("at").unwrap_or_else(wm_resolution::now_utc),
            values.remove("message"),
        )
        .map_err(twin_error)?;
    Ok((
        201,
        format!("{{\"event_id\":\"{}\"}}", json_escape(event_id.as_str())),
    ))
}

fn list_twin_commands(engine: &mut Engine, id: &str) -> ApiResult {
    let events = TwinGateway::new(engine)
        .commands(&EntityId::from(id))
        .map_err(twin_error)?;
    Ok((200, wm_twin::events_json(&events)))
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
    let attributes = values
        .remove("attributes")
        .map(|raw| parse_typed_attributes(engine, &entity_type, &raw))
        .transpose()?
        .unwrap_or_default();
    let id = engine
        .create_entity(entity_type, name, aliases, attributes)
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
    let object = parse_observation_object(engine, &subject, &predicate, &raw_object)?;
    let id = engine
        .observe(NewObservation {
            source_id: source.into(),
            subject_entity_id: subject.into(),
            predicate,
            object,
            observed_at,
            ingested_at: values.remove("ingested_at"),
            claimed_valid_from: values.remove("claimed_valid_from"),
            claimed_valid_to: values.remove("claimed_valid_to"),
            cardinality: parse_cardinality(
                values.remove("cardinality").as_deref().unwrap_or("single"),
            )?,
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

fn post_relationship(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let source = required_any(&mut values, &["source_entity_id", "source"])?;
    let relationship_type = required_any(&mut values, &["relationship_type", "type"])?;
    let target = required_any(&mut values, &["target_entity_id", "target"])?;
    let valid_from = values
        .remove("valid_from")
        .unwrap_or_else(wm_resolution::now_utc);
    let valid_to = values.remove("valid_to").filter(|value| !value.is_empty());
    let confidence = number(&mut values, "confidence", 1.0)?;
    let observation_ids = values
        .remove("observation_ids")
        .map(|raw| string_list(&raw).into_iter().map(Into::into).collect())
        .unwrap_or_default();
    let id = engine
        .add_relationship(
            source.into(),
            relationship_type,
            target.into(),
            valid_from,
            valid_to,
            confidence,
            observation_ids,
        )
        .map_err(engine_error)?;
    Ok((
        201,
        format!("{{\"relationship_id\":\"{}\"}}", json_escape(id.as_str())),
    ))
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

fn ontology_summary(engine: &Engine) -> ApiResult {
    let summary = formal_state_summary(&engine.store.state);
    Ok((
        200,
        format!(
            "{{\"entities\":{},\"relationships\":{},\"schema_types\":{},\"constraints\":{},\"rules\":{},\"functions\":{},\"actions\":{},\"permissions\":{},\"history_records\":{},\"provenance_records\":{}}}",
            summary.entities,
            summary.relationships,
            summary.schema_types,
            summary.constraints,
            summary.rules,
            summary.functions,
            summary.actions,
            summary.permissions,
            summary.history_records,
            summary.provenance_records
        ),
    ))
}

fn post_ontology_definition(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let kind = required_any(&mut values, &["kind"])?;
    match kind.as_str() {
        "schema" => engine.register_schema(OntologySchemaVersion {
            id: required_any(&mut values, &["id"])?.into(),
            version: integer(&mut values, "version", 1)? as u32,
            supersedes: values
                .remove("supersedes")
                .filter(|value| !value.is_empty())
                .map(Into::into),
            compatibility: parse_compatibility(
                values
                    .remove("compatibility")
                    .as_deref()
                    .unwrap_or("backward"),
            )?,
            created_at: values
                .remove("created_at")
                .unwrap_or_else(wm_resolution::now_utc),
        }),
        "module" => engine.register_module(OntologyModule {
            id: required_any(&mut values, &["id"])?.into(),
            namespace: required_any(&mut values, &["namespace"])?,
            version: integer(&mut values, "version", 1)? as u32,
            dependencies: values
                .remove("dependencies")
                .map(|raw| string_list(&raw).into_iter().map(Into::into).collect())
                .unwrap_or_default(),
        }),
        "interface" => engine.register_interface(InterfaceDefinition {
            name: required_any(&mut values, &["name"])?,
            required_properties: parse_property_specs(
                values.remove("properties").as_deref().unwrap_or(""),
            )?,
        }),
        "object_type" => {
            let identity_properties = values
                .remove("identity_properties")
                .map(|raw| string_list(&raw))
                .unwrap_or_default();
            let identity_weights = values
                .remove("identity_weights")
                .map(|raw| parse_float_list(&raw))
                .transpose()?
                .unwrap_or_default();
            let identity = (!identity_properties.is_empty()).then(|| IdentityRule {
                properties: identity_properties,
                weights: identity_weights,
                threshold: values
                    .remove("identity_threshold")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0.8),
            });
            engine.register_object_type(ObjectTypeDefinition {
                name: required_any(&mut values, &["name"])?,
                namespace: values.remove("namespace").unwrap_or_else(|| "world".into()),
                version: integer(&mut values, "version", 1)? as u32,
                parent_types: values
                    .remove("parents")
                    .map(|raw| string_list(&raw))
                    .unwrap_or_default(),
                interfaces: values
                    .remove("interfaces")
                    .map(|raw| string_list(&raw))
                    .unwrap_or_default(),
                properties: parse_property_specs(
                    values.remove("properties").as_deref().unwrap_or(""),
                )?,
                identity,
                disjoint_with: values
                    .remove("disjoint_with")
                    .map(|raw| string_list(&raw))
                    .unwrap_or_default(),
            })
        }
        "relationship_type" => engine.register_relationship_type(RelationshipTypeDefinition {
            name: required_any(&mut values, &["name"])?,
            version: integer(&mut values, "version", 1)? as u32,
            domain_types: values
                .remove("domain")
                .map(|raw| string_list(&raw))
                .unwrap_or_default(),
            range_types: values
                .remove("range")
                .map(|raw| string_list(&raw))
                .unwrap_or_default(),
            min_outgoing: integer(&mut values, "min_outgoing", 0)?,
            max_outgoing: values
                .remove("max_outgoing")
                .filter(|raw| !raw.is_empty())
                .map(|raw| {
                    raw.parse()
                        .map_err(|_| (400, "max_outgoing must be an integer".into()))
                })
                .transpose()?,
            transitive: boolean(&mut values, "transitive", false)?,
            symmetric: boolean(&mut values, "symmetric", false)?,
            inverse_of: values
                .remove("inverse_of")
                .filter(|value| !value.is_empty()),
            compositions: parse_composition_specs(
                values.remove("compositions").as_deref().unwrap_or(""),
            )?,
            acyclic: boolean(&mut values, "acyclic", false)?,
            connected: boolean(&mut values, "connected", false)?,
            weight: number(&mut values, "weight", 1.0)?,
        }),
        "computed_property" => engine.register_computed_property(ComputedPropertyDefinition {
            id: required_any(&mut values, &["id"])?.into(),
            target_type: required_any(&mut values, &["target_type"])?,
            property: required_any(&mut values, &["property"])?,
            expression: required_any(&mut values, &["expression"])?,
            materialized: boolean(&mut values, "materialized", false)?,
        }),
        "inference_rule" => engine.register_inference_rule(InferenceRule {
            id: required_any(&mut values, &["id"])?.into(),
            relationship_path: values
                .remove("path")
                .map(|raw| string_list(&raw))
                .unwrap_or_default(),
            implies_relationship: required_any(&mut values, &["implies"])?,
            materialized: boolean(&mut values, "materialized", true)?,
        }),
        "derived_class" => engine.register_derived_class(DerivedClassDefinition {
            name: required_any(&mut values, &["name"])?,
            base_type: required_any(&mut values, &["base_type"])?,
            conditions: parse_condition_specs(
                values.remove("conditions").as_deref().unwrap_or(""),
            )?,
        }),
        "action" => engine.register_action(ActionDefinition {
            id: required_any(&mut values, &["id"])?.into(),
            name: required_any(&mut values, &["name"])?,
            target_type: required_any(&mut values, &["target_type"])?,
            preconditions: parse_condition_specs(
                values.remove("preconditions").as_deref().unwrap_or(""),
            )?,
            effects: parse_effect_specs(values.remove("effects").as_deref().unwrap_or(""))?,
            postconditions: parse_condition_specs(
                values.remove("postconditions").as_deref().unwrap_or(""),
            )?,
            allowed_roles: values
                .remove("allowed_roles")
                .map(|raw| string_list(&raw))
                .unwrap_or_default(),
        }),
        "permission" => engine.register_permission(PermissionRule {
            id: required_any(&mut values, &["id"])?.into(),
            principal: values.remove("principal").filter(|value| !value.is_empty()),
            role: values.remove("role").filter(|value| !value.is_empty()),
            action: required_any(&mut values, &["action"])?,
            object_type: values
                .remove("object_type")
                .filter(|value| !value.is_empty()),
            object_id: values
                .remove("object_id")
                .filter(|value| !value.is_empty())
                .map(Into::into),
            conditions: parse_condition_specs(
                values.remove("conditions").as_deref().unwrap_or(""),
            )?,
            effect: match values
                .remove("effect")
                .unwrap_or_else(|| "allow".into())
                .as_str()
            {
                "allow" => PermissionEffect::Allow,
                "deny" => PermissionEffect::Deny,
                _ => return Err((400, "permission effect must be allow or deny".into())),
            },
            priority: values
                .remove("priority")
                .unwrap_or_else(|| "0".into())
                .parse()
                .map_err(|_| (400, "priority must be an integer".into()))?,
        }),
        "mapping" => engine.register_mapping(SchemaMapping {
            id: required_any(&mut values, &["id"])?.into(),
            source_namespace: required_any(&mut values, &["source_namespace"])?,
            source_type: required_any(&mut values, &["source_type"])?,
            target_type: required_any(&mut values, &["target_type"])?,
            semantic_id_template: required_any(&mut values, &["semantic_id_template"])?,
            fields: parse_mapping_specs(values.remove("fields").as_deref().unwrap_or(""))?,
        }),
        _ => {
            return Err((
                400,
                format!("unsupported ontology definition kind '{kind}'"),
            ));
        }
    }
    .map_err(engine_error)?;
    Ok((
        201,
        format!(
            "{{\"kind\":\"{}\",\"registered\":true}}",
            json_escape(&kind)
        ),
    ))
}

fn ontology_consistency(engine: &Engine) -> ApiResult {
    let report = check_consistency(&engine.store.state);
    Ok((
        200,
        format!(
            "{{\"consistent\":{},\"checked_entities\":{},\"checked_relationships\":{},\"violations\":[{}]}}",
            report.is_consistent(),
            report.checked_entities,
            report.checked_relationships,
            report
                .violations
                .iter()
                .map(|violation| format!(
                    "{{\"code\":\"{}\",\"object_id\":\"{}\",\"message\":\"{}\"}}",
                    json_escape(&violation.code),
                    json_escape(&violation.object_id),
                    json_escape(&violation.message)
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ))
}

fn ontology_materialize(engine: &mut Engine) -> ApiResult {
    let (computed, inferred) = engine.materialize_ontology().map_err(engine_error)?;
    Ok((
        200,
        format!("{{\"computed_facts\":{computed},\"inferred_relationships\":{inferred}}}"),
    ))
}

fn ontology_computed(engine: &Engine, id: &str) -> ApiResult {
    let values = computed_properties(&engine.store.state, &EntityId::from(id))
        .map_err(|message| (404, message))?;
    Ok((200, object_map_json(&values)))
}

fn ontology_execute_action(engine: &mut Engine, action: &str, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let actor = required_any(&mut values, &["actor", "agent_id"])?;
    let target = required_any(&mut values, &["target_entity_id", "target"])?;
    let roles = values
        .remove("roles")
        .map(|raw| string_list(&raw))
        .unwrap_or_default();
    let id = engine
        .execute_ontology_action(action, &actor, &roles, &EntityId::from(target))
        .map_err(engine_error)?;
    Ok((
        200,
        format!(
            "{{\"action_execution_id\":\"{}\",\"committed\":true}}",
            json_escape(id.as_str())
        ),
    ))
}

fn ontology_resolve(engine: &mut Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let left = required_any(&mut values, &["left", "left_entity_id"])?;
    let right = required_any(&mut values, &["right", "right_entity_id"])?;
    let persist = boolean(&mut values, "persist", false)?;
    let result = engine
        .resolve_entity_pair(&left.into(), &right.into(), persist)
        .map_err(engine_error)?;
    Ok((
        200,
        format!(
            "{{\"left\":\"{}\",\"right\":\"{}\",\"score\":{},\"equivalent\":{},\"evidence\":[{}]}}",
            json_escape(result.left.as_str()),
            json_escape(result.right.as_str()),
            result.score,
            result.equivalent,
            result
                .evidence
                .iter()
                .map(|value| format!("\"{}\"", json_escape(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ))
}

fn ontology_apply_mapping(engine: &mut Engine, id: &str, body: &str) -> ApiResult {
    let record = fields(body)?;
    let entity_id = engine
        .apply_mapping(&SchemaMappingId::from(id), &record)
        .map_err(engine_error)?;
    let entity = engine
        .store
        .state
        .entity(&entity_id)
        .expect("mapped entity was just persisted");
    Ok((200, entity_json(entity)))
}

fn ontology_authorized_entities(engine: &Engine, query: &BTreeMap<String, String>) -> ApiResult {
    let principal = query
        .get("principal")
        .ok_or((400, "principal query parameter is required".into()))?;
    let action = query.get("action").map(String::as_str).unwrap_or("read");
    let roles = query
        .get("roles")
        .map(|raw| string_list(raw))
        .unwrap_or_default();
    let entities = authorized_entities(&engine.store.state, principal, &roles, action);
    Ok((
        200,
        format!(
            "[{}]",
            entities
                .iter()
                .map(entity_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
    ))
}

fn ontology_blast_radius(engine: &Engine, query: &BTreeMap<String, String>) -> ApiResult {
    let root = query
        .get("root")
        .ok_or((400, "root query parameter is required".into()))?;
    let depth = query
        .get("max_depth")
        .map(|raw| raw.parse())
        .transpose()
        .map_err(|_| (400, "max_depth must be an integer".into()))?
        .unwrap_or(5);
    Ok((
        200,
        ids_json(&blast_radius(
            &engine.store.state,
            &root.as_str().into(),
            depth,
        )),
    ))
}

fn ontology_centrality(engine: &Engine) -> ApiResult {
    Ok((
        200,
        format!(
            "[{}]",
            centrality(&engine.store.state)
                .iter()
                .map(|score| format!(
                    "{{\"entity_id\":\"{}\",\"degree\":{},\"betweenness\":{},\"pagerank\":{},\"business_weight\":{}}}",
                    json_escape(score.entity_id.as_str()),
                    score.degree,
                    score.betweenness,
                    score.pagerank,
                    score.business_weight
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ))
}

fn ontology_shortest_path(engine: &Engine, query: &BTreeMap<String, String>) -> ApiResult {
    let from = query
        .get("from")
        .ok_or((400, "from query parameter is required".into()))?;
    let to = query
        .get("to")
        .ok_or((400, "to query parameter is required".into()))?;
    let (cost, path) = shortest_semantic_path(
        &engine.store.state,
        &EntityId::from(from.as_str()),
        &EntityId::from(to.as_str()),
    )
    .ok_or((404, "semantic path not found".into()))?;
    Ok((
        200,
        format!("{{\"cost\":{cost},\"entities\":{}}}", ids_json(&path)),
    ))
}

fn ontology_query(engine: &Engine, body: &str) -> ApiResult {
    let mut values = fields(body)?;
    let query = SemanticQuery {
        object_type: values
            .remove("object_type")
            .filter(|value| !value.is_empty()),
        conditions: parse_condition_specs(values.remove("conditions").as_deref().unwrap_or(""))?,
        traverse_relationship: values
            .remove("traverse_relationship")
            .filter(|value| !value.is_empty()),
        from_entity: values
            .remove("from_entity")
            .filter(|value| !value.is_empty())
            .map(Into::into),
        max_depth: integer(&mut values, "max_depth", 5)?,
        valid_at: values.remove("valid_at").filter(|value| !value.is_empty()),
        include_inferred: boolean(&mut values, "include_inferred", true)?,
    };
    let result = semantic_query(&engine.store.state, &query);
    Ok((
        200,
        format!(
            "{{\"entities\":[{}],\"relationships\":[{}],\"aggregates\":{}}}",
            result.entities.iter().map(entity_json).collect::<Vec<_>>().join(","),
            result
                .relationships
                .iter()
                .map(|relationship| format!(
                    "{{\"relationship_id\":\"{}\",\"source\":\"{}\",\"type\":\"{}\",\"target\":\"{}\",\"confidence\":{}}}",
                    json_escape(relationship.id.as_str()),
                    json_escape(relationship.source_entity_id.as_str()),
                    json_escape(&relationship.relationship_type),
                    json_escape(relationship.target_entity_id.as_str()),
                    relationship.confidence
                ))
                .collect::<Vec<_>>()
                .join(","),
            string_usize_map_json(&result.aggregates)
        ),
    ))
}

fn parse_typed_attributes(
    engine: &Engine,
    entity_type: &str,
    raw: &str,
) -> Result<BTreeMap<String, ObjectValue>, (u16, String)> {
    let values = wm_ingest::parse_object(raw).map_err(|error| (400, error.to_string()))?;
    if engine.store.state.ontology.object_types.is_empty() {
        return Ok(values
            .into_iter()
            .map(|(key, value)| (key, ObjectValue::String(value)))
            .collect());
    }
    let schemas = effective_properties(&engine.store.state.ontology, entity_type)
        .map_err(|message| (400, message))?;
    values
        .into_iter()
        .map(|(key, raw)| {
            let schema = schemas
                .get(&key)
                .ok_or_else(|| (400, format!("property '{key}' is not declared")))?;
            Ok((key, parse_typed_value(&raw, &schema.value_type)?))
        })
        .collect()
}

fn parse_observation_object(
    engine: &Engine,
    subject: &str,
    predicate: &str,
    raw: &str,
) -> Result<ObjectValue, (u16, String)> {
    if raw.starts_with('{') {
        let mut nested = wm_ingest::parse_object(raw).map_err(|error| (400, error.to_string()))?;
        return Ok(nested
            .remove("entity_id")
            .map(|id| ObjectValue::Entity(id.into()))
            .unwrap_or_else(|| ObjectValue::Json(raw.into())));
    }
    if engine.store.state.ontology.object_types.is_empty() {
        return Ok(ObjectValue::String(raw.into()));
    }
    let entity = engine
        .store
        .state
        .entity(&EntityId::from(subject))
        .ok_or((404, format!("entity {subject} not found")))?;
    let properties = effective_properties(&engine.store.state.ontology, &entity.entity_type)
        .map_err(|message| (400, message))?;
    let schema = properties
        .get(predicate)
        .ok_or((400, format!("property '{predicate}' is not declared")))?;
    parse_typed_value(raw, &schema.value_type)
}

fn parse_property_specs(raw: &str) -> Result<Vec<PropertySchema>, (u16, String)> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(';')
        .map(|item| {
            let fields = item.split(':').collect::<Vec<_>>();
            if !(2..=4).contains(&fields.len()) {
                return Err((400, "property spec must be name:type[:min[:max]]".into()));
            }
            Ok(PropertySchema {
                name: fields[0].trim().into(),
                value_type: parse_value_type(fields[1].trim())?,
                min_count: fields
                    .get(2)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.parse())
                    .transpose()
                    .map_err(|_| (400, "property minimum must be an integer".into()))?
                    .unwrap_or(0),
                max_count: fields
                    .get(3)
                    .filter(|value| !value.is_empty() && **value != "*")
                    .map(|value| value.parse())
                    .transpose()
                    .map_err(|_| (400, "property maximum must be an integer or *".into()))?,
                allowed_values: Vec::new(),
            })
        })
        .collect()
}

fn parse_composition_specs(raw: &str) -> Result<Vec<RelationshipComposition>, (u16, String)> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(';')
        .map(|item| {
            let (then_relationship, implies_relationship) = item
                .split_once('>')
                .ok_or((400, "composition spec must be then>implies".into()))?;
            Ok(RelationshipComposition {
                then_relationship: then_relationship.trim().into(),
                implies_relationship: implies_relationship.trim().into(),
            })
        })
        .collect()
}

fn parse_condition_specs(raw: &str) -> Result<Vec<Condition>, (u16, String)> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(';')
        .map(|item| {
            let fields = item.splitn(4, ':').collect::<Vec<_>>();
            if fields.len() < 2 {
                return Err((
                    400,
                    "condition spec must be property:operator[:type:value]".into(),
                ));
            }
            let operator = match fields[1] {
                "eq" => ComparisonOperator::Equals,
                "ne" => ComparisonOperator::NotEquals,
                "gt" => ComparisonOperator::GreaterThan,
                "ge" => ComparisonOperator::GreaterOrEqual,
                "lt" => ComparisonOperator::LessThan,
                "le" => ComparisonOperator::LessOrEqual,
                "exists" => ComparisonOperator::Exists,
                _ => return Err((400, format!("unknown comparison operator '{}'", fields[1]))),
            };
            let value = if operator == ComparisonOperator::Exists {
                None
            } else if fields.len() == 4 {
                Some(parse_typed_value(fields[3], &parse_value_type(fields[2])?)?)
            } else {
                return Err((
                    400,
                    "non-existence condition requires type and value".into(),
                ));
            };
            Ok(Condition {
                property: fields[0].into(),
                operator,
                value,
            })
        })
        .collect()
}

fn parse_effect_specs(raw: &str) -> Result<Vec<ActionEffect>, (u16, String)> {
    if raw.trim().is_empty() {
        return Err((400, "action effects cannot be empty".into()));
    }
    raw.split(';')
        .map(|item| {
            let fields = if item.starts_with("relationship:") {
                item.splitn(3, ':').collect::<Vec<_>>()
            } else {
                item.splitn(4, ':').collect::<Vec<_>>()
            };
            match fields.as_slice() {
                ["set", property, kind, value] => Ok(ActionEffect::SetProperty {
                    property: (*property).into(),
                    value: parse_typed_value(value, &parse_value_type(kind)?)?,
                }),
                ["remove", property] => Ok(ActionEffect::RemoveProperty {
                    property: (*property).into(),
                }),
                ["relationship", relationship_type, target] => {
                    Ok(ActionEffect::AddRelationship {
                        relationship_type: (*relationship_type).into(),
                        target_entity_id: (*target).into(),
                    })
                }
                ["event", event_type] => Ok(ActionEffect::EmitEvent {
                    event_type: (*event_type).into(),
                }),
                _ => Err((
                    400,
                    "effect must be set:property:type:value, remove:property, relationship:type:target, or event:type"
                        .into(),
                )),
            }
        })
        .collect()
}

fn parse_mapping_specs(raw: &str) -> Result<Vec<FieldMapping>, (u16, String)> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(';')
        .map(|item| {
            let fields = item.splitn(4, ':').collect::<Vec<_>>();
            if fields.len() < 3 {
                return Err((
                    400,
                    "mapping field must be source:target:transform[:argument]".into(),
                ));
            }
            let transform = match fields[2] {
                "identity" => FieldTransform::Identity,
                "lowercase" => FieldTransform::Lowercase,
                "uppercase" => FieldTransform::Uppercase,
                "trim" => FieldTransform::Trim,
                "prefix" if fields.len() == 4 => FieldTransform::Prefix(fields[3].into()),
                other => return Err((400, format!("unsupported field transform '{other}'"))),
            };
            Ok(FieldMapping {
                source_field: fields[0].into(),
                target_property: fields[1].into(),
                transform,
            })
        })
        .collect()
}

fn parse_float_list(raw: &str) -> Result<Vec<f64>, (u16, String)> {
    string_list(raw)
        .into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|_| (400, "identity_weights must contain numbers".into()))
        })
        .collect()
}

fn parse_compatibility(raw: &str) -> Result<CompatibilityMode, (u16, String)> {
    match raw {
        "backward" => Ok(CompatibilityMode::Backward),
        "forward" => Ok(CompatibilityMode::Forward),
        "full" => Ok(CompatibilityMode::Full),
        "breaking" => Ok(CompatibilityMode::Breaking),
        _ => Err((
            400,
            "compatibility must be backward, forward, full, or breaking".into(),
        )),
    }
}

fn parse_value_type(raw: &str) -> Result<ValueType, (u16, String)> {
    match raw {
        "entity" => Ok(ValueType::Entity),
        "string" => Ok(ValueType::String),
        "integer" => Ok(ValueType::Integer),
        "float" => Ok(ValueType::Float),
        "boolean" => Ok(ValueType::Boolean),
        "timestamp" => Ok(ValueType::Timestamp),
        "json" => Ok(ValueType::Json),
        _ => Err((400, format!("unknown ontology value type '{raw}'"))),
    }
}

fn parse_typed_value(raw: &str, value_type: &ValueType) -> Result<ObjectValue, (u16, String)> {
    Ok(match value_type {
        ValueType::Entity => ObjectValue::Entity(raw.into()),
        ValueType::String => ObjectValue::String(raw.into()),
        ValueType::Integer => ObjectValue::Integer(
            raw.parse()
                .map_err(|_| (400, format!("'{raw}' is not an integer")))?,
        ),
        ValueType::Float => ObjectValue::Float(
            raw.parse()
                .map_err(|_| (400, format!("'{raw}' is not a number")))?,
        ),
        ValueType::Boolean => ObjectValue::Boolean(
            raw.parse()
                .map_err(|_| (400, format!("'{raw}' is not a boolean")))?,
        ),
        ValueType::Timestamp => ObjectValue::Timestamp(raw.into()),
        ValueType::Json => ObjectValue::Json(raw.into()),
    })
}

fn object_map_json(values: &BTreeMap<String, ObjectValue>) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(|(key, value)| format!(
                "\"{}\":{}",
                json_escape(key),
                wm_query::object_json(value)
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn string_usize_map_json(values: &BTreeMap<String, usize>) -> String {
    format!(
        "{{{}}}",
        values
            .iter()
            .map(|(key, value)| format!("\"{}\":{}", json_escape(key), value))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn ids_json(values: &[EntityId]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|id| format!("\"{}\"", json_escape(id.as_str())))
            .collect::<Vec<_>>()
            .join(",")
    )
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

fn twin_error(error: wm_twin::TwinError) -> (u16, String) {
    match error {
        wm_twin::TwinError::Invalid(message) => (400, message),
        wm_twin::TwinError::NotFound(message) => (404, message),
        wm_twin::TwinError::Engine(error) => engine_error(error),
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
fn parse_cardinality(value: &str) -> Result<PredicateCardinality, (u16, String)> {
    match value.to_ascii_lowercase().as_str() {
        "single" | "single_exclusive" | "single-exclusive" => {
            Ok(PredicateCardinality::SingleExclusive)
        }
        "multi" | "multi_value" | "multi-value" => Ok(PredicateCardinality::MultiValue),
        _ => Err((400, "cardinality must be single or multi".into())),
    }
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
        "{{\"observation_id\":\"{}\",\"source_id\":\"{}\",\"subject_entity_id\":\"{}\",\"predicate\":\"{}\",\"object\":{},\"observed_at\":\"{}\",\"ingested_at\":\"{}\",\"claimed_valid_from\":\"{}\",\"claimed_valid_to\":{},\"cardinality\":\"{:?}\",\"confidence\":{},\"retracted\":{}}}",
        json_escape(v.id.as_str()),
        json_escape(v.source_id.as_str()),
        json_escape(v.subject_entity_id.as_str()),
        json_escape(&v.predicate),
        wm_query::object_json(&v.object),
        v.observed_at,
        v.ingested_at,
        v.claimed_valid_from,
        json_opt(v.claimed_valid_to.as_deref()),
        v.cardinality,
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

    #[test]
    fn ontology_routes_enforce_schema_and_execute_guarded_actions() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wm-server-ontology-{nonce}.redb"));
        {
            let mut engine = Engine::init(&path).unwrap();
            let registered = route(
                &mut engine,
                request(
                    "POST",
                    "/ontology/definitions",
                    r#"{"kind":"object_type","name":"company","namespace":"world","properties":"status:string:1:1"}"#,
                ),
            );
            assert_eq!(registered.status, 201);
            let invalid = route(
                &mut engine,
                request(
                    "POST",
                    "/entities",
                    r#"{"type":"company","name":"Missing Status"}"#,
                ),
            );
            assert_eq!(invalid.status, 400);
            let created = route(
                &mut engine,
                request(
                    "POST",
                    "/entities",
                    r#"{"type":"company","name":"Acme","attributes":{"status":"active"}}"#,
                ),
            );
            assert_eq!(created.status, 201);
            assert!(created.body.contains("company:acme"));
            let action = route(
                &mut engine,
                request(
                    "POST",
                    "/ontology/definitions",
                    r#"{"kind":"action","id":"action:close","name":"close","target_type":"company","preconditions":"status:eq:string:active","effects":"set:status:string:closed;event:company_closed","postconditions":"status:eq:string:closed","allowed_roles":["operator"]}"#,
                ),
            );
            assert_eq!(action.status, 201);
            let executed = route(
                &mut engine,
                request(
                    "POST",
                    "/ontology/actions/close/execute",
                    r#"{"actor":"agent:ops","roles":["operator"],"target":"company:acme"}"#,
                ),
            );
            assert_eq!(executed.status, 200);
            assert_eq!(engine.store.state.ontology.action_executions.len(), 1);
            let mapping = route(
                &mut engine,
                request(
                    "POST",
                    "/ontology/definitions",
                    r#"{"kind":"mapping","id":"mapping:crm","source_namespace":"crm","source_type":"customer","target_type":"company","semantic_id_template":"crm:customer:{id}","fields":"state:status:identity"}"#,
                ),
            );
            assert_eq!(mapping.status, 201);
            let mapped = route(
                &mut engine,
                request(
                    "POST",
                    "/ontology/mappings/mapping:crm/apply",
                    r#"{"id":"42","state":"active"}"#,
                ),
            );
            assert_eq!(mapped.status, 200);
            assert!(mapped.body.contains("crm:customer:42"));
        }
        let engine = Engine::open(&path).unwrap();
        assert_eq!(engine.store.state.ontology.object_types.len(), 1);
        assert_eq!(engine.store.state.ontology.actions.len(), 1);
        assert_eq!(engine.store.state.ontology.mappings.len(), 1);
        assert_eq!(engine.store.state.ontology.action_executions.len(), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn twin_routes_cover_registration_state_drift_and_commands() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wm-server-twin-{nonce}.redb"));
        {
            let mut engine = Engine::init(&path).unwrap();
            let created = route(
                &mut engine,
                request(
                    "POST",
                    "/twins",
                    r#"{"kind":"industrial.pump","name":"Pump 7","model_id":"dtmi:example:pump;1","capabilities":["telemetry","commands"]}"#,
                ),
            );
            assert_eq!(created.status, 201);
            assert!(created.body.contains("digital-twin:pump-7"));

            let telemetry = route(
                &mut engine,
                request(
                    "POST",
                    "/twins/digital-twin:pump-7/telemetry",
                    r#"{"signal":"rpm","value":"1450","value_type":"integer","observed_at":"2026-10-11T09:00:00Z","unit":"rpm","adapter_id":"opcua-1","protocol":"opcua"}"#,
                ),
            );
            assert_eq!(telemetry.status, 201);
            let desired = route(
                &mut engine,
                request(
                    "POST",
                    "/twins/digital-twin:pump-7/desired",
                    r#"{"signal":"rpm","value":"1500","value_type":"integer","observed_at":"2026-10-11T09:00:01Z","adapter_id":"controller-1","protocol":"agent"}"#,
                ),
            );
            assert_eq!(desired.status, 201);
            let state = route(
                &mut engine,
                request("GET", "/twins/digital-twin:pump-7/state", ""),
            );
            assert_eq!(state.status, 200);
            assert!(state.body.contains("\"reported\":{\"rpm\":1450}"));
            assert!(state.body.contains("\"desired\":{\"rpm\":1500}"));
            assert!(state.body.contains("\"drift\":[{"));

            let command = route(
                &mut engine,
                request(
                    "POST",
                    "/twins/digital-twin:pump-7/commands",
                    r#"{"type":"set_speed","requested_by":"agent:operator","key":"run-1-step-1","requested_at":"2026-10-11T09:01:00Z","parameters":{"rpm":1500}}"#,
                ),
            );
            assert_eq!(command.status, 201);
            assert!(command.body.contains("event:1"));
            let acknowledgement = route(
                &mut engine,
                request(
                    "POST",
                    "/twins/digital-twin:pump-7/commands/event:1/ack",
                    r#"{"status":"succeeded","at":"2026-10-11T09:01:03Z"}"#,
                ),
            );
            assert_eq!(acknowledgement.status, 201);
            let commands = route(
                &mut engine,
                request("GET", "/twins/digital-twin:pump-7/commands", ""),
            );
            assert_eq!(commands.status, 200);
            assert!(commands.body.contains("twin.command.requested"));
            assert!(commands.body.contains("twin.command.succeeded"));
        }
        std::fs::remove_file(path).unwrap();
    }
}
