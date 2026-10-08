use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use wm_core::{EntityId, FactId, ObjectValue, ObservationId, SourceId};
use wm_resolution::{Engine, NewObservation, ResolutionEngine};

#[derive(Clone, Debug)]
struct Config {
    entities: usize,
    observations: usize,
    operations: usize,
    output: PathBuf,
    database: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            entities: 200,
            observations: 2_000,
            operations: 20_000,
            output: PathBuf::from("benchmarks/results/latest.md"),
            database: PathBuf::from("target/benchmarks/worldmodel.redb"),
        }
    }
}

#[derive(Clone, Debug)]
struct Measurement {
    workload: String,
    operations: usize,
    elapsed: Duration,
    samples_ns: Vec<u128>,
    detail: String,
}

impl Measurement {
    fn throughput(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64()
    }

    fn percentile_us(&self, percentile: f64) -> f64 {
        if self.samples_ns.is_empty() {
            return 0.0;
        }
        let mut samples = self.samples_ns.clone();
        samples.sort_unstable();
        let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
        samples[index] as f64 / 1_000.0
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("wm-bench: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = parse_args(env::args().skip(1).collect())?;
    if let Some(parent) = config.database.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    if let Some(parent) = config.output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    if config.database.exists() {
        fs::remove_file(&config.database).map_err(|error| error.to_string())?;
    }

    println!(
        "Preparing {} entities and {} observations...",
        config.entities, config.observations
    );
    let mut engine = Engine::init(&config.database).map_err(|error| error.to_string())?;
    let source = engine
        .create_source(
            "benchmark",
            "urn:wm-bench:source",
            "benchmark",
            100,
            BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?;

    let entity_start = Instant::now();
    let mut entity_samples = Vec::with_capacity(config.entities);
    for index in 0..config.entities {
        let started = Instant::now();
        engine
            .create_entity(
                "company",
                format!("bench-{index:06}"),
                vec![],
                BTreeMap::new(),
            )
            .map_err(|error| error.to_string())?;
        entity_samples.push(started.elapsed().as_nanos());
    }
    let entity_elapsed = entity_start.elapsed();

    let ingest_start = Instant::now();
    let mut ingest_samples = Vec::with_capacity(config.observations);
    for index in 0..config.observations {
        let started = Instant::now();
        engine
            .observe(observation(
                index,
                config.entities,
                source.clone(),
                "metric",
            ))
            .map_err(|error| error.to_string())?;
        ingest_samples.push(started.elapsed().as_nanos());
    }
    let ingest_elapsed = ingest_start.elapsed();

    for index in 0..config.entities.saturating_sub(1) {
        engine
            .add_relationship(
                entity_id(index),
                "DEPENDS_ON",
                entity_id(index + 1),
                "2026-01-01T00:00:00Z",
                None,
                1.0,
                vec![],
            )
            .map_err(|error| error.to_string())?;
    }

    let mut measurements = vec![
        Measurement {
            workload: "Entity insert".into(),
            operations: config.entities,
            elapsed: entity_elapsed,
            samples_ns: entity_samples,
            detail: "individual transactional inserts".into(),
        },
        Measurement {
            workload: "Bulk observation ingest".into(),
            operations: config.observations,
            elapsed: ingest_elapsed,
            samples_ns: ingest_samples,
            detail: "resolve + provenance + redb commit per observation".into(),
        },
    ];

    measurements.push(measure_loop(
        "YCSB-C point entity read",
        config.operations,
        "100% reads; deterministic key distribution",
        |index| {
            let wanted = entity_id(index % config.entities);
            black_box(engine.store.state.entity(&wanted));
        },
    ));

    measurements.push(measure_loop(
        "YCSB-C point observation read",
        config.operations,
        "100% reads over immutable observation IDs",
        |index| {
            let wanted = ObservationId(format!("observation:{}", index % config.observations + 1));
            black_box(engine.store.state.observation(&wanted));
        },
    ));

    measurements.push(measure_loop(
        "Current entity state",
        config.operations / 10,
        "bitemporal current-view lookup",
        |index| {
            black_box(wm_query::entity_state(
                &engine.store.state,
                &entity_id(index % config.entities),
                None,
                None,
            ));
        },
    ));

    measurements.push(measure_loop(
        "Combined bitemporal state",
        config.operations / 10,
        "VALID AT plus KNOWN AT",
        |index| {
            black_box(wm_query::entity_state(
                &engine.store.state,
                &entity_id(index % config.entities),
                Some("2026-01-01T00:15:00Z"),
                Some("2026-01-01T00:15:00Z"),
            ));
        },
    ));

    measurements.push(measure_loop(
        "WHY provenance expansion",
        config.operations / 10,
        "fact + supporting observations + sources + conflicts",
        |index| {
            black_box(wm_query::why(
                &engine.store.state,
                &FactId(format!("fact:{}", index % config.observations + 1)),
            ));
        },
    ));

    measurements.push(measure_loop(
        "Entity changes",
        config.operations / 100,
        "facts, relationships, and events in a known-time window",
        |index| {
            black_box(wm_query::changes(
                &engine.store.state,
                &entity_id(index % config.entities),
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:30:00Z",
            ));
        },
    ));

    measurements.push(measure_loop(
        "OLAP columnar GROUP BY",
        (config.operations / 100).max(20),
        "SELECT predicate, COUNT(*) FROM wm_facts GROUP BY predicate",
        |_| {
            black_box(
                wm_query::aggregate_fact_counts(&engine.store.state, "predicate")
                    .expect("benchmark dimension is valid"),
            );
        },
    ));

    measurements.push(measure_loop(
        "World diff",
        (config.operations / 100).max(20),
        "full historical transition scan",
        |_| {
            black_box(wm_query::diff_world(
                &engine.store.state,
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:30:00Z",
            ));
        },
    ));

    measurements.push(measure_loop(
        "Temporal graph path",
        100,
        "chain path from first to last entity",
        |_| {
            black_box(wm_graph::find_path(
                &engine.store.state,
                &entity_id(0),
                &entity_id(config.entities - 1),
                config.entities,
                Some("DEPENDS_ON"),
                Some("2026-02-01T00:00:00Z"),
            ));
        },
    ));

    measurements.push(measure_loop(
        "Database restart/open",
        30,
        "redb open + canonical snapshot decode",
        |_| {
            black_box(Engine::open(&config.database).expect("benchmark database reopens"));
        },
    ));

    let mut rebuild_engine = engine.clone();
    measurements.push(measure_loop(
        "Current-state rebuild",
        5,
        "replay immutable observations and deterministic resolution",
        |_| {
            rebuild_engine
                .rebuild_current()
                .expect("benchmark state rebuilds");
            black_box(rebuild_engine.store.state.current_facts().count());
        },
    ));

    measurements.push(mixed_workload(
        &mut engine,
        &source,
        config.operations / 10,
        config.entities,
    )?);

    let bytes = fs::metadata(&config.database)
        .map_err(|error| error.to_string())?
        .len();
    let digest = state_digest(&engine);
    let report = report(&config, &measurements, bytes, digest, &engine);
    fs::write(&config.output, &report).map_err(|error| error.to_string())?;
    println!("{report}");
    println!("Saved {}", config.output.display());
    Ok(())
}

fn observation(
    index: usize,
    entity_count: usize,
    source_id: SourceId,
    prefix: &str,
) -> NewObservation {
    NewObservation {
        source_id,
        subject_entity_id: entity_id(index % entity_count),
        predicate: format!("{prefix}_{:02}", (index / entity_count) % 32),
        object: ObjectValue::Integer(index as i64),
        observed_at: timestamp(index),
        ingested_at: Some(timestamp(index)),
        claimed_valid_from: None,
        claimed_valid_to: None,
        cardinality: wm_core::PredicateCardinality::SingleExclusive,
        confidence: 0.95,
        raw_payload: format!("benchmark-record-{index}"),
        metadata: BTreeMap::new(),
        retracted: false,
    }
}

fn entity_id(index: usize) -> EntityId {
    EntityId(format!("company:bench-{index:06}"))
}

fn timestamp(index: usize) -> String {
    let seconds = index % 60;
    let minutes = (index / 60) % 60;
    let hours = (index / 3_600) % 24;
    format!("2026-01-01T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn measure_loop(
    name: &str,
    operations: usize,
    detail: &str,
    mut operation: impl FnMut(usize),
) -> Measurement {
    for index in 0..operations.min(100) {
        operation(index);
    }
    let mut samples = Vec::with_capacity(operations);
    let started = Instant::now();
    for index in 0..operations {
        let sample = Instant::now();
        operation(index);
        samples.push(sample.elapsed().as_nanos());
    }
    Measurement {
        workload: name.into(),
        operations,
        elapsed: started.elapsed(),
        samples_ns: samples,
        detail: detail.into(),
    }
}

fn mixed_workload(
    engine: &mut Engine,
    source: &SourceId,
    operations: usize,
    entity_count: usize,
) -> Result<Measurement, String> {
    let mut samples = Vec::with_capacity(operations);
    let started = Instant::now();
    for index in 0..operations {
        let sample = Instant::now();
        if index % 20 == 0 {
            let mut input = observation(
                100_000 + index,
                entity_count,
                source.clone(),
                "mixed_update",
            );
            input.ingested_at = Some(format!(
                "2026-02-01T00:{:02}:{:02}Z",
                (index / 60) % 60,
                index % 60
            ));
            input.observed_at = input.ingested_at.clone().unwrap();
            engine.observe(input).map_err(|error| error.to_string())?;
        } else {
            let wanted = entity_id(index % entity_count);
            black_box(
                engine
                    .store
                    .state
                    .entities
                    .iter()
                    .find(|entity| entity.id == wanted),
            );
        }
        samples.push(sample.elapsed().as_nanos());
    }
    Ok(Measurement {
        workload: "YCSB-B mixed 95/5".into(),
        operations,
        elapsed: started.elapsed(),
        samples_ns: samples,
        detail: "95% point reads, 5% resolved transactional writes".into(),
    })
}

fn report(
    config: &Config,
    measurements: &[Measurement],
    bytes: u64,
    digest: u64,
    engine: &Engine,
) -> String {
    let unix_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut output = format!(
        "# World Model DB benchmark results\n\n- Run timestamp (Unix UTC): `{unix_time}`\n- Build: `--release`\n- Platform: `{}/{}`\n- Schema version: `{}`\n- Dataset: {} entities, {} initial observations\n- Final records: {} observations, {} fact versions, {} relationships, {} conflicts\n- Source priority: `100`; generated conflict ratio: `0%`\n- Timed operation budget: {}\n- Database size after mixed workload: {} bytes ({:.2} MiB)\n- Deterministic state digest (FNV-1a): `{digest:016x}`\n\n> These are deterministic workload analogues, not certified YCSB or TPC results.\n\n| Workload | Operations | Throughput ops/s | p50 µs | p95 µs | p99 µs | Detail |\n|---|---:|---:|---:|---:|---:|---|\n",
        env::consts::OS,
        env::consts::ARCH,
        engine.store.state.schema_version,
        config.entities,
        config.observations,
        engine.store.state.observations.len(),
        engine.store.state.facts.len(),
        engine.store.state.relationships.len(),
        engine.store.state.conflicts.len(),
        config.operations,
        bytes,
        bytes as f64 / 1_048_576.0,
    );
    for measurement in measurements {
        output.push_str(&format!(
            "| {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {} |\n",
            measurement.workload,
            measurement.operations,
            measurement.throughput(),
            measurement.percentile_us(0.50),
            measurement.percentile_us(0.95),
            measurement.percentile_us(0.99),
            measurement.detail,
        ));
    }
    output
}

fn state_digest(engine: &Engine) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut add = |value: &str| {
        for byte in value.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    };
    add(&engine.store.state.schema_version.to_string());
    for entity in &engine.store.state.entities {
        add(&format!("{entity:?}"));
    }
    for source in &engine.store.state.sources {
        add(&format!("{source:?}"));
    }
    for observation in &engine.store.state.observations {
        add(&format!("{observation:?}"));
    }
    for fact in &engine.store.state.facts {
        add(&format!("{fact:?}"));
    }
    for relationship in &engine.store.state.relationships {
        add(&format!("{relationship:?}"));
    }
    for event in &engine.store.state.events {
        add(&format!("{event:?}"));
    }
    for evidence in &engine.store.state.evidence {
        add(&format!("{evidence:?}"));
    }
    for conflict in &engine.store.state.conflicts {
        add(&format!("{conflict:?}"));
    }
    for correlation in &engine.store.state.correlations {
        add(&format!("{correlation:?}"));
    }
    hash
}

fn parse_args(mut args: Vec<String>) -> Result<Config, String> {
    let mut config = Config::default();
    while !args.is_empty() {
        let flag = args.remove(0);
        let value = if flag == "--help" {
            println!(
                "Usage: wm-bench [--entities N] [--observations N] [--operations N] [--database PATH] [--output PATH]"
            );
            std::process::exit(0);
        } else if args.is_empty() {
            return Err(format!("missing value for {flag}"));
        } else {
            args.remove(0)
        };
        match flag.as_str() {
            "--entities" => config.entities = positive(&value, &flag)?,
            "--observations" => config.observations = positive(&value, &flag)?,
            "--operations" => config.operations = positive(&value, &flag)?,
            "--database" => config.database = Path::new(&value).to_path_buf(),
            "--output" => config.output = Path::new(&value).to_path_buf(),
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    Ok(config)
}

fn positive(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{flag} must be a positive integer"))
}
