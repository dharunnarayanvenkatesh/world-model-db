//! SpyTime-compatible bitemporal workload for cross-database comparisons.
//!
//! The workload follows the ten query classes and SF1 size described by the
//! SpyTime authors. It emits the exact generated rows so another database can
//! import identical input rather than merely recreating a similar distribution.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use wm_core::{EntityId, ObjectValue, Observation, PredicateCardinality};
use wm_resolution::{Engine, NewObservation, ResolutionEngine};

const HORIZON_SECONDS: i64 = 6 * 365 * 86_400 + 2 * 86_400;
const SPY_COUNT: usize = 100;
const CITY_COUNT: usize = 20;
const REPORTER_COUNT: usize = 20;

#[derive(Clone, Debug)]
struct Config {
    records: usize,
    repetitions: usize,
    seed: u64,
    database: PathBuf,
    dataset: PathBuf,
    json: PathBuf,
    markdown: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            records: 10_000,
            repetitions: 10,
            seed: 0x5350_5954_494d_4531,
            database: PathBuf::from("target/benchmarks/spytime-sf1.redb"),
            dataset: PathBuf::from("benchmarks/spytime/dataset-sf1.csv"),
            json: PathBuf::from("benchmarks/results/spytime-wqmdb-sf1.json"),
            markdown: PathBuf::from("benchmarks/results/spytime-wqmdb-sf1.md"),
        }
    }
}

#[derive(Clone, Debug)]
struct Row {
    id: usize,
    spy: usize,
    city: usize,
    valid_begin_s: i64,
    valid_end_s: i64,
    tx_begin_s: i64,
    tx_end_s: i64,
    reporter: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct QueryOutcome {
    rows: u64,
    checksum: u64,
}

#[derive(Clone, Debug)]
struct QueryResult {
    id: &'static str,
    description: &'static str,
    samples: Vec<Duration>,
    rows: u64,
    checksum: u64,
}

impl QueryResult {
    fn mean_ms(&self) -> f64 {
        self.samples.iter().map(Duration::as_secs_f64).sum::<f64>() * 1_000.0
            / self.samples.len() as f64
    }

    fn stddev_ms(&self) -> f64 {
        let mean = self.mean_ms();
        let variance = self
            .samples
            .iter()
            .map(|sample| {
                let delta = sample.as_secs_f64() * 1_000.0 - mean;
                delta * delta
            })
            .sum::<f64>()
            / self.samples.len() as f64;
        variance.sqrt()
    }

    fn min_ms(&self) -> f64 {
        self.samples
            .iter()
            .map(|sample| sample.as_secs_f64() * 1_000.0)
            .fold(f64::INFINITY, f64::min)
    }

    fn max_ms(&self) -> f64 {
        self.samples
            .iter()
            .map(|sample| sample.as_secs_f64() * 1_000.0)
            .fold(0.0, f64::max)
    }
}

#[derive(Clone, Debug)]
struct Params {
    spy: String,
    other_spy: String,
    valid_at: String,
    tx_at: String,
    valid_begin: String,
    valid_end: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("wm-spytime: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = parse_args(env::args().skip(1).collect())?;
    for path in [
        &config.database,
        &config.dataset,
        &config.json,
        &config.markdown,
    ] {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
    }
    if config.database.exists() {
        fs::remove_file(&config.database).map_err(|error| error.to_string())?;
    }

    println!(
        "Generating SpyTime SF{:.2}: {} rows, seed {}",
        config.records as f64 / 10_000.0,
        config.records,
        config.seed
    );
    let rows = generate_rows(config.records, config.seed);
    write_dataset(&config.dataset, &rows)?;

    let mut engine = Engine::init(&config.database).map_err(|error| error.to_string())?;
    let mut sources = Vec::with_capacity(REPORTER_COUNT);
    for reporter in 0..REPORTER_COUNT {
        sources.push(
            engine
                .create_source(
                    "spytime-reporter",
                    format!("urn:spytime:reporter:{reporter:03}"),
                    format!("reporter-{reporter:03}"),
                    100,
                    BTreeMap::new(),
                )
                .map_err(|error| error.to_string())?,
        );
    }
    for spy in 0..SPY_COUNT {
        engine
            .create_entity("spy", format!("spy-{spy:03}"), Vec::new(), BTreeMap::new())
            .map_err(|error| error.to_string())?;
    }

    println!(
        "Loading {} bitemporal rows through the public observation API...",
        rows.len()
    );
    let load_started = Instant::now();
    for (position, row) in rows.iter().enumerate() {
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "spytime.transaction_end".into(),
            ObjectValue::Timestamp(timestamp(row.tx_end_s)),
        );
        metadata.insert(
            "spytime.reporter".into(),
            ObjectValue::String(format!("reporter-{:03}", row.reporter)),
        );
        engine
            .observe(NewObservation {
                source_id: sources[row.reporter].clone(),
                subject_entity_id: EntityId(format!("spy:spy-{:03}", row.spy)),
                // A row-specific physical key prevents resolution from collapsing
                // historical visits. The logical property remains LOCATED_IN.
                predicate: format!("SPYTIME_LOCATED_IN_{:06}", row.id),
                object: ObjectValue::String(format!("city-{:03}", row.city)),
                observed_at: timestamp(row.valid_begin_s),
                ingested_at: Some(timestamp(row.tx_begin_s)),
                claimed_valid_from: Some(timestamp(row.valid_begin_s)),
                claimed_valid_to: Some(timestamp(row.valid_end_s)),
                cardinality: PredicateCardinality::MultiValue,
                confidence: 1.0,
                raw_payload: format!("spytime-row-{}", row.id),
                metadata,
                retracted: false,
            })
            .map_err(|error| error.to_string())?;
        if (position + 1) % 1_000 == 0 {
            println!("  loaded {}/{}", position + 1, rows.len());
        }
    }
    let load_elapsed = load_started.elapsed();

    let observations = &engine.store.state.observations;
    if observations.len() != config.records {
        return Err(format!(
            "correctness failure: expected {} observations, found {}",
            config.records,
            observations.len()
        ));
    }
    let params = make_params(&rows, config.repetitions);
    println!(
        "Running all 10 SpyTime query classes (one warm-up + {} measured runs)...",
        config.repetitions
    );
    let results = run_queries(observations, &params);
    let geometric_mean = geometric_mean_ms(&results);
    let database_bytes = fs::metadata(&config.database)
        .map_err(|error| error.to_string())?
        .len();
    let dataset_bytes = fs::metadata(&config.dataset)
        .map_err(|error| error.to_string())?
        .len();
    let run_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let git_revision = command_output("git", &["rev-parse", "HEAD"]);
    let rustc = command_output("rustc", &["--version"]);
    let cpu = env::var("PROCESSOR_IDENTIFIER").unwrap_or_else(|_| "unknown".into());
    let logical_cpus = env::var("NUMBER_OF_PROCESSORS").unwrap_or_else(|_| "unknown".into());

    let context = ReportContext {
        run_timestamp,
        git_revision,
        rustc,
        cpu,
        logical_cpus,
        load_elapsed,
        database_bytes,
        dataset_bytes,
        geometric_mean,
    };
    fs::write(&config.json, json_report(&config, &results, &context))
        .map_err(|error| error.to_string())?;
    let markdown = markdown_report(&config, &results, &context);
    fs::write(&config.markdown, &markdown).map_err(|error| error.to_string())?;
    println!("{markdown}");
    println!("Machine-readable result: {}", config.json.display());
    println!("Exact import dataset: {}", config.dataset.display());
    Ok(())
}

fn generate_rows(count: usize, seed: u64) -> Vec<Row> {
    let mut rng = Lcg(seed);
    (0..count)
        .map(|id| {
            let tx_begin_s = ((id as i64 + 1) * (HORIZON_SECONDS - 86_400) / (count as i64 + 1))
                + id as i64 % 59;
            let tx_duration = (7 + rng.range(180)) as i64 * 86_400;
            let valid_begin_s = rng.range(HORIZON_SECONDS as u64 - 181 * 86_400) as i64;
            let valid_duration = (7 + rng.range(180)) as i64 * 86_400;
            Row {
                id,
                spy: rng.range(SPY_COUNT as u64) as usize,
                city: rng.range(CITY_COUNT as u64) as usize,
                valid_begin_s,
                valid_end_s: valid_begin_s + valid_duration,
                tx_begin_s,
                tx_end_s: (tx_begin_s + tx_duration).min(HORIZON_SECONDS),
                reporter: rng.range(REPORTER_COUNT as u64) as usize,
            }
        })
        .collect()
}

fn write_dataset(path: &Path, rows: &[Row]) -> Result<(), String> {
    let mut csv = String::from(
        "row_id,spy,city,valid_begin,valid_end,transaction_begin,transaction_end,reporter\n",
    );
    for row in rows {
        csv.push_str(&format!(
            "{},spy-{:03},city-{:03},{},{},{},{},reporter-{:03}\n",
            row.id,
            row.spy,
            row.city,
            timestamp(row.valid_begin_s),
            timestamp(row.valid_end_s),
            timestamp(row.tx_begin_s),
            timestamp(row.tx_end_s),
            row.reporter
        ));
    }
    fs::write(path, csv).map_err(|error| error.to_string())
}

fn make_params(rows: &[Row], repetitions: usize) -> Vec<Params> {
    (0..repetitions)
        .map(|repeat| {
            let row = &rows[(repeat * 997 + 17) % rows.len()];
            Params {
                spy: format!("spy:spy-{:03}", row.spy),
                other_spy: format!("spy:spy-{:03}", (row.spy + 1) % SPY_COUNT),
                valid_at: timestamp((row.valid_begin_s + row.valid_end_s) / 2),
                tx_at: timestamp((row.tx_begin_s + row.tx_end_s) / 2),
                valid_begin: timestamp(row.valid_begin_s),
                valid_end: timestamp(row.valid_end_s),
            }
        })
        .collect()
}

fn run_queries(observations: &[Observation], params: &[Params]) -> Vec<QueryResult> {
    vec![
        measure("Q1", "spy attribute lookup", observations, params, q1),
        measure("Q2", "bitemporal time slice", observations, params, q2),
        measure(
            "Q3",
            "when valid-day events were recorded",
            observations,
            params,
            q3,
        ),
        measure(
            "Q4",
            "history known as of transaction time",
            observations,
            params,
            q4,
        ),
        measure(
            "Q5",
            "valid-time interval overlap",
            observations,
            params,
            q5,
        ),
        measure(
            "Q6",
            "same-city spies known at transaction time",
            observations,
            params,
            q6,
        ),
        measure(
            "Q7",
            "simultaneous same-city spies",
            observations,
            params,
            q7,
        ),
        measure(
            "Q8",
            "shared-city transaction-time overlap",
            observations,
            params,
            q8,
        ),
        measure(
            "Q9",
            "inconsistent simultaneous locations",
            observations,
            params,
            q9,
        ),
        measure("Q10", "meeting discovery lag", observations, params, q10),
    ]
}

fn measure(
    id: &'static str,
    description: &'static str,
    observations: &[Observation],
    params: &[Params],
    query: fn(&[Observation], &Params) -> QueryOutcome,
) -> QueryResult {
    black_box(query(observations, &params[0]));
    let mut samples = Vec::with_capacity(params.len());
    let mut rows = 0u64;
    let mut checksum = FNV_OFFSET;
    for param in params {
        let started = Instant::now();
        let outcome = black_box(query(observations, param));
        samples.push(started.elapsed());
        rows = rows.wrapping_add(outcome.rows);
        mix(&mut checksum, outcome.checksum);
    }
    QueryResult {
        id,
        description,
        samples,
        rows,
        checksum,
    }
}

fn q1(observations: &[Observation], p: &Params) -> QueryOutcome {
    fold_matches(
        observations
            .iter()
            .filter(|o| o.subject_entity_id.0 == p.spy),
    )
}

fn q2(observations: &[Observation], p: &Params) -> QueryOutcome {
    fold_matches(
        observations
            .iter()
            .filter(|o| valid_contains(o, &p.valid_at) && transaction_contains(o, &p.tx_at)),
    )
}

fn q3(observations: &[Observation], p: &Params) -> QueryOutcome {
    fold_matches(
        observations
            .iter()
            .filter(|o| valid_contains(o, &p.valid_at)),
    )
}

fn q4(observations: &[Observation], p: &Params) -> QueryOutcome {
    fold_matches(
        observations
            .iter()
            .filter(|o| transaction_contains(o, &p.tx_at)),
    )
}

fn q5(observations: &[Observation], p: &Params) -> QueryOutcome {
    fold_matches(observations.iter().filter(|o| {
        intervals_overlap(
            &o.claimed_valid_from,
            o.claimed_valid_to.as_deref().unwrap_or(MAX_TIME),
            &p.valid_begin,
            &p.valid_end,
        )
    }))
}

fn q6(observations: &[Observation], p: &Params) -> QueryOutcome {
    let cities = observations
        .iter()
        .filter(|o| o.subject_entity_id.0 == p.spy && transaction_contains(o, &p.tx_at))
        .map(city)
        .collect::<BTreeSet<_>>();
    fold_matches(observations.iter().filter(|o| {
        o.subject_entity_id.0 != p.spy
            && transaction_contains(o, &p.tx_at)
            && cities.contains(city(o))
    }))
}

fn q7(observations: &[Observation], p: &Params) -> QueryOutcome {
    let target = observations
        .iter()
        .filter(|o| o.subject_entity_id.0 == p.spy && transaction_contains(o, &p.tx_at))
        .collect::<Vec<_>>();
    fold_pairs(target.iter().flat_map(|left| {
        observations.iter().filter_map(move |right| {
            (right.subject_entity_id.0 != p.spy
                && transaction_contains(right, &p.tx_at)
                && city(left) == city(right)
                && valid_overlap(left, right))
            .then_some((*left, right))
        })
    }))
}

fn q8(observations: &[Observation], p: &Params) -> QueryOutcome {
    let target = observations
        .iter()
        .filter(|o| o.subject_entity_id.0 == p.spy)
        .collect::<Vec<_>>();
    fold_pairs(target.iter().flat_map(|left| {
        observations.iter().filter_map(move |right| {
            (right.subject_entity_id.0 != p.spy
                && city(left) == city(right)
                && transaction_overlap(left, right))
            .then_some((*left, right))
        })
    }))
}

fn q9(observations: &[Observation], p: &Params) -> QueryOutcome {
    let target = observations
        .iter()
        .filter(|o| o.subject_entity_id.0 == p.spy)
        .collect::<Vec<_>>();
    let mut outcome = QueryOutcome::default();
    for left_index in 0..target.len() {
        for right in &target[left_index + 1..] {
            let left = target[left_index];
            if city(left) != city(right)
                && valid_overlap(left, right)
                && transaction_overlap(left, right)
            {
                add_pair(&mut outcome, left, right);
            }
        }
    }
    outcome
}

fn q10(observations: &[Observation], p: &Params) -> QueryOutcome {
    let left = observations
        .iter()
        .filter(|o| o.subject_entity_id.0 == p.spy)
        .collect::<Vec<_>>();
    let right = observations
        .iter()
        .filter(|o| o.subject_entity_id.0 == p.other_spy)
        .collect::<Vec<_>>();
    let mut outcome = QueryOutcome::default();
    for a in left {
        for b in &right {
            if city(a) == city(b) && valid_overlap(a, b) {
                outcome.rows += 1;
                let event = parse_timestamp(&max_str(&a.claimed_valid_from, &b.claimed_valid_from));
                let discovered = parse_timestamp(&max_str(&a.ingested_at, &b.ingested_at));
                mix(
                    &mut outcome.checksum,
                    discovered.saturating_sub(event) as u64,
                );
                hash_text(&mut outcome.checksum, a.id.as_str());
                hash_text(&mut outcome.checksum, b.id.as_str());
            }
        }
    }
    outcome
}

fn fold_matches<'a>(values: impl Iterator<Item = &'a Observation>) -> QueryOutcome {
    let mut outcome = QueryOutcome::default();
    for observation in values {
        outcome.rows += 1;
        hash_text(&mut outcome.checksum, observation.id.as_str());
        hash_text(&mut outcome.checksum, city(observation));
    }
    outcome
}

fn fold_pairs<'a>(
    values: impl Iterator<Item = (&'a Observation, &'a Observation)>,
) -> QueryOutcome {
    let mut outcome = QueryOutcome::default();
    for (left, right) in values {
        add_pair(&mut outcome, left, right);
    }
    outcome
}

fn add_pair(outcome: &mut QueryOutcome, left: &Observation, right: &Observation) {
    outcome.rows += 1;
    hash_text(&mut outcome.checksum, left.id.as_str());
    hash_text(&mut outcome.checksum, right.id.as_str());
}

fn valid_contains(observation: &Observation, point: &str) -> bool {
    observation.claimed_valid_from.as_str() <= point
        && point < observation.claimed_valid_to.as_deref().unwrap_or(MAX_TIME)
}

fn transaction_contains(observation: &Observation, point: &str) -> bool {
    observation.ingested_at.as_str() <= point && point < tx_end(observation)
}

fn valid_overlap(left: &Observation, right: &Observation) -> bool {
    intervals_overlap(
        &left.claimed_valid_from,
        left.claimed_valid_to.as_deref().unwrap_or(MAX_TIME),
        &right.claimed_valid_from,
        right.claimed_valid_to.as_deref().unwrap_or(MAX_TIME),
    )
}

fn transaction_overlap(left: &Observation, right: &Observation) -> bool {
    intervals_overlap(
        &left.ingested_at,
        tx_end(left),
        &right.ingested_at,
        tx_end(right),
    )
}

fn intervals_overlap(a_begin: &str, a_end: &str, b_begin: &str, b_end: &str) -> bool {
    a_begin < b_end && b_begin < a_end
}

fn city(observation: &Observation) -> &str {
    match &observation.object {
        ObjectValue::String(value) => value,
        _ => unreachable!("SpyTime city is a string"),
    }
}

fn tx_end(observation: &Observation) -> &str {
    match observation.metadata.get("spytime.transaction_end") {
        Some(ObjectValue::Timestamp(value)) => value,
        _ => unreachable!("SpyTime transaction end is present"),
    }
}

const MAX_TIME: &str = "9999-12-31T23:59:59Z";
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn mix(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn hash_text(hash: &mut u64, value: &str) {
    for byte in value.bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn geometric_mean_ms(results: &[QueryResult]) -> f64 {
    (results
        .iter()
        .map(|result| result.mean_ms().ln())
        .sum::<f64>()
        / results.len() as f64)
        .exp()
}

struct ReportContext {
    run_timestamp: u64,
    git_revision: String,
    rustc: String,
    cpu: String,
    logical_cpus: String,
    load_elapsed: Duration,
    database_bytes: u64,
    dataset_bytes: u64,
    geometric_mean: f64,
}

fn markdown_report(config: &Config, results: &[QueryResult], c: &ReportContext) -> String {
    let mut output = format!(
        "# SpyTime benchmark: World Model DB\n\n- Workload: `SpyTime-compatible Q1-Q10`\n- Scale: `{}` bitemporal rows (SF `{:.2}`; SF1 is 10,000)\n- Seed: `{}`\n- Measured selections per query: `{}` after one unmeasured warm-up\n- Overall response time (geometric mean of query means): **{:.3} ms**\n- Load time through public durable API: `{:.3} s` ({:.1} rows/s)\n- Database size: `{}` bytes; import CSV: `{}` bytes\n- Platform: `{}/{}`; CPU: `{}`; logical CPUs: `{}`\n- Rust: `{}`\n- Git revision: `{}`\n- Run timestamp (Unix UTC): `{}`\n\n| Query | Meaning | Mean ms | Stddev ms | Min ms | Max ms | Rows across runs | Checksum |\n|---|---|---:|---:|---:|---:|---:|---|\n",
        config.records,
        config.records as f64 / 10_000.0,
        config.seed,
        config.repetitions,
        c.geometric_mean,
        c.load_elapsed.as_secs_f64(),
        config.records as f64 / c.load_elapsed.as_secs_f64(),
        c.database_bytes,
        c.dataset_bytes,
        env::consts::OS,
        env::consts::ARCH,
        c.cpu,
        c.logical_cpus,
        c.rustc,
        c.git_revision,
        c.run_timestamp,
    );
    for result in results {
        output.push_str(&format!(
            "| {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {} | `{:016x}` |\n",
            result.id,
            result.description,
            result.mean_ms(),
            result.stddev_ms(),
            result.min_ms(),
            result.max_ms(),
            result.rows,
            result.checksum,
        ));
    }
    output.push_str(
        "\n## Interpretation\n\nUse the geometric mean as SpyTime's single response-time score. Compare only release builds on the same machine, importing the checked-in CSV and preserving half-open `[begin, end)` interval semantics. Row totals and checksums are correctness guards; a faster run with different values is not comparable. The physical WQMDB mapping stores each visit as an immutable observation, valid time in `claimed_valid_*`, transaction begin in `ingested_at`, and transaction end in metadata.\n",
    );
    output
}

fn json_report(config: &Config, results: &[QueryResult], c: &ReportContext) -> String {
    let query_json = results
        .iter()
        .map(|result| {
            format!(
                "    {{\"id\":\"{}\",\"description\":\"{}\",\"mean_ms\":{:.6},\"stddev_ms\":{:.6},\"min_ms\":{:.6},\"max_ms\":{:.6},\"rows\":{},\"checksum\":\"{:016x}\"}}",
                result.id,
                json_escape(result.description),
                result.mean_ms(),
                result.stddev_ms(),
                result.min_ms(),
                result.max_ms(),
                result.rows,
                result.checksum,
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "{{\n  \"benchmark\": \"SpyTime-compatible\",\n  \"system\": \"World Model DB\",\n  \"records\": {},\n  \"scale_factor\": {:.4},\n  \"seed\": {},\n  \"repetitions\": {},\n  \"warmups_per_query\": 1,\n  \"interval_semantics\": \"half-open [begin,end)\",\n  \"geometric_mean_ms\": {:.6},\n  \"load_seconds\": {:.6},\n  \"database_bytes\": {},\n  \"dataset_bytes\": {},\n  \"run_timestamp_unix_utc\": {},\n  \"git_revision\": \"{}\",\n  \"platform\": \"{}/{}\",\n  \"cpu\": \"{}\",\n  \"logical_cpus\": \"{}\",\n  \"rustc\": \"{}\",\n  \"queries\": [\n{}\n  ]\n}}\n",
        config.records,
        config.records as f64 / 10_000.0,
        config.seed,
        config.repetitions,
        c.geometric_mean,
        c.load_elapsed.as_secs_f64(),
        c.database_bytes,
        c.dataset_bytes,
        c.run_timestamp,
        json_escape(&c.git_revision),
        env::consts::OS,
        env::consts::ARCH,
        json_escape(&c.cpu),
        json_escape(&c.logical_cpus),
        json_escape(&c.rustc),
        query_json,
    )
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn parse_args(mut args: Vec<String>) -> Result<Config, String> {
    let mut config = Config::default();
    while !args.is_empty() {
        let flag = args.remove(0);
        if flag == "--help" {
            println!(
                "Usage: wm-spytime [--records N] [--repetitions N] [--seed N] [--database PATH] [--dataset PATH] [--json PATH] [--markdown PATH]"
            );
            std::process::exit(0);
        }
        if args.is_empty() {
            return Err(format!("missing value for {flag}"));
        }
        let value = args.remove(0);
        match flag.as_str() {
            "--records" => config.records = positive(&value, &flag)?,
            "--repetitions" => config.repetitions = positive(&value, &flag)?,
            "--seed" => config.seed = value.parse().map_err(|_| "--seed must be a u64")?,
            "--database" => config.database = PathBuf::from(value),
            "--dataset" => config.dataset = PathBuf::from(value),
            "--json" => config.json = PathBuf::from(value),
            "--markdown" => config.markdown = PathBuf::from(value),
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

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn range(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn timestamp(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = date_from_2020(days);
    let hour = second_of_day / 3_600;
    let minute = second_of_day % 3_600 / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn date_from_2020(mut days: i64) -> (i64, i64, i64) {
    let mut year = 2020i64;
    loop {
        let days_in_year = if leap(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let month_lengths = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    for (month, length) in (1i64..).zip(month_lengths) {
        if days < length {
            return (year, month, days + 1);
        }
        days -= length;
    }
    unreachable!()
}

fn leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn parse_timestamp(value: &str) -> i64 {
    let year = value[0..4].parse::<i64>().unwrap();
    let month = value[5..7].parse::<usize>().unwrap();
    let day = value[8..10].parse::<i64>().unwrap();
    let hour = value[11..13].parse::<i64>().unwrap();
    let minute = value[14..16].parse::<i64>().unwrap();
    let second = value[17..19].parse::<i64>().unwrap();
    let mut days = 0i64;
    for y in 2020..year {
        days += if leap(y) { 366 } else { 365 };
    }
    let month_lengths = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    days += month_lengths[..month - 1].iter().sum::<i64>() + day - 1;
    days * 86_400 + hour * 3_600 + minute * 60 + second
}

fn max_str(left: &str, right: &str) -> String {
    if left >= right { left } else { right }.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_across_leap_years() {
        for seconds in [0, 59, 86_400, 59_616_000, HORIZON_SECONDS] {
            assert_eq!(parse_timestamp(&timestamp(seconds)), seconds);
        }
    }

    #[test]
    fn generator_is_deterministic_and_monotonic_in_transaction_time() {
        let left = generate_rows(100, 42);
        let right = generate_rows(100, 42);
        assert_eq!(format!("{left:?}"), format!("{right:?}"));
        assert!(
            left.windows(2)
                .all(|pair| pair[0].tx_begin_s < pair[1].tx_begin_s)
        );
    }
}
