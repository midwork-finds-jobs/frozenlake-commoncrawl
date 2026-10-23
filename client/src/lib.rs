//! DuckDB extension for querying the frozenlake-commoncrawl lambdas from a laptop.
//!
//!   FROM commoncrawl('SELECT ... WHERE in_part(url)', parts := 40);
//!
//! Sends the SQL to `parts` lambdas at the same time with `&part=k&parts=n`, where in_part() and surtkey_in_part()
//! choose the rows and files of every part, and returns the zstd compressed Arrow IPC results of all parts as one
//! table. With crawls := '<condition on crawls()>' every crawl gets its own `parts` lambdas and `{crawl}` in the SQL
//! is replaced with the crawl id, so a filter like crawl = {crawl} skips the other crawls:
//!
//!   FROM commoncrawl('... WHERE crawl = {crawl} AND in_part(url)', crawls := 'first_fetch >= ''2026-01-01''', parts := 4);
//!
//! Signs the CloudFront cookies itself and checks the monthly lambda usage before starting many lambdas.
//!
//! Configuration from named parameters or the environment:
//!   url               CC_URL               https://xxxx.cloudfront.net/
//!   key_pair_id       CC_KEY_PAIR_ID       tofu output -raw cloudfront_key_pair_id
//!   private_key_path  CC_PRIVATE_KEY_PATH  tofu output -raw cloudfront_private_key_pem > key.pem (or CC_PRIVATE_KEY)

use std::error::Error;
use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_ipc::reader::StreamReader;
use base64::Engine;
use duckdb::arrow::array::Array;
use duckdb::arrow::datatypes::SchemaRef;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type_for_field};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, duckdb_entrypoint_c_api};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use sha1::Sha1;
use tokio::sync::{Semaphore, mpsc};

type BoxError = Box<dyn Error + Send + Sync>;

/// DuckDB's vector size, a chunk can't have more rows
const VECTOR_SIZE: usize = 2048;
/// Signed cookies are valid this long and renewed when less than half of it is left
const COOKIE_SECONDS: u64 = 3600;
/// Unreserved characters of RFC 3986 stay as they are in the query string
const QUERY: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("commoncrawl")
            .enable_all()
            .build()
            .expect("failed to start the tokio runtime")
    })
}

fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            // CloudFront multiplexes many requests over one HTTP/2 connection
            .pool_max_idle_per_host(16)
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("failed to create the HTTP client")
    })
}

#[derive(Clone)]
struct Config {
    url: String,
    key_pair_id: String,
    private_key_pem: String,
}

fn setting(bind: &BindInfo, name: &str, env: &str) -> Option<String> {
    bind.get_named_parameter(name)
        .filter(|value| !value.is_null())
        .map(|value| value.to_string())
        .or_else(|| std::env::var(env).ok())
        .filter(|value| !value.is_empty())
}

fn config(bind: &BindInfo) -> Result<Config, BoxError> {
    let mut url = setting(bind, "url", "CC_URL").ok_or("set CC_URL or url := 'https://xxxx.cloudfront.net/'")?;
    if !url.ends_with('/') {
        url.push('/');
    }
    let key_pair_id = setting(bind, "key_pair_id", "CC_KEY_PAIR_ID")
        .ok_or("set CC_KEY_PAIR_ID or key_pair_id := (tofu output -raw cloudfront_key_pair_id)")?;
    let private_key_pem = match setting(bind, "private_key_path", "CC_PRIVATE_KEY_PATH") {
        Some(path) => std::fs::read_to_string(&path).map_err(|e| format!("failed to read {path}: {e}"))?,
        None => std::env::var("CC_PRIVATE_KEY")
            .map_err(|_| "set CC_PRIVATE_KEY_PATH, CC_PRIVATE_KEY or private_key_path := 'key.pem'")?,
    };
    Ok(Config { url, key_pair_id, private_key_pem })
}

/// CloudFront's URL safe base64: + -> -, = -> _ and / -> ~
fn cloudfront_base64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(data)
        .chars()
        .map(|c| match c {
            '+' => '-',
            '=' => '_',
            '/' => '~',
            c => c,
        })
        .collect()
}

/// Signed cookies with a custom policy for everything under the distribution url, cached until half of the validity
fn cookie(config: &Config) -> Result<String, BoxError> {
    static CACHE: Mutex<Option<(String, String, u64)>> = Mutex::new(None);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let cache_key = format!("{}\n{}", config.url, config.key_pair_id);
    let mut cache = CACHE.lock().unwrap();
    if let Some((key, cookie, expires)) = cache.as_ref() {
        if *key == cache_key && *expires > now + COOKIE_SECONDS / 2 {
            return Ok(cookie.clone());
        }
    }
    let key = rsa::RsaPrivateKey::from_pkcs1_pem(&config.private_key_pem)
        .or_else(|_| rsa::RsaPrivateKey::from_pkcs8_pem(&config.private_key_pem))
        .map_err(|e| format!("failed to read the CloudFront private key: {e}"))?;
    let expires = now + COOKIE_SECONDS;
    let policy = format!(
        r#"{{"Statement":[{{"Resource":"{}*","Condition":{{"DateLessThan":{{"AWS:EpochTime":{expires}}}}}}}]}}"#,
        config.url
    );
    let signature = SigningKey::<Sha1>::new(key).sign(policy.as_bytes()).to_vec();
    let cookie = format!(
        "CloudFront-Policy={}; CloudFront-Signature={}; CloudFront-Key-Pair-Id={}",
        cloudfront_base64(policy.as_bytes()),
        cloudfront_base64(&signature),
        config.key_pair_id
    );
    *cache = Some((cache_key, cookie.clone(), expires));
    Ok(cookie)
}

struct Usage {
    json: serde_json::Value,
}

impl Usage {
    fn number(&self, name: &str) -> f64 {
        self.json.get(name).and_then(|value| value.as_f64()).unwrap_or(0.0)
    }
}

async fn usage(config: &Config) -> Result<Usage, BoxError> {
    let response = client()
        .get(format!("{}usage", config.url))
        .header("Cookie", cookie(config)?)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("usage request failed with {status}: {body}").into());
    }
    Ok(Usage { json: serde_json::from_str(&body)? })
}

/// Statistics of one part of the last query, see commoncrawl_stats()
#[derive(Clone, Default)]
struct PartStats {
    crawl: Option<String>,
    part: usize,
    status: u16,
    seconds: f64,
    lambda_ms: Option<i64>,
    bytes: usize,
    rows: usize,
    cache: String,
    attempts: usize,
    error: Option<String>,
    /// warc_record() fetch counters of the lambda
    fetches: String,
}

fn last_stats() -> &'static Mutex<Vec<PartStats>> {
    static STATS: Mutex<Vec<PartStats>> = Mutex::new(Vec::new());
    &STATS
}

enum Message {
    Batch(RecordBatch),
    Error(String),
}

/// The error with its causes, reqwest's own message is only "error sending request"
fn error_chain(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    message
}

/// Fetches one part, retrying throttling and server errors but not timeouts which already used a whole lambda
async fn fetch_part(config: &Config, url: &str, timeout: Duration, stats: &mut PartStats) -> Result<Vec<u8>, BoxError> {
    let started = Instant::now();
    loop {
        stats.attempts += 1;
        let response = client()
            .get(url)
            .header("Cookie", cookie(config)?)
            .timeout(timeout)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(e) if e.is_connect() && stats.attempts < 3 => {
                tokio::time::sleep(Duration::from_millis(500 * stats.attempts as u64)).await;
                continue;
            }
            Err(e) => return Err(error_chain(&e).into()),
        };
        let status = response.status();
        stats.status = status.as_u16();
        stats.cache = response
            .headers()
            .get("x-cache")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        stats.fetches = response
            .headers()
            .get("x-warc-stats")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        stats.lambda_ms = stats.lambda_ms.or_else(|| {
            response.headers().get("x-duration-ms").and_then(|value| value.to_str().ok()?.parse().ok())
        });
        // Lambda's throttling is retried, the monthly limit of the lambda wouldn't pass and costs an invocation
        let limit_reached = response.headers().contains_key("x-limit-reached");
        let retryable = matches!(status.as_u16(), 429 | 503) && !limit_reached;
        if retryable && stats.attempts < 4 {
            tokio::time::sleep(Duration::from_millis(1000 * stats.attempts as u64)).await;
            continue;
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = response.bytes().await.map_err(|e| error_chain(&e))?;
        stats.seconds = started.elapsed().as_secs_f64();
        stats.bytes = body.len();
        if !status.is_success() {
            return Err(format!("{status}: {}", String::from_utf8_lossy(&body)).into());
        }
        // A lambda which crashed after starting the response returns its error as JSON
        if !content_type.starts_with("application/vnd.apache.arrow.stream") {
            return Err(format!("unexpected {content_type} response: {}", String::from_utf8_lossy(&body)).into());
        }
        return Ok(body.to_vec());
    }
}

/// Decodes one part into the channel. The schema is sent first: bind waits for it before anything reads the batches.
fn decode_part(
    body: Vec<u8>,
    batches: &mpsc::Sender<Message>,
    schemas: &mpsc::UnboundedSender<Result<SchemaRef, String>>,
    stats: &mut PartStats,
) -> Result<(), BoxError> {
    let reader = StreamReader::try_new(Cursor::new(body), None)?;
    let _ = schemas.send(Ok(reader.schema()));
    for batch in reader {
        let batch = batch?;
        stats.rows += batch.num_rows();
        if batch.num_rows() > 0 && batches.blocking_send(Message::Batch(batch)).is_err() {
            // The query was stopped, for example by a LIMIT
            break;
        }
    }
    Ok(())
}

struct Query {
    config: Config,
    sql: String,
    /// Crawls for {crawl}, or one request per part without it
    crawls: Option<Vec<String>>,
    /// Requests planned by split := ...
    planned: Option<Vec<Job>>,
    parts: usize,
    level: Option<i64>,
    concurrency: usize,
}

/// One lambda request
#[derive(Clone)]
struct Job {
    crawl: Option<String>,
    part: usize,
    parts: usize,
    /// url_surtkey range for surtkey_in_part(), chosen by plan_split()
    low: Option<String>,
    high: Option<String>,
}

impl Query {
    fn jobs(&self) -> Vec<Job> {
        if let Some(jobs) = &self.planned {
            return jobs.clone();
        }
        let crawls: Vec<Option<String>> = match &self.crawls {
            Some(crawls) => crawls.iter().cloned().map(Some).collect(),
            None => vec![None],
        };
        crawls
            .into_iter()
            .flat_map(|crawl| {
                (0..self.parts).map(move |part| Job { crawl: crawl.clone(), part, parts: self.parts, low: None, high: None })
            })
            .collect()
    }

    fn url(&self, job: &Job) -> String {
        let sql = match &job.crawl {
            Some(crawl) => self.sql.replace("{crawl}", &format!("'{}'", crawl.replace('\'', "''"))),
            None => self.sql.clone(),
        };
        let part = job.part;
        let mut url = format!("{}?sql={}", self.config.url, utf8_percent_encode(&sql, QUERY));
        if job.parts > 1 {
            url.push_str(&format!("&part={part}&parts={}", job.parts));
        }
        for (name, value) in [("low", &job.low), ("high", &job.high)] {
            if let Some(value) = value {
                url.push_str(&format!("&{name}={}", utf8_percent_encode(value, QUERY)));
            }
        }
        if let Some(level) = self.level {
            url.push_str(&format!("&level={level}"));
        }
        url
    }
}

/// Runs a small query in one lambda and returns its rows with every value as a string
async fn query_strings(config: &Config, sql: &str) -> Result<Vec<Vec<Option<String>>>, BoxError> {
    let url = format!("{}?sql={}", config.url, utf8_percent_encode(sql, QUERY));
    let mut stats = PartStats::default();
    let body = fetch_part(config, &url, Duration::from_secs(180), &mut stats).await?;
    let mut rows = Vec::new();
    for batch in StreamReader::try_new(Cursor::new(body), None)? {
        let batch = batch?;
        let mut columns = Vec::new();
        for column in batch.columns() {
            let column = duckdb::arrow::compute::cast(column, &duckdb::arrow::datatypes::DataType::Utf8)?;
            let column = column
                .as_any()
                .downcast_ref::<duckdb::arrow::array::StringArray>()
                .ok_or("expected strings")?
                .clone();
            columns.push(column);
        }
        for row in 0..batch.num_rows() {
            rows.push(columns.iter().map(|column| column.is_valid(row).then(|| column.value(row).to_string())).collect());
        }
    }
    Ok(rows)
}

/// Crawl ids of crawls() matching the condition, read through the lambda
fn crawl_ids(config: &Config, condition: &str) -> Result<Vec<String>, BoxError> {
    let sql = format!("SELECT crawl FROM crawls() WHERE {condition} ORDER BY crawl");
    let crawls: Vec<String> = runtime().block_on(query_strings(config, &sql))?.into_iter().filter_map(|row| row[0].clone()).collect();
    if crawls.is_empty() {
        return Err(format!("no crawls match {condition}").into());
    }
    Ok(crawls)
}

/// Quantiles of url_surtkey which the split query returns, per crawl
const SPLIT_QUANTILES: usize = 64;

/// Runs the split query of every crawl and divides `parts` lambdas between the crawls by their number of rows. Every
/// lambda gets the url_surtkey range [low, high) of about the same number of rows.
fn plan_split(
    config: &Config,
    split: &str,
    weight: &str,
    crawls: Option<&Vec<String>>,
    parts: usize,
) -> Result<Vec<Job>, BoxError> {
    let crawls: Vec<Option<String>> = match crawls {
        Some(crawls) => crawls.iter().cloned().map(Some).collect(),
        None => vec![None],
    };
    let samples = runtime().block_on(async {
        let tasks: Vec<_> = crawls
            .iter()
            .map(|crawl| {
                let split = match crawl {
                    Some(crawl) => split.replace("{crawl}", &format!("'{}'", crawl.replace('\'', "''"))),
                    None => split.to_string(),
                };
                // Total weight and the url_surtkey where each 1/SPLIT_QUANTILES of the weight starts
                let sql = format!(
                    "WITH s AS (SELECT url_surtkey AS k, greatest(coalesce(({weight})::DOUBLE, 0), 0) AS w FROM ({split})), \
                     c AS (SELECT k, sum(w) OVER (ORDER BY k ROWS UNBOUNDED PRECEDING) - w AS before, sum(w) OVER () AS total \
                     FROM s), b AS (SELECT least({last}, floor(before * {SPLIT_QUANTILES} / total)) AS bucket, min(k) AS bound \
                     FROM c WHERE total > 0 GROUP BY bucket) \
                     SELECT (SELECT coalesce(sum(w), 0)::BIGINT FROM s), \
                     (SELECT array_to_string(list(bound ORDER BY bound), chr(10)) FROM b WHERE bucket >= 1)",
                    last = SPLIT_QUANTILES - 1
                );
                let config = config.clone();
                runtime().spawn(async move { query_strings(&config, &sql).await })
            })
            .collect();
        let mut samples = Vec::new();
        for task in tasks {
            samples.push(task.await?);
        }
        Ok::<_, BoxError>(samples)
    })?;
    let mut counts = Vec::new();
    for rows in samples {
        let rows = rows?;
        let row = rows.first().ok_or("split query returned nothing")?;
        let count: usize = row[0].as_deref().unwrap_or("0").parse()?;
        let quantiles: Vec<String> = row[1].as_deref().map(|q| q.split('\n').map(str::to_string).collect()).unwrap_or_default();
        counts.push((count, quantiles));
    }
    let total: usize = counts.iter().map(|(count, _)| count).sum();
    let mut jobs = Vec::new();
    for (crawl, (count, quantiles)) in crawls.into_iter().zip(counts) {
        if count == 0 {
            continue;
        }
        // At least one lambda for every crawl with rows, more for the larger ones
        let share = ((parts * count) as f64 / total as f64).round() as usize;
        let lambdas = share.clamp(1, SPLIT_QUANTILES);
        // The bounds split the weight into equal shares, empty shares have no bound
        let mut bounds: Vec<String> = if quantiles.is_empty() {
            Vec::new()
        } else {
            (1..lambdas)
                .map(|i| quantiles[(i * (quantiles.len() + 1) / lambdas).saturating_sub(1).min(quantiles.len() - 1)].clone())
                .collect()
        };
        bounds.dedup();
        for part in 0..=bounds.len() {
            jobs.push(Job {
                crawl: crawl.clone(),
                part,
                parts: bounds.len() + 1,
                low: part.checked_sub(1).map(|i| bounds[i].clone()),
                high: bounds.get(part).cloned(),
            });
        }
    }
    if jobs.is_empty() {
        return Err("the split query returned no rows".into());
    }
    Ok(jobs)
}

/// Starts every part and returns the schema of the first part which finished and the channel of all batches
fn start(query: Query, timeout: Duration) -> Result<(SchemaRef, mpsc::Receiver<Message>), BoxError> {
    let (batches_tx, batches_rx) = mpsc::channel::<Message>(64);
    let (schema_tx, mut schema_rx) = mpsc::unbounded_channel::<Result<SchemaRef, String>>();
    let query = Arc::new(query);
    let limit = Arc::new(Semaphore::new(query.concurrency));
    *last_stats().lock().unwrap() = Vec::new();
    for job in query.jobs() {
        let (query, limit, batches_tx, schema_tx) = (query.clone(), limit.clone(), batches_tx.clone(), schema_tx.clone());
        runtime().spawn(async move {
            let _permit = limit.acquire().await;
            // The query was cancelled, failed or has enough rows (LIMIT): don't pay for parts nobody reads
            if batches_tx.is_closed() {
                return;
            }
            let part = job.part;
            let mut stats = PartStats { crawl: job.crawl.clone(), part, ..Default::default() };
            let result = match fetch_part(&query.config, &query.url(&job), timeout, &mut stats).await {
                Ok(body) => {
                    let (tx, schemas) = (batches_tx.clone(), schema_tx.clone());
                    let mut decode_stats = stats.clone();
                    let decoded = tokio::task::spawn_blocking(move || {
                        let decoded = decode_part(body, &tx, &schemas, &mut decode_stats);
                        (decoded, decode_stats)
                    })
                    .await;
                    match decoded {
                        Ok((decoded, decode_stats)) => {
                            stats = decode_stats;
                            decoded.map_err(|e| e.to_string())
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
                Err(e) => Err(e.to_string()),
            };
            if let Err(e) = &result {
                stats.error = Some(e.clone());
                let message = match (&job.crawl, job.parts > 1) {
                    (Some(crawl), true) => format!("{crawl} part {part}: {e}"),
                    (Some(crawl), false) => format!("{crawl}: {e}"),
                    (None, true) => format!("part {part}: {e}"),
                    (None, false) => e.clone(),
                };
                // bind may still wait for a schema while nothing reads the full batch channel
                let _ = schema_tx.send(Err(message.clone()));
                let _ = batches_tx.send(Message::Error(message)).await;
            }
            last_stats().lock().unwrap().push(stats);
        });
    }
    drop(schema_tx);
    // The result columns are needed at bind time, so wait for the first part
    let schema = runtime()
        .block_on(schema_rx.recv())
        .ok_or("no parts were started")?
        .map_err(|e| -> BoxError { e.into() })?;
    // The other parts are checked when their batches arrive
    Ok((schema, batches_rx))
}

struct QueryBindData {
    schema: SchemaRef,
    batches: Mutex<mpsc::Receiver<Message>>,
}

struct QueryInitData {
    /// Batch which didn't fit into the previous chunk and the offset of its next row
    current: Mutex<Option<(RecordBatch, usize)>>,
}

struct CommonCrawlVTab;

impl VTab for CommonCrawlVTab {
    type InitData = QueryInitData;
    type BindData = QueryBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let config = config(bind).map_err(|e| e.to_string())?;
        let sql = bind.get_parameter(0).to_string();
        let integer = |name: &str| bind.get_named_parameter(name).filter(|value| !value.is_null()).map(|v| v.to_int64());
        let parts = integer("parts").unwrap_or(1).max(1) as usize;
        let level = integer("level");
        let max_gb_seconds = bind
            .get_named_parameter("max_gb_seconds")
            .filter(|value| !value.is_null())
            .map(|value| value.to_double());
        let crawls = match setting(bind, "crawls", "") {
            Some(condition) => {
                if !sql.contains("{crawl}") {
                    return Err("crawls := ... needs {crawl} in the SQL, for example WHERE crawl = {crawl}".into());
                }
                Some(crawl_ids(&config, &condition).map_err(|e| e.to_string())?)
            }
            None => None,
        };
        let planned = match setting(bind, "split", "") {
            Some(split) => {
                if !sql.contains("surtkey_in_part") {
                    return Err("split := ... needs surtkey_in_part(url_surtkey) in the SQL".into());
                }
                let weight = setting(bind, "weight", "").unwrap_or_else(|| "1".to_string());
                Some(plan_split(&config, &split, &weight, crawls.as_ref(), parts).map_err(|e| e.to_string())?)
            }
            None => None,
        };
        let requests = planned.as_ref().map_or(parts * crawls.as_ref().map_or(1, Vec::len), Vec::len);
        let concurrency = integer("concurrency").unwrap_or(requests as i64).clamp(1, 1000) as usize;

        let mut timeout = Duration::from_secs(180);
        if requests > 1 || max_gb_seconds.is_some() {
            // Worst case every lambda runs until the timeout
            let usage = runtime().block_on(usage(&config)).map_err(|e| e.to_string())?;
            let seconds = usage.number("timeout_seconds").max(1.0);
            timeout = Duration::from_secs_f64(seconds + 15.0);
            let worst = requests as f64 * seconds * usage.number("memory_mb") / 1024.0;
            let remaining = usage.number("remaining_gb_seconds");
            if usage.number("limit_gb_seconds") > 0.0 && worst > remaining {
                return Err(format!(
                    "{requests} lambdas can use up to {worst:.0} GB-seconds but only {remaining:.0} of this month's \
                     limit are left, use fewer parts"
                )
                .into());
            }
            if let Some(max) = max_gb_seconds {
                if worst > max {
                    return Err(
                        format!("{requests} lambdas can use up to {worst:.0} GB-seconds, over max_gb_seconds {max}")
                            .into(),
                    );
                }
            }
        }

        let query = Query { config, sql, crawls, planned, parts, level, concurrency };
        let (schema, batches) = start(query, timeout).map_err(|e| e.to_string())?;
        for field in schema.fields() {
            bind.add_result_column(field.name(), to_duckdb_logical_type_for_field(field)?);
        }
        Ok(QueryBindData { schema, batches: Mutex::new(batches) })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(QueryInitData { current: Mutex::new(None) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let bind = func.get_bind_data();
        let mut current = func.get_init_data().current.lock().unwrap();
        loop {
            if let Some((batch, offset)) = current.take() {
                let rows = (batch.num_rows() - offset).min(VECTOR_SIZE);
                record_batch_to_duckdb_data_chunk(&batch.slice(offset, rows), output)?;
                if offset + rows < batch.num_rows() {
                    *current = Some((batch, offset + rows));
                }
                return Ok(());
            }
            match bind.batches.lock().unwrap().blocking_recv() {
                Some(Message::Batch(batch)) => {
                    if batch.schema().fields() != bind.schema.fields() {
                        return Err(format!(
                            "the parts returned different columns: {:?} and {:?}",
                            bind.schema.fields(),
                            batch.schema().fields()
                        )
                        .into());
                    }
                    *current = Some((batch, 0));
                }
                Some(Message::Error(e)) => return Err(e.into()),
                None => {
                    output.set_len(0);
                    return Ok(());
                }
            }
        }
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        let bigint = || LogicalTypeHandle::from(LogicalTypeId::Bigint);
        Some(vec![
            ("parts".to_string(), bigint()),
            ("crawls".to_string(), varchar()),
            ("split".to_string(), varchar()),
            ("weight".to_string(), varchar()),
            ("concurrency".to_string(), bigint()),
            ("level".to_string(), bigint()),
            ("max_gb_seconds".to_string(), LogicalTypeHandle::from(LogicalTypeId::Double)),
            ("url".to_string(), varchar()),
            ("key_pair_id".to_string(), varchar()),
            ("private_key_path".to_string(), varchar()),
        ])
    }
}

/// Sets one value of a vector whose type is T
fn set<T: Copy>(vector: &mut duckdb::core::FlatVector, row: usize, value: T) {
    // Safety: the callers only use the type of the column they created, and row is below the vector size
    unsafe { vector.as_mut_slice::<T>()[row] = value };
}

/// commoncrawl_usage(): lambda usage of this month
struct UsageVTab;

struct OnceData {
    done: Mutex<bool>,
}

struct UsageBindData {
    usage: serde_json::Value,
}

const USAGE_COLUMNS: [&str; 7] = [
    "month_start",
    "invocations",
    "used_gb_seconds",
    "limit_gb_seconds",
    "remaining_gb_seconds",
    "memory_mb",
    "remaining_seconds_at_this_memory",
];

impl VTab for UsageVTab {
    type InitData = OnceData;
    type BindData = UsageBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let config = config(bind).map_err(|e| e.to_string())?;
        let usage = runtime().block_on(usage(&config)).map_err(|e| e.to_string())?;
        for column in USAGE_COLUMNS {
            let type_id = if column == "month_start" { LogicalTypeId::Varchar } else { LogicalTypeId::Double };
            bind.add_result_column(column, LogicalTypeHandle::from(type_id));
        }
        Ok(UsageBindData { usage: usage.json })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(OnceData { done: Mutex::new(false) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let mut done = func.get_init_data().done.lock().unwrap();
        if *done {
            output.set_len(0);
            return Ok(());
        }
        *done = true;
        let usage = &func.get_bind_data().usage;
        for (index, column) in USAGE_COLUMNS.iter().enumerate() {
            let value = usage.get(*column);
            let mut vector = output.flat_vector(index);
            if *column == "month_start" {
                let text = value.and_then(|v| v.as_str()).unwrap_or_default();
                vector.insert(0, std::ffi::CString::new(text)?);
            } else {
                set::<f64>(&mut vector, 0, value.and_then(|v| v.as_f64()).unwrap_or(0.0));
            }
        }
        output.set_len(1);
        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        Some(vec![
            ("url".to_string(), varchar()),
            ("key_pair_id".to_string(), varchar()),
            ("private_key_path".to_string(), varchar()),
        ])
    }
}

/// commoncrawl_stats(): one row per part of the last commoncrawl() query
struct StatsVTab;

struct StatsBindData {
    stats: Vec<PartStats>,
}

impl VTab for StatsVTab {
    type InitData = OnceData;
    type BindData = StatsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let columns = [
            ("part", LogicalTypeId::Bigint),
            ("status", LogicalTypeId::Integer),
            ("seconds", LogicalTypeId::Double),
            ("lambda_seconds", LogicalTypeId::Double),
            ("bytes", LogicalTypeId::Bigint),
            ("rows", LogicalTypeId::Bigint),
            ("cache", LogicalTypeId::Varchar),
            ("attempts", LogicalTypeId::Bigint),
            ("error", LogicalTypeId::Varchar),
            ("crawl", LogicalTypeId::Varchar),
            ("fetches", LogicalTypeId::Varchar),
        ];
        for (name, type_id) in columns {
            bind.add_result_column(name, LogicalTypeHandle::from(type_id));
        }
        let mut stats = last_stats().lock().unwrap().clone();
        stats.sort_by(|a, b| (&a.crawl, a.part).cmp(&(&b.crawl, b.part)));
        Ok(StatsBindData { stats })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(OnceData { done: Mutex::new(false) })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let mut done = func.get_init_data().done.lock().unwrap();
        if *done {
            output.set_len(0);
            return Ok(());
        }
        *done = true;
        let stats = &func.get_bind_data().stats;
        let rows = stats.len().min(VECTOR_SIZE);
        for (row, part) in stats.iter().take(rows).enumerate() {
            set::<i64>(&mut output.flat_vector(0), row, part.part as i64);
            set::<i32>(&mut output.flat_vector(1), row, part.status as i32);
            set::<f64>(&mut output.flat_vector(2), row, part.seconds);
            let mut lambda = output.flat_vector(3);
            match part.lambda_ms {
                Some(ms) => set::<f64>(&mut lambda, row, ms as f64 / 1000.0),
                None => lambda.set_null(row),
            }
            set::<i64>(&mut output.flat_vector(4), row, part.bytes as i64);
            set::<i64>(&mut output.flat_vector(5), row, part.rows as i64);
            output.flat_vector(6).insert(row, std::ffi::CString::new(part.cache.clone())?);
            set::<i64>(&mut output.flat_vector(7), row, part.attempts as i64);
            let mut error = output.flat_vector(8);
            match &part.error {
                Some(e) => error.insert(row, std::ffi::CString::new(e.replace('\0', ""))?),
                None => error.set_null(row),
            }
            match &part.crawl {
                Some(crawl) => output.flat_vector(9).insert(row, std::ffi::CString::new(crawl.clone())?),
                None => output.flat_vector(9).set_null(row),
            }
            output.flat_vector(10).insert(row, std::ffi::CString::new(part.fetches.clone())?);
        }
        output.set_len(rows);
        Ok(())
    }
}

#[duckdb_entrypoint_c_api(ext_name = "commoncrawl", min_duckdb_version = "v1.2.0")]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<CommonCrawlVTab>("commoncrawl")?;
    con.register_table_function::<UsageVTab>("commoncrawl_usage")?;
    con.register_table_function::<StatsVTab>("commoncrawl_stats")?;
    Ok(())
}
