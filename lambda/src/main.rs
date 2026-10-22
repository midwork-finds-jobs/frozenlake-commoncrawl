//! Runs read-only SQL against the frozen Common Crawl DuckLake and streams the result as
//! zstd compressed Arrow IPC.
//!
//!   GET /?sql=SELECT ...&level=1   level is the optional zstd compression level
//!   GET /?sql=...&part=3&parts=40    one part of a query which the client splits over many lambdas
//!   GET /usage                       billed GB-seconds of this month as JSON
//!
//! Results over 5MB are uploaded into S3 and the response redirects to them.
//!
//! The DuckLake catalog is in the lambda package. The parquet files are read
//! straight from s3://commoncrawl which is in the same region as this lambda.

use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow_ipc::CompressionType;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use axum::Router;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{AccessMode, Config, Connection};
use lambda_http::{Error, run_with_streaming_response, tracing};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

/// The catalog is in the lambda package so that cold starts don't download it, see build.sh
const CATALOG_PATH: &str = "/var/task/commoncrawl.ducklake";
const CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
/// Query results are passed from DuckDB in chunks of this size
const CHUNK_BYTES: usize = 1 << 20;
/// Larger results are uploaded into S3 because Lambda streams responses over 6MB at only 2MB/s
const INLINE_BYTES: usize = 5 << 20;
const PART_BYTES: usize = 16 << 20;
const RESULTS_PREFIX: &str = "results/";
/// zstd level used when the request doesn't have one
const DEFAULT_ZSTD_LEVEL: i32 = 1;

/// httpfs needs to read one of these to verify TLS certificates
const CA_BUNDLES: [&str; 5] = [
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/ssl/cert.pem",
];

#[derive(Deserialize)]
struct QueryParams {
    sql: String,
    /// zstd compression level
    level: Option<i32>,
    /// This request is part `part` (from 0) of `parts`, used by in_part() and surtkey_in_part()
    part: Option<usize>,
    parts: Option<usize>,
    /// url_surtkey range of surtkey_in_part() chosen by the client instead of the file quantiles
    low: Option<String>,
    high: Option<String>,
}

/// The n - 1 url_surtkey bounds between n parts: quantiles of the smallest url_surtkey of every file, so that every
/// part reads about the same number of files of every crawl
const PART_BOUNDS_SQL: &str = "
    SELECT quantile_disc(min_value, list_transform(range(1, ?1), lambda i: i / ?1))
    FROM __ducklake_metadata_commoncrawl.ducklake_file_column_stats
    WHERE column_id = (SELECT any_value(column_id) FROM __ducklake_metadata_commoncrawl.ducklake_column
                       WHERE column_name = 'url_surtkey')";

/// Bounds by the number of parts, the catalog doesn't change while the lambda runs
fn part_bounds(conn: &Connection, parts: usize) -> Result<Vec<String>, Error> {
    static CACHE: Mutex<Option<std::collections::HashMap<usize, Vec<String>>>> = Mutex::new(None);
    if let Some(bounds) = CACHE
        .lock()
        .unwrap()
        .get_or_insert_with(Default::default)
        .get(&parts)
    {
        return Ok(bounds.clone());
    }
    let joined: String = conn.query_row(
        &format!("SELECT array_to_string(({PART_BOUNDS_SQL}), chr(10))"),
        [parts as i64],
        |row| row.get(0),
    )?;
    let bounds: Vec<String> = joined.split('\n').map(str::to_string).collect();
    if bounds.len() != parts - 1 {
        return Err(format!(
            "expected {} part bounds but got {}",
            parts - 1,
            bounds.len()
        )
        .into());
    }
    CACHE
        .lock()
        .unwrap()
        .get_or_insert_with(Default::default)
        .insert(parts, bounds.clone());
    Ok(bounds)
}

/// Sets the variables of in_part() and surtkey_in_part() for this request
fn set_part(conn: &Connection, params: &QueryParams) -> Result<(), Error> {
    let parts = params.parts.unwrap_or(1).max(1);
    let part = params.part.unwrap_or(0);
    if parts > 10_000 {
        return Err("parts can be at most 10000".into());
    }
    if part >= parts {
        return Err(format!("part {part} must be smaller than parts {parts}").into());
    }
    let (low, high) = if params.low.is_some() || params.high.is_some() {
        (params.low.clone(), params.high.clone())
    } else if parts == 1 {
        (None, None)
    } else {
        let bounds = part_bounds(conn, parts)?;
        (
            part.checked_sub(1).map(|i| bounds[i].clone()),
            bounds.get(part).cloned(),
        )
    };
    let quoted = |value: Option<String>| {
        value
            .map(|v| quote(&v))
            .unwrap_or_else(|| "NULL::VARCHAR".to_string())
    };
    conn.execute_batch(&format!(
        "SET VARIABLE cc_part = {part}; SET VARIABLE cc_parts = {parts};
         SET VARIABLE cc_part_low = {}; SET VARIABLE cc_part_high = {};",
        quoted(low),
        quoted(high)
    ))?;
    Ok(())
}

/// Arrow IPC bytes or the error of the query
type Chunks = mpsc::Sender<Result<Bytes, String>>;

fn env(name: &str) -> Result<String, Error> {
    std::env::var(name).map_err(|_| format!("{name} env is missing").into())
}

fn env_number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Opens the DuckLake and locks DuckDB down so that the SQL can only read Common Crawl data
fn open_database() -> Result<Connection, Error> {
    let started = std::time::Instant::now();
    // Extensions are statically linked into libduckdb.so, see duckdb/extension_config.cmake
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)?
        .with("autoinstall_known_extensions", "false")?
        .with("temp_directory", "/tmp/duckdb")?;
    // The DuckLake is the main database so that the tables can be used without a prefix
    let conn = Connection::open_with_flags(format!("ducklake:{CATALOG_PATH}"), config)?;

    // Crawls with their fetch times for cdx_captures(), which uses them to skip crawls outside of a fetch_time filter
    let cdx_crawls: String = conn.query_row(
        "SELECT string_agg(crawl || ' ' || strftime(first_fetch, '%Y-%m-%dT%H:%M:%S') || ' '
             || strftime(last_fetch, '%Y-%m-%dT%H:%M:%S'), ';' ORDER BY crawl) FROM crawls()",
        [],
        |row| row.get(0),
    )?;

    let session_token = std::env::var("AWS_SESSION_TOKEN").unwrap_or_default();
    let ca_bundles: Vec<String> = CA_BUNDLES
        .iter()
        .filter(|path| Path::new(path).exists())
        .map(|path| quote(path))
        .collect();
    // Reading from S3 is network bound so use more threads than there are vCPUs
    let threads = env_number("DUCKDB_THREADS", 16);
    let warc_concurrency = env_number("WARC_CONCURRENCY", 128);

    conn.execute_batch(&format!(
        "
        -- Credentials of the lambda execution role
        CREATE SECRET commoncrawl (
            TYPE S3,
            KEY_ID {key_id},
            SECRET {secret},
            SESSION_TOKEN {session_token},
            REGION 'us-east-1',
            SCOPE 's3://commoncrawl/'
        );

        SET GLOBAL threads = {threads};
        SET GLOBAL parquet_metadata_cache = true;

        -- warc_record() fetches records from s3://commoncrawl in parallel. GLOBAL because every request gets a
        -- cloned connection which would otherwise have the default.
        LOAD extract;
        -- Moves DuckDB's memory limit with the memory used outside of it, like fetched records and parsed HTML,
        -- so that Lambda doesn't kill the process when it runs out of memory. See extract/src/memory_governor.cpp
        SET GLOBAL memory_target_mb = {memory_mb};
        SET GLOBAL warc_concurrency = {warc_concurrency};
        SET GLOBAL cdx_crawls = {cdx_crawls};
        -- The HTTP client pool is sized by the CPU count (16 clients on a 2 vCPU lambda) and requests over it
        -- wait. Every warc_record() fetch holds one client and the parquet reads need their own.
        SET GLOBAL http_client_pool_capacity = {http_clients};

        SET allowed_directories = ['s3://commoncrawl/'];
        SET allowed_paths = [{ca_bundles}];
        SET enable_external_access = false;
        SET lock_configuration = true;
        ",
        key_id = quote(&env("AWS_ACCESS_KEY_ID")?),
        secret = quote(&env("AWS_SECRET_ACCESS_KEY")?),
        session_token = quote(&session_token),
        ca_bundles = ca_bundles.join(", "),
        warc_concurrency = warc_concurrency,
        cdx_crawls = quote(&cdx_crawls),
        // The Rust runtime, Arrow buffers and result uploads are outside of what the governor can move
        memory_mb = env_number("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", 3008).saturating_sub(256).max(512),
        http_clients = warc_concurrency + threads,
    ))?;
    tracing::info!("Opened DuckLake in {:?}", started.elapsed());
    Ok(conn)
}

/// The SQL must be exactly one SELECT, optionally with EXPLAIN [ANALYZE]. DuckDB runs all but the last statement
/// of a multi statement query in prepare(), and statements like CREATE SECRET work even when the configuration
/// is locked, so a secret with a custom endpoint could read other buckets with the lambda's credentials.
fn check_sql(conn: &Connection, sql: &str) -> Result<(), Error> {
    let trimmed = sql.trim_start();
    let mut select = trimmed;
    for keyword in ["EXPLAIN", "ANALYZE"] {
        if select.len() >= keyword.len()
            && select[..keyword.len()].eq_ignore_ascii_case(keyword)
            && select[keyword.len()..].starts_with(|c: char| c.is_whitespace())
        {
            select = select[keyword.len()..].trim_start();
        }
    }
    // json_serialize_sql parses without binding and fails for everything but SELECT statements
    let (error, statements, message): (bool, i64, Option<String>) = conn.query_row(
        "SELECT (j->>'error')::BOOLEAN, coalesce(json_array_length(j->'statements'), 0), j->>'error_message'
         FROM (SELECT json_serialize_sql(?::VARCHAR) AS j)",
        [select],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if error {
        return Err(format!(
            "Only SELECT statements are allowed: {}",
            message.unwrap_or_default()
        )
        .into());
    }
    if statements != 1 {
        return Err("Exactly one SELECT statement is allowed".into());
    }
    Ok(())
}

fn error_response(status: StatusCode, message: String) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], message).into_response()
}

/// Collects written bytes and sends them to the request handler in CHUNK_BYTES sized pieces
struct ChunkWriter {
    tx: Chunks,
    buffer: Vec<u8>,
}

impl ChunkWriter {
    fn send(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::take(&mut self.buffer));
        self.tx.blocking_send(Ok(chunk)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "request handler went away")
        })
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() >= CHUNK_BYTES {
            self.send()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send()
    }
}

/// Runs the query in a blocking thread and sends the zstd compressed Arrow IPC stream into the channel
fn run_query(conn: Connection, params: QueryParams, tx: Chunks) {
    let errors = tx.clone();
    let result = (|| -> Result<(), Error> {
        check_sql(&conn, &params.sql)?;
        set_part(&conn, &params)?;
        let mut stmt = conn.prepare(&params.sql)?;
        // Streaming keeps memory usage low for large results
        let schema = stmt.stream_arrow([])?.get_schema();
        let sink = ChunkWriter {
            tx,
            buffer: Vec::with_capacity(CHUNK_BYTES),
        };
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))?
            .try_with_compression_level(Some(params.level.unwrap_or(DEFAULT_ZSTD_LEVEL)))?;
        let mut writer = StreamWriter::try_new_with_options(sink, &schema, options)?;
        // step() instead of the Arrow iterator, which panics when DuckDB fails to fetch a chunk
        while let Some(array) = stmt.step()? {
            writer.write(&RecordBatch::from(&array))?;
        }
        writer.finish()?;
        writer.into_inner()?.flush()?;
        Ok(())
    })();
    if let Err(e) = result {
        let _ = errors.blocking_send(Err(e.to_string()));
    }
}

/// Multipart upload of a large result into the results bucket
struct ResultUpload {
    s3: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    upload_id: String,
    parts: Vec<CompletedPart>,
    buffer: Vec<u8>,
}

impl ResultUpload {
    async fn start(app: &App, key: String) -> Result<Self, Error> {
        let upload = app
            .s3
            .create_multipart_upload()
            .bucket(&app.results_bucket)
            .key(&key)
            .content_type(CONTENT_TYPE)
            .send()
            .await?;
        Ok(Self {
            s3: app.s3.clone(),
            bucket: app.results_bucket.clone(),
            key,
            upload_id: upload.upload_id().unwrap_or_default().to_string(),
            parts: Vec::new(),
            buffer: Vec::with_capacity(PART_BYTES),
        })
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), Error> {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() >= PART_BYTES {
            self.upload_part().await?;
        }
        Ok(())
    }

    async fn upload_part(&mut self) -> Result<(), Error> {
        let part_number = self.parts.len() as i32 + 1;
        let body = std::mem::replace(&mut self.buffer, Vec::with_capacity(PART_BYTES));
        let part = self
            .s3
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await?;
        self.parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .set_e_tag(part.e_tag)
                .build(),
        );
        Ok(())
    }

    async fn finish(mut self) -> Result<String, Error> {
        if !self.buffer.is_empty() {
            self.upload_part().await?;
        }
        let parts = CompletedMultipartUpload::builder()
            .set_parts(Some(self.parts))
            .build();
        self.s3
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .multipart_upload(parts)
            .send()
            .await?;
        Ok(self.key)
    }

    async fn abort(self) {
        let _ = self
            .s3
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await;
    }
}

/// Same query on the same catalog always has the same result
fn result_key(params: &QueryParams) -> String {
    let catalog = std::env::var("CATALOG_MD5").unwrap_or_default();
    let level = params.level.unwrap_or(DEFAULT_ZSTD_LEVEL);
    let part = format!(
        "{}/{}/{:?}/{:?}",
        params.part.unwrap_or(0),
        params.parts.unwrap_or(1),
        params.low,
        params.high
    );
    let hash = Sha256::digest(format!("{catalog}\n{level}\n{part}\n{}", params.sql).as_bytes());
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{RESULTS_PREFIX}{hex}.arrow")
}

/// Small results are returned directly. Lambda streams responses larger than 6MB at only 2MB/s
/// so larger results are uploaded into S3 and the client is redirected to download them through CloudFront.
async fn query(State(app): State<Arc<App>>, Query(params): Query<QueryParams>) -> Response {
    match app.usage().await {
        Ok(usage) if usage.over_limit() => {
            return error_response(
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "The monthly limit of {:.0} GB-seconds is used ({:.0} GB-seconds since {}). \
                     Raise gb_seconds_limit in infra/main.tf to continue.",
                    usage.limit_gb_seconds, usage.used_gb_seconds, usage.month_start
                ),
            );
        }
        Ok(_) => {}
        // Not knowing the usage doesn't stop the queries, CloudWatch can be briefly unavailable
        Err(e) => tracing::warn!("Failed to read the usage: {e}"),
    }
    let conn = match app.db.lock().unwrap().try_clone() {
        Ok(conn) => conn,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let key = result_key(&params);
    let started = std::time::Instant::now();
    let fetches_before = fetch_stats(&app);

    let (tx, mut rx) = mpsc::channel(8);
    tokio::task::spawn_blocking(move || run_query(conn, params, tx));

    let mut inline = Vec::new();
    let mut upload: Option<ResultUpload> = None;
    while let Some(chunk) = rx.recv().await {
        let written = match chunk {
            Err(e) => {
                if let Some(upload) = upload {
                    upload.abort().await;
                }
                return error_response(StatusCode::BAD_REQUEST, e);
            }
            Ok(data) => match upload.as_mut() {
                Some(upload) => upload.write(&data).await,
                None if inline.len() + data.len() <= INLINE_BYTES => {
                    inline.extend_from_slice(&data);
                    Ok(())
                }
                None => match ResultUpload::start(&app, key.clone()).await {
                    Ok(mut started) => {
                        let written = started.write(&std::mem::take(&mut inline)).await;
                        let written = match written {
                            Ok(()) => started.write(&data).await,
                            Err(e) => Err(e),
                        };
                        upload = Some(started);
                        written
                    }
                    Err(e) => Err(e),
                },
            },
        };
        if let Err(e) = written {
            if let Some(upload) = upload {
                upload.abort().await;
            }
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to upload the result: {e}"),
            );
        }
    }

    // The lake is frozen so results can be cached
    let cache = (header::CACHE_CONTROL, "public, max-age=86400");
    // Time spent in the lambda and warc_record() fetches for the client's statistics
    let duration = started.elapsed().as_millis().to_string();
    let duration = (
        header::HeaderName::from_static("x-duration-ms"),
        duration.as_str(),
    );
    let fetches = fetch_stats(&app)
        .zip(fetches_before)
        .map(|(after, before)| {
            format!(
                "fetches={} retries={} errors={} slow={} max_ms={} hedges={}",
                after[0] - before[0],
                after[1] - before[1],
                after[2] - before[2],
                after[3] - before[3],
                after[4],
                after[5] - before[5]
            )
        })
        .unwrap_or_default();
    let fetches = (
        header::HeaderName::from_static("x-warc-stats"),
        fetches.as_str(),
    );
    match upload {
        None => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, CONTENT_TYPE),
                cache,
                duration,
                fetches,
            ],
            inline,
        )
            .into_response(),
        Some(upload) => match upload.finish().await {
            Ok(key) => (
                StatusCode::SEE_OTHER,
                [
                    (header::LOCATION, format!("/{key}").as_str()),
                    cache,
                    duration,
                    fetches,
                ],
            )
                .into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to upload the result: {e}"),
            ),
        },
    }
}

/// fetches, retries, errors, slow fetches and the slowest fetch of warc_record() in this process
fn fetch_stats(app: &App) -> Option<[u64; 6]> {
    let conn = app.db.lock().unwrap().try_clone().ok()?;
    conn.query_row(
        "SELECT s.fetches, s.retries, s.errors, s.slow_fetches, s.max_fetch_ms, s.hedges
         FROM (SELECT warc_fetch_stats() AS s)",
        [],
        |row| {
            Ok([
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ])
        },
    )
    .ok()
}

async fn usage(State(app): State<Arc<App>>) -> Response {
    match app.usage().await {
        Ok(usage) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            usage.to_json(),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

/// Billed duration of this function in the current calendar month (UTC), which is when the free tier resets
#[derive(Clone)]
struct Usage {
    month_start: String,
    invocations: f64,
    used_gb_seconds: f64,
    limit_gb_seconds: f64,
    memory_mb: usize,
    timeout_seconds: usize,
    checked: std::time::Instant,
}

impl Usage {
    fn over_limit(&self) -> bool {
        self.limit_gb_seconds > 0.0 && self.used_gb_seconds >= self.limit_gb_seconds
    }

    fn to_json(&self) -> String {
        format!(
            r#"{{"month_start":"{}","invocations":{},"used_gb_seconds":{:.1},"limit_gb_seconds":{:.0},"remaining_gb_seconds":{:.1},"memory_mb":{},"timeout_seconds":{},"remaining_seconds_at_this_memory":{:.0}}}"#,
            self.month_start,
            self.invocations,
            self.used_gb_seconds,
            self.limit_gb_seconds,
            (self.limit_gb_seconds - self.used_gb_seconds).max(0.0),
            self.memory_mb,
            self.timeout_seconds,
            (self.limit_gb_seconds - self.used_gb_seconds).max(0.0)
                / (self.memory_mb as f64 / 1024.0),
        )
    }
}

/// Days since 1970-01-01 -> (year, month), from http://howardhinnant.github.io/date_algorithms.html
fn year_month(days: i64) -> (i64, i64) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(month <= 2), month)
}

/// (year, month) -> days since 1970-01-01 of its first day
fn days_from_year_month(year: i64, month: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// CloudWatch has the usage with a delay of about a minute, so it is read at most once a minute
const USAGE_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(60);

struct App {
    db: Mutex<Connection>,
    s3: aws_sdk_s3::Client,
    cloudwatch: aws_sdk_cloudwatch::Client,
    results_bucket: String,
    usage: tokio::sync::Mutex<Option<Usage>>,
}

impl App {
    async fn usage(&self) -> Result<Usage, Error> {
        let mut cached = self.usage.lock().await;
        if let Some(usage) = cached.as_ref() {
            if usage.checked.elapsed() < USAGE_MAX_AGE {
                return Ok(usage.clone());
            }
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let (year, month) = year_month(now.div_euclid(86400));
        let start = days_from_year_month(year, month) * 86400;
        let function = env("AWS_LAMBDA_FUNCTION_NAME")?;
        let stats = self
            .cloudwatch
            .get_metric_statistics()
            .namespace("AWS/Lambda")
            .metric_name("Duration")
            .dimensions(
                aws_sdk_cloudwatch::types::Dimension::builder()
                    .name("FunctionName")
                    .value(function)
                    .build(),
            )
            .start_time(aws_sdk_cloudwatch::primitives::DateTime::from_secs(start))
            .end_time(aws_sdk_cloudwatch::primitives::DateTime::from_secs(
                now + 60,
            ))
            // One datapoint per day, older than 15 days the period must be a multiple of 300 seconds
            .period(86400)
            .statistics(aws_sdk_cloudwatch::types::Statistic::Sum)
            .statistics(aws_sdk_cloudwatch::types::Statistic::SampleCount)
            .send()
            .await?;
        let (mut milliseconds, mut invocations) = (0.0, 0.0);
        for point in stats.datapoints() {
            milliseconds += point.sum().unwrap_or(0.0);
            invocations += point.sample_count().unwrap_or(0.0);
        }
        let memory_mb = env_number("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", 3008);
        let usage = Usage {
            month_start: format!("{year:04}-{month:02}-01T00:00:00Z"),
            invocations,
            // Billed duration is rounded up to the next millisecond for every invocation
            used_gb_seconds: (milliseconds + invocations) / 1000.0 * memory_mb as f64 / 1024.0,
            limit_gb_seconds: env_number("GB_SECONDS_LIMIT", 400_000) as f64,
            memory_mb,
            timeout_seconds: env_number("TIMEOUT_SECONDS", 120),
            checked: std::time::Instant::now(),
        };
        *cached = Some(usage.clone());
        Ok(usage)
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    let aws = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws);

    let app = Arc::new(App {
        db: Mutex::new(open_database()?),
        s3,
        cloudwatch: aws_sdk_cloudwatch::Client::new(&aws),
        results_bucket: env("RESULTS_BUCKET")?,
        usage: tokio::sync::Mutex::new(None),
    });
    let router = Router::new()
        .route("/", get(query))
        .route("/usage", get(usage))
        .with_state(app);
    run_with_streaming_response(router).await
}
