//! Runs read-only SQL against the frozen Common Crawl DuckLake and streams the result as
//! zstd compressed Arrow IPC.
//!
//!   GET /?sql=SELECT ...&level=1   level is the optional zstd compression level
//!
//! Results over 5MB are uploaded into S3 and the response redirects to them.
//!
//! The DuckLake catalog is downloaded from S3 into /tmp on cold start. The parquet files are read
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
use duckdb::{AccessMode, Config, Connection};
use lambda_http::{Error, run_with_streaming_response, tracing};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

const CATALOG_PATH: &str = "/tmp/commoncrawl.ducklake";
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
}

/// Arrow IPC bytes or the error of the query
type Chunks = mpsc::Sender<Result<Bytes, String>>;

fn env(name: &str) -> Result<String, Error> {
    std::env::var(name).map_err(|_| format!("{name} env is missing").into())
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn download_catalog(s3: &aws_sdk_s3::Client) -> Result<(), Error> {
    if Path::new(CATALOG_PATH).exists() {
        return Ok(());
    }
    let (bucket, key) = (env("CATALOG_BUCKET")?, env("CATALOG_KEY")?);
    let started = std::time::Instant::now();

    let object = s3.get_object().bucket(&bucket).key(&key).send().await?;

    // Download into a temporary file so that a failed download is not used later
    let partial = format!("{CATALOG_PATH}.partial");
    let mut file = tokio::fs::File::create(&partial).await?;
    tokio::io::copy(&mut object.body.into_async_read(), &mut file).await?;
    file.sync_all().await?;
    tokio::fs::rename(&partial, CATALOG_PATH).await?;
    tracing::info!("Downloaded s3://{bucket}/{key} in {:?}", started.elapsed());
    Ok(())
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

    let session_token = std::env::var("AWS_SESSION_TOKEN").unwrap_or_default();
    let ca_bundles: Vec<String> = CA_BUNDLES
        .iter()
        .filter(|path| Path::new(path).exists())
        .map(|path| quote(path))
        .collect();
    // Reading from S3 is network bound so use more threads than there are vCPUs
    let threads: usize = std::env::var("DUCKDB_THREADS")
        .ok()
        .and_then(|threads| threads.parse().ok())
        .unwrap_or(16);

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

        SET threads = {threads};
        SET parquet_metadata_cache = true;

        SET allowed_directories = ['s3://commoncrawl/'];
        SET allowed_paths = [{ca_bundles}];
        SET enable_external_access = false;
        SET lock_configuration = true;
        ",
        key_id = quote(&env("AWS_ACCESS_KEY_ID")?),
        secret = quote(&env("AWS_SECRET_ACCESS_KEY")?),
        session_token = quote(&session_token),
        ca_bundles = ca_bundles.join(", "),
    ))?;
    tracing::info!("Opened DuckLake in {:?}", started.elapsed());
    Ok(conn)
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
        let mut stmt = conn.prepare(&params.sql)?;
        // Streaming keeps memory usage low for large results
        let batches = stmt.stream_arrow([])?;
        let sink = ChunkWriter {
            tx,
            buffer: Vec::with_capacity(CHUNK_BYTES),
        };
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))?
            .try_with_compression_level(Some(params.level.unwrap_or(DEFAULT_ZSTD_LEVEL)))?;
        let mut writer = StreamWriter::try_new_with_options(sink, &batches.get_schema(), options)?;
        for batch in batches {
            writer.write(&batch)?;
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
    let hash = Sha256::digest(format!("{catalog}\n{level}\n{}", params.sql).as_bytes());
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{RESULTS_PREFIX}{hex}.arrow")
}

/// Small results are returned directly. Lambda streams responses larger than 6MB at only 2MB/s
/// so larger results are uploaded into S3 and the client is redirected to download them through CloudFront.
async fn query(State(app): State<Arc<App>>, Query(params): Query<QueryParams>) -> Response {
    let conn = match app.db.lock().unwrap().try_clone() {
        Ok(conn) => conn,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let key = result_key(&params);

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
    match upload {
        None => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, CONTENT_TYPE), cache],
            inline,
        )
            .into_response(),
        Some(upload) => match upload.finish().await {
            Ok(key) => (
                StatusCode::SEE_OTHER,
                [(header::LOCATION, format!("/{key}").as_str()), cache],
            )
                .into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to upload the result: {e}"),
            ),
        },
    }
}

struct App {
    db: Mutex<Connection>,
    s3: aws_sdk_s3::Client,
    results_bucket: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    let s3 = aws_sdk_s3::Client::new(&aws_config::load_from_env().await);
    download_catalog(&s3).await?;

    let app = Arc::new(App {
        db: Mutex::new(open_database()?),
        s3,
        results_bucket: env("RESULTS_BUCKET")?,
    });
    let router = Router::new().route("/", get(query)).with_state(app);
    run_with_streaming_response(router).await
}
