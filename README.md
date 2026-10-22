# FrozeLake CommonCrawl

Convert Common Crawl columnar index to a read only frozen [DuckLake](https://ducklake.select/) for efficient querying.

As of 2026 October there are `98385` parquet files in the [Common Crawl columnar index](https://data.commoncrawl.org/cc-index/table/cc-main/index.html).

It would be neat if one would be able to query from all of them in efficient and ergonomic way.

This script uses ducklake and [ducklake_add_data_files()](https://ducklake.select/docs/stable/duckdb/metadata/adding_files) calls to convert all of the files into a ~65MB frozen catalog to have easy access to all data in common crawl.

Requires DuckDB 2.0 and DuckLake 1.1.

`ducklake_add_data_files()` only reads the parquet footers so it's fast and multiple calls can run in parallel. Adding all 98,385 files from Finland through S3 (`--access-mode s3 --workers 16 --batch-size 25`) ran at ~25 files/s and took ~75 minutes. HTTP access works without AWS credentials but Common Crawl rate limits it with `403 Forbidden` responses, in which case the script pauses all workers for 255s.

Hive partitions (`crawl` and `subset`) from the file paths are stored as DuckLake partition values ([duckdb/ducklake#579](https://github.com/duckdb/ducklake/issues/579) is fixed) so filtering by `crawl` or `subset` skips the other files.

## Overview

This project provides tools to fetch Common Crawl's columnar index data and organize it into DuckLake tables. All crawls are stored in a single table and the schema is evolved when newer crawls add columns or change types.

## How does it work?
```sql
LOAD ducklake;
LOAD httpfs;

ATTACH 'ducklake:commoncrawl.ducklake'
AS commoncrawl (DATA_PATH 'tmp_always_empty');

SET VARIABLE cc_file = 'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2014-41/subset=warc/part-00103-e1115d40-d2cc-4445-873c-2b206f427726.c000.gz.parquet';

CREATE TABLE commoncrawl.archives AS
    FROM read_parquet($cc_file)
    WITH NO DATA;

ALTER TABLE commoncrawl.archives SET PARTITIONED BY (crawl, subset);

-- Then this part is repeated for all parquet files in Common Crawl
CALL ducklake_add_data_files(
    'commoncrawl',
    'archives',
    $cc_file,
    allow_missing => true
);

-- When newer crawls change the schema, the table is evolved before adding their files
ALTER TABLE commoncrawl.archives ADD COLUMN url_host_name_reversed VARCHAR;
ALTER TABLE commoncrawl.archives ALTER COLUMN fetch_time SET DATA TYPE TIMESTAMPTZ;
```

Read this post to learn more:
https://ducklake.select/2025/10/24/frozen-ducklake/

## Features

- **Idempotent execution** - safely rerun without reprocessing existing data
- **Graceful Ctrl+C handling** - interrupt anytime without corruption
- **Parallel** - adds multiple files at the same time (`--workers`)
- **Automatic retry logic** - pauses on `403` rate limits and retries
- **Schema evolution** - crawls are added from oldest to newest and new columns or wider types are added to the table automatically:
  - `CC-MAIN-2018-39`: `content_charset`, `content_languages`
  - `CC-MAIN-2019-47`: `fetch_redirect`, `content_truncated`
  - `CC-MAIN-2021-49`: `url_host_name_reversed`, `fetch_time` `TIMESTAMP` -> `TIMESTAMPTZ` (old values are UTC)
  - `CC-MAIN-2026-30`: `warc_record_id`

Once `fetch_time` has been changed to `TIMESTAMPTZ` older files can't be added anymore. Because of this all files of a crawl must be added before moving to the next crawl. Failed files are retried 6 times in smaller batches and if they still fail the script stops so it can be rerun later. At the end the script verifies that every parquet file from Common Crawl is in the `archives` table.

## Usage

Run with devenv:

```bash
# Auto-detect access mode (checks AWS credentials)
devenv shell ./columnar.py

# Force S3 access (requires AWS credentials)
devenv shell ./columnar.py --access-mode s3

# Force HTTP access (rate limited, no credentials needed)
devenv shell ./columnar.py --access-mode http

# Change amount of parallel workers (default 8)
devenv shell ./columnar.py --workers 4
```

Or manually with uv:

```bash
uv run --no-project --with 'duckdb>=2.0.0' ./columnar.py --access-mode http
```

### Access Modes

- `s3` - Force S3 access (faster, requires AWS credentials from env or `~/.aws`)
- `http` - Force HTTP access (slower, rate limited, no credentials needed)

## Output

Creates `commoncrawl.ducklake` (or `commoncrawl_s3.ducklake` in s3 mode) database with a single `archives` table which contains all crawls, partitioned by `crawl` and `subset` (`warc`, `crawldiagnostics`, `robotstxt`).

After all files are added the catalog is frozen into `commoncrawl.frozen.ducklake` (or `commoncrawl_s3.frozen.ducklake`) for querying. Freezing compacts the catalog, removes min/max stats of columns which can't be used for skipping files (`url`, `warc_filename`, ...) and adds the [`url_surtkey` macros](#query-by-domain-or-url-with-url_surtkey). This makes it ~40% smaller which makes the lambda cold starts faster.

```sql
ATTACH 'ducklake:commoncrawl.frozen.ducklake' AS commoncrawl (READ_ONLY);
USE commoncrawl;

SELECT url, fetch_time, fetch_status
FROM domain_pages('example.com')
WHERE crawl = 'CC-MAIN-2026-43'
  AND subset = 'warc';
```

Catalogs created by older versions of this project are migrated to DuckLake 1.1 automatically. The old `archives` view is replaced by the table, the old `CC_MAIN_2013_TO_2021` and `CC_MAIN_2021_AND_FORWARD` tables are left untouched.

## Download the frozen catalog

Ready made catalogs are attached to the [GitHub releases](https://github.com/midwork-finds-jobs/frozenlake-commoncrawl/releases):

- `commoncrawl.frozen.ducklake` reads the files from `https://data.commoncrawl.org` and works without AWS credentials but is rate limited.
- `commoncrawl_s3.frozen.ducklake` reads the files from `s3://commoncrawl` and needs AWS credentials. Use this from inside AWS `us-east-1` or with the lambda below.

```sql
ATTACH 'ducklake:https://github.com/midwork-finds-jobs/frozenlake-commoncrawl/releases/latest/download/commoncrawl.frozen.ducklake' AS commoncrawl (READ_ONLY);
USE commoncrawl;

FROM domain_pages('helsinki.fi') WHERE crawl = 'CC-MAIN-2026-43' AND subset = 'warc' LIMIT 10;
```

## Querying from your laptop through AWS Lambda

Reading Common Crawl directly from Finland is slow and rate limited. Instead the frozen DuckLake is deployed next to the `s3://commoncrawl` bucket in `us-east-1` and queried through AWS Lambda. Lambda reads the parquet files with S3 credentials from inside AWS so there are no rate limits and only the results are transferred to your laptop as zstd compressed [Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).

```
laptop (DuckDB) ──https──> CloudFront ──> Lambda (DuckDB + DuckLake) ──> s3://commoncrawl
```

- **CloudFront** checks the [signed cookies](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-signed-cookies.html) and is used because of the AWS free tier: CloudFront includes 1TB of data transfer out per month while all other services share 100GB per month. Transfer from Lambda to CloudFront is free.
- **Lambda** runs a minimal DuckDB build with only `parquet`, `httpfs` and `ducklake` statically linked ([lambda/duckdb/extension_config.cmake](lambda/duckdb/extension_config.cmake)) which keeps the package small and cold starts fast. It includes the unreleased [parquet page index](#parquet-page-index) support from [duckdb/duckdb#27326](https://github.com/duckdb/duckdb/pull/27326).

### Deploy

Requirements: AWS account, `devenv` and Apple [`container`](https://github.com/apple/container) for building DuckDB for linux arm64.

```bash
# 1. Login with AWS CLI v2. DuckDB's AWS SDK doesn't support `aws login` sessions,
#    so add a profile which exports the credentials for it:
devenv shell -- aws login --profile commoncrawl
cat >> ~/.aws/config <<CONFIG

[profile commoncrawl-duckdb]
region = us-east-1
credential_process = aws configure export-credentials --profile commoncrawl --format process
CONFIG
export AWS_PROFILE=commoncrawl-duckdb

# 2. Create the frozen DuckLake. Paths point to s3://commoncrawl so the lambda can read them.
devenv shell -- ./columnar.py --access-mode s3 --workers 16

# 3. Build the lambda. The first run builds a minimal DuckDB in an Amazon Linux 2023
#    container with Apple's `container` tool which takes a while.
devenv shell -- lambda/build.sh

# 4. Deploy into us-east-1
cd infra
devenv shell -- tofu init
devenv shell -- tofu apply
```

To update the DuckLake later run `columnar.py` again and `tofu apply`. Lambdas download the new catalog on their next cold start.

### AWS quotas

These quotas limit how long and how fast queries can run. Increasing them is free but needs a request which AWS support reviews.

| Quota | Code | Default | Requested | Why |
|---|---|---|---|---|
| CloudFront: Response timeout per origin | `L-AECE9FA7` | 120s | 180s | CloudFront waits this long for the lambda to send its first bytes. The lambda sends nothing before the query is done, so this is the maximum query duration. |
| Lambda: Network bandwidth per execution environment | `L-14320ECE` | 625 Mbps | 3000 Mbps | Without an increase every memory size has the same bandwidth. After the increase bandwidth scales with memory from 2048MB up to 3000 Mbps at 10240MB, which makes reading from `s3://commoncrawl` faster for larger lambdas. |

```bash
# CloudFront quotas are global but requested through us-east-1
aws service-quotas request-service-quota-increase --region us-east-1 \
    --service-code cloudfront --quota-code L-AECE9FA7 --desired-value 180

aws service-quotas request-service-quota-increase --region us-east-1 \
    --service-code lambda --quota-code L-14320ECE --desired-value 3000

# Status of the requests: PENDING -> CASE_OPENED -> APPROVED
for SERVICE in cloudfront lambda; do
    aws service-quotas list-requested-service-quota-change-history --region us-east-1 \
        --service-code "$SERVICE" --query 'RequestedQuotas[].[QuotaName,DesiredValue,Status]' --output text
done
```

OpenTofu reads the current CloudFront timeout quota and uses it for both CloudFront and the lambda timeout, so after an increase is approved just run `tofu apply` again. Nobody would receive results which take longer than CloudFront waits, so the lambda stops at the same time. Check the current values with `tofu output quotas`.

Queries which scan a lot of files before returning anything (for example filtering by `url` over all crawls) can take longer than the timeout. Use the [`url_surtkey` macros](#query-by-domain-or-url-with-url_surtkey) and filter by `crawl` to read fewer files.

### Query

The lambda answers `GET /?sql=...` with a zstd compressed Arrow IPC stream. `&level=N` changes the zstd compression level.

Lambda streams responses larger than 6MB at only 2MB/s and bills the whole time. Because of this results larger than 5MB are uploaded into the S3 bucket and the lambda responds with a `303` redirect to `/results/<hash>.arrow` which CloudFront serves from S3 at full speed. DuckDB follows the redirect automatically. Results are deleted after 1 day.

Create the signed cookies with `infra/cloudfront-cookie.sh` (valid for 24 hours) or with the [duckdb-cloudfront](https://github.com/midwork-finds-jobs/duckdb-cloudfront) extension once it's published for DuckDB 2.0. Arrow IPC is read with the [nanoarrow](https://duckdb.org/community_extensions/extensions/nanoarrow) community extension.

```bash
cd infra
export CC_URL="$(tofu output -raw cloudfront_url)"
export CC_COOKIE="$(./cloudfront-cookie.sh)"
duckdb
```

```sql
INSTALL nanoarrow FROM community;
LOAD nanoarrow;

CREATE SECRET commoncrawl (
    TYPE http,
    EXTRA_HTTP_HEADERS MAP {'Cookie': getenv('CC_COOKIE')},
    SCOPE getenv('CC_URL')
);

-- The result is streamed so download it at once
SET force_download = true;

CREATE MACRO commoncrawl(sql) AS TABLE
    FROM read_arrow(getenv('CC_URL') || '?sql=' || url_encode(sql));

-- All .fi domains of the latest crawl
CREATE TABLE fi_domains AS FROM commoncrawl($$
    SELECT DISTINCT url_host_registered_domain
    FROM archives
    WHERE crawl = 'CC-MAIN-2026-43'
      AND subset = 'warc'
      AND url_host_tld = 'fi'
$$);
```

### Query by domain or url with `url_surtkey`

Common Crawl sorts the files by [SURT](https://github.com/internetarchive/surt) url keys (`https://www.yle.fi/uutiset/` becomes `fi,yle)/uutiset`). DuckLake knows the min and max `url_surtkey` of every file, so filtering with it skips almost all files. The frozen catalog contains macros which create the keys for you:

| Macro | Example | Matches |
|---|---|---|
| `domain_pages(domain)` | `FROM domain_pages('helsinki.fi')` | all pages of the domain and its subdomains |
| `surtkey_in_domain(url_surtkey, domain)` | `WHERE surtkey_in_domain(url_surtkey, 'helsinki.fi')` | same as above as a filter |
| `surtkey_in_host(url_surtkey, host)` | `WHERE surtkey_in_host(url_surtkey, 'studies.helsinki.fi')` | only this host |
| `surtkey_starts_with(url_surtkey, url)` | `WHERE surtkey_starts_with(url_surtkey, 'yle.fi/uutiset')` | urls starting with this |
| `surt_host(host)` / `surt_prefix(url)` | `surt_prefix('https://www.yle.fi/uutiset')` | `fi,yle)/uutiset` |

```sql
FROM commoncrawl($$
    SELECT url_host_name, count(*) AS pages
    FROM domain_pages('helsinki.fi')
    WHERE crawl = 'CC-MAIN-2026-43' AND subset = 'warc'
    GROUP BY ALL
    ORDER BY pages DESC
$$);
```

Files read from the 300 `warc` files of `CC-MAIN-2026-43` with different filters:

| Filter | Files read |
|---|---|
| `domain_pages('helsinki.fi')` | 1 |
| `url_host_registered_domain = 'helsinki.fi'` | 121 |
| `url_host_name = 'studies.helsinki.fi'` | 300 |
| `url = 'https://www.helsinki.fi/fi'` | 300 (doesn't finish within the CloudFront timeout) |

With Python and pyarrow:

```python
import os, requests, pyarrow.ipc

response = requests.get(
    os.environ["CC_URL"],
    params={"sql": "SELECT url FROM archives WHERE crawl = 'CC-MAIN-2026-43' AND url_host_tld = 'fi' LIMIT 100"},
    headers={"Cookie": os.environ["CC_COOKIE"]},
    stream=True,
)
table = pyarrow.ipc.open_stream(response.raw).read_all()
```

### Performance and free tier

Measured from Finland with `infra/benchmark.sh` in October 2026 with DuckDB v2.0.0 before the [page index](#parquet-page-index) patch. `seconds` is the time to load the result into a local DuckDB table, `GB-s` is the billed lambda duration multiplied by memory. The free tier includes 400 000 GB-s and 1 million requests per month.

Small result: the 117 988 distinct `.fi` domains of one crawl (1MB of Arrow, returned directly):

| Memory | Billed | GB-s | Seconds |
|---|---|---|---|
| 1769MB | 0.8s | 1.4 | 1.7 |
| 3008MB | 0.5s | 1.5 | 1.4 |
| 5307MB | 0.35s | 1.8 | 1.3 |
| 10240MB | 0.3s | 3.1 | 1.25 |

Large result: url, fetch time and status of all 7.6M `.fi` pages of one crawl (166MB of Arrow, uploaded to S3 and downloaded through CloudFront):

| Memory | zstd level | Billed | GB-s | Seconds | Size |
|---|---|---|---|---|---|
| 1769MB | 1 | 6.5s | 11 | 12.7 | 166.5MB |
| 3008MB | 1 | 4.3s | 12 | 10.5 | 166.5MB |
| 5307MB | 1 | 3.7s | 19 | 10.3 | 166.5MB |
| 10240MB | 1 | 4.3s | 43 | 10.5 | 166.5MB |
| 3008MB | 3 | 4.6s | 13.5 | 10.9 | 166.2MB |
| 3008MB | 6 | 8.5s | 25 | 14.7 | 155.8MB |
| 3008MB | 9 | 13s | 38 | 19.2 | 155.0MB |

- **zstd level 1** is the default. Higher levels make the results only ~7% smaller but take 2-3x longer.
- **3008MB** is the default memory. It's 20-35% faster than 1769MB for ~10% more GB-s. Larger lambdas don't help before the network bandwidth quota is increased, see [AWS quotas](#aws-quotas).
- With 3008MB the free tier covers ~250 000 small or ~30 000 large queries per month.
- Cold start takes ~3.5s: downloading the 65MB catalog from S3 takes ~3s and opening DuckDB ~0.35s.
- Most of the large query time is the download from CloudFront to Finland, not the lambda.
- Lambda responses over 6MB are streamed at only 2MB/s. Before results were uploaded into S3 the large query took ~48s and ~140 GB-s.

### Parquet page index

Common Crawl writes the files with Spark (`parquet-mr`), which stores a [page index](https://parquet.apache.org/docs/file-format/pageindex/) (`ColumnIndex` and `OffsetIndex`) for every column but no bloom filters. Every file has ~7.2M rows in only 5 row groups. DuckDB v2.0.0 can only skip whole row groups, so even a lookup of a single domain downloads ~1.45M rows of every selected column.

The rows are sorted by `url_surtkey`, so its page index tells exactly which ~1MB pages contain a domain. The lambda is built from DuckDB v2.0.0 with [duckdb/duckdb#27326](https://github.com/duckdb/duckdb/pull/27326) applied ([lambda/duckdb/patches](lambda/duckdb/patches)), which uses the page index to skip the other pages. The query results are identical. Measured in October 2026 ([results in the pull request](https://github.com/duckdb/duckdb/pull/27326#issuecomment-6327388097)):

| Query | MB downloaded v2.0.0 | MB downloaded with page index | Lambda billed v2.0.0 | Lambda billed with page index |
|---|---:|---:|---:|---:|
| `url` of one domain in one crawl | 26.41 | 2.79 | 529 ms | 362 ms |
| `*` of one domain in one crawl | 90.24 | 10.55 | 1040 ms | 619 ms |
| `url` with an url prefix | 26.41 | 1.15 | 544 ms | 354 ms |
| `crawl, url` of one domain in 10 crawls | 455.09 | 37.69 | 6054 ms | 1445 ms |
| 3 columns of the whole `.fi` TLD | 147.16 | 126.95 | | |

The page index only helps when filtering with `url_surtkey`, which is what the [`url_surtkey` macros](#query-by-domain-or-url-with-url_surtkey) do. The patch is applied by default. Build the official DuckDB v2.0.0 instead with:

```bash
rm -rf lambda/target/duckdb-out
DUCKDB_PATCH= lambda/build.sh
```

Remove the patch once the pull request is in a DuckDB release.

### jemalloc

DuckDB uses [jemalloc](https://jemalloc.net/) as the allocator on Linux by default, but it's disabled in the lambda build because it was slower there. Loading the 7.6M `.fi` pages of one crawl billed ~5.0s with jemalloc and ~4.2s without it at 3008MB (3 runs each, measured twice). The `url_surtkey` lookups were equally fast with both. Build with jemalloc to compare:

```bash
rm -rf lambda/target/duckdb-out
JEMALLOC=1 lambda/build.sh
```

### Security

The lambda opens the DuckLake read only and then disables all file system and network access except `s3://commoncrawl/` with `enable_external_access = false` and `lock_configuration = true`. The execution role can only read `s3://commoncrawl/cc-index/*` and the catalog and write the query results into `results/` of its own bucket.

## License

MIT
