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

Both need DuckDB v2.0.0 or newer with the `ducklake` and `httpfs` extensions, which are installed automatically. The S3 catalog needs an S3 secret, for example `CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')`. Queries read only the parquet pages they need, but from outside `us-east-1` even small lookups take seconds to minutes, so the [lambda](#querying-from-your-laptop-through-aws-lambda) is much faster.

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
- **Lambda** runs a minimal DuckDB build with only `parquet`, `json`, `httpfs`, `ducklake` and our [`extract`](#extract-data-from-the-crawled-pages) extension statically linked ([lambda/duckdb/extension_config.cmake](lambda/duckdb/extension_config.cmake)) which keeps the package small and cold starts fast. It includes the unreleased [parquet page index](#parquet-page-index) support from [duckdb/duckdb#27326](https://github.com/duckdb/duckdb/pull/27326).

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
#    container with Apple's `container` tool which takes a while. DuckDB is rebuilt
#    when something in lambda/duckdb/ changes.
devenv shell -- lambda/build.sh

# 4. Deploy into us-east-1
cd infra
devenv shell -- tofu init
devenv shell -- tofu apply
```

To update the DuckLake later run `columnar.py` again, then `lambda/build.sh` and `tofu apply`. The catalog is packaged into the lambda zip so that cold starts don't download it.

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

Requirements on the laptop:

- [DuckDB](https://duckdb.org/docs/installation/) v2.0.0 or newer. The catalog is DuckLake 1.1 which older versions can't read.
- The `commoncrawl` extension of this repository ([client](client)): `client/build.sh` builds it with Rust (`cargo`), no DuckDB build needed. It's not signed, so DuckDB has to be started with `-unsigned`.
- The CloudFront url, key pair id and private key from `tofu output`. No AWS credentials are needed for querying.
- The [`extract`](#extract-data-from-the-crawled-pages) functions, `cdx_captures()` and the memory governor run only in the lambda, the laptop just reads the results. The SURT macros work everywhere because they are in the catalog.

```bash
client/build.sh
cd infra
export CC_URL="$(tofu output -raw cloudfront_url)"
export CC_KEY_PAIR_ID="$(tofu output -raw cloudfront_key_pair_id)"
tofu output -raw cloudfront_private_key_pem > ~/.commoncrawl.pem && chmod 600 ~/.commoncrawl.pem
export CC_PRIVATE_KEY_PATH=~/.commoncrawl.pem
duckdb -unsigned
```

```sql
LOAD 'client/target/commoncrawl.duckdb_extension';

-- All .fi domains of the latest crawl
CREATE TABLE fi_domains AS FROM commoncrawl($$
    SELECT DISTINCT url_host_registered_domain
    FROM archives
    WHERE crawl = 'CC-MAIN-2026-43'
      AND subset = 'warc'
      AND url_host_tld = 'fi'
$$);

FROM commoncrawl_usage();  -- lambda GB-seconds used this month
FROM commoncrawl_stats();  -- timings, sizes and fetch counters of every lambda of the last query
```

`commoncrawl()` signs the [CloudFront cookies](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-signed-cookies.html) itself, downloads the results of all [parts](#parallel-queries) in parallel over HTTP/2 and decodes the Arrow IPC with arrow-rs. The settings can also be given as named parameters: `url`, `key_pair_id` and `private_key_path`.

The lambda answers `GET /?sql=...` with a zstd compressed Arrow IPC stream. `&level=N` changes the zstd compression level. `GET /usage` returns the lambda usage of the month and queries get `429` after the [monthly limit](#monthly-usage-limit).

Lambda streams responses larger than 6MB at only 2MB/s and bills the whole time. Because of this results larger than 5MB are uploaded into the S3 bucket and the lambda responds with a `303` redirect to `/results/<hash>.arrow` which CloudFront serves from S3 at full speed. Results are deleted after 1 day. Successful results are cached by CloudFront for a day, errors aren't.

Without the extension the results can be read with the [nanoarrow](https://duckdb.org/community_extensions/extensions/nanoarrow) community extension and cookies from `infra/cloudfront-cookie.sh`:

```sql
LOAD nanoarrow;
CREATE SECRET (TYPE http, EXTRA_HTTP_HEADERS MAP {'Cookie': getenv('CC_COOKIE')}, SCOPE getenv('CC_URL'));
SET force_download = true;
-- Queries can take up to the CloudFront timeout. Without this DuckDB retries after 30s and the lambda runs again.
SET http_timeout = 130;
SET http_retries = 0;
FROM read_arrow(getenv('CC_URL') || '?sql=' || url_encode('SELECT 42'));
```

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

### Query by domain or url with `url_surtkey`

Common Crawl sorts the files by [SURT](https://github.com/internetarchive/surt) url keys (`https://www.yle.fi/uutiset/` becomes `fi,yle)/uutiset`). DuckLake knows the min and max `url_surtkey` of every file, so filtering with it skips almost all files. The frozen catalog contains macros which create the keys for you:

| Macro | Example | Matches |
|---|---|---|
| `domain_pages(domain)` | `FROM domain_pages('helsinki.fi')` | all pages of the domain and its subdomains |
| `surtkey_in_domain(url_surtkey, domain)` | `WHERE surtkey_in_domain(url_surtkey, 'helsinki.fi')` | same as above as a filter |
| `surtkey_in_host(url_surtkey, host)` | `WHERE surtkey_in_host(url_surtkey, 'studies.helsinki.fi')` | only this host |
| `surtkey_starts_with(url_surtkey, url)` | `WHERE surtkey_starts_with(url_surtkey, 'yle.fi/uutiset')` | urls starting with this |
| `surt_host(host)` / `surt_prefix(url)` | `surt_prefix('https://www.yle.fi/uutiset')` | `fi,yle)/uutiset` |
| `surt_url(url)` | `surt_url('https://yle.fi/uutiset/')` | the exact `url_surtkey` of an url: `fi,yle)/uutiset` |

The scheme, `www.`, default ports and `#fragment` are dropped and query parameters sorted like in Common Crawl's keys. `surt_url()` is plain SQL so that it works on the laptop too, but it doesn't decode `%26` style escapes like Common Crawl does, so it matches ~95% of the keys. In the lambda `surt_key(url)` of the extract extension implements the same canonicalization as Common Crawl's indexer and matched all 78 278 urls of yle.fi, helsinki.fi and verkkokauppa.com in a crawl: `WHERE url_surtkey = surt_key('https://...')`. `cdx_captures()` uses it.

Table macros for enumerating domains and urls and their history:

| Macro | Returns |
|---|---|
| `host_pages(host)` | all pages of exactly this host |
| `domain_history(domain)` | pages, ok pages, hosts and first and last fetch of the domain in every crawl |
| `domain_hosts(domain)` | subdomains with page counts and the first and last crawl they were seen in |
| `host_urls(host)` | distinct urls of a host with their capture count and first and last crawl |
| `domain_redirects(domain)` | redirects of the domain, for example when it moved to a new domain. `fetch_redirect` is in the index since `CC-MAIN-2019-47` |
| `domain_robots_txt(domain)` | `robots.txt` fetches of the domain, read them with `warc_record()` |
| `tld_domains(tld, crawl)` | registered domains of a TLD in one crawl with page and host counts |
| `url_captures(url)` | every capture of one url in every crawl, the scheme and `www.` don't matter |
| `cdx_captures(url[, prefix := true])` | same from the CDX index, much faster for the old crawls, see [below](#history-of-one-url-with-the-cdx-index). Lambda only |
| `url_versions(url)` | the distinct contents (`content_digest`) of a page with when each was first and last seen. Lambda only |
| `url_capture_at(url, time)` | the successful capture of a page closest to a time within a year, like the Wayback Machine. Lambda only |
| `crawls()` | crawl ids with their names and first and last fetch time |

#### Crawl dates

`crawls()` has the name and the first and last fetch time of every crawl from [collinfo.json](https://index.commoncrawl.org/collinfo.json) (a copy is in [collinfo.json](collinfo.json), `columnar.py` downloads it when it's missing and fails if it doesn't have a crawl of the catalog). Use it to find the crawls of a time range:

```sql
FROM crawls() WHERE last_fetch >= '2019-01-01' AND first_fetch <= '2019-12-31';
```

`fetch_time` filters on `archives` skip files with DuckLake's file statistics for crawls since `CC-MAIN-2017-43`. The older files store `fetch_time` as `INT96` without statistics, so for them add a `crawl` filter. The parquet statistics aren't used for the crawl dates because of this, and because a few records have a `fetch_time` years before their crawl (for example 2002 in `CC-MAIN-2016-36`).

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

#### History of one url with the CDX index

Crawls before 2018 were written with an older `parquet-mr` which has no [page index](#parquet-page-index), so a lookup of one url decompresses the whole `url_surtkey` column chunk of every matching file. `url_captures('https://www.helsinki.fi/')` matches 328 files with 109GB of parquet over all 126 crawls and doesn't finish within the CloudFront timeout, while the 2024 and newer crawls alone take 2.7s.

`cdx_captures()` reads Common Crawl's [CDX index](https://index.commoncrawl.org) instead. It's a [ZipNum](https://pywb.readthedocs.io/en/latest/manual/indexing.html#zipnum-sharded-index) index: `cluster.idx` (~100MB per crawl) has the first SURT key of every block of 3000 captures. The lambda binary searches it with 8KB range requests, reads the ~250KB blocks which can contain the url and does that for every crawl in parallel. It returns the same columns as `archives` where possible.

```sql
-- 306 captures of the front page in 126 crawls in ~8s
FROM commoncrawl($$ FROM cdx_captures('https://www.helsinki.fi/') $$);

-- Every url under a path in every crawl, here 1.36M captures in ~8s
FROM commoncrawl($$
    SELECT crawl, count(*) AS captures, count(DISTINCT url) AS urls
    FROM cdx_captures('yle.fi/uutiset/', prefix := true) GROUP BY crawl ORDER BY crawl
$$);

-- Filters on fetch_time and crawl choose which crawls are read, here 15 crawls in ~5s
FROM commoncrawl($$
    FROM cdx_captures('https://www.helsinki.fi/') WHERE fetch_time BETWEEN '2019-01-01' AND '2020-12-31'
$$);

-- When the front page changed
FROM commoncrawl($$ FROM url_versions('https://www.helsinki.fi/') $$);

-- The page as it was around a date: closest capture and its HTML
FROM commoncrawl($$
    SELECT fetch_time, url, warc_record(warc_filename, warc_record_offset, warc_record_length).body AS html
    FROM url_capture_at('https://yle.fi/', '2014-01-01')
$$);
```

- The crawls are chosen with the dates of `crawls()`. The lambda passes them to the extension at startup (`cdx_crawls` setting), and the `cdx_scan()` table function under `cdx_captures()` compares them with `fetch_time` comparisons, `BETWEEN`, `crawl = ...` and `crawl IN (...)`. The filters are still applied to every row.
- A crawl which couldn't be read is one row with only `crawl` and `error`, so one failing crawl doesn't fail the whole query.
- A prefix can match at most 64 blocks (192 000 captures) per crawl, use `domain_pages()` for whole domains.


### Extract data from the crawled pages

Every index row points to the crawled page in a WARC file: `warc_filename`, `warc_record_offset` and `warc_record_length`. The lambda has an extension ([lambda/duckdb/extract](lambda/duckdb/extract)) which fetches those records from `s3://commoncrawl` and extracts data from the HTML, so one query finds the pages, fetches them and returns only the extracted fields.

| Function | Returns |
|---|---|
| `warc_record(warc_filename, warc_record_offset, warc_record_length[, content_charset])` | `STRUCT(status, headers MAP, body, charset, warc MAP, error, truncated)`: HTTP status, response headers, body as UTF-8, the charset it was decoded from and WARC headers. `truncated` is true when `warc_fetch()` [stopped reading](#streaming) the record early. Errors of single records, like a failed fetch or a record over 16MB, are in `error` instead of failing the query |
| `warc_fetch((SELECT ... FROM archives ...))` | the input rows with a `record` column like `warc_record()`. Fetches ahead in a window and emits the rows in chunks of ~4MB of compressed records, so pages of 1MB don't fill the memory with vectors of 2048 pages. Use it for many or large pages |
| `html_select(html, css)` | every match of a CSS selector as `VARCHAR[]` |
| `html_select_first(html, css)` | the first match |
| `html_extract(html, {name: css, ...})` | `STRUCT(name, ...)` of the first matches, the HTML is parsed only once |
| `html_text(html)` | readable text without scripts, styles and markup |
| `js_object(html, 'window.__appData')` | JSON object or array assigned to a JavaScript variable, for single page apps |
| `json_ld(html)` | every [schema.org](https://schema.org) JSON-LD object of the page as `JSON[]` |
| `json_ld_of_type(html, ['JobPosting'])` | JSON-LD objects of the given types |
| `microdata(html, 'baseSalary')` | value of a schema.org microdata property |
| `meta(html, 'og:title')` | content of a `<meta name>` or `<meta property>` |
| `html_links(html, page_url)` | absolute `http(s)` urls of the `<a href>` links of the page |
| `tracking_ids(html)` | Google Analytics, Tag Manager, Ads and AdSense ids. Domains with the same ids usually have the same owner |
| `emails(text)` | email addresses, for example `emails(html_text(html))` |
| `surt_key(url)` | the `url_surtkey` Common Crawl gives the url: `https://www.yle.fi/a/?s=1&b=2` -> `fi,yle)/a?b=2&s=1` |
| `url_resolve(base, href)` / `url_host(url)` | link resolved like browsers do ([WHATWG URL](https://url.spec.whatwg.org)) / host of an url |
| `product_record(html, page_url)` | `STRUCT(name, brand, sku, price, currency, listing, gtin, gtins, confidence)` of a product page, see [product data](#product-data-and-gtins) |
| `product_gtins(html)` | every GTIN (EAN, UPC) of the page as 14 digits, valid check digit, no coupons or placeholders |
| `html_gtin_hits(html)` | `LIST(STRUCT(gtin, digits, key, kind, offset))`: every number next to gtin, ean, barcode or upc with where and how it was found (`json`, `attr`, `label`, `text`) |
| `html_gtin_near(html, id)` | the GTIN closest to an occurrence of `id`, like the product id of the url |
| `gtin_info(code)` | `STRUCT(gtin14, kind, prefix, flags)`, flags like `restricted` (in-store codes), `isbn`, `issn`, `coupon`, `invalid` |
| `gtin_valid(code)` | whether the GS1 check digit is right |
| `html_json_ld_raw(html)` | contents of the `<script type="application/ld+json">` elements without parsing the HTML, used by `json_ld()` |
| `in_part(value)` / `surtkey_in_part(url_surtkey)` | which rows or files belong to this part of a [split query](#parallel-queries) |
| `warc_fetch_stats()` | fetches, retries, errors, slow fetches, hedged requests and the slowest fetch of this lambda |
| `cdx_lookup(crawl, url_surtkey, prefix)` | `STRUCT(lines VARCHAR[], error)`: raw CDX lines of one crawl |
| `cdx_scan(url_surtkey, prefix)` | table of `crawl, url_surtkey, fetch_time, cdx, error` over the crawls, used by `cdx_captures()` |
| `memory_governor()` | current state of the [memory governor](#memory) |

CSS selectors return the text of the matching elements by default. Add `::raw` for the text as it is, `::html` for the inner HTML, `::outer_html` or `::attr(name)` for an attribute, like in [Scrapy](https://docs.scrapy.org/en/latest/topics/selectors.html): `html_select(body, 'a.job::attr(href)')`.

Job postings with salaries from [Ashby](https://www.ashbyhq.com) which has schema.org `JobPosting` JSON-LD on every job page:

```sql
FROM commoncrawl($$
    WITH pages AS (
        SELECT url, warc_record(warc_filename, warc_record_offset, warc_record_length) AS record
        FROM archives
        WHERE crawl = 'CC-MAIN-2026-43' AND subset = 'warc' AND fetch_status = 200
          AND surtkey_in_host(url_surtkey, 'jobs.ashbyhq.com')
          AND regexp_matches(url_path, '^/[^/]+/[0-9a-f-]{36}$')
    ), jobs AS (
        SELECT url, unnest(json_ld_of_type(record.body, ['JobPosting'])) AS job FROM pages
    )
    SELECT url,
        job->>'title' AS title,
        job->'hiringOrganization'->>'name' AS company,
        job->'baseSalary'->>'currency' AS currency,
        (job->'baseSalary'->'value'->>'minValue')::DOUBLE AS salary_min,
        (job->'baseSalary'->'value'->>'maxValue')::DOUBLE AS salary_max
    FROM jobs
$$);
```

This fetches and parses all ~4000 Ashby job pages of the crawl in the lambda and returns 2393 postings from 646 companies, 1057 of them with a salary, in ~7 seconds.

[Greenhouse](https://www.greenhouse.com) job pages keep the data in JavaScript instead:

```sql
SELECT json_extract(js_object(record.body, 'window.__remixContext'), '$.state.loaderData.*.jobPost')->0 AS post
-- post->>'title', post->'pay_ranges'->0->>'min', post->'pay_ranges'->0->>'max'
```

The legal entity behind a domain from its privacy policy and contact pages:

```sql
WITH pages AS (
    SELECT url_host_registered_domain AS domain, warc_record(warc_filename, warc_record_offset, warc_record_length) AS record
    FROM archives
    WHERE crawl = 'CC-MAIN-2026-43' AND subset = 'warc' AND fetch_status = 200
      AND url_host_tld = 'fi' AND url_host_registered_domain IN ('kesko.fi', 'misa.fi')
      AND (url_path ILIKE '%tietosuoja%' OR url_path ILIKE '%privacy%' OR url_path ILIKE '%yhteystiedot%')
)
SELECT domain,
    list_distinct(flatten(list(regexp_extract_all(html_text(record.body), '\b(\d{7}-\d)\b', 1)))) AS business_ids,
    list_distinct(flatten(list(list_transform(json_ld_of_type(record.body, ['Organization']), lambda o: o->>'name')))) AS organizations
FROM pages GROUP BY ALL;
```

Tips:
- Filter the index as much as possible before `warc_record()`: every row is one S3 request. `LIMIT` in the outer query doesn't stop the fetching because records are fetched a vector (up to 2048 rows) at a time, put it into the subquery which selects the rows instead.
- Use parentheses with JSON operators in conditions, `(job->'baseSalary') IS NOT NULL`, because `job->'baseSalary' IS NOT NULL` is parsed as `job->('baseSalary' IS NOT NULL)`.
- `warc_record()` and `warc_fetch()` read only the byte range of the record without the HEAD request DuckDB normally sends before reading a file. One thread runs all requests of the lambda without blocking with libcurl's multi interface (`warc_concurrency` in `infra/main.tf` requests at a time, at most 768 because lambda allows ~1024 threads and files), signed with SigV4 using DuckDB's S3 secret. DuckDB 2.0's async I/O can't be used: table in-out functions like `warc_fetch()` can't wait for it, and its pool still blocks a thread per request. A few percent of the S3 requests take seconds, so a request which hasn't answered in 1s gets a second identical request and the first answer wins (`warc_hedge_ms`). This cut the slowest fetches from up to 25s to ~2s.
- `body` is converted to UTF-8 with [lexbor](https://lexbor.com)'s implementation of the [WHATWG Encoding Standard](https://encoding.spec.whatwg.org), the same encodings and labels browsers support. The encoding is chosen like browsers do: byte order mark, `Content-Type` charset, `<meta charset>` in the first 1024 bytes, then the optional fourth argument of `warc_record()` (pass `content_charset` of the index, which Common Crawl detected from the content) and UTF-8. `record.charset` tells which one was used. In `CC-MAIN-2026-43` 94% of the pages are UTF-8, 2.5% ISO-8859-1 (decoded as windows-1252 like browsers do), 0.4% windows-1251 and the rest Shift_JIS, GBK, EUC-KR, Big5 and so on, which were all checked to decode without replacement characters. gzip and deflate `Content-Encoding` is decoded.
- <a id="streaming"></a>`warc_fetch()` stops downloading a record once it has what the query needs. When the body is only used for first matches, like `meta()`, `html_select_first()` or `html_extract()`, an optimizer rule passes their selectors to `warc_fetch()`, which inflates the record as it arrives, parses it incrementally with lexbor and cancels the request once every selector has a match whose element is closed. Anything else using the body, like `html_select()`, `json_ld()`, `length(record.body)` or returning the record, reads it completely. The extracted values are the same, `record.body` is then only the beginning of the page and `record.truncated` is true. `meta(record.body, 'og:title')` of 300 verkkokauppa.com pages (~1MB each) took 4-6s instead of 7s. The first JSON-LD script, `html_select_first(record.body, 'script[type="application/ld+json" i]::raw')`, is at 2-11% of the page on power.fi, gigantti.fi and prisma.fi, but at the end on verkkokauppa.com. Selectors which look at later elements, like `:last-child` or `:has()`, aren't streamed. `SET warc_fetch_stream = false` disables it.
- Sample pages with DuckDB's own syntax, `FROM archives TABLESAMPLE 10% (bernoulli) WHERE ...`: a patch pushes the filters below bernoulli and percentage system samples so that the index is still pruned by them. `system` samples whole vectors of 2048 rows, so it returns nothing for small sets of rows, and `reservoir` (`TABLESAMPLE 100 ROWS`) samples before the filters, so use it on a filtered subquery: `FROM (FROM archives WHERE ...) USING SAMPLE 100 ROWS`.
- Common Crawl stores only the beginning of large pages (1MiB for most crawls). Check `record.warc['WARC-Truncated']` or `content_truncated` of the index when the end of a page matters.
- The HTML of a row is parsed only once even if the query uses many `html_select()`, `html_select_first()`, `html_extract()`, `html_text()` or macros like `json_ld()` on it: an optimizer rule combines them into one `html_query()` call. Six selectors on 1000 pages took 115ms combined and 241ms separately. `SET html_parse_once = false` disables it.

### Product data and GTINs

`product_record(html, page_url)` returns the product of a product page with its GTIN (EAN/UPC) and a confidence:

- `high`: the GTINs inside the page's main schema.org `Product` JSON-LD: the Product whose `url` or `@id` is the page, else the only one, else the first with offers. GTINs of related products in other Product objects aren't mixed in.
- `medium`: exactly one GTIN on a page with Product JSON-LD or `og:type` product, or on a page with several GTINs the one closest to the product id of the url, like `1002730` in `/product/1002730/`. Shops keep a product's data next to its id, for example verkkokauppa.com's `"itemsByPid":{"1002730":{..."eans":["6411923686860"]}}`. On 1461 pages where the JSON-LD also had the GTIN, this picked the same one every time.
- `low`: other GTINs, for example on listing pages (`ItemList`, `CollectionPage` JSON-LD) or in related products.

The GTINs come from a scanner which finds 8, 12, 13 or 14 digit numbers after `gtin`, `gtin13`, `ean`, `eans`, `ean-koodi`, `barcode`, `viivakoodi`, `streckkod` or `upc`, separated only by punctuation, quotes, entities like `&quot;` or up to 3 tags, so it finds `"gtin13":"..."`, `\"gtin\":\"...\"` in Next.js data, `data-ls-gtin="..."`, `<meta property="product:ean" content="...">`, `"eans":["...","..."]` and `EAN:</strong><div class="value">...`. Letters, commas and `null` end the separator, so `"ean":null,"updatedAt":1791523489425` doesn't match. The check digit has to be valid, `gtin13` can't have 8 digits, and coupons and placeholders like `0000000000000` are dropped. It scans a 1MB page in ~1ms without parsing it. On 160 random `.fi` pages every match was a real GTIN.

The EANs of 11 Finnish web shops in `CC-MAIN-2026-43`, 58 000 pages in 64 lambdas in ~75s:

```sql
FROM commoncrawl($$
    SELECT url_host_name AS host, url, fetch_time, p.*
    FROM (
        SELECT url_host_name, url, fetch_time, product_record(record.body, url) AS p
        FROM warc_fetch((
            SELECT url_host_name, url, fetch_time, warc_filename, warc_record_offset, warc_record_length, content_charset
            FROM archives
            WHERE crawl = {crawl} AND subset = 'warc' AND fetch_status = 200
              AND (surtkey_in_host(url_surtkey, 'www.verkkokauppa.com') OR surtkey_in_host(url_surtkey, 'www.power.fi')
                   OR surtkey_in_host(url_surtkey, 'www.gigantti.fi') OR surtkey_in_host(url_surtkey, 'www.prisma.fi'))
              AND surtkey_in_part(url_surtkey)
        ))
    )
    WHERE len(p.gtins) > 0
$$, crawls := 'crawl = ''CC-MAIN-2026-43''',
    split := 'SELECT url_surtkey, warc_record_length FROM archives WHERE crawl = {crawl} AND subset = ''warc'' AND ...',
    weight := 'warc_record_length', parts := 80);
```

| Shop | Pages with GTINs | high | medium | Distinct GTINs |
|---|---:|---:|---:|---:|
| verkkokauppa.com | 15 409 | 7 312 | 788 | 8 530 |
| power.fi | 2 748 | 2 748 | 0 | 2 747 |
| prisma.fi | 2 662 | 2 662 | 0 | 2 654 |
| xxl.fi | 1 484 | 1 484 | 0 | 456 (sizes of the same product) |
| gigantti.fi | 1 452 | 1 451 | 1 | 1 443 |
| puuilo.fi | 1 005 | 0 | 1 005 | 1 004 |
| stockmann.com | 806 | 806 | 0 | 806 |
| jimms.fi | 68 | 0 | 68 | 68 |

clasohlson.com, motonet.fi and biltema.fi don't publish GTINs in their HTML. The GTIN of a URL stayed the same across crawls in every sampled case, while prices change, so the crawls are a price history. Restricted in-store codes (prefix 20-29) and ISBNs are kept, check `gtin_info(gtin).flags`.

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
- Cold start takes ~0.8s: the catalog is in the lambda package instead of being downloaded from S3, which took ~3s.
- Most of the large query time is the download from CloudFront to Finland, not the lambda.
- Lambda responses over 6MB are streamed at only 2MB/s. Before results were uploaded into S3 the large query took ~48s and ~140 GB-s.

### Parallel queries

One lambda has 2 vCPUs and the CloudFront timeout, but up to 1000 lambdas run at the same time. `commoncrawl()` splits a query over many lambdas and returns all of their results as one table. The lambda gets the same SQL with `&part=k&parts=n` and these macros choose what each part does:

- `in_part(value)`: true for 1/n of the values, for example `WHERE in_part(url)`. Every part reads the same index rows but fetches and parses only its own pages.
- `surtkey_in_part(url_surtkey)`: a `url_surtkey` range of this part. Without `split` the ranges are quantiles of the smallest `url_surtkey` of every file, so every part reads about 1/n of the files of every crawl.

```sql
-- 4 lambdas per crawl of 2026: {crawl} is replaced with the crawl of each request
FROM commoncrawl($$
    SELECT crawl, url, unnest(json_ld_of_type(record.body, ['JobPosting'])) AS job
    FROM warc_fetch((
        SELECT crawl, url, warc_filename, warc_record_offset, warc_record_length
        FROM archives
        WHERE crawl = {crawl} AND subset = 'warc' AND fetch_status = 200
          AND surtkey_in_host(url_surtkey, 'jobs.ashbyhq.com') AND in_part(url)
    ))
$$, crawls := 'first_fetch >= ''2026-01-01''', parts := 4);

-- 80 lambdas with an equal amount of work: split runs the given query for every crawl first, then divides
-- the url_surtkey range into parts of equal weight and gives every lambda its own range
FROM commoncrawl($$ ... WHERE crawl = {crawl} AND ... AND surtkey_in_part(url_surtkey) ... $$,
    crawls := 'first_fetch >= ''2026-01-01''',
    split := 'SELECT url_surtkey, warc_record_length FROM archives WHERE crawl = {crawl} AND ...',
    weight := 'warc_record_length', parts := 80);
```

Named parameters:

| Parameter | |
|---|---|
| `parts` | lambdas, or lambdas per crawl with `crawls` and no `split` |
| `crawls` | condition on `crawls()`, every matching crawl gets its own lambdas and `{crawl}` is replaced with its id. A literal `crawl = ...` filter skips the other crawls' files for sure |
| `split` | query returning `url_surtkey` (and the weight column) of the rows to process, the parts get ranges of equal weight. The ranges are in the url, so CloudFront can still cache the results |
| `weight` | expression for the weight of a row in `split`, `warc_record_length` balances fetching better than counting rows |
| `concurrency` | lambdas running at the same time, all of them by default |
| `max_gb_seconds` | fail before starting if the parts could use more |

Before starting more than one lambda the extension checks the [monthly usage](#monthly-usage-limit) and fails if every part running until the timeout could go over it.

Measured in October 2026 (3008MB lambdas, warm unless noted):

| Workload | Parts | Seconds | GB-s |
|---|---:|---:|---:|
| JSON-LD of 65 194 Ashby job pages in the 10 crawls of 2026 | 1 | over 120 (timeout) | |
| | 10 (one per crawl) | 49 | 876 |
| | 40, `split` | 21 | 1 885 |
| | 80, `split` | 20 | 3 148 |
| `product_record()` of 58 000 pages of 11 web shops, 1 crawl, ~500KB per page | 64, `split`, `weight` | 76 | ~4 800 |
| `.fi` pages and domains of all 126 crawls, index only | 126 (one per crawl) | 18 | 2 609 |
| 7.6M `.fi` urls of one crawl, 166MB of Arrow | 1 | 14 | 22 |
| | 4, `split` | 6 | 27 |
| | 16, `split` | 13 | 217 |

- More parts make extraction faster until every part is dominated by its own index lookup, cold start and its slowest fetches. 40-80 parts is usually the sweet spot, beyond that the GB-s grow without much gain.
- Large results download fastest in ~4 parts, more parts mostly add cold starts.
- A lambda handles ~500 small pages per second. Large pages are CPU bound: 1MB of HTML takes ~2ms to parse with lexbor on the laptop and 2-3 times that on the lambda's Graviton.
- Larger lambdas didn't help: 12 lambdas of 10GB (6 vCPUs) took 62s and ~4000 GB-s for the web shop workload, 40 lambdas of 3GB 38s and ~2000 GB-s.
- Scans of the old crawls are expensive: the `min(fetch_time)` of all 1.8 billion records of `CC-MAIN-2013-20` cost ~2700 GB-s because the old files are large and `INT96` decoding is CPU bound.

What made the lambdas faster for these workloads:

- [httpfs](lambda/duckdb/patches/extensions/httpfs) reuses HTTP connections across file handles. `warc_record()` opens a handle for every record and every record needed a new TLS connection, ~7ms of CPU each.
- [DuckDB](lambda/duckdb/patches/duckdb) waits instead of busy looping while all tasks run on other threads, like a worker which waits for fetches. The loop took about half of the CPU of a 2 vCPU lambda.
- [libdeflate](https://github.com/ebiggers/libdeflate) inflates the records and checks their CRC32 with the ARM CRC instructions, 6x less CPU than miniz.
- Hedged requests for slow S3 reads and `warc_fetch()`'s fetch window, see [above](#extract-data-from-the-crawled-pages).
- The memory governor samples `mallinfo2()`, which locks every malloc arena, only once a second.
- JSON-LD and GTINs are found by scanning the HTML text instead of parsing it, and `meta()` calls with selectors built by `||` are combined into one parse too.

### Monthly usage limit

The free tier includes 400 000 GB-s of lambda per month, ~37 hours of the 3008MB lambda. The lambda reads its billed duration of the calendar month from CloudWatch (at most once a minute) and answers `429` when it has used `gb_seconds_limit` (`infra/main.tf`, default 400 000, 0 disables it). CloudWatch is a minute or two behind and running queries finish, so many parallel parts can go a little over.

```bash
curl -H "Cookie: $CC_COOKIE" "${CC_URL}usage"
# {"month_start":"2026-10-01T00:00:00Z","invocations":315,"used_gb_seconds":18610.7,"limit_gb_seconds":400000,
#  "remaining_gb_seconds":381389.3,"memory_mb":3008,"remaining_seconds_at_this_memory":129835}
```

```sql
FROM read_json(getenv('CC_URL') || 'usage');
```

`aws freetier get-free-tier-usage` shows the same for the whole account but is updated only about once a day.

### Memory

DuckDB's `memory_limit` only covers its buffer pool, but fetched WARC records, parsed HTML and HTTP buffers are allocated outside of it. A fixed limit would be either too low for queries which only scan parquet or too high for queries which also fetch pages, so the lambda sets `memory_target_mb` to its memory size minus 256MB for the Rust runtime and Arrow buffers instead. Every 20ms the extension sets

```
buffer pool limit = target - (memory in use - buffer pool usage) - headroom
```

where memory in use is the resident set (or the cgroup usage when it's larger), and the headroom follows how fast the untracked memory has recently grown (at least 64MB, at most a quarter of the target). Queries without `warc_record()` get almost all of the memory and fetch heavy queries get more room for bursts. `warc_record()` and `cdx_captures()` also reserve memory before fetching and wait while the process with the other reservations is close to the target, so 128 parallel fetches can't all pass the check at once, and freed heap memory is returned to the system with `malloc_trim` (checked once a second, `mallinfo2()` locks every malloc arena). The `warc_record()` calls of all threads may hold at most half of the target in fetched records at once, and `warc_fetch()` keeps at most 32MB of compressed records in flight per thread. A query which doesn't fit fails with an out of memory error instead of the lambda being killed. Check it with `SELECT memory_governor()`.

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

The lambda accepts exactly one `SELECT` statement (or `EXPLAIN` of one), checked with `json_serialize_sql()` before it runs. Without this, statements before the last one of a multi statement query would run, and statements like `CREATE SECRET` work even with a locked configuration, so a secret with a custom endpoint could send the lambda's credentials elsewhere. It opens the DuckLake read only and then disables all file system and network access except `s3://commoncrawl/` with `enable_external_access = false` and `lock_configuration = true`. The execution role can only read `s3://commoncrawl/cc-index/*` (the index), `s3://commoncrawl/crawl-data/*` (the WARC files) and the catalog, write the query results into `results/` of its own bucket and read its own `Duration` metric from CloudWatch for the [usage limit](#monthly-usage-limit).

## License

MIT
