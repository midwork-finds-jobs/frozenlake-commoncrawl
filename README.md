# FrozeLake CommonCrawl

Convert Common Crawl columnar index to a read only frozen [DuckLake](https://ducklake.select/) for efficient querying.

As of 2025 November there are `88492` parquet files in the [Common Crawl columnar index](https://data.commoncrawl.org/cc-index/table/cc-main/index.html).

It would be neat if one would be able to query from all of them in efficient and ergonomic way.

This script uses ducklake and [ducklake_add_data_files()](https://ducklake.select/docs/stable/duckdb/metadata/adding_files) calls to convert all of the files into a fast ~150Mb reference table to have easy access to all data in common crawl.

It takes ~20 hours to run this (outside of AWS with 1Gbps connection) because ducklake_add_data_files() calls are slow and can't be run in parallel.

**NOTE: Before this is actually usable [duckdb/ducklake#579](https://github.com/duckdb/ducklake/issues/579) needs to be solved. Currently ducklake doesn't use hive partitioning to skip reading files. Because of this ducklake reads metadata from all files even when it would not need to.**

```sql
CALL ducklake_add_data_files(
    'commoncrawl',
    'CC_MAIN_2021_AND_FORWARD',
    'https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2014-41/subset=warc/part-00103-e1115d40-d2cc-4445-873c-2b206f427726.c000.gz.parquet'
);
```

Read this post to learn more:
https://ducklake.select/2025/10/24/frozen-ducklake/

## Overview

This project provides tools to fetch Common Crawl's columnar index data and organize it into DuckLake tables. It handles schema changes across different crawl periods and creates a unified view for querying.

## Features

- **Idempotent execution** - safely rerun without reprocessing existing data
- **Graceful Ctrl+C handling** - interrupt anytime without corruption
- **Automatic retry logic** - robust HTTP retry configuration for reliability
- **Schema management** - handles different Common Crawl schema versions by combining them into a unified view

## Usage

Run with devenv:

```bash
# Auto-detect access mode (checks AWS credentials)
devenv shell ./columnar.py

# Force S3 access (requires AWS credentials)
devenv shell ./columnar.py --access-mode s3

# Force HTTP access (rate limited, no credentials needed)
devenv shell ./columnar.py --access-mode http
```

Or manually with Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb
python3 columnar.py --access-mode http
```

### Access Modes

- `auto` (default) - Auto-detects based on AWS credentials in environment
- `s3` - Force S3 access (faster, requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)
- `http` - Force HTTP access (slower, rate limited, no credentials needed)

## Output

Creates `commoncrawl.ducklake` database with tables:
- `CC_MAIN_2013_TO_2021` - older crawls (2013-2021)
- `CC_MAIN_2021_AND_FORWARD` - newer crawls (2021+)
- `archives` - unified view combining both tables

## License

MIT
