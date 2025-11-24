# FrozeLake CommonCrawl

Convert Common Crawl columnar index to a read only frozen DuckLake format for efficient querying.

It referenced external files, download their parquet metadata and then store this parquet metadata into efficient ~150Mb blob to have faster access to all data in common crawl.

It takes ~20 hours to run this (outside of AWS with 1Gbps connection) because [ducklake_add_data_files()](https://ducklake.select/docs/stable/duckdb/metadata/adding_files) calls are slow and can't be run in parallel:

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
