# FrozeLake CommonCrawl

Convert Common Crawl columnar index to DuckLake format for efficient querying.

## Overview

This project provides tools to fetch Common Crawl's columnar index data and organize it into DuckLake tables. It handles schema changes across different crawl periods and creates a unified view for querying.

## Features

- **Idempotent execution** - safely rerun without reprocessing existing data
- **Graceful Ctrl+C handling** - interrupt anytime without corruption
- **Automatic retry logic** - robust HTTP retry configuration for reliability
- **Schema management** - handles different Common Crawl schema versions

## Usage

Run with devenv:

```bash
$ devenv shell ./columnar.py
```

Or manually with Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb
python3 columnar.py
```

## Output

Creates `commoncrawl.ducklake` database with:
- `CC_MAIN_2013_TO_2021` - older crawls (2013-2021)
- `CC_MAIN_2021_AND_FORWARD` - newer crawls (2021+)
- `archives` - unified view combining both tables

## License

MIT
