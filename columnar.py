#!/usr/bin/env python
"""
Convert Common Crawl columnar index to DuckLake format.

This script:
1. Fetches crawl metadata from Common Crawl
2. Downloads parquet file lists for each crawl
3. Creates DuckLake tables organized by schema changes
4. Iteratively adds data files to DuckLake

Requirements:
  pip install duckdb

Or run with virtual environment:
  python3 -m venv .venv
  source .venv/bin/activate
  pip install duckdb
  python3 columnar.py
"""

import duckdb
import signal
import sys
import os

def main():
    # Create DuckDB connection
    con = duckdb.connect()

    # Flag for graceful shutdown
    shutdown_requested = {'flag': False}

    # Set up signal handler for Ctrl+C
    def signal_handler(sig, frame):
        print("\n\nInterrupt received (Ctrl+C). Finishing current operation and exiting...")
        shutdown_requested['flag'] = True

    signal.signal(signal.SIGINT, signal_handler)

    # Install and load extensions
    print("Installing extensions...")
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    # Set HTTP retry parameters for reliability
    print("Configuring HTTP settings...")
    con.execute("SET http_retries = 1000")
    con.execute("SET http_retry_backoff = 6")
    con.execute("SET http_retry_wait_ms = 500")

    # Fetch crawl info (excluding oldest crawls without columnar index)
    # Use local file if it exists, otherwise fetch from URL
    collinfo_source = 'collinfo.json' if os.path.exists('collinfo.json') else 'https://index.commoncrawl.org/collinfo.json'
    print(f"Fetching crawl metadata from {collinfo_source}...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE crawl_info AS
        FROM read_json(?)
        WHERE id NOT IN (
            'CC-MAIN-2012',
            'CC-MAIN-2009-2010',
            'CC-MAIN-2008-2009'
        )
    """, [collinfo_source])

    # Get list of crawl IDs
    crawl_ids = con.execute("SELECT id FROM crawl_info ORDER BY id").fetchall()
    crawl_ids = [row[0] for row in crawl_ids]

    print(f"Found {len(crawl_ids)} crawls to process")

    # Generate parquet file URLs for all crawls
    print("Generating parquet file lists...")
    con.execute("SET VARIABLE crawl_ids = ?", [crawl_ids])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE crawl_parquet_files AS
        SELECT 'https://data.commoncrawl.org/' || column0 as url
        FROM read_csv(
            list_transform(
                getvariable('crawl_ids'),
                n -> format(
                    'https://data.commoncrawl.org/crawl-data/{}/cc-index-table.paths.gz',
                    n
                )
            ),
            header=false
        )
    """)

    # Attach DuckLake database
    print("Attaching DuckLake database...")
    con.execute("ATTACH 'ducklake:commoncrawl.ducklake' AS commoncrawl (DATA_PATH 'tmp_always_empty')")

    # Create tables with schemas from newest parquet files
    print("\n=== Creating table schemas ===")

    # Get newest parquet file for CC_MAIN_2021_AND_FORWARD
    newest_parquet_file = con.execute("""
        SELECT MAX(url)
        FROM crawl_parquet_files
    """).fetchone()[0]

    print(f"Creating CC_MAIN_2021_AND_FORWARD schema from: {newest_parquet_file}")
    con.execute("""
        CREATE TABLE IF NOT EXISTS commoncrawl.CC_MAIN_2021_AND_FORWARD AS
        FROM read_parquet(?)
        WITH NO DATA
    """, [newest_parquet_file])

    # Get newest parquet file with old timestamp schema
    newest_parquet_file_old = con.execute("""
        SELECT MAX(url)
        FROM crawl_parquet_files
        INNER JOIN crawl_info ON contains(url, '/crawl=' || crawl_info.id || '/')
        WHERE crawl_info.id = 'CC-MAIN-2021-43'
    """).fetchone()[0]

    print(f"Creating CC_MAIN_2013_TO_2021 schema from: {newest_parquet_file_old}")
    con.execute("""
        CREATE TABLE IF NOT EXISTS commoncrawl.CC_MAIN_2013_TO_2021 AS
        FROM read_parquet(?)
        WITH NO DATA
    """, [newest_parquet_file_old])

    # Ensure required columns exist (added in later crawls but missing in older schemas)
    print("Checking for required columns in CC_MAIN_2013_TO_2021...")
    existing_columns = con.execute("""
        SELECT column_name FROM (DESCRIBE FROM commoncrawl.CC_MAIN_2013_TO_2021)
    """).fetchall()
    existing_columns = {row[0].lower() for row in existing_columns}
    print(f"  Existing columns: {sorted(existing_columns)}")

    for col in ['content_languages', 'content_charset', 'fetch_redirect']:
        if col not in existing_columns:
            print(f"  Adding missing column: {col}")
            con.execute(f"ALTER TABLE commoncrawl.CC_MAIN_2013_TO_2021 ADD COLUMN {col} VARCHAR")

    # Create idempotent parquet files view (filters out already-added files)
    print("\n=== Creating idempotent file list ===")
    con.execute("""
        CREATE OR REPLACE TEMP VIEW idempotent_parquet_files AS (
            SELECT url FROM crawl_parquet_files
            EXCEPT
            SELECT data_file as url
            FROM ducklake_list_files('commoncrawl', 'CC_MAIN_2013_TO_2021')
            EXCEPT
            SELECT data_file as url
            FROM ducklake_list_files('commoncrawl', 'CC_MAIN_2021_AND_FORWARD')
        )
    """)

    # ========================================
    # Table 1: CC-MAIN-2013-20 to CC-MAIN-2021-43
    # (years 2013-2023 without timezone in fetch_time)
    # ========================================
    print("\n=== Processing CC-MAIN-2013-20 to CC-MAIN-2021-43 ===")

    # Get parquet files for this period (already filtered by idempotent view)
    parquet_files_2013_2021 = con.execute("""
        SELECT url
        FROM idempotent_parquet_files
        INNER JOIN crawl_info ON contains(url, '/crawl=' || crawl_info.id || '/')
        WHERE id BETWEEN 'CC-MAIN-2013-20' AND 'CC-MAIN-2021-43'
        ORDER BY url
    """).fetchall()

    parquet_files_2013_2021 = [row[0] for row in parquet_files_2013_2021]
    print(f"Found {len(parquet_files_2013_2021)} new parquet files to add for 2013-2021 period")

    # Add each file to DuckLake
    if parquet_files_2013_2021:
        print(f"Adding {len(parquet_files_2013_2021)} files to CC_MAIN_2013_TO_2021...")
        for i, file_url in enumerate(parquet_files_2013_2021, 1):
            if shutdown_requested['flag']:
                print(f"  Stopped at file {i}/{len(parquet_files_2013_2021)}")
                break

            if i % 10 == 0 or i == 1:
                print(f"  Adding file {i}/{len(parquet_files_2013_2021)}: {file_url}")
            try:
                con.execute("""
                    CALL ducklake_add_data_files(
                        'commoncrawl',
                        'CC_MAIN_2013_TO_2021',
                        ?,
                        allow_missing => true
                    )
                """, [file_url])
            except Exception as e:
                print(f"  WARNING: Failed to add {file_url}: {e}")

    if shutdown_requested['flag']:
        print("\nShutdown requested. Closing connection and exiting...")
        con.close()
        sys.exit(0)

    # ========================================
    # Table 2: CC-MAIN-2021-49 onwards
    # (fetch_time has timezone info)
    # ========================================
    print("\n=== Processing CC-MAIN-2021-49 onwards ===")

    # Get parquet files for this period (already filtered by idempotent view)
    parquet_files_2021_forward = con.execute("""
        SELECT url
        FROM idempotent_parquet_files
        INNER JOIN crawl_info ON contains(url, '/crawl=' || crawl_info.id || '/')
        WHERE id >= 'CC-MAIN-2021-49'
        ORDER BY url
    """).fetchall()

    parquet_files_2021_forward = [row[0] for row in parquet_files_2021_forward]
    print(f"Found {len(parquet_files_2021_forward)} new parquet files to add for 2021-forward period")

    # Add each file to DuckLake
    if parquet_files_2021_forward:
        print(f"Adding {len(parquet_files_2021_forward)} files to CC_MAIN_2021_AND_FORWARD...")
        for i, file_url in enumerate(parquet_files_2021_forward, 1):
            if shutdown_requested['flag']:
                print(f"  Stopped at file {i}/{len(parquet_files_2021_forward)}")
                break

            if i % 10 == 0 or i == 1:
                print(f"  Adding file {i}/{len(parquet_files_2021_forward)}: {file_url}")
            try:
                con.execute("""
                    CALL ducklake_add_data_files(
                        'commoncrawl',
                        'CC_MAIN_2021_AND_FORWARD',
                        ?
                    )
                """, [file_url])
            except Exception as e:
                print(f"  WARNING: Failed to add {file_url}: {e}")

    if shutdown_requested['flag']:
        print("\nShutdown requested. Closing connection and exiting...")
        con.close()
        sys.exit(0)

    # ========================================
    # Create unified view
    # ========================================
    print("\n=== Creating unified view ===")
    con.execute("""
        CREATE OR REPLACE VIEW commoncrawl.archives AS
        SELECT *
        FROM commoncrawl.CC_MAIN_2021_AND_FORWARD
        UNION ALL BY NAME
        -- Add UTC timezone to fetch_time for older data
        SELECT * REPLACE (fetch_time::TIMESTAMPTZ AS fetch_time)
        FROM commoncrawl.CC_MAIN_2013_TO_2021
    """)

    print("\n✓ DuckLake database created successfully!")
    print("  Database: commoncrawl.ducklake")
    print("  View: commoncrawl.archives")

    con.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user (Ctrl+C). Exiting gracefully...")
        exit(0)
