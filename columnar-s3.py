#!/usr/bin/env python
"""
Convert Common Crawl columnar index to DuckLake format using S3 access.

This script uses S3 URLs (s3://commoncrawl/) instead of HTTPS for direct access
to Common Crawl data. Requires AWS credentials.

Requirements:
  pip install duckdb

Environment variables required:
  AWS_ACCESS_KEY_ID - AWS access key
  AWS_SECRET_ACCESS_KEY - AWS secret key

Usage:
  export AWS_ACCESS_KEY_ID=your_key
  export AWS_SECRET_ACCESS_KEY=your_secret
  devenv shell ./columnar-s3.py
"""

import duckdb
import signal
import sys
import os

def main():
    # Check for required AWS credentials
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    if not aws_access_key or not aws_secret_key:
        print("ERROR: AWS credentials not found!")
        print("Required environment variables:")
        print("  - AWS_ACCESS_KEY_ID")
        print("  - AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

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

    # Configure S3 credentials
    print("Configuring S3 access...")
    con.execute(f"""
        CREATE SECRET IF NOT EXISTS commoncrawl_s3 (
            TYPE S3,
            KEY_ID '{aws_access_key}',
            SECRET '{aws_secret_key}',
            REGION 'us-east-1'
        )
    """)

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

    # Generate parquet file URLs for all crawls (using S3 URLs)
    print("Generating parquet file lists...")
    con.execute("SET VARIABLE crawl_ids = ?", [crawl_ids])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE crawl_parquet_files AS
        SELECT 's3://commoncrawl/' || column0 as url
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

    # Set HTTP retry parameters for reliability
    print("Configuring HTTP settings...")
    con.execute("SET http_retries = 1000")
    con.execute("SET http_retry_backoff = 6")
    con.execute("SET http_retry_wait_ms = 500")

    # Attach DuckLake database
    print("Attaching DuckLake database...")
    con.execute("ATTACH 'ducklake:commoncrawl_s3.ducklake' AS commoncrawl (DATA_PATH 'tmp_always_empty')")

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

    if 'content_charset' not in existing_columns:
        print("  Adding missing column: content_charset")
        con.execute("ALTER TABLE commoncrawl.CC_MAIN_2013_TO_2021 ADD COLUMN content_charset VARCHAR")

    if 'fetch_redirect' not in existing_columns:
        print("  Adding missing column: fetch_redirect")
        con.execute("ALTER TABLE commoncrawl.CC_MAIN_2013_TO_2021 ADD COLUMN fetch_redirect VARCHAR")

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
    print("  Database: commoncrawl_s3.ducklake")
    print("  View: commoncrawl.archives")

    con.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user (Ctrl+C). Exiting gracefully...")
        exit(0)
