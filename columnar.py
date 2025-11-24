#!/usr/bin/env python
"""
Convert Common Crawl columnar index to DuckLake format.
"""

import duckdb
import os
import time
import signal

# Configuration
DB_PATH = "ducklake:commoncrawl.ducklake"
# Crawl ID where the schema changed (timestamp format)
SCHEMA_CHANGE_ID = "CC-MAIN-2021-49"
TABLE_OLD = "CC_MAIN_2013_TO_2021"
TABLE_NEW = "CC_MAIN_2021_AND_FORWARD"

CC_BASE = "https://data.commoncrawl.org"
S3_BUCKET_BASE = "s3://commoncrawl"

# 1. Environment Verification
KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

if KEY_ID and SECRET_KEY:
    print("AWS credentials found. Using S3 access for Common Crawl data.")
    CC_BASE = S3_BUCKET_BASE
    DB_PATH = "ducklake:commoncrawl_s3.ducklake"
else:
    print("No AWS credentials found. Using rate limited HTTP access for Common Crawl data.")

stop_signal = False

def handle_sigint(signum, frame):
    global stop_signal
    print("\nStopping gracefully...")
    stop_signal = True

def safe_add_file(con, table, url):
    """Adds file to DuckLake with retry logic for 403s."""
    while not stop_signal:
        try:
            con.execute(f"CALL ducklake_add_data_files('commoncrawl', '{table}', ['{CC_BASE}{url}'], allow_missing=>true)")
            return True
        except Exception as e:
            if '403' in str(e):
                print(f"  [403 Forbidden] Retrying {url} in 255s...")
                time.sleep(255)
            else:
                print(f"  [Error] Failed {url}: {e}")
                return False
    return False

def main():
    signal.signal(signal.SIGINT, handle_sigint)
    
    print("Initializing DuckDB and extensions...")
    con = duckdb.connect()
    for ext in ['ducklake', 'httpfs', 'netquack']:
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    
    # Optimization settings
    con.execute("SET http_retries = 1000; SET http_retry_backoff = 6;")
    con.execute(f"ATTACH '{DB_PATH}' AS commoncrawl (DATA_PATH 'tmp_always_empty')")

    # 1. Fetch Metadata & Generate File List
    print("Fetching metadata and generating file lists...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE all_files AS
        WITH crawls AS (
            SELECT id FROM read_json('https://index.commoncrawl.org/collinfo.json')
            WHERE id NOT IN ('CC-MAIN-2012', 'CC-MAIN-2009-2010', 'CC-MAIN-2008-2009')
        ),
        paths AS (
            SELECT id, 
                   format('?/crawl-data/{}/cc-index-table.paths.gz', id) as path_url
            FROM crawls
        )
        SELECT paths.id as crawl_id, column0 as file_path
        FROM paths, read_csv(paths.path_url, header=false)
    """, [CC_BASE])

    # 2. Ensure Tables Exist (Schema Initialization)
    print("Ensuring table schemas...")
    
    # Get sample files to initialize schemas if tables don't exist
    sample_new = con.sql(f"SELECT max(file_path) FROM all_files WHERE crawl_id >= '{SCHEMA_CHANGE_ID}'").fetchone()[0]
    sample_old = con.sql(f"SELECT max(file_path) FROM all_files WHERE crawl_id < '{SCHEMA_CHANGE_ID}'").fetchone()[0]

    con.execute(f"CREATE TABLE IF NOT EXISTS commoncrawl.{TABLE_NEW} AS FROM read_parquet('{CC_BASE}{sample_new}') LIMIT 0")
    con.execute(f"CREATE TABLE IF NOT EXISTS commoncrawl.{TABLE_OLD} AS FROM read_parquet('{CC_BASE}{sample_old}') LIMIT 0")

    # Patch older schema with missing columns if necessary
    for col in ['content_languages', 'content_charset', 'fetch_redirect']:
        try:
            con.execute(f"ALTER TABLE commoncrawl.{TABLE_OLD} ADD COLUMN {col} VARCHAR")
            print(f"  Added missing column: {col}")
        except duckdb.Error:
            pass  # Column likely exists

    # 3. Calculate Delta (Files to process)
    print("Calculating missing files...")
    files_to_process = con.execute(f"""
        SELECT crawl_id, file_path 
        FROM all_files
        WHERE extract_path(file_path) NOT IN (
            SELECT extract_path(data_file) FROM ducklake_list_files('commoncrawl', '{TABLE_OLD}')
            UNION ALL
            SELECT extract_path(data_file) FROM ducklake_list_files('commoncrawl', '{TABLE_NEW}')
        )
        ORDER BY crawl_id, file_path
    """).fetchall()

    total = len(files_to_process)
    print(f"Found {total} new files to process.")

    # 4. Processing Loop
    for i, (crawl_id, file_path) in enumerate(files_to_process, 1):
        if stop_signal:
            break
        
        target_table = TABLE_NEW if crawl_id >= SCHEMA_CHANGE_ID else TABLE_OLD
        
        if i % 10 == 0 or i == 1:
            print(f"[{i}/{total}] Adding to {target_table}: {file_path}")
            
        safe_add_file(con, target_table, file_path)

    # 5. Final View
    print("\nUpdating unified view...")
    con.execute(f"""
        CREATE OR REPLACE VIEW commoncrawl.archives AS
        SELECT * FROM commoncrawl.{TABLE_NEW}
        UNION ALL BY NAME
        SELECT * REPLACE (fetch_time::TIMESTAMPTZ AS fetch_time) FROM commoncrawl.{TABLE_OLD}
    """)
    
    con.close()
    print("Done.")

if __name__ == "__main__":
    main()