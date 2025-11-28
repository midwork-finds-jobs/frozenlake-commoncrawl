#!/usr/bin/env bash
#
# Convert Common Crawl columnar index to DuckLake format.
# Processes crawls from oldest (CC-MAIN-2013-20) to newest (CC-MAIN-2025-47).
#

set -euo pipefail

# Graceful shutdown on Ctrl+C
STOP_SIGNAL=false
trap 'echo ""; echo "Stopping gracefully..."; STOP_SIGNAL=true' INT

HTTP_BASE="https://data.commoncrawl.org"
DB_PATH="ducklake:commoncrawl.ducklake"
TABLE="warc"
# Override DUCKDB env var for custom builds (e.g. unreleased features like TIMESTAMPTZ migration)
# Example: DUCKDB="../ducklake/build/release/duckdb -unsigned --init /dev/null" ./columnar.sh
DUCKDB="${DUCKDB:-duckdb}"

# Track which crawl we're currently processing (for first-file migrations)
CURRENT_CRAWL=""

# Run migrations if this is the first file for a crawl
run_migration_if_needed() {
    local crawl_id="$1"

    # Skip if already processed this crawl
    [[ "$crawl_id" == "$CURRENT_CRAWL" ]] && return 0
    CURRENT_CRAWL="$crawl_id"

    case "$crawl_id" in
        "CC-MAIN-2018-39")
            echo "  [Migration] Adding content_charset and content_languages columns..."
            $DUCKDB -c "
                ATTACH '${DB_PATH}' AS cc;
                ALTER TABLE cc.${TABLE} ADD COLUMN content_charset VARCHAR;
                ALTER TABLE cc.${TABLE} ADD COLUMN content_languages VARCHAR;
            " || { echo "  [Migration] FAILED for ${crawl_id}"; exit 1; }
            ;;
        "CC-MAIN-2019-47")
            echo "  [Migration] Adding fetch_redirect and content_truncated columns..."
            $DUCKDB -c "
                ATTACH '${DB_PATH}' AS cc;
                ALTER TABLE cc.${TABLE} ADD COLUMN fetch_redirect VARCHAR;
                ALTER TABLE cc.${TABLE} ADD COLUMN content_truncated VARCHAR;
            " || { echo "  [Migration] FAILED for ${crawl_id}"; exit 1; }
            ;;
        "CC-MAIN-2021-49")
            echo "  [Migration] Altering fetch_time to TIMESTAMPTZ and adding url_host_name_reversed..."
            $DUCKDB -c "
                -- Keep the time in UTC for consistency
                SET TimeZone = 'UTC';

                ATTACH '${DB_PATH}' AS cc;
                ALTER TABLE cc.${TABLE} ALTER COLUMN fetch_time TYPE TIMESTAMPTZ;
                ALTER TABLE cc.${TABLE} ADD COLUMN url_host_name_reversed VARCHAR;
            " || { echo "  [Migration] FAILED for ${crawl_id}"; exit 1; }
            ;;
    esac
}

# Use local collinfo.json if exists
if [[ -f "./collinfo.json" ]]; then
    COLLINFO="./collinfo.json"
else
    COLLINFO="https://index.commoncrawl.org/collinfo.json"
fi

echo "Initializing DuckDB and generating file lists..."

# Generate all crawl files list
$DUCKDB paths.duckdb -c "
    CREATE TABLE IF NOT EXISTS crawl_info AS
        FROM read_json('${COLLINFO}')
        WHERE id NOT IN (
            'CC-MAIN-2012',
            'CC-MAIN-2009-2010',
            'CC-MAIN-2008-2009'
        );

    SET VARIABLE crawl_ids = (SELECT ARRAY_AGG(id) FROM crawl_info);

    SELECT 'Reading datafiles from common crawl csvs...';
    CREATE TABLE IF NOT EXISTS all_data_files AS
    SELECT
        crawl_info.id as crawl_id,
        '/' || csv.column0 as data_file
    FROM read_csv(
        list_transform(
            getvariable('crawl_ids'),
            n -> format('${HTTP_BASE}/crawl-data/{}/cc-index-table.paths.gz', n)
        ),
        header=false
    ) as csv
    INNER JOIN crawl_info ON contains(column0, '/crawl=' || crawl_info.id || '/')
    ORDER BY crawl_id, data_file;

    SELECT COUNT(*) FROM all_data_files;

    SELECT 'Setting up DuckDB database...';

    SET VARIABLE oldest_sample = ( SELECT MIN(data_file) FROM all_data_files );

    SELECT 'Starting from:' || getvariable('oldest_sample');

    ATTACH '${DB_PATH}' AS commoncrawl (DATA_PATH 'tmp_always_empty');

    CREATE TABLE IF NOT EXISTS commoncrawl.${TABLE} AS
    FROM read_parquet('${HTTP_BASE}' || getvariable('oldest_sample')) WITH NO DATA;

    ALTER TABLE commoncrawl.${TABLE} SET PARTITIONED BY (crawl, subset);

    CREATE VIEW IF NOT EXISTS files_to_process AS (
        SELECT data_file
        FROM all_data_files
        EXCEPT
        SELECT REPLACE(data_file,'${HTTP_BASE}','') as data_file
        FROM ducklake_list_files('commoncrawl', '${TABLE}')
    )
"

# Get unique crawl IDs in order
CRAWLS=$($DUCKDB paths.duckdb -csv -noheader -c "SELECT DISTINCT id FROM crawl_info ORDER BY id")

for CRAWL_ID in $CRAWLS; do
    [[ "$STOP_SIGNAL" == "true" ]] && break

    echo ""
    echo "[${CRAWL_ID}] Checking files..."

    # Get pending files for this crawl
    CRAWL_FILES=$($DUCKDB paths.duckdb -csv -noheader -c "ATTACH '${DB_PATH}' AS commoncrawl (DATA_PATH 'tmp_always_empty'); FROM files_to_process WHERE contains(data_file,'crawl=${CRAWL_ID}') ORDER BY data_file ASC;")
    
    PENDING_COUNT=$(echo "$CRAWL_FILES" | wc -l)
    echo "found files: $PENDING_COUNT"

    if [[ "$CRAWL_FILES" == "" ]]; then
        echo "[${CRAWL_ID}] processed completely!"
        continue
    fi

    # Run migrations if this is first file for this crawl
    run_migration_if_needed "$CRAWL_ID"

    for CRAWL_FILE in $CRAWL_FILES; do
        [[ "$STOP_SIGNAL" == "true" ]] && break

        echo "  Adding data files from ${CRAWL_FILE}..."

        # Add data files to DuckLake with retry on 403
        while true; do
            [[ "$STOP_SIGNAL" == "true" ]] && break

            ERROR_OUTPUT=$($DUCKDB -c "
                SET http_retries = 1000;
                SET http_retry_backoff = 3;
                SET http_retry_wait_ms = 500;

                ATTACH '${DB_PATH}' AS cc;
                CALL ducklake_add_data_files('cc', '${TABLE}', '${HTTP_BASE}${CRAWL_FILE}');
            " 2>&1) && break  # Success, exit loop

            # Check if error contains 403
            if [[ "$ERROR_OUTPUT" == *"403"* ]]; then
                echo "  [403 Forbidden] Retrying ${CRAWL_FILE} in 255s..."
                sleep 255
            else
                echo "  [Error] ${CRAWL_FILE}: ${ERROR_OUTPUT}"
                break
            fi
        done
    done

    echo "  Completed ${CRAWL_ID}"
done

echo ""
echo "Done."
