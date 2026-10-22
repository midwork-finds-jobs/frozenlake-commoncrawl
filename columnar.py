#!/usr/bin/env python
"""
Convert Common Crawl columnar index to a frozen DuckLake.

All crawls are added into a single `archives` table. Crawls are processed from oldest
to newest and the table schema is evolved (new columns, wider types) whenever a crawl
introduces schema changes, e.g.:
  CC-MAIN-2018-39: content_charset, content_languages
  CC-MAIN-2019-47: fetch_redirect, content_truncated
  CC-MAIN-2021-49: url_host_name_reversed, fetch_time TIMESTAMP -> TIMESTAMPTZ
  CC-MAIN-2026-30: warc_record_id
"""

import argparse
import os
import signal
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import duckdb

TABLE = "archives"
HTTP_BASE = "https://data.commoncrawl.org"
S3_BASE = "s3://commoncrawl"
# Columnar index is not available for the oldest crawls
SKIPPED_CRAWLS = ("CC-MAIN-2012", "CC-MAIN-2009-2010", "CC-MAIN-2008-2009")
RATE_LIMIT_WAIT_SECONDS = 255
# Failed files of a crawl are retried before moving to the next crawl
RETRY_ROUNDS = 6
RETRY_WAIT_SECONDS = 60

stop_event = threading.Event()
# Cleared while workers are backing off from a 403 rate limit
not_rate_limited = threading.Event()
not_rate_limited.set()


def handle_sigint(signum, frame):
    print("\nStopping gracefully after running calls finish...")
    stop_event.set()
    not_rate_limited.set()


def wait_for_rate_limit(file_url):
    """Pause all workers once when Common Crawl starts responding with 403."""
    if not_rate_limited.is_set():
        not_rate_limited.clear()
        print(f"  [403 Forbidden] {file_url}, pausing all workers for {RATE_LIMIT_WAIT_SECONDS}s...")
        stop_event.wait(RATE_LIMIT_WAIT_SECONDS)
        not_rate_limited.set()
    else:
        not_rate_limited.wait()


def add_files(con, file_urls):
    """Adds parquet files into DuckLake in a single commit. Returns error message on failure."""
    cursor = con.cursor()
    try:
        while not stop_event.is_set():
            not_rate_limited.wait()
            try:
                cursor.execute(
                    f"CALL ducklake_add_data_files('commoncrawl', '{TABLE}', $files, allow_missing => true)",
                    {"files": file_urls},
                )
                return None
            except duckdb.Error as e:
                if "403" in str(e):
                    wait_for_rate_limit(file_urls[0])
                else:
                    return str(e)
        return "stopped"
    finally:
        cursor.close()


def table_columns(con):
    return dict(con.execute(f"SELECT column_name, column_type FROM (DESCRIBE commoncrawl.{TABLE})").fetchall())


def evolve_schema(con, crawl_id, sample_url):
    """Adds new columns and changes column types to match the given crawl."""
    existing = table_columns(con)
    file_columns = con.execute(
        "SELECT column_name, column_type FROM (DESCRIBE FROM read_parquet($file))",
        {"file": sample_url},
    ).fetchall()

    for name, column_type in file_columns:
        if name not in existing:
            print(f"  [{crawl_id}] Adding column {name} {column_type}")
            con.execute(f'ALTER TABLE commoncrawl.{TABLE} ADD COLUMN "{name}" {column_type}')
        elif existing[name] != column_type:
            # Files with the older type can't be added after this anymore.
            # This is safe because all files of older crawls have been added before.
            print(f"  [{crawl_id}] Changing column {name} {existing[name]} -> {column_type}")
            con.execute(f'ALTER TABLE commoncrawl.{TABLE} ALTER COLUMN "{name}" SET DATA TYPE {column_type}')


# Not sorted so their min/max stats can't skip files. Removing them makes the catalog ~40% smaller.
UNUSED_STATS_COLUMNS = (
    "url",
    "url_path",
    "url_query",
    "warc_filename",
    "warc_segment",
    "warc_record_id",
    "content_digest",
    "fetch_redirect",
)

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
COLLINFO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collinfo.json")

# Common Crawl files are sorted by url_surtkey so filtering with it skips most files.
# SURT reverses the host: https://www.yle.fi/uutiset/ -> fi,yle)/uutiset
SURT_MACROS = r"""
    -- yle.fi -> fi,yle
    CREATE MACRO surt_host(host) AS
        array_to_string(list_reverse(string_split(regexp_replace(lower(host), '^www[0-9]*\.', ''), '.')), ',');

    -- yle.fi/uutiset?a=1#top -> yle.fi/uutiset?a=1, without the scheme and the fragment
    CREATE MACRO surt_rest(url) AS
        regexp_replace(regexp_replace(url, '^[a-z][a-z0-9+.-]*://', '', 'i'), '#.*$', '');

    -- Host and port of the url
    CREATE MACRO surt_authority(url) AS regexp_extract(surt_rest(url), '^[^/?]*');

    -- https://yle.fi/uutiset -> fi,yle)/uutiset. Default ports are dropped like in Common Crawl's keys.
    CREATE MACRO surt_prefix(url) AS
        surt_host(regexp_replace(surt_authority(url), ':[0-9]*$', ''))
        || CASE WHEN regexp_extract(surt_authority(url), ':([0-9]+)$', 1) IN ('', '80', '443') THEN ''
                ELSE ':' || regexp_extract(surt_authority(url), ':([0-9]+)$', 1) END
        || ')'
        || CASE WHEN starts_with(substr(surt_rest(url), length(surt_authority(url)) + 1), '?') THEN '/' ELSE '' END
        || lower(substr(surt_rest(url), length(surt_authority(url)) + 1));

    -- Pages of the host and its subdomains. ')' ends the host and ',' continues to a subdomain, both are before '-'.
    CREATE MACRO surtkey_in_domain(url_surtkey, domain) AS
        url_surtkey >= surt_host(domain) || ')' AND url_surtkey < surt_host(domain) || '-';

    -- Pages of exactly this host
    CREATE MACRO surtkey_in_host(url_surtkey, host) AS
        url_surtkey >= surt_host(host) || ')' AND url_surtkey < surt_host(host) || '*';

    -- Pages which start with the url, for example 'yle.fi/uutiset'
    CREATE MACRO surtkey_starts_with(url_surtkey, url) AS
        url_surtkey >= surt_prefix(url) AND url_surtkey < surt_prefix(url) || chr(127);

    -- SURT key of one url like in url_surtkey: https://yle.fi/uutiset/ -> fi,yle)/uutiset, https://yle.fi -> fi,yle)/
    -- and https://yle.fi/a/?s=1&a=2 -> fi,yle)/a?a=2&s=1 with the query parameters sorted like Common Crawl does.
    CREATE MACRO surt_url(url) AS
        CASE WHEN NOT contains(surt_prefix(url), ')/') THEN surt_prefix(url) || '/'
             ELSE regexp_replace(split_part(surt_prefix(url), '?', 1), '([^)])/$', '\1')
                  || CASE WHEN contains(surt_prefix(url), '?')
                          THEN '?' || array_to_string(list_sort(string_split(
                              substr(surt_prefix(url), strpos(surt_prefix(url), '?') + 1), '&')), '&')
                          ELSE '' END END;

    CREATE MACRO domain_pages(domain) AS TABLE
        FROM archives WHERE surtkey_in_domain(url_surtkey, domain);

    -- Pages of exactly this host
    CREATE MACRO host_pages(host) AS TABLE
        FROM archives WHERE surtkey_in_host(url_surtkey, host);

    -- Every capture of one url in every crawl. The scheme and www. don't matter.
    CREATE MACRO url_captures(page_url) AS TABLE
        SELECT crawl, subset, url, fetch_time, fetch_status, fetch_redirect, content_mime_detected,
            content_languages, content_digest, warc_filename, warc_record_offset, warc_record_length
        FROM archives WHERE url_surtkey = surt_url(page_url) ORDER BY fetch_time;

    -- Pages, hosts and fetch times of a domain in every crawl
    CREATE MACRO domain_history(domain) AS TABLE
        SELECT crawl, count(*) AS pages, count(*) FILTER (fetch_status = 200) AS ok_pages,
            count(DISTINCT url_host_name) AS hosts, min(fetch_time) AS first_fetch, max(fetch_time) AS last_fetch
        FROM domain_pages(domain) WHERE subset = 'warc' GROUP BY crawl ORDER BY crawl;

    -- Subdomains of a domain with the crawls they were seen in
    CREATE MACRO domain_hosts(domain) AS TABLE
        SELECT url_host_name AS host, count(*) AS pages, min(crawl) AS first_crawl, max(crawl) AS last_crawl
        FROM domain_pages(domain) WHERE subset = 'warc' GROUP BY ALL ORDER BY pages DESC;

    -- Distinct urls of a host with the crawls they were seen in
    CREATE MACRO host_urls(host) AS TABLE
        SELECT url, count(*) AS captures, min(crawl) AS first_crawl, max(crawl) AS last_crawl
        FROM host_pages(host) WHERE subset = 'warc' GROUP BY url ORDER BY url;

    -- Redirects of a domain, for example to a new domain. fetch_redirect is in the index since CC-MAIN-2019-47.
    CREATE MACRO domain_redirects(domain) AS TABLE
        SELECT crawl, url, fetch_status, fetch_redirect
        FROM domain_pages(domain) WHERE subset = 'crawldiagnostics' AND fetch_redirect IS NOT NULL
        ORDER BY crawl, url;

    -- robots.txt fetches of a domain, read them with warc_record()
    CREATE MACRO domain_robots_txt(domain) AS TABLE
        SELECT crawl, url, fetch_status, warc_filename, warc_record_offset, warc_record_length
        FROM domain_pages(domain) WHERE subset = 'robotstxt' ORDER BY crawl, url;

    -- Registered domains of a top level domain in one crawl
    CREATE MACRO tld_domains(tld, crawl_id) AS TABLE
        SELECT url_host_registered_domain AS domain, count(*) AS pages, count(DISTINCT url_host_name) AS hosts
        FROM archives WHERE crawl = crawl_id AND subset = 'warc' AND url_host_tld = tld
        GROUP BY ALL ORDER BY pages DESC;
"""


def freeze(catalog_path, frozen_path):
    """Writes a compacted copy of the catalog for querying with the SURT macros."""
    print(f"Freezing {catalog_path} into {frozen_path}...")
    compacting_path = f"{frozen_path}.tmp"
    for path in (frozen_path, compacting_path):
        if os.path.exists(path):
            os.remove(path)

    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    columns = ", ".join(f"'{column}'" for column in UNUSED_STATS_COLUMNS)
    con.execute(f"""
        ATTACH '{catalog_path}' AS source (READ_ONLY);
        ATTACH '{compacting_path}' AS compacting;
        COPY FROM DATABASE source TO compacting;
        DELETE FROM compacting.ducklake_file_column_stats
        WHERE column_id IN (SELECT column_id FROM compacting.ducklake_column WHERE column_name IN ({columns}));
        DETACH source;
        DETACH compacting;

        -- Copying again leaves out the deleted rows
        ATTACH '{compacting_path}' AS compacting (READ_ONLY);
        ATTACH '{frozen_path}' AS frozen;
        COPY FROM DATABASE compacting TO frozen;
        DETACH compacting;
        DETACH frozen;
    """)
    os.remove(compacting_path)

    con.execute(f"ATTACH 'ducklake:{frozen_path}' AS frozen; USE frozen;")
    con.execute(SURT_MACROS)
    # Crawls with their names and fetch times from index.commoncrawl.org/collinfo.json, as a constant table which
    # doesn't scan anything. The lambda passes them to cdx_captures() of the extract extension so that fetch_time
    # filters skip the other crawls. The parquet statistics can't be used for this: crawls before CC-MAIN-2017-43 have
    # none and some crawls have a few records with a fetch_time years before the crawl.
    if not os.path.exists(COLLINFO_PATH):
        urllib.request.urlretrieve(COLLINFO_URL, COLLINFO_PATH)
    rows, missing = con.execute("""
        WITH catalog AS (
            SELECT DISTINCT regexp_extract(data_file, 'crawl=([^/]+)/', 1) AS crawl
            FROM ducklake_list_files('frozen', 'archives')
        ), info AS (
            SELECT id AS crawl, name, "from"::TIMESTAMP AS first_fetch, "to"::TIMESTAMP AS last_fetch
            FROM read_json($path)
        )
        SELECT string_agg(format('(''{}'', ''{}'', ''{}''::TIMESTAMP, ''{}''::TIMESTAMP)', crawl, replace(name, chr(39), chr(39) || chr(39)),
                first_fetch, last_fetch), ', ' ORDER BY crawl) FILTER (info.crawl IS NOT NULL),
            list(crawl) FILTER (info.crawl IS NULL)
        FROM catalog LEFT JOIN info USING (crawl)
    """, {"path": COLLINFO_PATH}).fetchall()[0]
    if missing:
        raise SystemExit(f"{COLLINFO_PATH} doesn't have {missing}, download a new one from {COLLINFO_URL}")
    con.execute(f"CREATE MACRO crawls() AS TABLE SELECT * FROM (VALUES {rows}) AS t(crawl, name, first_fetch, last_fetch)")
    con.close()
    print(f"Frozen catalog is {os.path.getsize(frozen_path) / 1e6:.0f}MB")


def main():
    parser = argparse.ArgumentParser(description="Convert Common Crawl columnar index to DuckLake format.")
    parser.add_argument(
        "--access-mode",
        choices=["s3", "http"],
        default="http",
        help="Access mode: s3 (requires AWS credentials), http (rate limited)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Amount of parallel ducklake_add_data_files() calls (default: 8)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Files added per commit. Larger batches cause less commit conflicts (default: 25)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_sigint)

    print("Initializing DuckDB and extensions...")
    con = duckdb.connect()
    for ext in ["ducklake", "httpfs"]:
        con.execute(f"INSTALL {ext}; LOAD {ext};")

    if args.access_mode == "s3":
        cc_base, db_path = S3_BASE, "ducklake:commoncrawl_s3.ducklake"
        con.execute("INSTALL aws; LOAD aws;")
        # Refreshes expired temporary credentials during long imports
        con.execute(
            "CREATE SECRET commoncrawl_s3 (TYPE S3, PROVIDER credential_chain, REGION 'us-east-1', REFRESH 'auto')"
        )
    else:
        cc_base, db_path = HTTP_BASE, "ducklake:commoncrawl.ducklake"

    # Old fetch_time values without timezone are UTC
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET http_retries = 1000; SET http_retry_backoff = 6; SET http_retry_wait_ms = 500;")
    # Parallel workers commit at the same time
    con.execute("SET ducklake_max_retry_count = 100")
    con.execute(f"ATTACH '{db_path}' AS commoncrawl (DATA_PATH 'tmp_always_empty', AUTOMATIC_MIGRATION TRUE)")
    # Older versions of this project used a view to combine 2 tables
    has_old_view = con.execute(
        "SELECT count(*) > 0 FROM duckdb_views() WHERE database_name = 'commoncrawl' AND view_name = $table",
        {"table": TABLE},
    ).fetchone()[0]
    if has_old_view:
        con.execute(f"DROP VIEW commoncrawl.{TABLE}")

    # 1. Fetch metadata & generate file list
    print("Fetching metadata and generating file lists...")
    collinfo = "./collinfo.json" if os.path.exists("./collinfo.json") else "https://index.commoncrawl.org/collinfo.json"

    con.execute(
        "SET VARIABLE crawl_ids = (SELECT list(id) FROM read_json($collinfo) WHERE NOT list_contains($skipped, id))",
        {"collinfo": collinfo, "skipped": list(SKIPPED_CRAWLS)},
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE all_files AS
            SELECT
                regexp_extract(column0, '/crawl=([^/]+)/', 1) AS crawl_id,
                '{cc_base}/' || column0 AS file_url
            FROM read_csv(
                list_transform(
                    $crawl_ids,
                    lambda n: format('{cc_base}/crawl-data/{{}}/cc-index-table.paths.gz', n)
                ),
                header = false
            )
            -- The paths also contain directories
            WHERE ends_with(column0, '.parquet')
        """
    )

    # 2. Create the table from the oldest file if it doesn't exist yet
    table_exists = con.execute(
        "SELECT count(*) > 0 FROM duckdb_tables() WHERE database_name = 'commoncrawl' AND table_name = $table",
        {"table": TABLE},
    ).fetchone()[0]
    if not table_exists:
        oldest_file = con.execute("SELECT min(file_url) FROM all_files").fetchone()[0]
        con.execute(
            f"CREATE TABLE commoncrawl.{TABLE} AS FROM read_parquet($file) WITH NO DATA",
            {"file": oldest_file},
        )
        # Hive partitions from the file paths allow skipping files when filtering by crawl or subset
        con.execute(f"ALTER TABLE commoncrawl.{TABLE} SET PARTITIONED BY (crawl, subset)")

    # 3. Calculate delta (files to process)
    print("Calculating missing files...")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE pending_files AS
            FROM all_files
            WHERE file_url NOT IN (SELECT data_file FROM ducklake_list_files('commoncrawl', '{TABLE}'))
        """
    )
    pending = con.execute(
        "SELECT crawl_id, list(file_url ORDER BY file_url) FROM pending_files GROUP BY crawl_id ORDER BY crawl_id"
    ).fetchall()
    total = sum(len(files) for _, files in pending)
    print(f"Found {total} new files in {len(pending)} crawls to process.")

    # 4. Evolve schema and add files crawl by crawl
    done = 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for crawl_id, files in pending:
            if stop_event.is_set():
                break

            evolve_schema(con, crawl_id, files[0])

            print(f"[{crawl_id}] Adding {len(files)} files...")
            for retry_round in range(RETRY_ROUNDS + 1):
                if retry_round > 0:
                    print(f"  [{crawl_id}] Retrying {len(files)} failed files in {RETRY_WAIT_SECONDS}s...")
                    stop_event.wait(RETRY_WAIT_SECONDS)
                failed = []
                # Smaller batches on retries so that a broken file fails less of the others
                batch_size = max(1, args.batch_size >> retry_round)
                batches = [files[i : i + batch_size] for i in range(0, len(files), batch_size)]
                for batch, error in zip(batches, pool.map(lambda b: add_files(con, b), batches)):
                    if error == "stopped":
                        continue
                    if error:
                        print(f"  [Error] {len(batch)} files from {batch[0]}: {error}")
                        failed.extend(batch)
                        continue
                    previous, done = done, done + len(batch)
                    if done // 1000 > previous // 1000:
                        rate = done / (time.monotonic() - started)
                        print(f"  [{done}/{total}] {rate:.1f} files/s")
                files = failed
                if not files or stop_event.is_set():
                    break

            # Newer crawls may change the schema so that these files couldn't be added anymore
            if files and not stop_event.is_set():
                con.close()
                sys.exit(f"[{crawl_id}] {len(files)} files still failed after {RETRY_ROUNDS} retries. Rerun to retry them.")

    if stop_event.is_set():
        con.close()
        print("Stopped.")
        return

    # 5. Verify that all files were added
    missing = con.execute(
        f"""
        SELECT file_url FROM all_files
        WHERE file_url NOT IN (SELECT data_file FROM ducklake_list_files('commoncrawl', '{TABLE}'))
        ORDER BY file_url
        """
    ).fetchall()
    con.close()
    if missing:
        for (file_url,) in missing:
            print(f"  [Missing] {file_url}")
        sys.exit(f"{len(missing)} files are missing from the {TABLE} table.")
    print("All files were added.")
    catalog_path = db_path.removeprefix("ducklake:")
    freeze(catalog_path, catalog_path.replace(".ducklake", ".frozen.ducklake"))
    print("Done.")


if __name__ == "__main__":
    main()
