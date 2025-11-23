INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;

-- Set larger retry values for httpfs
SET http_retries = 100;
SET http_retry_backoff = 5;
SET http_retry_wait_ms = 500;

CREATE OR REPLACE TEMP TABLE crawl_info AS
    FROM read_json('https://index.commoncrawl.org/collinfo.json')
    WHERE id NOT IN (
        -- Columnar index not available for the oldest crawls
        'CC-MAIN-2012',
        'CC-MAIN-2009-2010',
        'CC-MAIN-2008-2009'
    );

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
    );

ATTACH 'ducklake:commoncrawl.ducklake' AS commoncrawl (DATA_PATH 'tmp_always_empty');

-- Handle years 2013-2023 without timezone in fetch_time
-- CC-MAIN-2018-39: new columns "content_charset" + "content_languages"
-- CC-MAIN-2021-49: new column "url_host_name_reversed"
SET VARIABLE parquet_files = (
    SELECT ARRAY_AGG(url)
    FROM crawl_parquet_files
    INNER JOIN crawl_info ON contains(url, '/crawl=' || crawl_info.id || '/')
    WHERE id BETWEEN 'CC-MAIN-2013-20' AND 'CC-MAIN-2021-43'
);

CREATE OR REPLACE TABLE commoncrawl.CC_MAIN_2013_TO_2021 AS
    FROM read_parquet(list_max(getvariable('parquet_files')))
    WITH NO DATA;

-- FIXME: Loops are not supported in DuckDB SQL
-- TODO: Create a loop which calls this function for each crawl id in parquet files
CALL ducklake_add_data_files(
    'commoncrawl',
    'CC_MAIN_2013_TO_2021',
    list_min(getvariable('parquet_files')),
    allow_missing => true
);

-- From 2021-49 onwards, fetch_time has timezone info
SET VARIABLE parquet_files = (
    SELECT ARRAY_AGG(url)
    FROM crawl_parquet_files
    INNER JOIN crawl_info ON contains(url, '/crawl=' || crawl_info.id || '/')
    WHERE id >= 'CC-MAIN-2021-49'
);

CREATE OR REPLACE TABLE commoncrawl.CC_MAIN_2021_AND_FORWARD AS
    FROM read_parquet(list_max(getvariable('parquet_files')))
    WITH NO DATA;

-- FIXME: CALL this function for all items in parquet_files
CALL ducklake_add_data_files(
    'commoncrawl',
    'CC_MAIN_2021_AND_FORWARD',
    list_min(getvariable('parquet_files'))
);

-- Then create a view which combines all the data with unified types
-- Because migrations won't work, let's create a view with the correct types
CREATE OR REPLACE VIEW commoncrawl.archives AS
    SELECT *
    FROM commoncrawl.CC_MAIN_2021_AND_FORWARD;
    UNION ALL BY NAME
    -- Add UTC timezone to fetch_time
    SELECT * REPLACE (fetch_time::TIMESTAMPTZ AS fetch_time)
    FROM commoncrawl.CC_MAIN_2013_TO_2021