# Extensions statically linked into the minimal libduckdb.so of the lambda.
# parquet reads Common Crawl, httpfs reads S3, ducklake reads the frozen catalog, json parses JSON-LD and extract
# fetches WARC records and extracts data from HTML.
# jemalloc isn't an extension anymore, it's enabled with JEMALLOC=1 in build.sh.
# Versions are the same as in duckdb/.github/config/extensions/*.cmake of DuckDB v2.0.0.

duckdb_extension_load(core_functions)
duckdb_extension_load(parquet)
duckdb_extension_load(json)

# patches/extensions/httpfs reuses HTTP connections across file handles, see build.sh
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 77392a72077a85897db2519fae11e2c0da972b8a
    APPLY_PATCHES
)

duckdb_extension_load(ducklake
    GIT_URL https://github.com/duckdb/ducklake
    GIT_TAG 417a9117f590e8141742ac0579d02f6a90162e37
    APPLY_PATCHES
)

# Our own extension in lambda/duckdb/extract, /src is where build.sh mounts lambda/duckdb
duckdb_extension_load(extract
    SOURCE_DIR /src/extract
)

# Link everything into libduckdb.so so that nothing is loaded from disk at runtime
duckdb_extension_statically_link(core_functions)
duckdb_extension_statically_link(parquet)
duckdb_extension_statically_link(json)
duckdb_extension_statically_link(httpfs)
duckdb_extension_statically_link(ducklake)
duckdb_extension_statically_link(extract)
