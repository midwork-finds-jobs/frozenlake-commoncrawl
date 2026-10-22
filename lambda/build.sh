#!/usr/bin/env bash
#
# Builds the arm64 lambda package into lambda/target/lambda.zip:
#   bootstrap          Rust binary
#   lib/libduckdb.so   Minimal DuckDB with parquet, httpfs and ducklake statically linked
#   commoncrawl.ducklake  The frozen catalog (CATALOG), packaged so that cold starts don't download it
#

set -euo pipefail

cd "$(dirname "$0")"
# Another DuckDB build can be used with DUCKDB_LIB, for example one built with DUCKDB_PATCH
DUCKDB_LIB="${DUCKDB_LIB:-target/duckdb-out}"
PACKAGE="target/package"
CATALOG="${CATALOG:-../commoncrawl_s3.frozen.ducklake}"

# DuckDB v2.0.0 with the parquet page index pull request https://github.com/duckdb/duckdb/pull/27326
# applied. Common Crawl files have a page index so this reads 9-23x less data for url_surtkey lookups.
DUCKDB_PATCH="${DUCKDB_PATCH-patches/duckdb-pr-27326.diff}"

# DuckDB is rebuilt when the extension or the build configuration in duckdb/ changes. Remove target/duckdb-out to
# rebuild it anyway.
if [[ ! -f "${DUCKDB_LIB}/libduckdb.so" || -n "$(find duckdb -type f -newer "${DUCKDB_LIB}/libduckdb.so")" ]]; then
    echo "Building minimal DuckDB in an Amazon Linux 2023 container..."
    mkdir -p target/duckdb-build "$DUCKDB_LIB"
    container run --rm --arch arm64 --cpus "$(sysctl -n hw.ncpu)" --memory 16g -e DUCKDB_PATCH="$DUCKDB_PATCH" -e JEMALLOC="${JEMALLOC:-0}" -e VECTOR_SIZE="${VECTOR_SIZE:-}" \
        -v "$(pwd)/duckdb:/src" -v "$(pwd)/target/duckdb-build:/build" -v "$(pwd)/${DUCKDB_LIB}:/out" \
        public.ecr.aws/amazonlinux/amazonlinux:2023 bash /src/build.sh
fi

echo "Building lambda..."
DUCKDB_LIB_DIR="$(pwd)/${DUCKDB_LIB}" DUCKDB_INCLUDE_DIR="$(pwd)/${DUCKDB_LIB}" DUCKDB_DOWNLOAD_LIB=0 \
    cargo lambda build --release --arm64

rm -rf "$PACKAGE" target/lambda.zip
mkdir -p "${PACKAGE}/lib"
cp target/lambda/bootstrap/bootstrap "${PACKAGE}/bootstrap"
llvm-strip --strip-unneeded -o "${PACKAGE}/lib/libduckdb.so" "${DUCKDB_LIB}/libduckdb.so"
cp "$CATALOG" "${PACKAGE}/commoncrawl.ducklake"

(cd "$PACKAGE" && zip -qr9 ../lambda.zip .)
du -sh "$PACKAGE" target/lambda.zip
