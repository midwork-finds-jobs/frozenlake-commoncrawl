#!/usr/bin/env bash
#
# Builds the laptop extension into client/target/commoncrawl.duckdb_extension:
#
#   duckdb -unsigned -c "LOAD 'client/target/commoncrawl.duckdb_extension'"
#

set -euo pipefail

cd "$(dirname "$0")"
cargo build --release

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) PLATFORM=osx_arm64 LIBRARY=target/release/libcommoncrawl.dylib ;;
    Darwin-x86_64) PLATFORM=osx_amd64 LIBRARY=target/release/libcommoncrawl.dylib ;;
    Linux-aarch64) PLATFORM=linux_arm64 LIBRARY=target/release/libcommoncrawl.so ;;
    Linux-x86_64) PLATFORM=linux_amd64 LIBRARY=target/release/libcommoncrawl.so ;;
    *) echo "Unsupported platform $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac
OUT=target/commoncrawl.duckdb_extension

# DuckDB only loads files with its metadata footer, the same as extension-ci-tools/scripts/append_extension_metadata.py
# writes for C API extensions: a WebAssembly custom section header, 8 fields of 32 bytes in reverse order and 256
# bytes for the signature.
field() {
    printf '%s' "$1"
    head -c $((32 - ${#1})) /dev/zero
}
cp "$LIBRARY" "$OUT.tmp"
{
    printf '\x00\x93\x04\x10duckdb_signature\x80\x04'
    field ""
    field ""
    field ""
    field "C_STRUCT"
    field "$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)"
    # C API version, the same as min_duckdb_version in src/lib.rs
    field "v1.2.0"
    field "$PLATFORM"
    field "4"
    head -c 256 /dev/zero
} >> "$OUT.tmp"
mv "$OUT.tmp" "$OUT"
echo "Built $OUT"
