#!/usr/bin/env bash
#
# Builds a minimal libduckdb.so with only the extensions in extension_config.cmake.
# Runs inside an Amazon Linux 2023 arm64 container so that glibc matches the lambda runtime.
# Output: /out/libduckdb.so and /out/duckdb.h
#
# Optional environment variables:
#   DUCKDB_PATCH  patch file in /src which is applied to DuckDB, for example an unreleased pull request
#   BUILD_SHELL   1 also builds the duckdb CLI into /out/duckdb
#   JEMALLOC      1 uses jemalloc as the allocator. It made queries ~20% slower on lambda, see README.md
#   VECTOR_SIZE   rows per vector instead of 2048, smaller vectors hold fewer fetched pages in memory at once
#

set -euo pipefail

DUCKDB_VERSION="v2.0.0"

dnf install -y -q gcc14 gcc14-c++ cmake ninja-build git make python3 openssl-devel perl-core xz patch zip unzip tar pkgconf-pkg-config kernel-headers >/dev/null

# Patched builds get their own source tree so that switching between them doesn't rebuild everything
SOURCE="/build/duckdb${DUCKDB_PATCH:+-$(basename "$DUCKDB_PATCH" .diff)}${VECTOR_SIZE:+-vs${VECTOR_SIZE}}"
if [[ ! -d "$SOURCE" ]]; then
    git clone --quiet --depth 1 --branch "$DUCKDB_VERSION" https://github.com/duckdb/duckdb "$SOURCE"
    if [[ -n "${DUCKDB_PATCH:-}" ]]; then
        # Binary test files can't be applied from a diff and aren't needed
        git -C "$SOURCE" apply --exclude='data/*' "/src/${DUCKDB_PATCH}"
    fi
fi

# httpfs needs openssl and curl, ducklake needs roaring
if [[ ! -x /build/vcpkg/vcpkg ]]; then
    git clone --quiet https://github.com/microsoft/vcpkg /build/vcpkg
    /build/vcpkg/bootstrap-vcpkg.sh -disableMetrics
fi
export VCPKG_TOOLCHAIN_PATH=/build/vcpkg/scripts/buildsystems/vcpkg.cmake
export VCPKG_FORCE_SYSTEM_BINARIES=1
# Default GCC 11 is too old for httpfs
export CC=gcc14-gcc CXX=gcc14-g++

cd "$SOURCE"

# Our own DuckDB patches, applied once
for patch in /src/patches/duckdb/*.patch; do
    git apply --reverse --check "$patch" 2>/dev/null || git apply "$patch"
done

# Extension patches go where APPLY_PATCHES in extension_config.cmake looks for them. httpfs keeps HTTP connections
# only for one file handle, warc_record() opens one for every record so every record needed a new TLS connection.
for patches in /src/patches/extensions/*/; do
    name="$(basename "$patches")"
    mkdir -p ".github/patches/extensions/${name}"
    cp "$patches"*.patch ".github/patches/extensions/${name}/"
done

# Only the extensions in our config, statically linked into libduckdb.so
# Re-applies the patches when they change instead of failing on the old patched checkout
GEN=ninja \
    FORCE_APPLY_PATCHES=1 \
    DUCKDB_EXTENSIONS="" \
    EXTENSION_CONFIGS="/src/extension_config.cmake" \
    DISABLE_SHELL=$([[ "${BUILD_SHELL:-0}" == 1 ]] && echo 0 || echo 1) \
    EXTRA_CMAKE_VARIABLES="-DBUILD_UNITTESTS=0 -DENABLE_JEMALLOC=$([[ "${JEMALLOC:-0}" == 1 ]] && echo ON || echo OFF) -DSKIP_EXTENSIONS= ${VECTOR_SIZE:+-DSTANDARD_VECTOR_SIZE=${VECTOR_SIZE}}" \
    DISABLE_SANITIZER=1 \
    make release

cp build/release/src/libduckdb.so /out/libduckdb.so
cp src/include/duckdb.h /out/duckdb.h
if [[ "${BUILD_SHELL:-0}" == 1 ]]; then
    cp build/release/duckdb /out/duckdb
fi
ls -la /out
