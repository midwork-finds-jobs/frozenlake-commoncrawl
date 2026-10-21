#!/usr/bin/env bash
#
# Builds a minimal libduckdb.so with only the extensions in extension_config.cmake.
# Runs inside an Amazon Linux 2023 arm64 container so that glibc matches the lambda runtime.
# Output: /out/libduckdb.so and /out/duckdb.h
#

set -euo pipefail

DUCKDB_VERSION="v2.0.0"

dnf install -y -q gcc14 gcc14-c++ cmake ninja-build git make python3 openssl-devel perl-core xz patch zip unzip tar pkgconf-pkg-config kernel-headers >/dev/null

if [[ ! -d /build/duckdb ]]; then
    git clone --quiet --depth 1 --branch "$DUCKDB_VERSION" https://github.com/duckdb/duckdb /build/duckdb
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

cd /build/duckdb

# Only the extensions in our config, statically linked into libduckdb.so
GEN=ninja \
    DUCKDB_EXTENSIONS="" \
    SKIP_EXTENSIONS="jemalloc" \
    EXTENSION_CONFIGS="/src/extension_config.cmake" \
    DISABLE_SHELL=1 \
    EXTRA_CMAKE_VARIABLES="-DBUILD_UNITTESTS=0" \
    DISABLE_SANITIZER=1 \
    make release

cp build/release/src/libduckdb.so /out/libduckdb.so
cp src/include/duckdb.h /out/duckdb.h
ls -la /out
