cmake_minimum_required(VERSION 3.14)
set(TARGET_NAME extract)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# lexbor: HTML5 parser and CSS selectors, statically linked into libduckdb.so
include(FetchContent)
set(LEXBOR_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(LEXBOR_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(LEXBOR_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LEXBOR_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
FetchContent_Declare(
  lexbor
  GIT_REPOSITORY https://github.com/lexbor/lexbor
  # v3.0.0
  GIT_TAG 2ae88a1c6b5261830eff73ee12bb3cdf805f3cfe
  # Use after free when the HTML parser fails to initialize, applied once
  PATCH_COMMAND sh -c "git apply --reverse --check ${CMAKE_CURRENT_SOURCE_DIR}/patches/lexbor-parser-init.patch 2>/dev/null || git apply ${CMAKE_CURRENT_SOURCE_DIR}/patches/lexbor-parser-init.patch")
FetchContent_MakeAvailable(lexbor)

# Inflates WARC records and checks their CRC32 several times faster than miniz, with the ARM CRC instructions
set(LIBDEFLATE_BUILD_SHARED_LIB OFF CACHE BOOL "" FORCE)
set(LIBDEFLATE_BUILD_GZIP OFF CACHE BOOL "" FORCE)
set(LIBDEFLATE_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  libdeflate
  GIT_REPOSITORY https://github.com/ebiggers/libdeflate
  # v1.26
  GIT_TAG 75390569905da2f4aa03c5bc03e03c9d18b9b2d0)
FetchContent_MakeAvailable(libdeflate)

include_directories(src/include)
set(EXTENSION_SOURCES src/extract_extension.cpp src/warc_record.cpp src/html_functions.cpp src/macros.cpp src/charset.cpp src/memory_governor.cpp src/optimizer.cpp src/url_functions.cpp src/cdx.cpp src/fast_scan.cpp)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
target_link_libraries(${EXTENSION_NAME} lexbor_static libdeflate_static)
target_link_libraries(${LOADABLE_EXTENSION_NAME} lexbor_static libdeflate_static)

