#!/usr/bin/env bash
#
# Benchmarks the query lambda with different memory sizes and zstd levels.
#
# For every run it measures:
#   seconds    loading the result into a local DuckDB table with read_arrow()
#   bytes      size of the zstd compressed Arrow IPC response
#   billed_ms  billed lambda duration from CloudWatch
#   gb_s       lambda GB-seconds, the free tier has 400 000 per month
#
# Every run has a unique SQL comment so that CloudFront never serves it from cache.
#
# Usage: MEMORY_SIZES="1769 3008" LEVELS="1 3" RUNS=2 ./benchmark.sh "SELECT ... FROM archives WHERE ..."
#

set -euo pipefail

cd "$(dirname "$0")"
SQL="$1"
MEMORY_SIZES="${MEMORY_SIZES:-1769 3008 5307 10240}"
LEVELS="${LEVELS:-1}"
RUNS="${RUNS:-2}"
DUCKDB="${DUCKDB:-duckdb}"

FUNCTION="$(tofu output -raw query_function_name)"
export CC_URL="$(tofu output -raw cloudfront_url)"
export CC_COOKIE="$(./cloudfront-cookie.sh 2)"
ORIGINAL_MEMORY="$(aws lambda get-function-configuration --function-name "$FUNCTION" --query MemorySize --output text)"

# Billed duration of the latest invocation from CloudWatch
billed_ms() {
    local billed=""
    for _ in $(seq 1 20); do
        billed="$(aws logs filter-log-events --log-group-name "/aws/lambda/${FUNCTION}" \
            --start-time "$1" --filter-pattern REPORT --query 'events[-1].message' --output text |
            grep -oE 'Billed Duration: [0-9]+' | grep -oE '[0-9]+' | tail -1 || true)"
        [[ -n "$billed" ]] && break
        sleep 3
    done
    echo "${billed:-0}"
}

printf '%-7s %-6s %-4s %-9s %-10s %-11s %-10s %s\n' memory level run seconds rows bytes billed_ms gb_s
for MEMORY in $MEMORY_SIZES; do
    aws lambda update-function-configuration --function-name "$FUNCTION" --memory-size "$MEMORY" >/dev/null
    aws lambda wait function-updated-v2 --function-name "$FUNCTION"

    # Cold start once so that the measured runs are warm
    curl -s -o /dev/null -H "Cookie: $CC_COOKIE" -G "$CC_URL" --data-urlencode "sql=SELECT 1 /* $(date +%s%N) */"

    for LEVEL in $LEVELS; do
        for RUN in $(seq 1 "$RUNS"); do
            QUERY="$SQL /* ${MEMORY} ${LEVEL} ${RUN} $(date +%s%N) */"
            ENCODED="$(jq -rn --arg q "$QUERY" '$q | @uri')"
            STARTED_MS="$(( $(date +%s) * 1000 ))"
            OUTPUT="$("$DUCKDB" -no-agent -init /dev/null -csv -noheader <<SQL
LOAD nanoarrow;
CREATE SECRET commoncrawl (TYPE http, EXTRA_HTTP_HEADERS MAP {'Cookie': getenv('CC_COOKIE')}, SCOPE getenv('CC_URL'));
SET force_download = true;
.timer on
CREATE TABLE result AS FROM read_arrow(getenv('CC_URL') || '?level=${LEVEL}&sql=${ENCODED}');
.timer off
SELECT 'rows', count(*) FROM result;
SQL
)" || { echo "memory ${MEMORY} level ${LEVEL} failed: $OUTPUT" >&2; continue; }
            SECONDS_TAKEN="$(grep -oE 'real [0-9.]+' <<< "$OUTPUT" | tail -1 | cut -d' ' -f2)"
            ROWS="$(grep -E '^rows,' <<< "$OUTPUT" | cut -d, -f2)"
            # Same URL was just fetched so this comes from CloudFront cache. Large results redirect to S3.
            BYTES="$(curl -sL -o /dev/null -w '%{size_download}' -H "Cookie: $CC_COOKIE" "${CC_URL}?level=${LEVEL}&sql=${ENCODED}")"
            BILLED="$(billed_ms "$STARTED_MS")"
            GB_S="$(echo "scale=1; $MEMORY * $BILLED / 1024 / 1000" | bc)"
            printf '%-7s %-6s %-4s %-9s %-10s %-11s %-10s %s\n' "$MEMORY" "$LEVEL" "$RUN" "$SECONDS_TAKEN" "$ROWS" "$BYTES" "$BILLED" "$GB_S"
        done
    done
done

aws lambda update-function-configuration --function-name "$FUNCTION" --memory-size "$ORIGINAL_MEMORY" >/dev/null
