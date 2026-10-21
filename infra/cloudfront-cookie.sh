#!/usr/bin/env bash
#
# Prints a CloudFront signed cookie header for the query distribution.
# Same cookies as https://github.com/midwork-finds-jobs/duckdb-cloudfront creates.
#
# Usage: ./cloudfront-cookie.sh [valid hours, default 24]
#

set -euo pipefail

cd "$(dirname "$0")"
HOURS="${1:-24}"
URL="$(tofu output -raw cloudfront_url)"
KEY_PAIR_ID="$(tofu output -raw cloudfront_key_pair_id)"
EXPIRES=$(( $(date +%s) + HOURS * 3600 ))

# CloudFront uses its own URL safe base64 variant
cloudfront_base64() {
    openssl base64 -A | tr '+=/' '-_~'
}

POLICY="{\"Statement\":[{\"Resource\":\"${URL}*\",\"Condition\":{\"DateLessThan\":{\"AWS:EpochTime\":${EXPIRES}}}}]}"
SIGNATURE="$(printf '%s' "$POLICY" | openssl dgst -sha1 -sign <(tofu output -raw cloudfront_private_key_pem) | cloudfront_base64)"

printf 'CloudFront-Policy=%s; CloudFront-Signature=%s; CloudFront-Key-Pair-Id=%s\n' \
    "$(printf '%s' "$POLICY" | cloudfront_base64)" "$SIGNATURE" "$KEY_PAIR_ID"
