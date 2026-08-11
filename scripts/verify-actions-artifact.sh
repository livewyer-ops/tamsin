#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 ARTIFACT_ID ARTIFACT_DIGEST" >&2
  exit 2
fi
if [ -z "${GITHUB_REPOSITORY:-}" ] || [ -z "${GH_TOKEN:-}" ]; then
  echo "GITHUB_REPOSITORY and GH_TOKEN are required" >&2
  exit 2
fi

artifact_id="$1"
expected_digest="$2"
case "$artifact_id" in
  *[!0-9]*|'') echo "invalid Actions artifact ID '$artifact_id'" >&2; exit 2 ;;
esac
# upload-artifact's `artifact-digest` output is the bare hexadecimal SHA-256,
# while the Actions REST resource currently prefixes the same value with
# `sha256:`. Normalize the documented action output before comparing the two.
if [[ "$expected_digest" =~ ^[0-9a-f]{64}$ ]]; then
  expected_digest="sha256:$expected_digest"
elif [[ ! "$expected_digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "invalid Actions artifact digest '$expected_digest'" >&2
  exit 2
fi

actual_digest="$(gh api "/repos/$GITHUB_REPOSITORY/actions/artifacts/$artifact_id" --jq '.digest // empty')"
if [ "$actual_digest" != "$expected_digest" ]; then
  echo "Actions artifact $artifact_id has digest '$actual_digest', expected '$expected_digest'" >&2
  exit 1
fi

echo "using immutable Actions artifact $artifact_id ($actual_digest)"
