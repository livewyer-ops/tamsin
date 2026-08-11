#!/usr/bin/env bash
set -Eeuo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 VERSION_REF INDEX_DIGEST LATEST_REF" >&2
  exit 2
fi

version_ref="$1"
index_digest="$2"
latest_ref="$3"
if [[ ! "$index_digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "invalid OCI index digest '$index_digest'" >&2
  exit 2
fi

raw_manifest="$(mktemp)"
trap 'rm -f "$raw_manifest"' EXIT
copy_flags=()
fetch_flags=()
if [ "${TAMSIN_ORAS_PLAIN_HTTP:-false}" = "true" ]; then
  copy_flags+=(--from-plain-http --to-plain-http)
  fetch_flags+=(--plain-http)
fi

verify_remote_index() {
  local image_ref="$1"
  local description="$2"
  local actual_bytes_digest

  oras manifest fetch "${fetch_flags[@]}" --output "$raw_manifest" "$image_ref"
  actual_bytes_digest="sha256:$(sha256sum "$raw_manifest" | awk '{print $1}')"
  if [ "$actual_bytes_digest" != "$index_digest" ]; then
    echo "$description image bytes hash to '$actual_bytes_digest', expected '$index_digest'" >&2
    exit 1
  fi
}

verify_remote_index "$version_ref" version

oras cp --no-tty \
  "${copy_flags[@]}" \
  "$version_ref@$index_digest" \
  "$latest_ref"

verify_remote_index "$latest_ref" latest

echo "promoted byte-identical multi-platform index to $latest_ref@$index_digest"
