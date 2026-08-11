#!/usr/bin/env bash
set -Eeuo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 BUNDLE_DIR IMAGE_REF INDEX_DIGEST" >&2
  exit 2
fi

bundle="$1"
image_ref="$2"
index_digest="$3"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$script_dir/verify-oci-layout.sh" verify "$bundle" "$image_ref" "$index_digest"

layout="$(cd "$bundle/tamsin-image" && pwd -P)"
image_tag="${image_ref##*:}"
layout_ref="$layout:$image_tag"
raw_manifest="$(mktemp)"
existing_descriptor="$(mktemp)"
fetch_error="$(mktemp)"
trap 'rm -f "$raw_manifest" "$existing_descriptor" "$fetch_error"' EXIT
copy_flags=()
fetch_flags=()
if [ "${TAMSIN_ORAS_PLAIN_HTTP:-false}" = "true" ]; then
  copy_flags+=(--to-plain-http)
  fetch_flags+=(--plain-http)
fi

oras manifest fetch --oci-layout --output "$raw_manifest" "$layout_ref"
local_digest="sha256:$(sha256sum "$raw_manifest" | awk '{print $1}')"
if [ "$local_digest" != "$index_digest" ]; then
  echo "local OCI index bytes hash to '$local_digest', expected '$index_digest'" >&2
  exit 1
fi

# A rerun may find the version tag left by a completed or partially completed
# attempt. Reuse an identical index, but never replace a different release.
if oras manifest fetch \
  "${fetch_flags[@]}" \
  --descriptor \
  "$image_ref" >"$existing_descriptor" 2>"$fetch_error"; then
  existing_digest="$(python3 -c \
    'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["digest"])' \
    "$existing_descriptor")"
  if [ "$existing_digest" != "$index_digest" ]; then
    echo "refusing to replace $image_ref at $existing_digest with $index_digest" >&2
    exit 1
  fi
  echo "version image already contains exact index $image_ref@$index_digest"
elif grep -Eqi '(not found|manifest unknown|name unknown|404)' "$fetch_error"; then
  # ORAS explicitly supports copying an image index from an OCI image layout to
  # a registry. No Dockerfile or build context is involved here, so publication
  # cannot substitute a newly built image.
  oras cp --from-oci-layout --no-tty \
    "${copy_flags[@]}" \
    "$layout_ref" \
    "$image_ref"
else
  cat "$fetch_error" >&2
  echo "cannot establish whether $image_ref already exists; refusing to publish" >&2
  exit 1
fi

oras manifest fetch "${fetch_flags[@]}" --output "$raw_manifest" "$image_ref"
remote_bytes_digest="sha256:$(sha256sum "$raw_manifest" | awk '{print $1}')"
if [ "$remote_bytes_digest" != "$index_digest" ]; then
  echo "published OCI index bytes hash to '$remote_bytes_digest', expected '$index_digest'" >&2
  exit 1
fi

echo "published byte-identical OCI index $image_ref@$index_digest"
