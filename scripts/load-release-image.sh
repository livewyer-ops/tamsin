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

amd64_digest="$(<"$bundle/IMAGE_AMD64_DIGEST")"
arm64_digest="$(<"$bundle/IMAGE_ARM64_DIGEST")"
amd64_config_digest="$(<"$bundle/IMAGE_AMD64_CONFIG_DIGEST")"
arm64_config_digest="$(<"$bundle/IMAGE_ARM64_CONFIG_DIGEST")"

layout="$bundle/tamsin-image"
immutable_ref="$image_ref@$index_digest"

# Docker's containerd image store retains every variant from an OCI layout.
# The release and E2E jobs enable it explicitly; a legacy graphdriver daemon
# must fail here instead of silently retaining only the host architecture.
tar -C "$layout" -cf - . | docker load

platforms=(linux/amd64 linux/arm64)
manifest_digests=("$amd64_digest" "$arm64_digest")
config_digests=("$amd64_config_digest" "$arm64_config_digest")
inspect_json="$(mktemp)"
trap 'rm -f "$inspect_json"' EXIT
for index in "${!platforms[@]}"; do
  docker image inspect \
    --platform "${platforms[$index]}" \
    "$immutable_ref" \
    --format '{{json .}}' >"$inspect_json"
  python3 - \
    "$inspect_json" \
    "${platforms[$index]}" \
    "${manifest_digests[$index]}" \
    "${config_digests[$index]}" <<'PY'
import json
import sys

path, platform, expected_manifest, expected_config = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    image = json.load(handle)
expected_os, expected_architecture = platform.split("/", 1)
if image.get("Os") != expected_os or image.get("Architecture") != expected_architecture:
    raise SystemExit(
        f"loaded {platform} selection reports "
        f"{image.get('Os')}/{image.get('Architecture')}"
    )

# Docker's containerd image store exposes the selected OCI manifest through
# Descriptor and currently uses that digest as Id. The legacy graphdriver store
# has no Descriptor and uses the config digest as Id. Accept both documented
# representations, but never compare a manifest expectation to a config ID.
descriptor = image.get("Descriptor")
image_id = image.get("Id")
if descriptor:
    if descriptor.get("digest") != expected_manifest:
        raise SystemExit(
            f"loaded {platform} manifest is {descriptor.get('digest')!r}, "
            f"expected {expected_manifest!r}"
        )
    if image_id not in (expected_manifest, expected_config):
        raise SystemExit(
            f"loaded {platform} image ID is {image_id!r}; expected manifest "
            f"{expected_manifest!r} or config {expected_config!r}"
        )
elif image_id != expected_config:
    raise SystemExit(
        f"loaded {platform} config is {image_id!r}, expected {expected_config!r}"
    )
PY
done

echo "loaded exact multi-platform release image $immutable_ref"
