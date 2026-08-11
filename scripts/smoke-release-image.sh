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
immutable_ref="$image_ref@$index_digest"

"$script_dir/load-release-image.sh" "$bundle" "$image_ref" "$index_digest"

# Explicit --platform is significant: the arm64 invocation must execute via
# the QEMU handler installed by the release job, not accidentally smoke the
# runner-native amd64 member twice.
for platform in linux/amd64 linux/arm64; do
  user="$(docker image inspect \
    --platform "$platform" \
    "$immutable_ref" \
    --format '{{.Config.User}}')"
  if [ -z "$user" ]; then
    echo "$platform OCI image does not configure a non-root user" >&2
    exit 1
  fi
  docker run --rm --platform "$platform" "$immutable_ref" --help >/dev/null
  docker run --rm --platform "$platform" "$immutable_ref" --version >/dev/null
  docker run --rm --platform "$platform" "$immutable_ref" doctor --format json >/dev/null
  echo "smoked exact $platform member of $immutable_ref"
done
