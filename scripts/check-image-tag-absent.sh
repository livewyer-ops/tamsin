#!/usr/bin/env bash
# Succeed only when the registry reports that an image tag does not exist.
# Any other answer fails, including an error that is not "not found", so a
# published tag is never overwritten on a guess.
set -euo pipefail

image="${1:?usage: check-image-tag-absent.sh IMAGE}"
if lookup=$(docker buildx imagetools inspect "$image" 2>&1); then
  echo "$image already exists; publish under a new revision or version." >&2
  exit 1
fi
case "$lookup" in
  "ERROR: $image: not found") ;;
  *) printf 'Cannot establish that %s is absent: %s\n' "$image" "$lookup" >&2; exit 1 ;;
esac
