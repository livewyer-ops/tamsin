#!/usr/bin/env bash
set -Eeuo pipefail

if [ "$#" -ne 1 ] || [[ "$1" == *@* ]] || [[ "${1##*/}" != *:* ]]; then
  echo "usage: $0 IMAGE_TAG" >&2
  exit 2
fi

image_tag="$1"
temporary="$(mktemp)"
trap 'rm -f "$temporary"' EXIT
docker buildx imagetools inspect "$image_tag" --raw > "$temporary"

python3 - "$image_tag" "$temporary" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import sys

image_tag, raw_path = sys.argv[1:]
raw = Path(raw_path).read_bytes()
try:
    index = json.loads(raw)
except json.JSONDecodeError as error:
    raise SystemExit(f"{image_tag} does not resolve to JSON: {error}")

if not isinstance(index, dict) or index.get("schemaVersion") != 2:
    raise SystemExit(f"{image_tag} is not an OCI schema-version 2 index")
manifests = index.get("manifests")
if not isinstance(manifests, list):
    raise SystemExit(f"{image_tag} has no manifest list")

runnable = {}
attested = set()
digest_pattern = re.compile(r"^sha256:[0-9a-f]{64}$")
for descriptor in manifests:
    if not isinstance(descriptor, dict) or not isinstance(descriptor.get("platform"), dict):
        raise SystemExit(f"{image_tag} contains a descriptor without a platform")
    platform = descriptor["platform"]
    name = f"{platform.get('os')}/{platform.get('architecture')}"
    digest = descriptor.get("digest")
    if not isinstance(digest, str) or not digest_pattern.fullmatch(digest):
        raise SystemExit(f"{image_tag} contains an invalid manifest digest")
    if name == "unknown/unknown":
        annotations = descriptor.get("annotations")
        if isinstance(annotations, dict) and annotations.get("vnd.docker.reference.type") == "attestation-manifest":
            attested.add(annotations.get("vnd.docker.reference.digest"))
        continue
    if name in runnable:
        raise SystemExit(f"{image_tag} repeats {name}")
    runnable[name] = digest

expected = {"linux/amd64", "linux/arm64"}
if set(runnable) != expected:
    raise SystemExit(
        f"{image_tag} platforms are {', '.join(sorted(runnable))}; expected linux/amd64 and linux/arm64"
    )
if attested != set(runnable.values()):
    raise SystemExit(f"{image_tag} does not bind one SBOM/provenance attestation manifest to each platform")

digest = "sha256:" + hashlib.sha256(raw).hexdigest()
print(f"{image_tag}@{digest}")
PY
