#!/usr/bin/env bash
# Bundle dependency licences and corresponding source for binary distribution.
set -euo pipefail

output="${1:-dist/tamsin-third-party-licenses.tar.gz}"
source_date_epoch="${SOURCE_DATE_EPOCH:-0}"
if [[ ! "$source_date_epoch" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be a non-negative integer" >&2
  exit 2
fi

license_tmp="$(mktemp -d)"
trap 'rm -rf -- "$license_tmp"' EXIT

# The tool is a go.mod tool dependency, so its version and checksums are
# pinned in go.sum with everything else. Required source (not just licence
# names) is included for dependencies whose licences need it.
go tool go-licenses save ./cmd/tamsin \
  --save_path "$license_tmp/third-party-licenses"
tar --sort=name --mtime="@$source_date_epoch" --owner=0 --group=0 --numeric-owner \
  -C "$license_tmp" -cf - third-party-licenses | gzip -n > "$license_tmp/licenses.tar.gz"
install -m 0644 "$license_tmp/licenses.tar.gz" "$output"
