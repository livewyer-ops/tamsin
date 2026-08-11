#!/usr/bin/env bash
# Build the licence and corresponding-source bundle required by binary releases.
set -Eeuo pipefail

output="${1:-dist/tamsin-third-party-licenses.tar.gz}"
source_date_epoch="${SOURCE_DATE_EPOCH:-0}"

if [[ ! "$source_date_epoch" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be a non-negative integer" >&2
  exit 2
fi
if [[ ! -d "$(dirname "$output")" ]]; then
  echo "output directory does not exist: $(dirname "$output")" >&2
  exit 2
fi

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/tamsin-licenses.XXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT

license_root="$temporary_root/third-party-licenses"
archive="$temporary_root/tamsin-third-party-licenses.tar.gz"

# v2.0.1 is checksum-verified by the Go module proxy. go-licenses retains full
# corresponding source where a compiled dependency's licence requires it.
go run github.com/google/go-licenses/v2@v2.0.1 save \
  ./cmd/tamsin \
  --save_path "$license_root"

tar \
  --sort=name \
  --mtime="@$source_date_epoch" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -C "$temporary_root" \
  -cf - \
  third-party-licenses \
  | gzip -n > "$archive"

install -m 0644 "$archive" "$output"
