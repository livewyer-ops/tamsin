#!/usr/bin/env bash
set -euo pipefail

source_files="$(go list -f '{{range .GoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .TestGoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .XTestGoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}' ./...)"
if [ -z "$source_files" ]; then
  exit 0
fi
mapfile -t files <<<"$source_files"

unformatted="$(gofmt -l "${files[@]}")"
if [ -n "$unformatted" ]; then
  printf 'Go files require gofmt:\n%s\n' "$unformatted" >&2
  exit 1
fi
