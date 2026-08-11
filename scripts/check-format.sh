#!/usr/bin/env bash
set -euo pipefail

mapfile -t files < <(
  go list -f '{{range .GoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .TestGoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}' ./...
)

if [ "${#files[@]}" -eq 0 ]; then
  exit 0
fi

unformatted="$(gofmt -l "${files[@]}")"
if [ -n "$unformatted" ]; then
  printf 'Go files require gofmt:\n%s\n' "$unformatted" >&2
  exit 1
fi
