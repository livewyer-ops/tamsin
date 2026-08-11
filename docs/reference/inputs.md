# Inputs

Each `--input`/`-i` may be repeated. Expansion order is deterministic and duplicate canonical URIs are ingested once.

Expansion is capped at 10,000 unique inputs by default. Use `--max-inputs` to
set a deliberate lower or higher bound for a job. The cap is enforced while a
directory is walked, a manifest is read, or an S3 prefix is paged, so a broad
input cannot first consume unbounded memory and only then fail.

| Input | Behaviour |
| --- | --- |
| Regular file | Creates one Flow and one or more Media Objects. |
| Directory | Recursively imports regular files in lexical path order; symlinks are not followed. |
| `.txt` file | Reads one path or supported URI per UTF-8 line; blank lines and `#` comments are ignored. Relative paths resolve from the manifest directory. Manifest cycles fail. |
| `s3://bucket` or `s3://bucket/prefix/` | Imports all matching Objects in lexical key order using AWS SDK for Go v2. A complete object key imports one Object. |
| HTTP(S) URL | Streams one remote object; repeat `--input-header 'Name: value'` when the source requires a header. |
| `-` | Stages stdin once; use `--stdin-name` to supply an extension for media detection. An explicit `--stdin-name` also selects stdin when neither `--input` nor configured inputs exist. Its persisted locator is always `stdin:`. |

Examples:

```sh
# Recursive directory
tamsin --profile editorial -i /media/drop -o https://tams.example.com/v8.1

# Source manifest
tamsin --profile editorial -i sources.txt -o https://tams.example.com/v8.1

# S3-compatible store
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  tamsin --profile editorial -i s3://incoming/day-001/ \
  --s3-endpoint https://objects.example.com --s3-path-style \
  -o https://tams.example.com/v8.1

# Event payload on stdin. An explicit --stdin-name makes -i - optional.
cat event.ts | tamsin --profile editorial --stdin-name event.ts -o https://tams.example.com/v8.1

# Render the complete local plan without changing TAMS
tamsin --profile editorial -i sources.txt --dry-run=exact --format json

# Segments default to a 10s target; -d 0 stores the whole input as one Media Object
tamsin --profile editorial -i input.mov -d 0 -o https://tams.example.com/v8.1
```

## Resolution rules

- Expansion order is deterministic, and duplicate canonical URIs are ingested once.
- Directory traversal is lexical by path; symlinks are not followed.
- Manifest paths resolve relative to the manifest's own directory, and manifest cycles fail rather than looping.
- Duplicate canonical URIs do not count twice towards `--max-inputs`.
- A generated Flow graph is independent of the locator that delivered it. The
  staged content digest, resolved media treatment, and normalised technical
  interpretation determine its root; children derive from that root. Identical
  content reached through two manifests, signed URLs, local paths, or S3 keys
  therefore resumes one graph when its treatment and interpretation agree.
- Persisted provenance removes URL userinfo, the entire query, and the fragment.
  HTTP, file, and S3 paths remain visible in the `_tamsin_sources` Flow tag, so
  do not put credentials in a path component. A refreshed signed query never
  changes generated identity or appears in results, logs, descriptions, or
  Flow tags.
- File inputs are hashed in place. Keep them immutable until the command
  finishes. TAMSin checks immediately before the first rolling write and again
  after media processing, but only a private copy could close every
  change-and-revert window. A late change fails the input; already registered
  Segments remain an explicit resumable prefix rather than being hidden.

Resolution is an atomic batch boundary. TAMSin expands and structurally
validates every top-level selector before it starts the ingest pipeline,
contacts TAMS, or creates a result journal. If one path, manifest line,
directory walk, or S3
listing fails, exit code 5 describes that selection failure and no otherwise
valid sibling is processed. Once resolution succeeds, every indexed item gets
exactly one terminal `input.finished` event in JSON mode, a corresponding human
receipt unless a clean success is quieted, and an optional journal record.

This is deliberate in the ingest process contract: the synced journal start record
contains the complete, stable input manifest. Continuing past a selection error
would make that manifest unknowable and blur a job-definition error into a
media failure.

## Dry-run modes

Both modes skip every TAMS read and mutation. Bare `--dry-run` is the fast mode:
it stages and probes each source, builds and validates the Flow graph, but skips
FFmpeg demultiplexing and segmentation. It is the lower-cost choice for checking
selection, media support, identifiers, and metadata. Because no rendered bytes
exist, it does not claim exact Object IDs, timeranges, sizes, or checksums.

`--dry-run=exact` performs the complete local preparation path: staging,
probing, demultiplexing or segmentation, hashing, Flow-graph construction,
metadata/schema validation, and staging-capacity enforcement. It consumes
roughly the same FFmpeg time as a real ingest, but no network storage bandwidth.
Closed outputs are measured, reported, and removed through the same bounded
rolling window, so exact mode does not need to retain the whole render. Use it
only when exact Object records are needed.
The equals sign is required for an explicit optional mode.

In either mode, `flow.planned` publishes the graph and root/parent hierarchy as
soon as it is known. Exact mode then streams each `object.result`; the terminal
`flow.result` carries compact disposition and verification counters. The
`input.finished` payload's verification is `not_reached` when verification was
requested. Use `doctor` for the cheapest runtime/configuration check: it does
not stage or render media.

## Temporary-media capacity

Remote inputs are staged under `--temp-dir` before probing. Segmentation and
independent-essence extraction write their outputs there too. A real segmented
ingest commits and removes closed outputs in rolling batches; its peak therefore
includes the downloaded source plus a bounded output window, not the complete
generated programme. Zero-duration essence extraction still has to close its
single output before it can be uploaded.

`--staging-byte-budget auto` is the default and uses at most 80% of the space
currently available on that filesystem. A fixed-purpose job should normally
set an explicit size such as `--staging-byte-budget 80GiB` and provision a
larger filesystem or Kubernetes `emptyDir` around it. The budget is global:
concurrent inputs reserve from one ledger. An input with an unknown length
(HTTP or stdin without trusted size metadata) reserves the whole budget and
stages alone. TAMSin reports the estimated required and available byte counts
when preflight fails, and monitors FFmpeg output growth against the reservation.
Known-size real segmented ingests reserve at most 512 MiB of generated-output
headroom per active input. FFmpeg pauses at the reservation's high watermark and
resumes after committed files bring usage below the low watermark.

The non-rolling estimate assumes stream-copy output plus framing overhead.
Zero-duration whole-essence extraction with explicit FFmpeg arguments has an
unknowable output size and conservatively reserves the whole budget. Real and
exact-dry-run segmented custom treatments use the same rolling cap; fast dry
runs reserve no generated-output space. One Segment may exceed a window before
FFmpeg can close it; that case either extends the lease from free global
capacity or fails with required/available byte counts. The budget is a
process-level guard, not a filesystem quota; retain filesystem or pod-level
limits as the final enforcement boundary.

## See also

- [CLI reference](cli.md)
- [Configuration reference](configuration.md)
