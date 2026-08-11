# CLI reference

Checked against `tamsin --help`. The persistent and ingest settings below are
also settable through configuration; see the [configuration
reference](configuration.md) for the corresponding YAML keys and `TAMSIN_*`
environment names. Command-specific operands and utility switches listed later
are flags only.

## Commands

| Command | Purpose |
| --- | --- |
| `tamsin ingest` | Create one Flow graph per resolved input and ingest its media |
| `tamsin api` | Execute TAMS upload and ingest API operations |
| `tamsin api service` | Get TAMS service information |
| `tamsin api storage-backends` | List TAMS storage backends |
| `tamsin api flow` | Get or create a TAMS Flow |
| `tamsin api flow get` | Get Flow metadata |
| `tamsin api flow put` | Create or replace Flow metadata |
| `tamsin api storage` | Allocate TAMS Media Object storage |
| `tamsin api storage allocate` | Allocate upload URLs for a Flow |
| `tamsin api segment` | List, register, or delete TAMS Flow Segments |
| `tamsin api segment list` | List Flow Segments |
| `tamsin api segment register` | Register a Flow Segment |
| `tamsin api segment delete` | Delete the exact Flow Segment selected by Object ID and timerange |
| `tamsin api object` | Inspect Objects and manage Object instances |
| `tamsin api object get` | Get Object information |
| `tamsin api object instance` | Register or delete Object instances |
| `tamsin api object instance register` | Register a controlled or external Object instance |
| `tamsin api object instance delete` | Delete one Object instance |
| `tamsin api request` | Execute a raw JSON request against the pinned TAMS API |
| `tamsin config` | Validate and inspect configuration |
| `tamsin config validate` | Validate the effective configuration without running an ingest |
| `tamsin config show` | Show redacted effective values and their provenance |
| `tamsin doctor` | Check runtime dependencies and optional TAMS connectivity |
| `tamsin completion` | Generate a shell completion script |
| `tamsin help` | Show help for any command |

Invoking `tamsin` with no subcommand runs `ingest`, so
`tamsin --profile editorial -i input.mp4 -o URL` and
`tamsin ingest --profile editorial -i input.mp4 -o URL` are equivalent.

## Configuration commands

| Command | Purpose |
| --- | --- |
| `config validate` | Validate known keys, YAML/environment types, and static value constraints without running an ingest |
| `config show --effective` | Emit redacted resolved values with default/file/environment/flag provenance |

Both commands honour `--config`, `TAMSIN_CONFIG`, the default configuration
path, and the normal precedence rules. Output is pretty JSON in the default
human presentation and compact JSON with `--format json`. These finite
commands still write one document; only `ingest --format json` is NDJSON.

## Doctor flags

`doctor` resolves the same profile, media, staging, endpoint, authentication,
and transport settings as ingest. It does not require an input and never
creates a Flow, Object, or Segment. See the [doctor report reference](doctor.md)
for check and output semantics.

| Flag | Short | Type | Description |
| --- | --- | --- | --- |
| `--essence-storage` |  | string | how a muxed input is stored: independent or muxed (default "independent") |
| `--ffmpeg-arg` |  | stringArray | additional explicit FFmpeg argument (repeatable) |
| `--help` | `-h` |  | help for doctor |
| `--online` |  |  | also run the read-only TAMS startup preflight |
| `--profile` |  | string | versioned ingest profile: preserve, editorial, or streaming-ts |
| `--segment-duration` | `-d` | duration | target duration of each TAMS Flow Segment; 0 stores the whole input as one Media Object (default 10s) |
| `--segment-format` |  | string | container for Flow Segments: source or mpegts (default "source") |
| `--staging-byte-budget` |  | string | global temporary-media budget: auto or a byte size such as 80GiB (default "auto") |
| `--storage-id` |  | string | target TAMS storage backend ID |
| `--temp-dir` |  | string | staging directory |

## Ingest flags

| Flag | Short | Type | Description |
| --- | --- | --- | --- |
| `--concurrency` | `-j` | int | maximum concurrent input ingests (default: CPU count, at most 8) |
| `--dry-run` |  | string | local-only planning mode: fast or exact (default "off") |
| `--essence-storage` |  | string | how a muxed input is stored: independent (one Flow per essence) or muxed (keep the multiplex) (default "independent") |
| `--ffmpeg-arg` |  | stringArray | additional explicit FFmpeg argument (repeatable) |
| `--flow-id` |  | string | Flow UUID for a single resolved input |
| `--flow-metadata` |  | string | JSON Flow metadata overrides |
| `--help` | `-h` |  | help for ingest |
| `--input` | `-i` | stringArray | input path or URI (repeatable) |
| `--input-header` |  | stringArray | HTTP input header as 'Name: value' (repeatable) |
| `--journal` |  | string | create a new one-run durable JSONL result file |
| `--max-inputs` |  | int | maximum unique inputs after directory, manifest, and S3 prefix expansion (default 10000) |
| `--probe-concurrency` |  | int | maximum queued FFprobe measurements (local media processes are capped at two) |
| `--profile` |  | string | versioned ingest profile: preserve, editorial, or streaming-ts |
| `--s3-endpoint` |  | string | S3-compatible endpoint URL |
| `--s3-path-style` |  |  | use path-style S3 addressing |
| `--s3-region` |  | string | AWS region override for S3 inputs |
| `--segment-duration` | `-d` | duration | target duration of each TAMS Flow Segment; 0 stores the whole input as one Media Object (default 10s) |
| `--segment-format` |  | string | container for Flow Segments: source or mpegts (default "source") |
| `--source-id` |  | string | Source UUID for a single resolved input |
| `--start` |  | string | Flow start as a TAMS timestamp (default "0:0") |
| `--stdin-name` |  | string | filename hint; explicitly selects stdin unless input is configured or passed with --input (default "stdin.bin") |
| `--storage-id` |  | string | target TAMS storage backend ID |
| `--staging-byte-budget` |  | string | global temporary-media budget: auto or a byte size such as 80GiB (default "auto") |
| `--temp-dir` |  | string | staging directory |
| `--transfers` |  | int | maximum Media Object uploads and verifications in flight across the whole run (default: --concurrency) |
| `--verify` |  | string | Object integrity policy: auto, readback, or none (default "auto") |

`--profile` is required for ingest; doctor without one checks the dependencies
for every built-in profile. Bare `--dry-run` is shorthand for
`--dry-run=fast`. Use the equals form for `--dry-run=exact`, because an optional
flag value separated by a space is parsed as a positional argument.

`--verify=auto` accepts trustworthy SHA-256 evidence returned by storage for a
new upload and falls back to downloading the registered Object. Resumed Objects
always use readback because their original upload evidence is no longer
available. `--verify=readback` forces downloads and `--verify=none` is an
explicit integrity opt-out.

## Segment-list flags

`tamsin api segment list FLOW_ID` follows every page of the Segment listing.
By default it asks TAMS for a lean response with no `get_urls`; this avoids
generating or printing presigned storage URLs when only Object IDs and
timeranges are needed. TAMSin also removes any `get_urls` a non-conforming
service returns despite that request, on every page of the listing.

| Flag | Type | Description |
| --- | --- | --- |
| `--object-id` | string | filter by Object ID |
| `--include-download-urls` | bool | include presigned download URLs and verbose storage metadata |

Use `--include-download-urls` only when the output will be used immediately;
presigned URLs are credentials with a service-defined short lifetime. Paging
cursors may be absolute, path-relative, or query-only, but TAMSin follows them
only when RFC URL resolution keeps them on the configured API origin and under
its base path. Collection is all-or-nothing and stops with an error rather than
returning a partial list if it would exceed 1,000 pages, 100,000 Segments, or
64 MiB of successful JSON page bodies.

## Global flags

These apply to every command.

| Flag | Short | Type | Description |
| --- | --- | --- | --- |
| `--allow-insecure-auth-loopback` |  |  | allow credentials over HTTP to explicit loopback hosts (unsafe) |
| `--auth` |  | string | authentication: auto, none, basic, bearer, url-token, oauth-client, or oauth-code (default "auto") |
| `--authorization-url` |  | string | OAuth authorization endpoint |
| `--client-id` |  | string | OAuth client ID |
| `--client-secret` |  | string | OAuth client secret (prefer TAMSIN_AUTH_CLIENT_SECRET) |
| `--color` |  | string | colour output: auto, always, or never (default "auto") |
| `--config` |  | string | configuration file (default: $XDG_CONFIG_HOME/tamsin/config.yaml) |
| `--endpoint` | `-o` | string | TAMS API endpoint |
| `--ffmpeg` |  | string | ffmpeg executable (default "ffmpeg") |
| `--ffprobe` |  | string | ffprobe executable (default "ffprobe") |
| `--format` |  | string | result format: human or json (default "human") |
| `--insecure-skip-verify` |  |  | skip TLS certificate verification (unsafe) |
| `--log-format` |  | string | diagnostic log format: text or json (default "text") |
| `--log-level` |  | string | diagnostic level: debug, info, warn, or error (default "info") |
| `--oauth-code` |  | string | pre-obtained OAuth authorization code |
| `--password` |  | string | HTTP basic password (prefer TAMSIN_AUTH_PASSWORD) |
| `--pkce-verifier` |  | string | PKCE verifier for a pre-obtained OAuth code (prefer environment) |
| `--progress` |  | string | progress reporting: auto, tty, plain, or none (default "auto") |
| `--quiet` | `-q` |  | suppress successful human output |
| `--redirect-url` |  | string | OAuth authorization-code redirect URL (default "http://127.0.0.1:53682/callback") |
| `--retries` |  | int | retry count for safe HTTP operations (default 3) |
| `--scope` |  | strings | OAuth scope (repeat or comma-separate) |
| `--timeout` |  | duration | per-request timeout for metadata operations (default 30s) |
| `--token` |  | string | bearer token (prefer TAMSIN_AUTH_TOKEN) |
| `--token-url` |  | string | OAuth token endpoint |
| `--transfer-idle-timeout` |  | duration | maximum time a network media transfer may make no progress (default 1m0s) |
| `--transfer-timeout` |  | duration | optional deadline for a complete media transfer (0 disables) |
| `--url-token` |  | string | TAMS access_token value (prefer endpoint URL or environment) |
| `--username` |  | string | HTTP basic username |
| `--verbose` | `-v` |  | show expanded human result details |

## Root-only flag

| Flag | Short | Type | Description |
| --- | --- | --- | --- |
| `--version` |  |  | version for tamsin |

## Command-specific flags

Persistent flags from the preceding table work on every command. The following
flags belong only to the command named in the first column.

| Command | Flag | Short | Type | Description |
| --- | --- | --- | --- | --- |
| `api flow put` | `--file` | `-f` | string | Flow JSON file or `-` for stdin (default "-") |
| `api storage allocate` | `--object-id` |  | stringArray | requested Object ID (repeatable) |
| `api storage allocate` | `--storage-id` |  | string | storage backend ID |
| `api storage allocate` | `--limit` |  | int | number of server-assigned Object IDs |
| `api segment list` | `--object-id` |  | string | filter by Object ID |
| `api segment list` | `--include-download-urls` |  |  | include presigned download URLs and verbose storage metadata |
| `api segment register` | `--file` | `-f` | string | Segment JSON file or `-` for stdin (default "-") |
| `api segment delete` | `--timerange` |  | string | required TAMS timerange to delete, or `_` for the whole Flow |
| `api segment delete` | `--object-id` |  | string | required exact Object ID |
| `api object instance register` | `--storage-id` |  | string | controlled storage backend ID |
| `api object instance register` | `--url` |  | string | external Object URL |
| `api object instance register` | `--label` |  | string | instance label; required with `--url` |
| `api object instance delete` | `--storage-id` |  | string | controlled storage backend ID |
| `api object instance delete` | `--label` |  | string | external instance label |
| `api request` | `--file` | `-f` | string | JSON request body file or `-` for stdin |
| `config show` | `--effective` |  |  | show resolved values after precedence is applied |

`api storage allocate` requires exactly one allocation mode: repeat
`--object-id` for caller-selected identifiers, or supply one positive `--limit`
to request server-selected identifiers. The two modes cannot be combined.

For ingest, `--format human` writes grouped permanent receipts. `--verbose`
adds safe locators, Source IDs, dispositions, Object records, toolchain
provenance, byte counters, and non-zero retry/recovery details. `--quiet`
suppresses clean successful receipts and live progress, but never suppresses a
warning, failure, action-required state, or non-zero process status.

Ingest `--format json` writes the `tamsin.ingest.events` NDJSON process
protocol. Its stdout is machine-only; human progress is disabled regardless of
`--progress`. Finite `api`, `doctor`, and `config` commands retain one
command-specific JSON document. `--log-format` and `--log-level` independently
control sanitised support logs on stderr. See [ingest output protocol and
durable journal](result-contract.md) and [run and retry
observability](../explanation/observability.md).

## Argument forms

Both positional and flag forms are accepted:

```sh
tamsin --profile editorial input.mp4 https://tams.example.com     # input, then endpoint
tamsin --profile editorial -i input.mp4 -o https://tams.example.com
```

Values beginning with `-` must use the `=` form so they are not parsed as flags, which matters for `--ffmpeg-arg` and for OAuth client IDs that start with a hyphen:

```sh
tamsin --ffmpeg-arg=-c:v --ffmpeg-arg=libx264 ...
```

## See also

- [Configuration reference](configuration.md)
- [Doctor report](doctor.md)
- [Exit codes](exit-codes.md)
- [Ingest output protocol and durable journal](result-contract.md)
