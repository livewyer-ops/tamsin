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
| `tamsin config` | Validate and inspect configuration |
| `tamsin config validate` | Validate the effective configuration without running an ingest |
| `tamsin config show` | Show redacted effective values and their provenance |
| `tamsin doctor` | Check runtime dependencies and optional TAMS connectivity |
| `tamsin profiles` | List built-in ingest profiles and their resource trade-offs |
| `tamsin completion` | Generate a shell completion script |
| `tamsin help` | Show help for any command |

Invoking `tamsin` with no subcommand runs `ingest`, so
`tamsin --profile essence-segments -i input.mp4 -o URL` and
`tamsin ingest --profile essence-segments -i input.mp4 -o URL` are equivalent.

General TAMS inspection and administration are intentionally outside this
ingest CLI. Use the inspection and administration tooling provided for your
TAMS service for Flow, Profile, Segment, Object, storage-backend and raw API
operations.

## Configuration commands

| Command | Purpose |
| --- | --- |
| `config validate` | Validate known keys, YAML/environment types, and static value constraints without running an ingest |
| `config show --effective` | Emit redacted resolved values with default/file/environment/flag provenance |

Both commands honour `--config`, `TAMSIN_CONFIG`, the default configuration
path, and the normal precedence rules. Output is pretty JSON in the default
human presentation and compact JSON with `--format json`. These finite
commands still write one document; only `ingest --format json` is NDJSON.

## Profile catalogue

`tamsin profiles` lists all selectable profiles in stable presentation order,
including their version, storage arrangement, Segment target, format, FFmpeg
use, nominal Object pattern, intended use, and resource impact. It deliberately
does not load or validate configuration, so it remains available when a config
file is broken or absent.

With `--format json`, stdout is one document conforming to
[`profiles-report-v1.json`](../../contracts/tamsin/profiles-report-v1.json).
The report has `schema_version: "1.0"` and a separate
`profile_policy_version`. It is not ingest NDJSON.

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
| `--profile` |  | string | versioned ingest profile: preserve, demux, muxed-segments, essence-segments, mpegts-segments |
| `--segment-duration` | `-d` | duration | target duration of each TAMS Flow Segment; 0 disables segmentation, leaving storage to decide whole input or whole essence (default 10s) |
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
| `--profile` |  | string | versioned ingest profile: preserve, demux, muxed-segments, essence-segments, mpegts-segments |
| `--s3-endpoint` |  | string | S3-compatible endpoint URL |
| `--s3-path-style` |  |  | use path-style S3 addressing |
| `--s3-region` |  | string | AWS region override for S3 inputs |
| `--segment-duration` | `-d` | duration | target duration of each TAMS Flow Segment; 0 disables segmentation, leaving storage to decide whole input or whole essence (default 10s) |
| `--segment-format` |  | string | container for Flow Segments: source or mpegts (default "source") |
| `--source-id` |  | string | Source UUID for a single resolved input |
| `--start` |  | string | Flow start as a TAMS timestamp (default "0:0") |
| `--stdin-name` |  | string | filename hint; explicitly selects stdin unless input is configured or passed with --input (default "stdin.bin") |
| `--storage-id` |  | string | target TAMS storage backend ID |
| `--staging-byte-budget` |  | string | global temporary-media budget: auto or a byte size such as 80GiB (default "auto") |
| `--temp-dir` |  | string | staging directory |
| `--tams-flow-profile` |  | stringArray | assign a TAMS 8.2 Flow Profile as `[video|audio|image|data][:INDEX]=UUID` or a bare UUID (repeatable) |
| `--transfers` |  | int | maximum Media Object uploads and verifications in flight across the whole run (default: --concurrency) |
| `--verify` |  | string | Object integrity policy: auto, readback, or none (default "auto") |

`--profile` is required for ingest; doctor without one checks the dependencies
for every built-in profile. Bare `--dry-run` is shorthand for
`--dry-run=fast`. Use the equals form for `--dry-run=exact`, because an optional
flag value separated by a space is parsed as a positional argument.

`--tams-flow-profile` is independent of TAMSin's local treatment `--profile`.
It fetches an immutable TAMS 8.2 technical profile and requires the generated
Flow to match it exactly, except that the profile's target `avg_bit_rate` may
differ from measured output. Matching follows JSON value semantics: equal
numbers compare equal regardless of their decoded Go numeric type, including
integers larger than 2^53, while object fields, array order, strings, nulls,
omitted values, and empty values remain distinct as required by the Profile.
TAMSin does not materialise schema defaults to make a Profile match. A bare UUID
requires exactly one eligible essence Flow. Use `video=UUID`, `audio=UUID`, or
zero-based selectors such as `audio:1=UUID` when the graph contains several
candidates. Assignment becomes part of deterministic Flow identity. In dry-run
this option permits only the otherwise necessary `GET /service` and selected
Profile GETs; it never reads storage backends or mutates TAMS.

`--verify=auto` accepts trustworthy SHA-256 evidence returned by storage for a
new upload and falls back to downloading the registered Object. Resumed Objects
always use readback because their original upload evidence is no longer
available. `--verify=readback` forces downloads and `--verify=none` is an
explicit integrity opt-out.

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
| `config show` | `--effective` |  |  | show resolved values after precedence is applied |

For ingest, `--format human` writes grouped permanent receipts. `--verbose`
adds safe locators, Source IDs, dispositions, Object records, toolchain
provenance, byte counters, and non-zero retry/recovery details. `--quiet`
suppresses clean successful receipts and live progress, but never suppresses a
warning, failure, action-required state, or non-zero process status.

Ingest `--format json` writes the `tamsin.ingest.events` NDJSON process
protocol. Its stdout is machine-only; human progress is disabled regardless of
`--progress`. Finite `doctor`, `profiles`, and `config` commands retain one
command-specific JSON document. `--log-format` and `--log-level` independently
control sanitised support logs on stderr. See [ingest output protocol and
durable journal](result-contract.md) and [run and retry
observability](../explanation/observability.md).

## Argument forms

Both positional and flag forms are accepted:

```sh
tamsin --profile essence-segments input.mp4 https://tams.example.com     # input, then endpoint
tamsin --profile essence-segments -i input.mp4 -o https://tams.example.com
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
