# Configuration reference

TAMSin reads YAML from `--config`, `TAMSIN_CONFIG`, or the default platform user configuration path. Resolution precedence is:

1. command-line flags
2. `TAMSIN_*` environment variables
3. YAML configuration
4. built-in defaults

Nested YAML keys map to uppercase underscore-separated environment names. For example, `http.insecure_skip_verify` maps to `TAMSIN_HTTP_INSECURE_SKIP_VERIFY`.

Configuration is strict. A loaded file is rejected when it contains
an unknown key, a duplicate key, or a value of the wrong YAML type. Diagnostics
name the full dotted path and offer a likely correction for a misspelling. An
unsupported `TAMSIN_*` environment variable is rejected even when it is empty;
this prevents a misspelled deployment variable from silently selecting a
default now and becoming active in a later Secret or ConfigMap rollout. Empty
values for *known* environment variables remain unset, preserving the
precedence rules above.

Configuration paths must resolve to regular files and files are limited to 2
MiB, so parsing cannot block on a special file or consume unbounded startup
memory. Symlinks to regular files remain supported. This is a control-plane
document, not a media inventory; use an [input manifest](inputs.md) for an
inventory too large to fit comfortably in configuration.

Strings, booleans, integers, and lists of strings must use their corresponding
YAML types. Durations are strings with Go duration units (`30s`, `2m`, `1h30m`);
the integer `0` is also accepted where zero disables an option. In particular,
quote values such as TAMS timestamps that YAML might otherwise interpret as a
different scalar type.

List-valued environment variables accept the existing whitespace-separated
form for simple values. Use a JSON array whenever an individual value contains
spaces or when exact boundaries matter:

```sh
TAMSIN_INPUT='["asset one.mp4","asset,two.mp4"]'
TAMSIN_AUTH_SCOPES='["tams.read","tams.write"]'
TAMSIN_SOURCE_HTTP_HEADERS='["Authorization: Bearer value","X-Label: review copy"]'
```

A value beginning with `[` must be a valid JSON array containing only strings.
An empty JSON array deliberately overrides a lower-precedence configured list.

Validate the resolved file, environment, static value constraints, URL syntax,
and completeness of the selected authentication mode without probing media or
contacting TAMS:

```sh
tamsin --config /etc/tamsin/config.yaml config validate
```

Inspect every resolved value and whether it came from a default, file,
environment variable, or flag:

```sh
tamsin --config /etc/tamsin/config.yaml config show --effective
tamsin --format json config show --effective  # compact JSON
```

The output is deliberately not a reusable credential dump. Passwords, bearer
and URL tokens, client secrets, authorization codes, PKCE verifiers, HTTP
header values, and FFmpeg argument values are replaced with `<redacted>`.
User information and query values in configured URLs and HTTP(S) inputs are
redacted too. `source_detail` names the file, environment variable, or flag
without exposing its value.

Dynamic defaults are resolved to the value an ingest will use. For example,
`ingest.transfers` reports the resolved `ingest.concurrency` value and names it
in `resolved_from`; the original setting still reports `source: default` unless
the operator explicitly selected the dynamic default with file or environment
configuration.

`config show` accepts the global flags because they are meaningful to the
configuration command itself. Ingest-only flags such as `--concurrency` are
intentionally not accepted there; their file and environment equivalents are
still shown. Profile/treatment, staging, and storage-selection flags are also
accepted by `doctor`, where they resolve with the same precedence as ingest so
the readiness report describes the intended run.

An invalid file, environment name, or resolved value is represented by a
failed `configuration` check in `doctor` output. All checks that would consume
the partially resolved values are skipped, and `doctor --online` makes no TAMS
request in that state.

The keys most often set from the environment:

| YAML key | Environment | Flag |
| --- | --- | --- |
| `endpoint` | `TAMSIN_ENDPOINT` | `--endpoint`, `-o` |
| `format` | `TAMSIN_FORMAT` | `--format` |
| `color` | `TAMSIN_COLOR` | `--color` |
| `log.level` | `TAMSIN_LOG_LEVEL` | `--log-level` |
| `progress` | `TAMSIN_PROGRESS` | `--progress` |
| `quiet` | `TAMSIN_QUIET` | `--quiet`, `-q` |
| `verbose` | `TAMSIN_VERBOSE` | `--verbose`, `-v` |
| `ingest.profile` | `TAMSIN_INGEST_PROFILE` | `--profile` |
| `ingest.tams_flow_profiles` | `TAMSIN_INGEST_TAMS_FLOW_PROFILES` | `--tams-flow-profile` |
| `ingest.segment_duration` | `TAMSIN_INGEST_SEGMENT_DURATION` | `--segment-duration`, `-d` |
| `ingest.segment_format` | `TAMSIN_INGEST_SEGMENT_FORMAT` | `--segment-format` |
| `ingest.essence_storage` | `TAMSIN_INGEST_ESSENCE_STORAGE` | `--essence-storage` |
| `ingest.concurrency` | `TAMSIN_INGEST_CONCURRENCY` | `--concurrency`, `-j` |
| `ingest.max_inputs` | `TAMSIN_INGEST_MAX_INPUTS` | `--max-inputs` |
| `ingest.staging_byte_budget` | `TAMSIN_INGEST_STAGING_BYTE_BUDGET` | `--staging-byte-budget` |
| `ingest.transfers` | `TAMSIN_INGEST_TRANSFERS` | `--transfers` |
| `ingest.probe_concurrency` | `TAMSIN_INGEST_PROBE_CONCURRENCY` | `--probe-concurrency` |
| `ingest.dry_run` | `TAMSIN_INGEST_DRY_RUN` | `--dry-run` |
| `ingest.verify` | `TAMSIN_INGEST_VERIFY` | `--verify` |
| `ingest.journal` | `TAMSIN_INGEST_JOURNAL` | `--journal` |
| `http.timeout` | `TAMSIN_HTTP_TIMEOUT` | `--timeout` |
| `http.transfer_timeout` | `TAMSIN_HTTP_TRANSFER_TIMEOUT` | `--transfer-timeout` |
| `http.transfer_idle_timeout` | `TAMSIN_HTTP_TRANSFER_IDLE_TIMEOUT` | `--transfer-idle-timeout` |
| `http.insecure_skip_verify` | `TAMSIN_HTTP_INSECURE_SKIP_VERIFY` | `--insecure-skip-verify` |
| `auth.allow_insecure_loopback` | `TAMSIN_AUTH_ALLOW_INSECURE_LOOPBACK` | `--allow-insecure-auth-loopback` |

```yaml
endpoint: https://tams.example.com/v8.1
# The default is human. Select json for the ingest NDJSON process protocol.
format: json
progress: auto
color: auto
quiet: false
verbose: false

log:
  format: json
  level: info

http:
  # Bounds a TAMS metadata request end to end. These carry small JSON bodies,
  # so a wall-clock deadline suits them.
  timeout: 30s
  # Optional deadline for a complete Media Object transfer or HTTP source body.
  # Disabled by default: an absolute deadline covers reading or writing the
  # body, so it caps throughput, and a healthy transfer of a large Object would
  # fail once it outlived the clock.
  transfer_timeout: 0
  # A separate byte-progress deadline. It resets whenever bytes move, so a
  # healthy transfer may run for hours while a source, upload, or verification
  # body that stops moving fails and uses the configured retry budget.
  transfer_idle_timeout: 1m
  retries: 3
  insecure_skip_verify: false

auth:
  mode: oauth-client
  # Unsafe local-development exception. This permits credential-bearing HTTP
  # only for exact localhost or literal IPv4/IPv6 loopback destinations.
  allow_insecure_loopback: false
  token_url: https://identity.example.com/oauth/token
  client_id: tamsin-worker
  # Prefer TAMSIN_AUTH_CLIENT_SECRET rather than storing this value.
  scopes:
    - tams.write

ingest:
  # Required versioned media treatment. Run `tamsin profiles` for the catalogue.
  # A version may be pinned as essence-segments@1. Explicit media settings below
  # override it and a differing combination reports custom@1.
  profile: essence-segments
  # Optional immutable TAMS 8.2 technical contracts. A bare UUID is valid for
  # exactly one eligible Flow; repeated formats need a zero-based selector.
  tams_flow_profiles:
    - video=60d9df18-6d9d-4b86-84bf-d1dcf14b3a28
    - audio:0=8d5a25eb-35cb-423b-8e80-72258195ac2c
  # Optional stable root identities. Each is valid only when expansion resolves
  # exactly one input; an omitted identity is derived from the content.
  flow_id: ""
  source_id: ""
  concurrency: 4
  # Unique resolved inputs, after expanding directories, manifests, and S3
  # prefixes. This stops expansion while it is happening rather than after an
  # unbounded list has already been built.
  max_inputs: 10000
  # Global temporary-media capacity shared by every concurrent input. "auto"
  # reserves at most 80% of the staging filesystem's currently free space.
  # Real segmented inputs reserve a rolling completed-output window capped at
  # 512 MiB. Active Segments may temporarily cross that watermark but remain
  # subject to the global capacity guard. Unknown-length source streams take
  # the whole budget and stage alone.
  staging_byte_budget: auto
  # Media Object uploads and verifications in flight across the whole run.
  # Defaults to concurrency. This bound is global rather than per input, so a
  # single large file and many small ones draw on the same budget.
  transfers: 4
  # FFprobe measurements queued across the whole run. Defaults to two.
  # FFprobe and FFmpeg share a two-process local budget. Rolling FFmpeg renders
  # serialise so one slot remains available for the first-Segment probe.
  probe_concurrency: 2
  # off mutates TAMS; fast validates without rendering; exact performs the
  # complete local renderer/Object preparation path without TAMS access.
  # TAMS Flow Profile assignment is the exception: both modes read /service
  # and the selected immutable Profiles, but make no other TAMS request.
  dry_run: off
  # auto accepts trustworthy upload-side SHA-256 evidence and reads back when
  # evidence is unavailable; readback always downloads; none opts out.
  verify: auto
  # Exclusively create one JSONL journal, sync its complete input manifest,
  # each terminal input, and the final summary. Empty disables the journal.
  journal: ""
  start: "0:0"
  storage_id: ""
  # Target duration of each TAMS Flow Segment; 0 disables segmentation, while
  # essence storage decides whether Objects hold the input or each essence.
  # Cuts land on keyframes, so actual Segments vary around this. Any non-zero
  # value requires ffmpeg; zero with independent storage may require it too.
  segment_duration: 10s
  # Container for Flow Segments: source (the input's own) or mpegts.
  segment_format: source
  # How a muxed input is stored: independent (one Flow per essence) or muxed
  # (keep the multiplex, for retaining an original stream).
  essence_storage: independent
  temp_directory: ""
  flow_metadata: ""

source:
  # Hint-only when configured here. A fixed-purpose stdin job must also set
  # top-level input to ["-"]. The explicit CLI flag can select stdin itself.
  stdin_name: stdin.bin
  http_headers: []
  s3_region: ""
  s3_endpoint: ""
  s3_path_style: false

media:
  ffprobe: ffprobe
  ffmpeg: ffmpeg
  ffmpeg_args: []
```

Top-level `input` may be a YAML list for fixed-purpose jobs:

```yaml
input:
  - s3://incoming/day-001/
  - https://partner.example.com/asset.mp4
```

For a fixed-purpose stdin job, select the stream explicitly; the configured
name alone remains a reusable media-detection hint:

```yaml
input:
  - "-"
source:
  stdin_name: event.ts
```

## Authentication keys

| YAML key | Environment | Flag |
| --- | --- | --- |
| `auth.mode` | `TAMSIN_AUTH_MODE` | `--auth` |
| `auth.username` | `TAMSIN_AUTH_USERNAME` | `--username` |
| `auth.password` | `TAMSIN_AUTH_PASSWORD` | `--password` |
| `auth.token` | `TAMSIN_AUTH_TOKEN` | `--token` |
| `auth.url_token` | `TAMSIN_AUTH_URL_TOKEN` | `--url-token` |
| `auth.token_url` | `TAMSIN_AUTH_TOKEN_URL` | `--token-url` |
| `auth.authorization_url` | `TAMSIN_AUTH_AUTHORIZATION_URL` | `--authorization-url` |
| `auth.client_id` | `TAMSIN_AUTH_CLIENT_ID` | `--client-id` |
| `auth.client_secret` | `TAMSIN_AUTH_CLIENT_SECRET` | `--client-secret` |
| `auth.redirect_url` | `TAMSIN_AUTH_REDIRECT_URL` | `--redirect-url` |
| `auth.scopes` | `TAMSIN_AUTH_SCOPES` | `--scope` |
| `auth.code` | `TAMSIN_AUTH_CODE` | `--oauth-code` |
| `auth.pkce_verifier` | `TAMSIN_AUTH_PKCE_VERIFIER` | `--pkce-verifier` |
| `auth.allow_insecure_loopback` | `TAMSIN_AUTH_ALLOW_INSECURE_LOOPBACK` | `--allow-insecure-auth-loopback` |

Use environment injection from a Kubernetes Secret or workload identity where possible. Do not put credentials in an image, manifest source list, input URL other than the TAMS-defined `access_token`, or debug log. If a configuration file contains credentials, restrict it to the service account (`0600` on Unix-like systems).

The `config` selector itself cannot be placed inside a configuration file: the
file has already been selected by the time its contents are read. Use
`--config`, `TAMSIN_CONFIG`, or the platform default path.

Authenticated TAMS endpoints and OAuth token/authorization endpoints require HTTPS. Credential-bearing API requests are bound to the configured TAMS origin, including across redirects. The loopback exception above is opt-in and never widens to a private or remote address. The inbound authorization-code callback may use loopback HTTP without this setting; it is not an outbound credential destination. `http.insecure_skip_verify` changes certificate verification only and does not allow plaintext credentials.

## S3 credentials

The official AWS SDK for Go v2 resolves its standard chain, including environment variables, shared config/credentials files, ECS task roles, EKS workload identity, and EC2 instance roles. TAMSin-specific S3 flags only select region, endpoint, and path-style addressing; they do not implement a second credential format.

Common environment keys:

```sh
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...       # when issued
AWS_REGION=eu-west-1
```

## Output separation

`format` controls result stdout. `human` is the default. For ingest, `json`
means the versioned `tamsin.ingest.events` NDJSON stream, with one flushed
event per line; it does not mean one final batch document. Finite `doctor`,
`profiles`, and `config` operations continue to use one command-specific JSON
document. A wrapper should parse stdout and drain stderr concurrently as opaque
operator diagnostics.

`progress` accepts `auto`, `tty`, `plain`, or `none`:

- `auto` uses the coordinated inline renderer on a capable terminal and sparse
  newline-delimited milestones otherwise;
- `tty` requests the inline renderer, falling back to plain output for
  `TERM=dumb`;
- `plain` writes sparse milestones without cursor control;
- `none` disables transient progress without suppressing the permanent result.

Progress is phase-specific: storage and verification have separate cumulative
Object and byte counters. Before a total is final, no renderer claims a
percentage. In ingest JSON mode, progress is already carried by
`progress.snapshot` events and no human progress renderer writes to stderr.

`color` accepts `auto`, `always`, or `never`. `auto` uses colour only for a
capable terminal and honours the standard `NO_COLOR` environment variable and
`TERM=dumb`. Colour only reinforces status words; it never carries meaning.
`quiet` suppresses successful human receipts and live progress while retaining
warnings, failures, and action-required states. `verbose` expands safe human
provenance and recovery details; it does not change diagnostic log level.

`log.format` and `log.level` independently control sanitised support logs on
stderr. They do not alter event payloads or human receipts. Secret values are
never included in either channel, although paths, UUIDs, checksums, and media
metadata remain operationally sensitive.

`ingest.journal` names a new, one-run durable JSONL recovery companion. It is
not a copy of the stdout event stream: it syncs only the complete manifest,
terminal inputs, and final summary. The path must not exist. See the [ingest
output protocol and durable journal](result-contract.md) for stream handling,
versioning, completion indexes, and interruption semantics.
