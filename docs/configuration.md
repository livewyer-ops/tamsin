# Configuration, authentication and inputs

Use `tamsin --help` and `tamsin COMMAND --help` for the installed version's
flags and defaults. Settings resolve in order: command-line flags, non-empty
`TAMSIN_*` environment variables, YAML configuration, then built-in defaults.

The default file is `$XDG_CONFIG_HOME/tamsin/config.yaml`, or beneath the
platform configuration directory when `XDG_CONFIG_HOME` is unset. Select another
file with `--config PATH` or `TAMSIN_CONFIG`. An explicitly selected missing file
is an error; an absent default file is not. Unknown or duplicate keys, wrong
types and multiple YAML documents fail before source resolution or mutation.

Keep only the settings you need:

```yaml
endpoint: https://tams.example.com
ingest:
  profile: essence-segments
  concurrency: 2
  staging_byte_budget: 80GiB
  temp_directory: /var/tmp/tamsin
auth:
  mode: bearer
```

Every key has an environment form: uppercase it, replace dots with underscores
and prepend `TAMSIN_`. For example, `ingest.profile` becomes
`TAMSIN_INGEST_PROFILE`, and `http.timeout` becomes `TAMSIN_HTTP_TIMEOUT`.
Durations require units except integer `0`, which disables optional timeouts
or segmentation. YAML lists contain strings; environment lists accept
whitespace-separated values or a JSON array when an item contains spaces.
Repeat list flags where the CLI permits it.

## Authentication

Prefer workload secret injection or environment variables to flags, which can
appear in process listings. Literal exports may also enter shell history.
Select the mode explicitly for unattended work.

For an interactive bearer-token session, prompt without recording the token
in shell history (Bash):

```bash
read -r -s -p 'TAMS token: ' TAMSIN_AUTH_TOKEN
printf '\n'
export TAMSIN_AUTH_TOKEN
```

In a job, inject the same variable through your workload's secret mechanism.

| Mode (`TAMSIN_AUTH_MODE`) | Credential variables (prefix `TAMSIN_AUTH_`) |
| --- | --- |
| `bearer` | `TOKEN` |
| `basic` | `USERNAME`, `PASSWORD` |
| `url-token` | `URL_TOKEN`, or an endpoint containing `access_token` |
| `oauth-client` | `TOKEN_URL`, `CLIENT_ID`, `CLIENT_SECRET`; optional `SCOPES` |
| `oauth-code` | `TOKEN_URL`, `CLIENT_ID`, `CODE`, `REDIRECT_URL`; `PKCE_VERIFIER` when used to obtain the code |
| `none` | No credentials |

For example, with credentials supplied by the workload:

```sh
export TAMSIN_AUTH_MODE=oauth-client
export TAMSIN_AUTH_TOKEN_URL=https://identity.example.com/oauth/token
export TAMSIN_AUTH_SCOPES=tams.write
tamsin doctor --online
```

Authorisation-code mode exchanges a code obtained separately. Default mode
`auto` selects URL token, bearer, OAuth code, OAuth client credentials, Basic,
then none from the complete credentials present. Incomplete or ambiguous OAuth
settings fail.

Credentials require HTTPS and are sent only to the configured TAMS origin.
OAuth token redirects are not followed. `--allow-insecure-auth-loopback` is an
explicit development exception for `localhost`, `127.0.0.0/8` and `::1`, not
private-network or remote HTTP endpoints. `--insecure-skip-verify` disables
certificate checks for API, OAuth, storage, HTTP input and S3 requests; it is
unsafe and does not permit plaintext authentication.

Keep configuration private. Input headers and FFmpeg arguments may contain
secrets too. TAMSin redacts known credentials and URL queries from its own
diagnostics, but cannot make an unsafe external FFmpeg argument harmless.
See [Security](../SECURITY.md).

## Ingest an HTTP or S3 input

Configure your TAMS endpoint and [authentication](#authentication) first.
Source credentials are separate from credentials for the destination store.

For HTTP, use the media URL in an exact dry run, then ingest with the same
settings:

```sh
tamsin ingest --profile essence-segments --dry-run=exact \
  --input https://media.example.com/programme.ts
tamsin ingest --profile essence-segments \
  --input https://media.example.com/programme.ts
```

With `auto` input mode, a stable source with byte ranges can stream; otherwise
TAMSin logs a fallback and stages it. The exact dry run reads and renders media
but makes no TAMS writes. Keep a signed URL valid for the whole run, including
range reads. Supply private input headers through `source.http_headers` in a
protected configuration file or `TAMSIN_SOURCE_HTTP_HEADERS` from your secret
mechanism. TAMS credentials are not source credentials.

For S3, configure the normal AWS credential chain with read access to the
source, then select the region if the environment does not supply it:

```sh
tamsin ingest --profile essence-segments --s3-region eu-west-1 \
  --dry-run=exact --input s3://media-bucket/programme.ts
tamsin ingest --profile essence-segments --s3-region eu-west-1 \
  --input s3://media-bucket/programme.ts
```

For an S3-compatible service, add `--s3-endpoint` and, if required by that
service, `--s3-path-style` to both commands. A key ending in `/` expands to a
prefix, so use a concrete key when only one object is intended.

Add `--input-mode=stage` to both commands when the whole source must pass
preflight before the first upload. Allow space for the full source and rendered
output. Keep the mode and source revision unchanged for a retry; see
[input modes](#input-modes) and [recovery](operations.md#resume-an-interrupted-ingest).

## Inputs

Repeat `--input` for a batch. It accepts a file, directory, line-oriented
manifest, HTTP(S) URL, S3 URI, or `-` for standard input. A positional input is
equivalent to one `--input`; `--stdin-name` selects stdin only when no other
input was supplied. `--source-id` and `--flow-id` require exactly one resolved
input; otherwise identifiers are generated deterministically.

Directories expand recursively in lexical order, including regular files only
and never following symlinks. Manifest lines are trimmed; blanks and `#`
comments are ignored. Relative paths resolve from the manifest directory.
Nested manifests are allowed up to 32 levels; cycles fail. Duplicate local
paths and canonical remote locators are removed, keeping the first occurrence.
`--max-inputs` bounds the final set.

Expansion is atomic: if any requested source cannot be resolved, no partial
manifest is reported and no TAMS mutation begins.

For HTTP, repeat `--input-header 'Name: value'` as needed. Headers are removed
on cross-origin redirects; redirects to non-HTTP schemes are rejected.
Structured locators omit URL user information, queries and fragments.

S3 uses the standard AWS credential chain. `--s3-region`, `--s3-endpoint` and
`--s3-path-style` support compatible stores. A URI ending in `/` is a prefix;
a concrete key identifies one object.

Standard input is staged once because FFprobe and FFmpeg may reopen it. It may
appear once alongside other inputs. `--stdin-name` is a filename hint, not an
override of content-derived container detection:

```sh
producer | tamsin ingest --profile preserve --input - --stdin-name programme.ts
```

## Input modes

`--input-mode` (YAML `ingest.input_mode`, environment
`TAMSIN_INGEST_INPUT_MODE`) controls remote input access:

| Mode | Behaviour |
| --- | --- |
| `auto` (default) | Stream eligible HTTP(S) and S3 inputs; warn and stage when streaming is unavailable |
| `stream` | Require streaming for remote inputs; fail if it cannot be established |
| `stage` | Download remote inputs completely and run full preflight before uploading |

Streaming requires stream-copy segmentation, a known source length and working
byte ranges. HTTP needs a strong ETag; S3 uses a Version ID or ETag. Every read
is pinned to that revision. Changed sources, authentication errors and invalid
range responses fail; they do not trigger a restart onto different bytes.
HTTP ranges reuse the exact URL discovered after initial redirects. A signed
URL must remain valid for subsequent range requests; TAMSin does not refresh it
by following the original redirect again. A redirect from that pinned URL to a
different resource fails even if its ETag and length match.

`preserve`, whole-file `demux` and any `--ffmpeg-arg` use staging in `auto`.
Explicit `stream` rejects those remote workflows and cannot be combined with
`--ffmpeg-arg` for any input. Local files are read in place and stdin is staged
in every mode. A source that streams may be larger than `--staging-byte-budget`,
but its active segments must fit that budget.

Go handles remote credentials, redirects and TLS. FFprobe and FFmpeg read
through a private loopback endpoint containing no upstream credentials. No
persistent server or media cache is installed. Idle timeouts apply while
waiting for source bytes, not while uploads have paused the renderer.

If initial segments cannot establish metadata for every essence within the
spool, `auto` releases the spool and stages before any TAMS mutation. `stream`
fails instead. Later contradictions stop the ingest and can leave an already
committed valid prefix; see [recovery](operations.md#recovery).

## Settings

Unknown YAML keys and `TAMSIN_*` variables fail with exit `2` before input
resolution. A dash in the default column means the value is unset.
Use `--config` or `TAMSIN_CONFIG` to select the configuration file.

### Global settings

| Flag | YAML key | Environment | Default | Meaning |
| --- | --- | --- | --- | --- |
| `-o`, `--endpoint` | `endpoint` | `TAMSIN_ENDPOINT` | — | TAMS API endpoint |
| `--config` | — | `TAMSIN_CONFIG` | `$XDG_CONFIG_HOME/tamsin/config.yaml` | Configuration file |
| `--format` | `format` | `TAMSIN_FORMAT` | `human` | Result format: `human` or NDJSON `json` |
| `--progress` | `progress` | `TAMSIN_PROGRESS` | `auto` | Progress reporting: `auto` or `none`. `plain` is an alias for `auto` |
| `--log-level` | `log.level` | `TAMSIN_LOG_LEVEL` | `info` | Diagnostic level: `debug`, `info`, `warn` or `error` |
| `--log-format` | `log.format` | `TAMSIN_LOG_FORMAT` | `text` | Diagnostic log format: `text` or `json` |
| `--color` | `color` | `TAMSIN_COLOR` | `auto` | Colour output: `auto`, `always` or `never` |
| `-q`, `--quiet` | `quiet` | `TAMSIN_QUIET` | `false` | Suppress successful human output |
| `-v`, `--verbose` | `verbose` | `TAMSIN_VERBOSE` | `false` | Expanded human result details |
| `--ffmpeg` | `media.ffmpeg` | `TAMSIN_MEDIA_FFMPEG` | `ffmpeg` | FFmpeg executable |
| `--ffprobe` | `media.ffprobe` | `TAMSIN_MEDIA_FFPROBE` | `ffprobe` | FFprobe executable |
| `--auth` | `auth.mode` | `TAMSIN_AUTH_MODE` | `auto` | Authentication: `auto`, `none`, `basic`, `bearer`, `url-token`, `oauth-client` or `oauth-code` |
| `--token` | `auth.token` | `TAMSIN_AUTH_TOKEN` | — | Bearer token. Prefer `TAMSIN_AUTH_TOKEN` |
| `--username` | `auth.username` | `TAMSIN_AUTH_USERNAME` | — | HTTP Basic username |
| `--password` | `auth.password` | `TAMSIN_AUTH_PASSWORD` | — | HTTP Basic password |
| `--url-token` | `auth.url_token` | `TAMSIN_AUTH_URL_TOKEN` | — | TAMS `access_token` value. Prefer the endpoint URL or environment |
| `--client-id` | `auth.client_id` | `TAMSIN_AUTH_CLIENT_ID` | — | OAuth client identifier |
| `--client-secret` | `auth.client_secret` | `TAMSIN_AUTH_CLIENT_SECRET` | — | OAuth client secret |
| `--token-url` | `auth.token_url` | `TAMSIN_AUTH_TOKEN_URL` | — | OAuth token endpoint |
| `--oauth-code` | `auth.code` | `TAMSIN_AUTH_CODE` | — | Pre-obtained OAuth authorisation code |
| `--pkce-verifier` | `auth.pkce_verifier` | `TAMSIN_AUTH_PKCE_VERIFIER` | — | PKCE verifier for that code. Prefer the environment |
| `--redirect-url` | `auth.redirect_url` | `TAMSIN_AUTH_REDIRECT_URL` | `http://127.0.0.1:53682/callback` | Redirect URL used when obtaining the code |
| `--scope` | `auth.scopes` | `TAMSIN_AUTH_SCOPES` | — | OAuth scope. Repeat or comma-separate |
| `--timeout` | `http.timeout` | `TAMSIN_HTTP_TIMEOUT` | `30s` | Timeout for each attempt of a TAMS metadata request |
| `--deletion-timeout` | `http.deletion_timeout` | `TAMSIN_HTTP_DELETION_TIMEOUT` | `5m0s` | Deadline for one Segment deletion, including the service's deletion request and confirmation that the Segment is gone |
| `--transfer-idle-timeout` | `http.transfer_idle_timeout` | `TAMSIN_HTTP_TRANSFER_IDLE_TIMEOUT` | `1m0s` | Maximum time a media transfer may make no progress |
| `--transfer-timeout` | `http.transfer_timeout` | `TAMSIN_HTTP_TRANSFER_TIMEOUT` | `0` (disabled) | Deadline for a complete media transfer |
| `--retries` | `http.retries` | `TAMSIN_HTTP_RETRIES` | `3` | Retries for safe HTTP operations (0 to 20) |
| `--insecure-skip-verify` | `http.insecure_skip_verify` | `TAMSIN_HTTP_INSECURE_SKIP_VERIFY` | `false` | Skip TLS certificate verification (unsafe) |
| `--allow-insecure-auth-loopback` | `auth.allow_insecure_loopback` | `TAMSIN_AUTH_ALLOW_INSECURE_LOOPBACK` | `false` | Allow credentials over HTTP to explicit loopback hosts (unsafe) |

### Ingest settings

| Flag | YAML key | Environment | Default | Meaning |
| --- | --- | --- | --- | --- |
| `-i`, `--input` | `input` | `TAMSIN_INPUT` | — | MP4 or MPEG-TS path, URI or `-` for stdin. Repeat for a batch |
| `--profile` | `ingest.profile` | `TAMSIN_INGEST_PROFILE` | — | Required built-in profile: `preserve`, `demux`, `muxed-segments`, `essence-segments` or `mpegts-segments`, optionally suffixed `@1` |
| `--input-mode` | `ingest.input_mode` | `TAMSIN_INGEST_INPUT_MODE` | `auto` | Remote input access: `auto`, `stream` or `stage` |
| `--dry-run` | `ingest.dry_run` | `TAMSIN_INGEST_DRY_RUN` | `off` | Local-only planning: `fast` (source only) or `exact` (renders media) |
| `-j`, `--concurrency` | `ingest.concurrency` | `TAMSIN_INGEST_CONCURRENCY` | CPU count, at most 8 | Inputs ingested concurrently (1 to 256) |
| `--transfers` | `ingest.transfers` | `TAMSIN_INGEST_TRANSFERS` | `--concurrency` | Uploads and verifications in flight across the run (0 to 256), where `0` follows `--concurrency` |
| `--probe-concurrency` | `ingest.probe_concurrency` | `TAMSIN_INGEST_PROBE_CONCURRENCY` | `2` | Queued FFprobe measurements (0 to 256). At most two media processes run at once |
| `-d`, `--segment-duration` | `ingest.segment_duration` | `TAMSIN_INGEST_SEGMENT_DURATION` | profile | Overrides the profile's target Flow Segment duration. `0` disables segmentation |
| `--segment-format` | `ingest.segment_format` | `TAMSIN_INGEST_SEGMENT_FORMAT` | profile | Overrides the profile's Segment container: `source` or `mpegts` |
| `--essence-storage` | `ingest.essence_storage` | `TAMSIN_INGEST_ESSENCE_STORAGE` | profile | Overrides the profile's storage: `independent` (one Flow per essence) or `muxed` (keep the multiplex) |
| `--start` | `ingest.start` | `TAMSIN_INGEST_START` | `0:0` | Flow start as a TAMS timestamp |
| `--verify` | `ingest.verify` | `TAMSIN_INGEST_VERIFY` | `auto` | Object integrity policy: `auto`, `readback` or `none` |
| `--storage-id` | `ingest.storage_id` | `TAMSIN_INGEST_STORAGE_ID` | — | Target TAMS storage backend UUID |
| `--tams-flow-profile` | `ingest.tams_flow_profiles` | `TAMSIN_INGEST_TAMS_FLOW_PROFILES` | — | TAMS 8.2 Profile assignment as `FORMAT[:INDEX]=UUID` or a bare UUID. Repeat as required |
| `--ffmpeg-arg` | `media.ffmpeg_args` | `TAMSIN_MEDIA_FFMPEG_ARGS` | — | Explicit FFmpeg pass-through argument. Repeat as required |
| `--flow-metadata` | `ingest.flow_metadata` | `TAMSIN_INGEST_FLOW_METADATA` | — | Path to a JSON file of Flow metadata overrides, at most 2 MiB |
| `--collected-flow-metadata` | `ingest.collected_flow_metadata` | `TAMSIN_INGEST_COLLECTED_FLOW_METADATA` | — | Path to a JSON file of per-role overrides for collected essence Flows, keyed by collection role such as `video` or `audio 1`, at most 2 MiB |
| `--source-id` | `ingest.source_id` | `TAMSIN_INGEST_SOURCE_ID` | — | Source UUID for a single resolved input |
| `--flow-id` | `ingest.flow_id` | `TAMSIN_INGEST_FLOW_ID` | — | Flow UUID for a single resolved input |
| `--staging-byte-budget` | `ingest.staging_byte_budget` | `TAMSIN_INGEST_STAGING_BYTE_BUDGET` | `auto` | Global temporary-media budget: `auto` or a byte size such as `80GiB` |
| `--temp-dir` | `ingest.temp_directory` | `TAMSIN_INGEST_TEMP_DIRECTORY` | system temporary directory | Staging directory |
| `--max-inputs` | `ingest.max_inputs` | `TAMSIN_INGEST_MAX_INPUTS` | `10000` | Maximum unique inputs after directory, manifest and S3 prefix expansion |
| `--stdin-name` | `source.stdin_name` | `TAMSIN_SOURCE_STDIN_NAME` | `stdin.bin` | Filename hint for stdin. Selects stdin when no other input is given |
| `--input-header` | `source.http_headers` | `TAMSIN_SOURCE_HTTP_HEADERS` | — | HTTP input header as `Name: value`. Repeat as required |
| `--s3-endpoint` | `source.s3_endpoint` | `TAMSIN_SOURCE_S3_ENDPOINT` | — | S3-compatible endpoint URL |
| `--s3-region` | `source.s3_region` | `TAMSIN_SOURCE_S3_REGION` | — | AWS region override for S3 inputs |
| `--s3-path-style` | `source.s3_path_style` | `TAMSIN_SOURCE_S3_PATH_STYLE` | `false` | Use path-style S3 addressing |

Credential values, `media.ffmpeg_args` and `source.http_headers` are treated
as secrets and never appear in diagnostics. On Unix, a configuration file
containing secrets that is group- or world-readable produces a warning.
