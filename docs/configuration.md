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

The keys below are the complete set. Any other `TAMSIN_*` variable or YAML
key fails with exit `2` before source resolution. `config` is accepted as a
flag or variable only. See the [CLI reference](cli.md) for defaults, value bounds
and flag meanings.

| Key | Flag | Environment |
| --- | --- | --- |
| `endpoint` | `-o`, `--endpoint` | `TAMSIN_ENDPOINT` |
| `input` | `-i`, `--input` | `TAMSIN_INPUT` |
| `config` | `--config` | `TAMSIN_CONFIG` |
| `format` | `--format` | `TAMSIN_FORMAT` |
| `progress` | `--progress` | `TAMSIN_PROGRESS` |
| `color` | `--color` | `TAMSIN_COLOR` |
| `quiet` | `-q`, `--quiet` | `TAMSIN_QUIET` |
| `verbose` | `-v`, `--verbose` | `TAMSIN_VERBOSE` |
| `log.level` | `--log-level` | `TAMSIN_LOG_LEVEL` |
| `log.format` | `--log-format` | `TAMSIN_LOG_FORMAT` |
| `auth.mode` | `--auth` | `TAMSIN_AUTH_MODE` |
| `auth.token` | `--token` | `TAMSIN_AUTH_TOKEN` |
| `auth.username` | `--username` | `TAMSIN_AUTH_USERNAME` |
| `auth.password` | `--password` | `TAMSIN_AUTH_PASSWORD` |
| `auth.url_token` | `--url-token` | `TAMSIN_AUTH_URL_TOKEN` |
| `auth.token_url` | `--token-url` | `TAMSIN_AUTH_TOKEN_URL` |
| `auth.client_id` | `--client-id` | `TAMSIN_AUTH_CLIENT_ID` |
| `auth.client_secret` | `--client-secret` | `TAMSIN_AUTH_CLIENT_SECRET` |
| `auth.scopes` | `--scope` | `TAMSIN_AUTH_SCOPES` |
| `auth.code` | `--oauth-code` | `TAMSIN_AUTH_CODE` |
| `auth.pkce_verifier` | `--pkce-verifier` | `TAMSIN_AUTH_PKCE_VERIFIER` |
| `auth.redirect_url` | `--redirect-url` | `TAMSIN_AUTH_REDIRECT_URL` |
| `auth.allow_insecure_loopback` | `--allow-insecure-auth-loopback` | `TAMSIN_AUTH_ALLOW_INSECURE_LOOPBACK` |
| `http.timeout` | `--timeout` | `TAMSIN_HTTP_TIMEOUT` |
| `http.deletion_timeout` | `--deletion-timeout` | `TAMSIN_HTTP_DELETION_TIMEOUT` |
| `http.transfer_timeout` | `--transfer-timeout` | `TAMSIN_HTTP_TRANSFER_TIMEOUT` |
| `http.transfer_idle_timeout` | `--transfer-idle-timeout` | `TAMSIN_HTTP_TRANSFER_IDLE_TIMEOUT` |
| `http.retries` | `--retries` | `TAMSIN_HTTP_RETRIES` |
| `http.insecure_skip_verify` | `--insecure-skip-verify` | `TAMSIN_HTTP_INSECURE_SKIP_VERIFY` |
| `ingest.profile` | `--profile` | `TAMSIN_INGEST_PROFILE` |
| `ingest.input_mode` | `--input-mode` | `TAMSIN_INGEST_INPUT_MODE` |
| `ingest.dry_run` | `--dry-run` | `TAMSIN_INGEST_DRY_RUN` |
| `ingest.verify` | `--verify` | `TAMSIN_INGEST_VERIFY` |
| `ingest.concurrency` | `-j`, `--concurrency` | `TAMSIN_INGEST_CONCURRENCY` |
| `ingest.transfers` | `--transfers` | `TAMSIN_INGEST_TRANSFERS` |
| `ingest.probe_concurrency` | `--probe-concurrency` | `TAMSIN_INGEST_PROBE_CONCURRENCY` |
| `ingest.segment_duration` | `-d`, `--segment-duration` | `TAMSIN_INGEST_SEGMENT_DURATION` |
| `ingest.segment_format` | `--segment-format` | `TAMSIN_INGEST_SEGMENT_FORMAT` |
| `ingest.essence_storage` | `--essence-storage` | `TAMSIN_INGEST_ESSENCE_STORAGE` |
| `ingest.start` | `--start` | `TAMSIN_INGEST_START` |
| `ingest.storage_id` | `--storage-id` | `TAMSIN_INGEST_STORAGE_ID` |
| `ingest.tams_flow_profiles` | `--tams-flow-profile` | `TAMSIN_INGEST_TAMS_FLOW_PROFILES` |
| `ingest.flow_metadata` | `--flow-metadata` | `TAMSIN_INGEST_FLOW_METADATA` |
| `ingest.collected_flow_metadata` | `--collected-flow-metadata` | `TAMSIN_INGEST_COLLECTED_FLOW_METADATA` |
| `ingest.source_id` | `--source-id` | `TAMSIN_INGEST_SOURCE_ID` |
| `ingest.flow_id` | `--flow-id` | `TAMSIN_INGEST_FLOW_ID` |
| `ingest.staging_byte_budget` | `--staging-byte-budget` | `TAMSIN_INGEST_STAGING_BYTE_BUDGET` |
| `ingest.temp_directory` | `--temp-dir` | `TAMSIN_INGEST_TEMP_DIRECTORY` |
| `ingest.max_inputs` | `--max-inputs` | `TAMSIN_INGEST_MAX_INPUTS` |
| `media.ffmpeg` | `--ffmpeg` | `TAMSIN_MEDIA_FFMPEG` |
| `media.ffprobe` | `--ffprobe` | `TAMSIN_MEDIA_FFPROBE` |
| `media.ffmpeg_args` | `--ffmpeg-arg` | `TAMSIN_MEDIA_FFMPEG_ARGS` |
| `source.stdin_name` | `--stdin-name` | `TAMSIN_SOURCE_STDIN_NAME` |
| `source.http_headers` | `--input-header` | `TAMSIN_SOURCE_HTTP_HEADERS` |
| `source.s3_endpoint` | `--s3-endpoint` | `TAMSIN_SOURCE_S3_ENDPOINT` |
| `source.s3_region` | `--s3-region` | `TAMSIN_SOURCE_S3_REGION` |
| `source.s3_path_style` | `--s3-path-style` | `TAMSIN_SOURCE_S3_PATH_STYLE` |

Credential values, `media.ffmpeg_args` and `source.http_headers` are treated
as secrets and never appear in diagnostics. On Unix, a configuration file
containing secrets that is group- or world-readable produces a warning.
