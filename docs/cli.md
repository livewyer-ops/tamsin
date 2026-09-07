# CLI reference

`tamsin --help` and `tamsin COMMAND --help` print the installed version's
flags and defaults. Every flag has a configuration key and `TAMSIN_*`
environment variable; see [settings](configuration.md#settings).

## Commands

| Command | Purpose |
| --- | --- |
| `tamsin ingest` | Resolve inputs and create one TAMS Flow graph per input |
| `tamsin doctor` | Check local readiness; `--online` adds read-only TAMS checks |
| `tamsin profiles` | List the built-in profiles and their trade-offs |
| `tamsin completion SHELL` | Print a completion script for `bash`, `zsh`, `fish` or `powershell` |

The root command accepts every ingest flag plus two positional arguments, an
input and a TAMS endpoint, so these are equivalent:

```sh
tamsin ingest --profile preserve --input programme.ts --endpoint https://tams.example.com
tamsin --profile preserve programme.ts https://tams.example.com
```

Use the explicit form in scripts. `tamsin --version` prints the version.

## Ingest flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `-i`, `--input` | | Input path, URI or `-` for stdin; repeat for a batch |
| `--profile` | | Required built-in profile: `preserve`, `demux`, `muxed-segments`, `essence-segments` or `mpegts-segments`, optionally suffixed `@1` |
| `--input-mode` | `auto` | Remote input access: `auto`, `stream` or `stage` |
| `--dry-run` | `off` | Local-only planning: `fast` (source only) or `exact` (renders media) |
| `-j`, `--concurrency` | CPU count, at most 8 | Inputs ingested concurrently; 1 to 256 |
| `--transfers` | `--concurrency` | Uploads and verifications in flight across the run; 0 to 256, where `0` follows `--concurrency` |
| `--probe-concurrency` | `2` | Queued FFprobe measurements; 0 to 256; active media processes stay capped at two |
| `-d`, `--segment-duration` | `10s` | Target Flow Segment duration; `0` disables segmentation |
| `--segment-format` | `source` | Segment container: `source` or `mpegts` |
| `--essence-storage` | `independent` | `independent` (one Flow per essence) or `muxed` (keep the multiplex) |
| `--start` | `0:0` | Flow start as a TAMS timestamp |
| `--verify` | `auto` | Object integrity policy: `auto`, `readback` or `none` |
| `--storage-id` | | Target TAMS storage backend UUID |
| `--tams-flow-profile` | | TAMS 8.2 Profile assignment as `FORMAT[:INDEX]=UUID` or a bare UUID; repeat as required |
| `--ffmpeg-arg` | | Explicit FFmpeg pass-through argument; repeat as required |
| `--flow-metadata` | | Path to a JSON file of Flow metadata overrides, at most 2 MiB |
| `--source-id` | | Source UUID; requires exactly one resolved input |
| `--flow-id` | | Flow UUID; requires exactly one resolved input |
| `--staging-byte-budget` | `auto` | Global temporary-media budget: `auto` or a byte size such as `80GiB` |
| `--temp-dir` | system temporary directory | Staging directory |
| `--max-inputs` | `10000` | Maximum unique inputs after directory, manifest and S3 prefix expansion |
| `--stdin-name` | `stdin.bin` | Filename hint for stdin; selects stdin when no other input is given |
| `--input-header` | | HTTP input header as `Name: value`; repeat as required |
| `--s3-endpoint` | | S3-compatible endpoint URL |
| `--s3-region` | | AWS region override for S3 inputs |
| `--s3-path-style` | `false` | Use path-style S3 addressing |

Generated identities are deterministic when `--source-id` and `--flow-id` are
absent. Out-of-range values fail with exit `2`.

## Doctor flags

`doctor` takes no input. It accepts `--online`, which adds read-only service
and storage-backend requests, plus `--profile`, `--segment-duration`,
`--segment-format`, `--essence-storage`, `--ffmpeg-arg`,
`--staging-byte-budget`, `--storage-id` and `--temp-dir` with the meanings
above. See [operations](operations.md#check-readiness) for its checks and exit
categories.

## Global flags

Accepted by every command.

| Flag | Default | Purpose |
| --- | --- | --- |
| `-o`, `--endpoint` | | TAMS API endpoint |
| `--config` | `$XDG_CONFIG_HOME/tamsin/config.yaml` | Configuration file |
| `--format` | `human` | Result format: `human` or NDJSON `json` |
| `--progress` | `auto` | Progress reporting: `auto`, `plain` or `none` |
| `--log-level` | `info` | Diagnostic level: `debug`, `info`, `warn` or `error` |
| `--log-format` | `text` | Diagnostic log format: `text` or `json` |
| `--color` | `auto` | Colour output: `auto`, `always` or `never` |
| `-q`, `--quiet` | `false` | Suppress successful human output |
| `-v`, `--verbose` | `false` | Expanded human result details |
| `--ffmpeg` | `ffmpeg` | FFmpeg executable |
| `--ffprobe` | `ffprobe` | FFprobe executable |
| `--auth` | `auto` | Authentication: `auto`, `none`, `basic`, `bearer`, `url-token`, `oauth-client` or `oauth-code` |
| `--token` | | Bearer token; prefer `TAMSIN_AUTH_TOKEN` |
| `--username`, `--password` | | HTTP Basic credentials; prefer `TAMSIN_AUTH_PASSWORD` |
| `--url-token` | | TAMS `access_token` value; prefer the endpoint URL or environment |
| `--client-id`, `--client-secret` | | OAuth client credentials; prefer `TAMSIN_AUTH_CLIENT_SECRET` |
| `--token-url` | | OAuth token endpoint |
| `--oauth-code` | | Pre-obtained OAuth authorisation code |
| `--pkce-verifier` | | PKCE verifier for that code; prefer the environment |
| `--redirect-url` | `http://127.0.0.1:53682/callback` | Redirect URL used when obtaining the code |
| `--scope` | | OAuth scope; repeat or comma-separate |
| `--timeout` | `30s` | Per-request timeout for TAMS metadata operations |
| `--transfer-idle-timeout` | `1m0s` | Maximum time a media transfer may make no progress |
| `--transfer-timeout` | `0` (disabled) | Deadline for a complete media transfer |
| `--retries` | `3` | Retries for safe HTTP operations; 0 to 20 |
| `--insecure-skip-verify` | `false` | Skip TLS certificate verification; unsafe |
| `--allow-insecure-auth-loopback` | `false` | Allow credentials over HTTP to explicit loopback hosts; unsafe |
| `-h`, `--help` | | Help for the command |

Stdout carries receipts or ingest events. Human diagnostics and progress use
stderr; JSON diagnostics and progress are part of the event stream. Prefer
environment variables or a mode-`0600` configuration file for secrets; see
[authentication](configuration.md#authentication).

## Exit codes

| Code | Meaning |
| ---: | --- |
| `0` | Every input succeeded, resumed, or completed its dry run |
| `1` | Internal or output failure |
| `2` | Invalid arguments, configuration, environment variable or value |
| `3` | Authentication failed or credentials are unsafe for the transport |
| `4` | A batch completed with at least one failed input |
| `5` | Input discovery or reading failed |
| `6` | `doctor` only: FFprobe or FFmpeg is missing, fails its version check or is unsupported |
| `7` | TAMS preflight, mutation, transfer or verification failed |
| `8` | The run was interrupted or its parent context ended |
| `130` | A second SIGINT or SIGTERM forced exit before cleanup finished |

Ingest reports media failures as `4` or `7`. See [events](events.md#exit-codes)
for the JSON `run.finished` record and failure codes.
