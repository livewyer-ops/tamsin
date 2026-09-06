# CLI reference

Run `tamsin --help` or `tamsin COMMAND --help` for the complete flag list for
the installed version.

## Commands

| Command | Purpose |
| --- | --- |
| `tamsin ingest` | Resolve inputs and create one TAMS Flow graph per input |
| `tamsin doctor` | Check local readiness; `--online` adds read-only TAMS checks |
| `tamsin profiles` | List the five built-in profiles and their trade-offs |
| `tamsin completion SHELL` | Generate completion for bash, zsh, fish or PowerShell |

The root command also accepts ingest arguments, so these are equivalent:

```sh
tamsin ingest --profile preserve --input programme.ts --endpoint https://tams.example.com
tamsin --profile preserve programme.ts https://tams.example.com
```

The explicit form is recommended in scripts. The retired `tamsin api` command
returns a migration message; use
[tamsctl](https://github.com/livewyer-ops/tamsctl) for TAMS administration.

## Common ingest flags

| Flag | Purpose |
| --- | --- |
| `-i`, `--input` | Input path or URI; repeat for a batch |
| `--profile` | Required built-in profile, optionally suffixed with `@1` |
| `-o`, `--endpoint` | TAMS API endpoint |
| `--dry-run` | `fast` for source-only planning or `exact` for media planning |
| `-j`, `--concurrency` | Inputs processed concurrently |
| `--transfers` | Uploads and verifications in flight across the run |
| `--probe-concurrency` | Queued FFprobe measurements; processes remain capped at two |
| `-d`, `--segment-duration` | Target Flow Segment duration; `0` disables segmentation |
| `--segment-format` | `source` or `mpegts` |
| `--essence-storage` | `independent` or `muxed` |
| `--verify` | `auto`, `readback` or `none` |
| `--storage-id` | Target storage backend UUID |
| `--tams-flow-profile` | TAMS 8.2 Profile assignment; repeat as required |
| `--ffmpeg-arg` | Explicit FFmpeg pass-through argument; repeat as required |
| `--flow-metadata` | Complete JSON metadata override |
| `--staging-byte-budget` | `auto` or a byte size such as `80GiB` |
| `--temp-dir` | Staging directory |

`--source-id` and `--flow-id` are valid only when exactly one input resolves.
Generated identities are deterministic when these overrides are absent.

## Input flags

| Flag | Purpose |
| --- | --- |
| `--stdin-name` | Filename hint for standard input |
| `--input-header` | HTTP header in `Name: value` form; repeat as required |
| `--s3-endpoint` | S3-compatible endpoint |
| `--s3-region` | AWS region override |
| `--s3-path-style` | Use path-style S3 addressing |
| `--max-inputs` | Limit expanded unique inputs |

## Output flags

| Flag | Purpose |
| --- | --- |
| `--format` | `human` or NDJSON `json` |
| `--progress` | `auto`, `plain` or `none` |
| `--log-level` | `debug`, `info`, `warn` or `error` |
| `--log-format` | `text` or `json` |
| `--color` | `auto`, `always` or `never` |
| `-q`, `--quiet` | Suppress successful human receipts and progress |
| `-v`, `--verbose` | Add Flow metadata and retained recovery details; NDJSON carries all Object records |

Stdout carries receipts or ingest events. Human diagnostics and progress use
stderr; JSON diagnostics and progress are part of the event stream.

## Authentication and transport

| Flag | Purpose |
| --- | --- |
| `--auth` | `auto`, `none`, `basic`, `bearer`, `url-token`, `oauth-client` or `oauth-code` |
| `--token` | Bearer token |
| `--username`, `--password` | HTTP Basic credentials |
| `--url-token` | TAMS `access_token` value |
| `--client-id`, `--client-secret` | OAuth client credentials |
| `--token-url` | OAuth token endpoint |
| `--oauth-code` | Pre-obtained OAuth code |
| `--pkce-verifier` | Verifier associated with that code |
| `--redirect-url` | Redirect URL used when obtaining the code |
| `--scope` | OAuth scope; repeat or comma-separate |
| `--timeout` | Per-request metadata timeout |
| `--transfer-idle-timeout` | Maximum transfer time without progress |
| `--transfer-timeout` | Optional complete-transfer deadline |
| `--retries` | Retry count for safe HTTP operations |
| `--insecure-skip-verify` | Disable TLS certificate checks; unsafe |
| `--allow-insecure-auth-loopback` | Permit credentials over explicit loopback HTTP; unsafe |

Prefer environment variables for secrets. See [authentication](../how-to/authenticate.md)
and [configuration](configuration.md).
