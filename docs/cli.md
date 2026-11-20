# CLI reference

`tamsin --help` and `tamsin COMMAND --help` print the installed version's
flags and defaults. The [settings reference](configuration.md#settings) also lists YAML keys and environment variables.

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

See [ingest settings](configuration.md#ingest-settings) for profile, input, storage and verification flags.

Generated identities are deterministic when `--source-id` and `--flow-id` are
absent. Out-of-range values fail with exit `2`.

## Doctor flags

`doctor` takes no input. It accepts `--online`, which adds read-only service
and storage-backend requests, plus `--profile`, `--segment-duration`,
`--segment-format`, `--essence-storage`, `--ffmpeg-arg`,
`--staging-byte-budget`, `--storage-id` and `--temp-dir` from the
[settings reference](configuration.md#settings). See [operations](operations.md#check-readiness) for its checks and exit
categories.

## Global flags

Every command accepts the [global settings](configuration.md#global-settings).
Use `-h` or `--help` to show help for a command.

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
| `4` | At least one input failed, whether in its media, a TAMS write, a transfer or verification |
| `5` | Input discovery or reading failed |
| `6` | `doctor` only: FFprobe or FFmpeg is missing, fails its version check or is unsupported |
| `7` | Start-up failed before any input began: TAMS or Flow Profile preflight, FFprobe or the staging directory |
| `8` | The run was interrupted or its parent context ended |
| `130` | A second SIGINT or SIGTERM forced exit before cleanup finished |

Ingest reports a media failure in an input as `4` and a missing or unsupported
FFprobe as `7`. See [events](events.md#exit-codes)
for the JSON `run.finished` record and failure codes.
