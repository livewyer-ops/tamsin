# TAMSin

[![CI](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml/badge.svg)](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml)
[![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go)](go.mod)
[![TAMS 8.2](https://img.shields.io/badge/BBC%20TAMS-8.2-5B2C6F)](docs/compatibility.md)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

TAMSin is a command-line utility for ingesting media into a
[BBC Time-addressable Media Store](https://github.com/bbc/tams). It creates
Flow graphs, uploads and registers Media Objects, and verifies the stored bytes.

## Features

- Ingest files, directories, manifests, HTTP, S3 and standard input.
- Segment seekable HTTP and S3 inputs without downloading a complete local copy.
- Choose from five explicit packaging treatments, with FFmpeg passthrough for specialist options.
- Resume using deterministic identities, with SHA-256 verification by default.
- Run interactively or from jobs and applications, with human receipts or streamed NDJSON results.

## Install

Download a 64-bit binary from [8.2.0-in2](https://github.com/livewyer-ops/tamsin/releases/tag/8.2.0-in2):

- Linux: [Intel/AMD](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in2/tamsin-linux-amd64), [ARM](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in2/tamsin-linux-arm64)
- macOS: [Intel](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in2/tamsin-darwin-amd64), [Apple Silicon (ARM)](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in2/tamsin-darwin-arm64)

Requires FFprobe 5.1+; rendered treatments also need FFmpeg 5.1+.
The [Docker image](#using-docker) includes both.

Download [SHA256SUMS](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in2/SHA256SUMS)
into the same directory and verify the binary before installing:

<details>
<summary>Verify the download</summary>

Run the command for your OS, replacing the filename with your download's name.
Linux:

```sh
grep ' tamsin-linux-amd64$' SHA256SUMS | sha256sum --check
```

macOS:

```sh
grep ' tamsin-darwin-arm64$' SHA256SUMS | shasum -a 256 --check
```

Continue only if the command succeeds and reports the file as `OK`.

</details>

Install the verified binary (Linux Intel/AMD shown; use your downloaded filename):

```sh
mkdir -p ~/.local/bin && install -m 0755 tamsin-linux-amd64 ~/.local/bin/tamsin
```

Run `tamsin --version`. If your shell cannot find it, see [PATH setup](docs/operations.md#troubleshooting).

## Quick start

You need:

- A [supported media file](docs/profiles.md); replace `./programme.ts` below with its path.
- A TAMS 8.2 service, or a compatible 8.1 service, and credentials authorised to ingest.

Go and Kubernetes are not required to run the binary. If you need a store for
evaluation, see [TAMOSS](https://github.com/livewyer-ops/tamoss#quickstart).

Supply `TAMSIN_AUTH_TOKEN` through secret injection or an
[interactive prompt](docs/configuration.md#authentication), then set your endpoint:

```sh
export TAMSIN_ENDPOINT=https://tams.example.com
export TAMSIN_AUTH_MODE=bearer

tamsin doctor --online --profile essence-segments &&
tamsin ingest --profile essence-segments --dry-run=exact --input ./programme.ts &&
tamsin ingest --profile essence-segments --input ./programme.ts &&
tamsin ingest --profile essence-segments --input ./programme.ts
```

Doctor checks readiness without writing to TAMS. The exact dry run renders
locally and reports `PLANNED - NO CHANGES MADE`. A first successful ingest reports
`INGESTED AND VERIFIED`; repeating the same input and treatment reports
`RESUMED AND VERIFIED`, verifying existing Objects without uploading duplicates.
Each successful command exits `0`; the chain stops on failure.

Allow temporary disk space and processing time, including for exact dry runs.
Video analysis can scan the entire input even with `preserve`, and default
verification may download uploaded Objects again. See
[resource budgets and verification costs](docs/operations.md).

For remote inputs, segmented treatments stream automatically when the server
provides stable byte ranges. Closed segments use temporary disk space; the
whole source need not fit. Use `--input-mode=stage` for complete input preflight
before upload. See [input modes](docs/configuration.md#input-modes) for fallback
and identity rules.

### Using Docker

The image includes FFmpeg and FFprobe, runs as a non-root user, and supports
linux/amd64 and linux/arm64, with SBOM and provenance attestations.
Using the same endpoint and credential environment as above:

```sh
docker run --rm \
  --mount "type=bind,src=$PWD/programme.ts,dst=/media/programme.ts,readonly" \
  -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
  ghcr.io/livewyer-ops/tamsin:8.2.0-in2 \
  ingest --profile essence-segments --input /media/programme.ts
```

The input must be readable by UID 65532 and the endpoint reachable from the
container. Append `--dry-run=exact` to plan locally without changing TAMS.
Pin deployed images by digest; see [container operation](docs/operations.md#containers)
for permissions and temporary storage.

## Profiles

A treatment determines how bytes are packaged. An essence is an individual
video, audio, image or data stream. Built-in treatments are versioned as `@1`:

| Profile | Storage | Segment target | Use when |
| --- | --- | --- | --- |
| `preserve` | Original multiplex | Whole file | You need the original bytes unchanged |
| `demux` | One Flow per essence | Whole essence | You need complete individual streams |
| `muxed-segments` | Multiplexed | 10 seconds | You need time-range access to the complete multiplex |
| `essence-segments` | One Flow per essence | 10 seconds | You need time-range access to individual streams |
| `mpegts-segments` | One Flow per essence | 2 seconds, MPEG-TS | Your downstream system requires short MPEG-TS Objects |

Durations are nominal: stream-copy boundaries follow source keyframes.
`--ffmpeg-arg` supports explicit single-essence custom treatments with supplied
output metadata. These treatments are distinct from service-owned TAMS 8.2 Flow
Profiles assigned through `--tams-flow-profile`. Run `tamsin profiles` or read
[profiles and supported media](docs/profiles.md) for the choices and limits.

## Automation

Human receipts summarise each input; `--verbose` adds Flow and recovery details.
For a complete per-Object record, capture the ingest event stream:

```sh
tamsin ingest --format json --profile preserve --input ./programme.ts \
  > run.events.jsonl
```

JSON mode writes one NDJSON event at a time to stdout, including progress and
diagnostics; supporting logs use stderr. Flow planning records describe
identifiers, relationships, format, container and assigned Profile, not complete
Flow metadata. Consumers must drain both streams and require `run.finished`;
EOF before it is incomplete. The finite `doctor` and `profiles` commands emit
one JSON document instead. See [events and exit codes](docs/events.md).

## Documentation

Use `tamsin --help` or `tamsin COMMAND --help` for installed flags and defaults.

| Guide | Use it to |
| --- | --- |
| [CLI reference](docs/cli.md) | Look up commands, flags, defaults, value bounds and exit codes |
| [Configuration and inputs](docs/configuration.md) | Set credentials, YAML, environment variables and source options |
| [Profiles and media](docs/profiles.md) | Choose packaging and understand supported metadata and codecs |
| [Operations and recovery](docs/operations.md) | Check readiness, budget resources and investigate failures |
| [Events and exit codes](docs/events.md) | Integrate with the streaming process output |
| [Compatibility](docs/compatibility.md) | Check TAMS versions, Profile matching and upgrade boundaries |
| [Releasing](docs/releasing.md) | Build and publish verified binary and container releases |

## Development

```sh
make verify
make dist
make image-smoke
```

`make e2e` exercises both pinned TAMOSS versions. See
[Contributing](CONTRIBUTING.md) for prerequisites and change guidance, and the
[Changelog](CHANGELOG.md) for release notes.

## Support and licence

Use [GitHub Issues](https://github.com/livewyer-ops/tamsin/issues) for bugs and
focused proposals, with the version, platform and redacted doctor report.
Community support is best-effort. The latest `-inN` release for the newest
supported TAMS API version is supported; `-rcM` tags and `main` are
development snapshots. `go install` is not a supported install path: release
tags carry no `v` prefix, so Go tooling cannot resolve them; use the release
binaries or the image. Report security issues privately through
[Security](SECURITY.md).

TAMSin is licensed under the [Apache License 2.0](LICENSE).
