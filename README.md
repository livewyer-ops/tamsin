# TAMSin

[![CI](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml/badge.svg)](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml)
[![Go 1.27](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](go.mod)
[![TAMS 8.2](https://img.shields.io/badge/BBC%20TAMS-8.2-5B2C6F)](docs/compatibility.md)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

TAMSin is a command-line utility for ingesting media into a
[BBC Time-addressable Media Store](https://github.com/bbc/tams). It creates
Flow graphs, uploads and registers Media Objects, and verifies the stored bytes.
It runs as a standalone client. [TAMOSS](https://github.com/livewyer-ops/tamoss)
is one compatible store; Go and Kubernetes are not required to run TAMSin.

Version `8.2.0-in3` accepts MP4 and MPEG-TS. Upgrading renews generated Flow
IDs. Read the [compatibility notes](docs/compatibility.md#media-policy-exception) before
resuming an ingest from an earlier version.

## Features

- Ingest files, directories, manifests, HTTP, S3 and standard input.
- Segment seekable HTTP and S3 inputs without downloading a complete local copy.
- Store a complete file, separate its tracks or cut it into time-addressable Segments.
- Resume using deterministic identities, with SHA-256 verification by default.
- Run interactively or from jobs and applications, with human receipts or streamed NDJSON results.

## Install

Download a 64-bit binary from [8.2.0-in3](https://github.com/livewyer-ops/tamsin/releases/tag/8.2.0-in3):

- Linux: [Intel/AMD](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in3/tamsin-linux-amd64), [ARM](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in3/tamsin-linux-arm64)
- macOS: [Intel](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in3/tamsin-darwin-amd64), [Apple Silicon (ARM)](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in3/tamsin-darwin-arm64)

Requires FFprobe 5.1+; rendered treatments also need FFmpeg 5.1+.
The [Docker image](#using-docker) includes both.

Download [SHA256SUMS](https://github.com/livewyer-ops/tamsin/releases/download/8.2.0-in3/SHA256SUMS)
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

To store an MP4 or MPEG-TS file as one Object with its original bytes, use
`--profile preserve`. Point `ENDPOINT` at a TAMS 8.2 or 8.1 service. Supply a
bearer token authorised to ingest in `TAMSIN_AUTH_TOKEN` through secret injection or an
[interactive prompt](docs/configuration.md#authentication).

```sh
ENDPOINT=https://tams.example.com
export TAMSIN_AUTH_MODE=bearer
tamsin ingest --profile preserve -i programme.mp4 -o "$ENDPOINT"
```

The first ingest reports `INGESTED AND VERIFIED`. Run the same command again
with the same file to read back the stored Object. The receipt reports
`RESUMED AND VERIFIED` with zero media bytes uploaded. [Your first ingest](docs/first-ingest.md)
walks through readiness checks, a generated audio sample and an exact dry run.

Allow temporary disk space and processing time, including for exact dry runs.
Video analysis can scan the entire input even with `preserve`, and default
verification may download uploaded Objects again. See
[resource budgets and verification costs](docs/operations.md).

When readers need short time ranges or individual tracks, choose a
[profile for that access pattern](#profiles). For remote media, follow
[Ingest an HTTP or S3 input](docs/configuration.md#ingest-an-http-or-s3-input).

### Using Docker

The image includes FFmpeg and FFprobe, runs as a non-root user, and supports
linux/amd64 and linux/arm64, with SBOM and provenance attestations.
Mount the same file and pass the endpoint and credentials to the container.

```sh
docker run --rm \
  --mount "type=bind,src=$PWD/programme.mp4,dst=/media/programme.mp4,readonly" \
  -e TAMSIN_ENDPOINT="$ENDPOINT" -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
  ghcr.io/livewyer-ops/tamsin:8.2.0-in3 \
  ingest --profile preserve --input /media/programme.mp4
```

The input must be readable by UID 65532 and the endpoint reachable from the
container. Append `--dry-run=exact` to plan locally without changing TAMS.
Pin deployed images by digest; see [container operation](docs/operations.md#containers)
for permissions and temporary storage.

## Profiles

TAMSin accepts MP4 and MPEG-TS, including audio-only MP4. Container detection
uses content and brands, not the filename. See [supported media](docs/profiles.md#supported-media-and-packaging)
for codec and subtitle carriage limits.

A treatment determines how bytes are packaged. An essence is an individual
video, audio, image or data stream. Built-in treatments are versioned as `@1`:

| Profile | Storage | Segment target | Use when |
| --- | --- | --- | --- |
| `preserve` | Original multiplex | Whole file | You need the original bytes unchanged |
| `demux` | One Flow per essence | Whole essence | You need complete individual streams |
| `muxed-segments` | Multiplexed | 10 seconds | You need time-range access to the complete multiplex |
| `essence-segments` | One Flow per essence | 10 seconds | You need time-range access to individual streams |
| `mpegts-segments` | One Flow per essence | 2 seconds, MPEG-TS | Your downstream system requires short MPEG-TS Objects |

Stream-copy cuts follow source keyframes. If you need to change the codec,
transcode with FFmpeg before ingest. To assign an existing TAMS 8.2 Flow
Profile, use `--tams-flow-profile`. Run `tamsin profiles` or read
[profiles and supported media](docs/profiles.md) for the choices and limits.

## Automation

Human receipts summarise each input; `--verbose` adds Flow and recovery details.
For a complete per-Object record, capture the ingest event stream:

```sh
tamsin ingest --format json --profile preserve --input ./programme.ts \
  > run.events.jsonl
```

JSON mode streams NDJSON on stdout; supporting logs use stderr. See the
[event protocol](docs/events.md) for payloads, examples and consumer rules.

## Documentation

Use the [documentation map](docs/README.md) to find a guide by task, or
`tamsin COMMAND --help` for the installed version's flags and defaults.

| Task | Start here |
| --- | --- |
| Ingest a sample and verify a retry | [Your first ingest](docs/first-ingest.md) |
| Understand the stored media and generated IDs | [Media, identity and verification](docs/concepts.md) |
| Configure a job or ingest remote media | [Configuration and inputs](docs/configuration.md) |
| Choose packaging | [Profiles and supported media](docs/profiles.md) |
| Recover an interrupted run | [Operations and recovery](docs/operations.md#resume-an-interrupted-ingest) |
| Integrate the CLI with an application | [CLI reference](docs/cli.md), [event protocol](docs/events.md) |
| Plan an upgrade | [Compatibility](docs/compatibility.md), [changelog](CHANGELOG.md) |

## Development

```sh
make verify
make dist
make image-smoke
```

`make e2e` exercises both pinned TAMOSS versions. See
[Contributing](CONTRIBUTING.md) for prerequisites and change guidance, and the
[release procedure](docs/releasing.md) for maintainers.

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
