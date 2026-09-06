# TAMSin

[![CI](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml/badge.svg)](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml)
[![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go)](go.mod)
[![TAMS 8.2](https://img.shields.io/badge/BBC%20TAMS-8.2-5B2C6F)](contracts/tams-v8.2.json)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

TAMSin is a small command-line utility for ingesting media into a
[Time-addressable Media Store](https://github.com/bbc/tams). It resolves local,
HTTP, S3 and standard-input sources; creates deterministic TAMS Flow graphs;
uploads and registers Media Objects; and verifies the stored bytes.

TAMSin only handles ingest. Use
[tamsctl](https://github.com/livewyer-ops/tamsctl) to inspect or administer a
TAMS service.

## Install

The latest published version is `v1.0.0-rc.3`, a pre-release. This branch
contains further changes for 1.0.0; the final release has not been published.

Download the appropriate binary from
[GitHub Releases](https://github.com/livewyer-ops/tamsin/releases), check it
against `SHA256SUMS`, and place it on `PATH`. For Linux amd64:

```sh
version=v1.0.0-rc.3
base="https://github.com/livewyer-ops/tamsin/releases/download/${version}"
curl --fail --location --remote-name "${base}/tamsin-linux-amd64"
curl --fail --location --remote-name "${base}/SHA256SUMS"
grep ' tamsin-linux-amd64$' SHA256SUMS | sha256sum --check
mkdir -p ~/.local/bin
install -m 0755 tamsin-linux-amd64 ~/.local/bin/tamsin
```

The container image includes FFmpeg and FFprobe, runs as a non-root user, and
is published for linux/amd64 and linux/arm64 with SBOM and provenance
attestations:

```sh
docker pull ghcr.io/livewyer-ops/tamsin:1.0.0-rc.3
docker run --rm ghcr.io/livewyer-ops/tamsin:1.0.0-rc.3 --version
```

Pin production images by digest.

## Quick start

The standalone binary requires FFmpeg and FFprobe 5.1 or newer. Set the TAMS
endpoint and credentials, check the environment, then ingest a file:

```sh
export TAMSIN_ENDPOINT=https://tams.example.com
export TAMSIN_AUTH_TOKEN='...'

tamsin doctor --online
tamsin ingest --profile essence-segments --input ./programme.ts
```

Use an exact dry run to inspect the planned Flow graph and media processing
without changing TAMS:

```sh
tamsin ingest --profile essence-segments --dry-run exact --input ./programme.ts
```

## Profiles

A profile makes the byte-packaging policy explicit. All built-in profiles are
versioned as `@1`:

| Profile | Storage | Object size | Typical use |
| --- | --- | --- | --- |
| `preserve` | Original multiplex | Whole file | Archive and interchange |
| `demux` | One Flow per essence | Whole essence | Analysis and downstream processing |
| `muxed-segments` | Multiplexed | 10 seconds | Time-range access to a complete multiplex |
| `essence-segments` | One Flow per essence | 10 seconds | TAMS-native production workflows |
| `mpegts-segments` | One Flow per essence | 2 seconds, MPEG-TS | Systems that require short MPEG-TS Objects |

`--ffmpeg-arg` deliberately remains available for media-tool options not
covered by TAMSin. Any profile override is reported as `custom@1`, making the
departure visible in receipts and events.

On TAMS 8.2, `--tams-flow-profile` assigns a service Flow Profile after TAMSin
has checked the generated technical metadata against it:

```sh
tamsin ingest --profile essence-segments \
  --tams-flow-profile video=60d9df18-6d9d-4b86-84bf-d1dcf14b3a28 \
  --tams-flow-profile audio:0=8d5a25eb-35cb-423b-8e80-72258195ac2c \
  --input ./programme.ts
```

See [profiles and supported media](docs/reference/profiles.md) for the complete
policy.

## Inputs and output

`--input` accepts files, directories, line-oriented manifests, HTTP URLs, S3
URIs and `-` for standard input. It may be repeated. Source expansion is
atomic: no TAMS mutation begins unless every requested input resolves.

Human output is a concise receipt. `--verbose` adds Flow identifiers, Object
totals and retained recovery details; use NDJSON for planned Flow metadata and
the complete per-Object record.
`--quiet` suppresses successful receipts. `--progress` accepts
`auto`, `plain` or `none`. Human progress writes only to stderr.

For automation, `--format json` writes one NDJSON event at a time to stdout.
Progress and diagnostics are events in that stream. The first event is `hello`;
a complete run ends with `run.finished`. Redirect stdout when a durable record
is needed:

```sh
tamsin ingest --format json --profile preserve --input ./programme.ts \
  > run.events.jsonl
```

Consumers must drain stdout and stderr concurrently and treat EOF before
`run.finished` as incomplete. See the [event protocol](docs/reference/result-contract.md)
and [exit codes](docs/reference/exit-codes.md).

## Configuration and authentication

TAMSin reads `tamsin/config.yaml` beneath the platform's user configuration
directory when present (`$XDG_CONFIG_HOME`, or `~/.config`, on Linux).
Precedence is flags, environment, file, then defaults. Unknown keys and invalid
types fail before ingest begins. Secrets should come from environment variables
or the standard AWS credential chain, not command-line arguments or YAML.

Bearer, basic, URL-token, OAuth client-credentials and pre-obtained OAuth
authorisation-code modes are supported. TAMSin never opens a browser or runs a
local OAuth callback server. See [configuration](docs/reference/configuration.md)
and [authentication](docs/how-to/authenticate.md).

## Compatibility

TAMSin targets TAMS 8.2 and retains a tested TAMS 8.1 compatibility floor.
The repository runs focused live TAMOSS tests for both versions. Read the
[compatibility policy](docs/explanation/compatibility.md),
[conformance notes](docs/explanation/conformance.md), and
[changelog](CHANGELOG.md) before updating a pinned deployment.

## Development

```sh
make verify
make dist
make image-smoke
```

`make e2e` runs the live TAMOSS compatibility matrix and requires Docker,
`kubectl`, `curl`, `jq`, Python 3, and the pinned tools installed through Aqua.
See [CONTRIBUTING.md](CONTRIBUTING.md) and the [documentation map](docs/README.md).
