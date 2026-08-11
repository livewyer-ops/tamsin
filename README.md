# TAMSin

[![CI](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml/badge.svg)](https://github.com/livewyer-ops/tamsin/actions/workflows/ci.yml)
[![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go)](go.mod)
[![TAMS 8.1](https://img.shields.io/badge/BBC%20TAMS-8.1-5B2C6F)](contracts/tams-v8.1.json)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

TAMSin is a process-oriented CLI for ingesting files and object collections
into a [Time-addressable Media Store](https://github.com/bbc/tams). It resolves
inputs, creates deterministic TAMS Flow graphs, stores Media Objects through
TAMS-provided URLs, registers timeline Segments, and verifies the stored bytes.

It is designed for operators at a shell and for automation in OCI containers,
Kubernetes Jobs, event handlers, services, and user interfaces. Human receipts
and versioned NDJSON events are projections of the same ingest result.

> [!IMPORTANT]
> TAMSin is early-stage software. Compatibility may change before 1.0. Pin
> deployments to a reviewed release and immutable image digest, and review the
> [changelog](CHANGELOG.md) before updating.

## Features

- Local files, directories, manifests, HTTP, S3, and standard input
- Deterministic Source and Flow identities for safe retry and resume
- Versioned ingest profiles for preservation, editorial, and streaming use
- Independent or multiplexed essence storage
- Byte verification, exact Segment rollback, and durable redacted journals
- Human receipts, terminal-aware progress, and a bounded NDJSON event protocol
- Strict configuration precedence, typed exit codes, and a read-only doctor
- Tested conformance with BBC TAMS 8.1 and the TAMOSS reference deployment

## Core model

TAMSin presents the important choices and TAMS entities consistently across
the CLI, receipts, event stream, journal, and documentation.

| Element | Meaning |
| --- | --- |
| Input | A file, URL, object, standard-input stream, or manifest entry resolved for one ingest |
| Profile | A versioned packaging policy covering essence storage, Segment duration, container, and compatibility |
| Source | The TAMS identity for the resolved input material |
| Essence Flow | A timeline that owns one video, audio, data, or muxed representation |
| Collector Flow | An empty Multi-Flow that groups independently stored essence Flows |
| Media Object | Immutable stored bytes allocated through TAMS |
| Flow Segment | The mapping from an Object into a Flow timerange |
| Run | One invocation with a receipt or complete `hello` … `run.finished` event stream |

See the [terminology and product model](docs/reference/terminology.md) for the
full taxonomy and the distinction between TAMSin, `tamsin`, TAMS, and TAMOSS.

## Profiles

Profiles put the packaging decision up front and make it reproducible.

| Profile | Essence storage | Segment target | Stored representation | Best suited to |
| --- | --- | --- | --- | --- |
| `preserve@1` | muxed | whole file | source bytes | archive, interchange, and evidence preservation |
| `editorial@1` | independent | 10 seconds | source-family remux | TAMS-native essence access and production work |
| `streaming-ts@1` | independent | 2 seconds | MPEG-TS | independently accessible short-form delivery Segments |

Ingest requires an explicit profile; it does not silently choose how to rewrite
or preserve media. Select one with `--profile`, then override an individual
policy only when you intentionally want a `custom@1` result. See
the [profile and supported-media reference](docs/reference/profiles.md) before
choosing a production policy.

## Install

Download a binary from [GitHub Releases](https://github.com/livewyer-ops/tamsin/releases),
verify it against `SHA256SUMS` and its GitHub build attestation, then place it on
`PATH`. For example, on Linux amd64:

```sh
version=v0.1.0-rc.1
asset=tamsin-linux-amd64
base="https://github.com/livewyer-ops/tamsin/releases/download/${version}"

curl --fail --location --remote-name "${base}/${asset}"
curl --fail --location --remote-name "${base}/SHA256SUMS"
grep " ${asset}$" SHA256SUMS | sha256sum --check
gh attestation verify "${asset}" --repo livewyer-ops/tamsin
sudo install -m 0755 "${asset}" /usr/local/bin/tamsin
tamsin --version
```

The release also publishes a non-root multi-platform image. OCI tags omit the
Git tag's leading `v`:

```sh
docker pull ghcr.io/livewyer-ops/tamsin:0.1.0-rc.1
docker run --rm ghcr.io/livewyer-ops/tamsin:0.1.0-rc.1 --version
```

Replace the example version with the release you intend to deploy. Pin
production images by the index digest shown by the release workflow. Each
release also includes the checksummed and attested
`tamsin-third-party-licenses.tar.gz` bundle for its compiled dependencies.

## Quick start

Install `ffprobe` and `ffmpeg`, place the `tamsin` binary on `PATH`, and provide
credentials for your TAMS service:

```sh
export TAMSIN_AUTH_TOKEN='...'
tamsin doctor --endpoint https://tams.example.com
tamsin ingest --profile editorial \
  --input ./programme.ts \
  --endpoint https://tams.example.com
```

The editorial profile stores a multiplexed input as one Flow per essence plus an
empty collector Flow. A successful command ends with a permanent receipt that
identifies the collection, essence Flows, verified Objects and bytes, profile,
input digest, and run.

New to TAMSin? Follow [your first ingest](docs/tutorials/first-ingest.md) for a
dry run, real ingest, result inspection, and retry.

## Automation and Kubernetes

Use `--format json` when another program consumes stdout. It emits the
versioned `tamsin.ingest.events` NDJSON protocol while work is running, not one
final JSON document. Consumers must drain stdout and stderr concurrently and
treat EOF without an exact final `run.finished` as incomplete.

Use `--journal PATH` for an independently synced, redacted record of the
resolved manifest and every terminal input. For a batch, exit code `4` means
that at least one item failed while every item result was retained.

- [Ingest output protocol and journal](docs/reference/result-contract.md)
- [Run TAMSin as a Kubernetes Job](docs/how-to/run-as-a-kubernetes-job.md)
- [Configuration and secret sources](docs/reference/configuration.md)
- [Exit-code contract](docs/reference/exit-codes.md)

## Documentation

The documentation follows [Diátaxis](https://diataxis.fr): tutorials teach,
how-to guides complete a task, reference describes the contract, and
explanation develops the design and trade-offs.

| If you want to… | Start here |
| --- | --- |
| Find the right page | [Documentation map](docs/README.md) |
| Learn TAMSin end to end | [Your first ingest](docs/tutorials/first-ingest.md) |
| Understand the nouns and graph | [Terminology and product model](docs/reference/terminology.md) |
| Choose a packaging policy | [Profiles and supported media](docs/reference/profiles.md) |
| Look up commands or settings | [CLI](docs/reference/cli.md) · [configuration](docs/reference/configuration.md) |
| Integrate a wrapper or UI | [Output protocol and journal](docs/reference/result-contract.md) |
| Understand safety and rollback | [Integrity model](docs/explanation/integrity.md) |
| Check specification behaviour | [TAMS conformance](docs/explanation/conformance.md) |

## Requirements

- Runtime: `ffprobe` and `ffmpeg` on `PATH` for `editorial@1` and
  `streaming-ts@1`; `preserve@1` uploads source bytes without invoking FFmpeg
- S3 inputs: credentials supplied through the standard AWS credential chain
- Development: Go 1.26 or newer
- Multi-platform distribution: Docker Buildx and amd64/arm64 binfmt handlers
- End-to-end tests: Docker, `kubectl`, `curl`, `jq`, Python 3, and standard Unix
  tools; the test harness installs its pinned Kind/Task toolchain

The built OCI image includes FFmpeg, runs as a non-root user, and supports
linux/amd64 and linux/arm64 in one OCI index.

Segmented ingests use a bounded rolling spool: closed outputs are committed and
removed in batches, and FFmpeg is paused at closed-Segment staging watermarks
when the store is slower than the renderer. An active Segment must be allowed to
finish before it can be reclaimed, so it may temporarily exceed the rolling
window; the global staging-capacity guard cancels a render that cannot fit. The
default terminal result retains compact per-Flow counters; object events or a
journal carry unbounded job detail.

## Conformance and integrity

The repository pins its supported BBC TAMS surface in
[`contracts/tams-v8.1.json`](contracts/tams-v8.1.json). End-to-end tests deploy
the pinned TAMOSS profile, exercise supported sources and authentication modes,
and read every resulting Flow and Segment back from the live service.

```sh
make e2e
```

TAMSin verifies uploaded bytes by default, using trustworthy storage SHA-256
evidence when available and readback otherwise. It retracts exact registered
Segments before reporting a failed input. See [TAMS conformance](docs/explanation/conformance.md)
and the [integrity model](docs/explanation/integrity.md) for the boundary and
recovery semantics.

## Development

```sh
make clean verify
make dist
```

`make verify` runs formatting, module-integrity, static-analysis, race,
vulnerability, secret-leak, and CLI smoke gates. `make dist` creates CGO-free
Linux/macOS binaries and a local, registry-ready amd64/arm64 OCI layout; it does
not publish artefacts.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[SECURITY.md](SECURITY.md) for private vulnerability reporting.

## Community and support

- Use [GitHub Issues](https://github.com/livewyer-ops/tamsin/issues) for bugs
  and focused feature requests.
- Read [SUPPORT.md](SUPPORT.md) for supported releases and the boundaries of
  community support.
- Read [CONTRIBUTING.md](CONTRIBUTING.md) before proposing a change.
- Report security issues through the private process in [SECURITY.md](SECURITY.md).
- Review [CHANGELOG.md](CHANGELOG.md) for user-visible changes.

## Related projects

- [BBC TAMS](https://github.com/bbc/tams) — the specification and application notes
- [TAMOSS](https://github.com/livewyer-ops/tamoss) — a Kubernetes-oriented open-source TAMS implementation used by the end-to-end suite

## License

Apache License 2.0. See [LICENSE](LICENSE).
