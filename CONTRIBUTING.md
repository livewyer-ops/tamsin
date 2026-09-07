# Contributing

TAMSin is an ingest utility. Prefer a small change that
solves a demonstrated operator problem over a new abstraction or subsystem.
Use existing code, the Go standard library, platform features and current
dependencies before adding another implementation or dependency.

Open an issue before investing in a large feature or public-contract change.
General TAMS administration is outside the project's scope.

## Verify a change

The build scripts target Linux with Bash 4+, GNU tar and `sha256sum`, Python 3,
curl 7.71+, `jq`, and the Go version in `go.mod`. FFmpeg and FFprobe 5.1+ are needed for media
tests; Docker is needed for image and live integration checks. Release binaries
also support macOS, but the release scripts require the GNU tools above.

```sh
make verify
make dist
make image-smoke
```

Run `make e2e` for changes to TAMS requests, authentication, storage transfer,
S3, FFmpeg packaging, TLS, or container runtime behaviour. It tests the pinned
TAMS 8.1 and 8.2 TAMOSS implementations.
For a focused rerun, use `make e2e TAMSIN_E2E_VERSION=8.2` (or 8.1).
Both versions must pass before release.

Changes to source resolution, authentication, Flow/Object identity, mutation,
verification, output events or exit codes need an observable behaviour test.
Tests must be deterministic and safe to run with the complete suite. Do not
commit generated binaries, media fixtures, credentials, Kind state or TAMOSS
source caches.

See [Releasing](docs/releasing.md) for the binary and container release steps.

## Pull requests

Use a Conventional Commit-style, user-meaningful title. Keep the description to
the problem, user-visible decision, security/resource impact and verification.
Keep stdout machine-readable, diagnostics on stderr, credentials redacted and
retries bounded.

Contributors are responsible for every submitted change regardless of the
tools used to produce it. Do not include customer media, private source or
credentials in external tools, commits, tests or issues.
