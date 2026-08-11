# Support

TAMSin is maintained as an open-source project. Community support is provided
on a best-effort basis; no response-time or availability commitment is implied.

## Where to ask

- Use [GitHub Issues](https://github.com/livewyer-ops/tamsin/issues) for a
  reproducible TAMSin bug or a focused feature proposal.
- Use the [documentation map](docs/README.md) for configuration, profiles,
  automation contracts, recovery, and operational guidance.
- Report suspected vulnerabilities only through the private process in
  [SECURITY.md](SECURITY.md).
- Report BBC TAMS specification questions to the
  [BBC TAMS project](https://github.com/bbc/tams) and TAMOSS deployment issues
  to the [TAMOSS project](https://github.com/livewyer-ops/tamoss).

## Supported scope

Only the latest stable TAMSin minor release is supported. Until `v0.1.0` is
stable, support follows the latest public release candidate. Reproduce a problem
with a published binary or immutable image digest when possible and include the
redacted `tamsin doctor --format json` report.

The supported distribution targets are Linux and macOS on amd64 and arm64, plus
the published linux/amd64 and linux/arm64 OCI image. Other source builds may
work but are not release-tested.
