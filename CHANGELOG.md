# Changelog

All notable changes are documented here. TAMSin follows semantic versioning.
Before 1.0, minor releases may change command or structured-output contracts;
pin automation to a reviewed release and read this file before upgrading.

## Unreleased

## [0.1.0] - 2026-08-11

The first public release establishes TAMSin's supported ingest, automation,
integrity, and distribution contracts.

### Highlights

- Ingest local files, recursive directories, manifests, HTTP(S), S3, and
  standard input into a BBC TAMS 8.1 service.
- Select one of five explicit, versioned media policies: `preserve@1`,
  `demux@1`, `muxed-segments@1`, `essence-segments@1`, or
  `mpegts-segments@1`.
- Inspect profile semantics and resource trade-offs with the config-independent
  human or versioned JSON `tamsin profiles` report.
- Create deterministic Source and Flow identities for safe retry and resume,
  while retaining renderer and source provenance separately from identity.
- Store muxed inputs as one Flow or split their essences into independently
  addressable Flows grouped by a collector Flow.
- Use a human receipt at a terminal or the versioned
  `tamsin.ingest.events` NDJSON protocol from automation and user interfaces.
- Retain a durable, redacted journal containing the resolved manifest and every
  terminal input result.
- Diagnose configuration, credentials, storage guarantees, staging capacity,
  and media-tool availability with the read-only `tamsin doctor` command.

### Integrity and security

- Upload verification uses trustworthy storage SHA-256 evidence when present
  and readback otherwise. A failed verification retracts the exact registered
  Segment; a failed retraction is reported as an operator action.
- Bulk registration reconciles partial responses and lost responses under one
  bounded deadline so every possibly registered Object reaches a known state.
- Credential-bearing TAMS and OAuth requests require HTTPS, except for an
  explicit loopback-only development mode. Redirects cannot carry TAMS
  credentials or presigned storage headers to another origin.
- Configuration files must be regular files and are limited to 2 MiB. Output,
  journals, diagnostics, and persisted locators redact credentials and URL
  query values.
- Input expansion, transfer concurrency, media measurement, child-process
  output, retries, temporary storage, and shutdown recovery are all bounded.

### Performance and resource use

- FFmpeg runs as a supervised child process only when the selected profile or
  custom treatment needs rendering. `preserve@1` uploads source bytes without
  invoking FFmpeg.
- Segmented rendering uses a bounded rolling spool. Closed outputs are
  committed and reclaimed in batches, and FFmpeg receives backpressure when
  storage is slower than rendering.
- Upload and verification workers share a process-wide transfer budget; media
  measurement has a separate process budget.
- Default results and terminal progress retain bounded summaries. Detailed
  per-Object records remain available through events or the journal.

### Automation and distribution

- Stable exit codes distinguish usage, configuration, partial-batch, remote,
  verification, and interrupted outcomes.
- JSON mode emits a sequenced `hello` through `run.finished` event stream with
  typed failure codes, progress, retry, Flow, Object, and terminal records.
- Release binaries target Linux and macOS on amd64 and arm64.
- The non-root OCI image publishes one linux/amd64 and linux/arm64 index. Each
  platform carries minimal provenance and an SPDX SBOM, and exact release
  members pass architecture-specific smoke tests before publication.
- Signed annotated release tags, immutable build artefacts, checksums, binary
  build attestations, and exact-image TAMOSS end-to-end tests protect the
  release handoff.
- A deterministic third-party licence bundle retains notices and corresponding
  source required by the dependencies compiled into release binaries.

### Compatibility and limitations

- TAMSin targets the pinned BBC TAMS 8.1 contract and the pinned TAMOSS
  reference profile recorded in `contracts/tams-v8.1.json`.
- Every profile requires `ffprobe`. Segmented and custom rendered treatments
  require `ffmpeg`; `demux@1` uses it when separating a multi-essence input,
  while `preserve@1` does not. The OCI image includes both tools.
- Custom transcoding currently supports one essence and requires explicit
  output codec and essence metadata. Prepare multi-stream transcodes before
  ingest until per-essence output metadata is supported.
- The public release supports finite file and object-collection ingest. It does
  not expose the separately incubated live-broadcast capture work.
- TAMSin is not a TAMS server and does not manage the lifecycle or availability
  of the target service or object store.
