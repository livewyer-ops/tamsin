# Changelog

All notable changes are documented here. TAMSin follows semantic versioning.
Before 1.0, minor releases may change command or structured-output contracts;
pin automation to a reviewed release and read this file before upgrading.

## Unreleased

## [1.0.0] - 2026-08-12

This first public release candidate establishes TAMSin's supported product,
automation, compatibility, and supply-chain contracts.

### TAMS compatibility

- Target BBC TAMS 8.2 while retaining TAMS 8.1 as the compatibility floor.
  Missing, malformed, 8.0, or different-major `api_version` values fail before
  the first write; both pinned TAMOSS implementations run in the release gate.
- Support immutable TAMS 8.2 Flow Profiles through typed list/get/create
  commands and `--tams-flow-profile [FORMAT[:INDEX]=]UUID` ingest assignment.
  Profile-backed Flow writes use the compact `profile_id` form, while planning,
  collision checks, results, and reads use expanded technical metadata.
- Apply the 8.2 Flow lifecycle without extra resume churn: `ingesting` before
  Object allocation, `closed_complete` after success, and `awaiting_content`
  after a failed written graph. TAMS 8.1 requests remain unchanged.
- Retain typed support for 8.2 storage allocation options, richer backend
  metadata, and `init_object_id`. High-level fragmented-MP4 preparation remains
  deliberately out of scope for this release.
- Stop inventing `generation: 0` from local stream-copy policy. Generation is
  upstream lineage metadata and is preserved or supplied by the operator.

### Product and performance

- Publish five explicit media-treatment profiles: `preserve@1`, `demux@1`,
  `muxed-segments@1`, `essence-segments@1`, and `mpegts-segments@1`, with a
  machine-readable catalogue of storage, process, and staging trade-offs.
- Keep `preserve@1` on the direct-upload path without FFmpeg; invoke supervised
  FFmpeg only for treatments that need rendering, and bound rolling staging,
  media processes, transfers, probing, retries, events, and retained results.
- Make Flow identities depend on the canonical TAMS Flow Profile assignment,
  while keeping FFmpeg patch versions and status transitions out of identity.

### Release and supply chain

- Publish four CGO-free binaries and one non-root amd64/arm64 OCI index for
  `v1.0.0-rc.1`; prereleases never move the `latest` image tag.
- Build the application on the immutable
  `tamsin-ffmpeg-runtime:5.1.9-bookworm-r1` base. Its Debian snapshot, FFmpeg
  package, two architectures, SPDX SBOM, and provenance are revisioned once so
  normal application builds reuse the expensive 202-package runtime layer.
- Upload checksums, third-party licences, container identity metadata, and a
  deterministic supply-chain archive containing each platform's SPDX SBOM and
  SLSA provenance statement. Release metadata records both application and
  FFmpeg-runtime index digests.

## [0.1.0] - 2026-08-11

The pre-release baseline established TAMSin's ingest, automation, integrity,
and distribution contracts.

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
