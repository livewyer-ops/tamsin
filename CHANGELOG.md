# Changelog

All notable changes are documented here. TAMSin follows semantic versioning;
the published 1.x compatibility commitments are described in
`docs/explanation/compatibility.md`.

## [1.0.0] - Unreleased

Working notes for the forthcoming 1.0.0 release. Published versions are release
candidates; set the release date before tagging the next candidate or final.

### Release hardening

- Follow paginated storage-backend listings, accept optional array-valued tags,
  and verify both presigned and non-presigned Object URLs. Apply 8.2 URL start
  deadlines according to the allocation flag; estimate upload warnings against
  Object registration lifetime rather than URL start lifetime.
- Distinguish Matroska from WebM using the EBML document type, keeping Flow
  media types and source-family remuxers consistent with the input bytes.
- Keep only the vendored schemas needed for ingest, share configuration and
  schema-loading code, and simplify per-invocation pipeline state. Check Python
  helper files directly alongside shell scripts.
- Preserve exact JSON numbers in metadata files and report Profile mismatches
  with bounded, value-free field paths. Skip the redundant packet scan when
  FFprobe reports B-frame reordering, with a real reordered-media regression.
- Validate and decode YAML from one document, check every shell script, and
  run both live TAMS versions by default through `make e2e`.
- Include dependency licences and required corresponding source with binaries
  and in the application image. Smoke both image architectures before assigning
  release tags; restrict runtime publication to main and reject ambiguous
  registry failures or attempts to reuse an existing runtime or application
  version tag.
- Pin the supported Go toolchain at 1.26.8 and run the OCI image as UID/GID
  65532 from a writable neutral work directory.
- Require FFprobe and FFmpeg 5.1 or newer before TAMS mutation. Media child
  processes receive an explicit runtime allow-list rather than inheriting cloud,
  proxy, TAMSin, or credential environment variables.
- Strip configured HTTP-input headers on cross-origin redirects, reject URL
  user information, redact URL values from structured usage hints, and warn
  when a Unix configuration file containing secrets is group/world-readable.
- Bound verification reads to one byte beyond the registered Object size,
  require response-side upload checksum evidence, tolerate generated Segment
  files disappearing during directory scans, and reject explicit Flow reuse
  when its existing `source_id` identifies different material.
- Pin the final BBC TAMS 8.2 schema and TAMOSS release contract, move vendored
  TAMS validation schemas behind the internal package boundary, and publish
  canonical TAMSin schema identifiers under `tamsin.livewyer.io`.
- Size idle HTTP connection pools to configured transfer concurrency, publish
  the complete stable failure-code vocabulary, and distinguish request timeout
  from parent cancellation without parsing diagnostic prose.

### Profile matching and command scope

- Pin exact JSON-number decoding at the TAMS Profile HTTP boundary, including
  technical metadata integers larger than 2^53, and strengthen CLI credential
  redaction coverage across bearer, URL-token and Basic authentication.
- Keep TAMSin documentation independent of a particular general TAMS control
  client, clarify the layered Segment-retraction test boundary, and make the
  retired `api` help path explain its migration rather than showing bare usage.
- Move general TAMS discovery and administration from `tamsin api` to the
  separate lightweight `tamsctl` client. TAMSin now retains only ingest,
  treatment profiles, diagnostics, configuration and shell completion, and no
  longer carries general Flow, Segment, Object, storage or raw-request commands.
- Compare generated Flow metadata with TAMS 8.2 Flow Profiles using JSON value
  semantics. Numerically equal metadata now matches across Go integer,
  `json.Number`, and exactly equivalent finite floating-point representations
  without losing precision for integers larger than 2^53; all non-numeric
  structure and value checks remain strict.

### TAMS compatibility

- Target BBC TAMS 8.2 while retaining TAMS 8.1 as the compatibility floor.
  Missing, malformed, 8.0, or different-major `api_version` values fail before
  the first write; both pinned TAMOSS implementations run in the release gate.
- Support immutable TAMS 8.2 Flow Profiles through
  `--tams-flow-profile [FORMAT[:INDEX]=]UUID` ingest assignment, with general
  Profile discovery and administration remaining outside TAMSin.
  Profile-backed Flow writes use the compact `profile_id` form, while planning,
  collision checks, results, and reads use expanded technical metadata.
- Apply the 8.2 Flow lifecycle without extra resume churn: `ingesting` before
  Object allocation, `closed_complete` after success, and `awaiting_content`
  after a failed written graph. TAMS 8.1 requests remain unchanged.
- Retain the 8.2 storage-allocation, storage-backend selection, and
  `init_object_id` fields used by ingest. High-level fragmented-MP4 preparation
  remains deliberately out of scope for this release.
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
- Keep FFmpeg pass-through for specialist media options, while replacing the
  general configuration framework with a focused YAML resolver.
- Remove the duplicate result journal and public Go consumer package. Durable
  automation output is ordinary redirection of the documented NDJSON stream.
- Require explicit dry-run and verification mode values instead of inferring
  a mode from a bare flag.
- Use append-only plain progress in `auto` mode, remove terminal redraw and
  rate/ETA calculations, and leave line wrapping to the terminal.
- Accept only pre-obtained OAuth authorisation codes; TAMSin no longer opens a
  browser or listens on a local callback port.

### Release and supply chain

- Publish four CGO-free binaries and one non-root amd64/arm64 OCI index as
  immutable versioned artefacts; prereleases never move the `latest` image tag.
- Build the application on the immutable
  `tamsin-ffmpeg-runtime:5.1.9-bookworm-r1` base pinned by index digest. Its
  Debian snapshot, FFmpeg package, two architectures, SPDX SBOM, and provenance
  are revisioned once so normal application builds reuse the expensive runtime
  layer.
- Attach BuildKit SBOM and provenance attestations directly to both the FFmpeg
  runtime and application images, and publish checksummed, attested binaries
  through GitHub Releases.

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
