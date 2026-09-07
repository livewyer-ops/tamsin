# Changelog

All notable changes are documented here. The release compatibility policy is
described in [Compatibility](docs/compatibility.md).

Release versions are `MAJOR.MINOR.PATCH-inN`. `MAJOR.MINOR.PATCH` is the BBC
TAMS API version the release targets and `-inN` is the Nth TAMSin release for
that API version, so `8.2.0-in1` is the first TAMSin release targeting TAMS
8.2. A trailing `-rcM` marks the Mth release candidate for that version, for
example `8.2.0-in2-rc1`. Tags carry no `v` prefix.

## [8.2.0-in1] - 2026-09-07

This release supersedes the `v1.0.0-rc.1` to `v1.0.0-rc.3` prereleases, which
remain published but receive no further changes.

### Remote input streaming

- Stream seekable HTTP and S3 inputs for segmented treatments, pinning every
  read to one source revision. Add `--input-mode=auto|stream|stage`; automatic
  staging fallback happens before any TAMS mutation.
- Validate closed segments before upload, retaining cadence evidence across
  boundaries. Commit the first valid batch without waiting for a full download;
  a later contradiction can leave a valid committed prefix.
- Use revision-based identities for streamed inputs and retain per-Object
  SHA-256 verification. Local and staged identities remain content-based.
- Keep remote credentials in Go, bound queued segments during slow probes and
  uploads, and generate deterministic streamed remux headers for resume.
- Report `source.stream_unavailable` when `--input-mode=stream` is explicit and
  the input cannot be streamed: no strong ETag or byte ranges, insufficient
  initial segment evidence, or an explicit `--flow-id` Flow created from
  staged input. In `auto` the same conditions fall back to staging.
- Stage, rather than fail, an explicit `--flow-id` Flow created from local or
  staged input when it is re-ingested from a streamable remote in `auto`. A
  Flow created from streamed input re-ingested in `stage` mode fails with
  guidance to re-run with `--input-mode=stream` or use a new Flow ID.
- Resolve HTTP redirects once and pin subsequent reads to that exact URL and
  its strong ETag. Keep cross-origin credentials stripped, and reject changes
  to the pinned resource even when its ETag and length match. Signed URLs must
  remain valid for subsequent range requests.
- Reset the loopback bridge reconnect budget after sustained progress, so one
  long FFmpeg range over a large input survives repeated idle disconnects.
- Commit the validated prefix and publish final totals when a streamed render
  fails after the Flow graph is written.
- Retain declared cadence when a later segment lacks timestamp evidence,
  without treating the gap as a single frame interval when evidence returns.

### Release hardening

- Follow paginated storage-backend listings, accept optional array-valued tags,
  and verify both presigned and non-presigned Object URLs. Apply 8.2 URL start
  deadlines according to the allocation flag; estimate upload warnings against
  Object registration lifetime rather than URL start lifetime.
- Distinguish Matroska from WebM using the EBML document type, keeping Flow
  media types and source-family remuxers consistent with the input bytes.
- Skip the redundant packet scan when FFprobe reports B-frame reordering.
- Include dependency licences and required corresponding source with binaries
  and in the application image. Smoke both image architectures before assigning
  release tags; restrict runtime publication to main and reject ambiguous
  registry failures or attempts to reuse an existing runtime or application
  version tag.
- Check each image architecture's running UID without requiring platform-aware
  image inspection, keeping live tests and release smoke checks compatible with
  the hosted Docker runner.
- Pin the supported Go toolchain at 1.26.8 and run the OCI image as UID/GID
  65532 from a writable neutral work directory.
- Require FFprobe and FFmpeg 5.1 or newer before TAMS mutation. Media child
  processes receive an explicit runtime allow-list rather than inheriting cloud,
  proxy, TAMSin, or credential environment variables.
- Run FFmpeg and FFprobe with a protocol allowlist (`file` for local and
  staged input, `http,tcp` for the loopback bridge) and a format allowlist of
  self-contained demuxers. Playlist, manifest, concatenation and pattern
  demuxers such as HLS, DASH, concat and image sequences are refused, so a
  crafted input cannot direct the media tools at other files or network
  hosts. `preserve@1` refuses an input whose container is outside the list.
- Stage remote and stdin inputs as `input` plus a short alphanumeric
  extension so the remote basename never reaches the media tools.
- Strip configured HTTP-input headers on cross-origin redirects, reject URL
  user information, redact URL values from structured usage hints, and warn
  when a Unix configuration file containing secrets is group/world-readable.
- Bound verification reads to one byte beyond the registered Object size,
  require response-side upload checksum evidence, tolerate generated Segment
  files disappearing during directory scans, and reject explicit Flow reuse
  when its existing `source_id` identifies different material.
- Pin the final BBC TAMS 8.2 schema and TAMOSS test revisions.
- Size idle HTTP connection pools to configured transfer concurrency, retain
  stable failure codes, and distinguish request timeout
  from parent cancellation without parsing diagnostic prose.

### Profile matching

- Compare generated Flow metadata with TAMS 8.2 Flow Profiles using JSON value
  semantics. Numerically equal values match, and integers larger than 2^53
  retain their precision when read from Profile responses or metadata files.
  Non-numeric values and structure remain strict. Mismatch diagnostics use
  bounded field paths without exposing metadata values.

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
- Preserve operator-supplied generation metadata as upstream lineage.

### Product and performance

- Publish five explicit media-treatment profiles: `preserve@1`, `demux@1`,
  `muxed-segments@1`, `essence-segments@1`, and `mpegts-segments@1`, with a
  machine-readable catalogue of storage, process, and staging trade-offs.
- Keep `preserve@1` on the direct-upload path without FFmpeg; invoke supervised
  FFmpeg only for treatments that need rendering, and bound rolling staging,
  media processes, transfers, probing, retries, events, and retained results.
- Make Flow identities depend on the canonical TAMS Flow Profile assignment,
  while keeping FFmpeg patch versions and status transitions out of identity.
- Support FFmpeg pass-through for specialist media options and YAML configuration.
- Capture durable automation output by redirecting the NDJSON event stream.
- Require explicit dry-run and verification mode values.
- Use append-only plain progress in `auto` mode and leave line wrapping to the terminal.
- Exchange pre-obtained OAuth authorisation codes.
- Keep a hidden `tamsin api` stub that reports the command's removal instead
  of ingesting a file named `api`.
- Spell TAMSin consistently in help text and label generated Flows
  `TAMSin <digest>` instead of `Tamsin <digest>`.

### Release and supply chain

- Adopt bare `MAJOR.MINOR.PATCH-inN` release tags. `go install` is not a
  supported install path; the binaries and the image are the distribution.
- Publish four CGO-free binaries and one non-root amd64/arm64 OCI index as
  immutable versioned artefacts; release candidates never move the `latest`,
  major or minor image tags.
- Build release binaries and the application image with the same Go 1.26
  toolchain.
- Pin the licence bundling tool in `go.mod` so its version and checksums are
  verified with every other dependency.
- Build the application on the immutable
  `tamsin-ffmpeg-runtime:5.1.9-bookworm-r2` base pinned by index digest. Its
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
  diagnostics and persisted locators redact credentials and URL
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
  per-Object records are available through events.

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

- TAMSin targets the pinned BBC TAMS 8.1 API and TAMOSS reference implementation.
- Every profile requires `ffprobe`. Segmented and custom rendered treatments
  require `ffmpeg`; `demux@1` uses it when separating a multi-essence input,
  while `preserve@1` does not. The OCI image includes both tools.
- Custom transcoding currently supports one essence and requires explicit
  output codec and essence metadata. Prepare multi-stream transcodes before
  ingest until per-essence output metadata is supported.
- Support finite file and object-collection ingest.
- TAMSin is not a TAMS server and does not manage the lifecycle or availability
  of the target service or object store.
