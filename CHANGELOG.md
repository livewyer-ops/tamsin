# Changelog

All notable changes are documented here. The release compatibility policy is
described in [Compatibility](docs/compatibility.md).

Release versions are `MAJOR.MINOR.PATCH-inN`. `MAJOR.MINOR.PATCH` is the BBC
TAMS API version the release targets and `-inN` is the Nth TAMSin release for
that API version, so `8.2.0-in1` is the first TAMSin release targeting TAMS
8.2. A trailing `-rcM` marks the Mth release candidate for that version, for
example `8.2.0-in2-rc1`. Tags carry no `v` prefix.

## Unreleased

Upgrading from `8.2.0-in2` renews generated identities through renderer epoch 4.
Test an exact dry run and a real ingest on evaluation media before upgrading
workers. A repeated input can create new Flows and upload media again; existing
Flows remain unchanged. Keep the previous binary and settings if an interrupted
job must resume its old identities. Do not force the old Flow UUID into a new
treatment to bypass a conflict.

- Build with Go 1.27.1: `go.mod` and the `golang:1.27-bookworm` build image
  move together.
- **Breaking for generated identities:** place each rendered Segment where
  FFmpeg cut it on the source timeline and size it by the stream it was cut on,
  instead of advancing by the Object's whole span. Multiplexed Flows no longer
  stretch by the audio/video offset at every cut (about 10 ms per Segment,
  620 ms over a ten-minute programme). Audio that leads or trails a cut stays
  inside `object_timerange` and outside `timerange`; gaps in the source are
  exposed; a cut that is not a stream access point is refused. The renderer
  identity epoch moves to 4, so re-ingests create new Flows rather than
  colliding with Flows written under the earlier placement.
- TAMS 8.2 Flow Profile matching adopts, inside `essence_parameters`, what
  the Profile declares and the probe could not establish (component type,
  chroma subsampling, codec profile and level), while every generated
  parameter must still be declared by the Profile with the same value; the
  BBC reference Profiles no longer fail before the first write. Top-level
  fields stay strict in both directions.
- A Flow that already declares technical metadata is never rewritten to
  describe other media, for staged input as well as streamed: an explicit
  `--flow-id` pointing at a different file fails before mutation, as the
  compatibility page already promised.
- Sources derived from Tamsin's Flows receive the Flow's label and
  description when they have none, and Tamsin's `_tamsin_` provenance tags
  (AppNote 0007). Operator edits are kept; a Source that cannot be written
  is a warning.
- `--start` must be a canonical TAMS timestamp: `+5:0`, `05:0` and `1:05`
  are refused instead of being read leniently.
- `bit_depth` from a pixel format reads only an explicit depth suffix:
  `nv12` and `yuv410p` no longer claim 12 and 10 bits, and `rgb48` is 16.
- **Breaking for generated identities of whole-file multiplexes:** collection
  items of a whole-file Flow now carry the container's own track identifier
  in `container_mapping` (`mp2ts_container.pid` for MPEG-TS,
  `isobmff_container.track_id` for MP4 and QuickTime) alongside the
  positional indices; rendered Segments, whose identifiers FFmpeg reassigns,
  keep positional mapping only. Generated identities already change in this
  release through the renderer epoch.
- `docs/compatibility.md` records the known deviation that a time-shifted
  re-ingest keeps its Source ID until the next TAMS API version.
- Upload checksum evidence: a response `Content-Digest` is no longer read as
  proof about the stored Object (RFC 9530 defines it over the response body);
  `Repr-Digest` is, alongside `X-Amz-Checksum-Sha256` and `Digest`. An upload
  to an unsigned URL now sends the Object's digest as `Content-Digest` so
  storage that verifies digests refuses a corrupted transfer; presigned URLs
  are sent exactly as issued.
- Segment deletion has its own deadline, `--deletion-timeout`
  (`http.deletion_timeout`, default five minutes), instead of the per-request
  metadata timeout; detached cleanup follows it. A 202 without a usable
  `Location` is confirmed by the Segment's absence instead of being reported
  as a stranded Segment, and a deletion request that ends in `error` is
  accepted when the Segment has nevertheless gone.
- A store on a newer TAMS minor revision than the pinned 8.2 schemas may
  return Flow fields they do not know; only the metadata Tamsin generates is
  validated against them, and the store's fields are sent back untouched.
- A service whose `min_presigned_url_timeout` exceeds `min_object_timeout`
  is no longer refused: URLs are scheduled against the Object lifetime and a
  warning is logged (`tamsin doctor` reports it under `service_lifetimes`).
- A resume lists only the span it is about to write rather than the whole
  Flow, and the fallback to service-assigned identifiers asks again when the
  service hands out fewer than requested.
- Data and attachment tracks with no coding media type (QuickTime timecode,
  MXF ancillary data, font attachments) no longer stop an ingest. They are not
  described as Flows: a whole-file Object keeps them, a rendered Object leaves
  them out, and the collection's `track_index` counts the container the
  Objects are actually in. A warning names each dropped track. Unknown video,
  audio and subtitle codecs still stop the ingest.
- Name more broadcast codecs: DV, DNxHD, MPEG-1 video, DTS, TrueHD, SMPTE
  302M, G.711 A-law and mu-law, QuickTime text, DVB subtitles and teletext and
  SCTE-35 (see `docs/profiles.md` for the vocabulary). **Breaking for
  generated identities of JPEG 2000 video:** moving-picture JPEG 2000 is now
  `video/jp2`, as the BBC reference Flows use; still images stay `image/jp2`.
  G.711 no longer carries `unc_parameters`, and 16- and 24-bit float PCM is
  no longer described as integer PCM.
- Add `--collected-flow-metadata` (`ingest.collected_flow_metadata`): a JSON
  object of overrides keyed by collection role, merged into one collected
  essence Flow each, for facts an operator knows about a single track of a
  multiplex. A role the input does not produce is an error.
- Send `key_frame_count` and `last_duration` with every Segment whose packets
  were measured: the reference stream's stream access points and the
  presentation duration of its last sample. Whole-file Objects, which are not
  measured, send neither.
- Regularise a fixed-rate video reference stream to its nominal period when
  the container's timestamps round it short, as Matroska's millisecond
  timestamps do: 240 frames at 24 fps make a 10 s Segment, not 9.999 s. Such
  Flows previously carried a one-millisecond gap at every Segment boundary.
- Never register a Segment that overlaps one already in the Flow. A Segment at
  the same timerange under another identifier is adopted when its bytes and
  timing match under readback; otherwise the input stops with the new
  `tams.segment_conflict` failure code before anything is uploaded. Renders
  are bit-exact for every input so an identical re-run yields identical
  Objects; Matroska outputs previously changed between runs.
- Send API credentials only with unsigned media URLs on the API origin. A
  presigned URL, whether on the API origin or elsewhere, is requested exactly
  as issued; a bearer header or `access_token` query added to it could
  invalidate its signature.
- Follow the store's HTTP request instructions completely: an upload with no
  `Content-Type` instruction is typed as the Flow's `container` instead of
  `application/octet-stream`, disagreeing `content-type` and header
  instructions are reported instead of one being silently preferred, and a
  `body` instruction on a Media Object upload is refused rather than sending
  either the text or the media.
- Retry an ingest whose earlier attempt uploaded an Object but never
  registered it. The store keeps the deterministic identifier occupied until
  it collects the orphan, so the batch continues under service-assigned
  identifiers instead of failing on the repeated allocation.
- Accept every spelling of a UUID the identifier packages understand and send
  the lowercase hyphenated form the schema requires; refuse versions and
  variants outside it before any Flow is written.
- Compare timeranges by value when matching registered Segments for resume,
  reconciliation and deletion readback, so `[t]` and `[t_t]`, leading zeros
  and omitted markers no longer make a registered Segment look missing.
- Try every download URL a Segment advertises during verification, presigned
  routes first, before reporting an Object unreadable. Bytes that disagree
  still fail verification immediately.
- Build release images on FFmpeg runtime `5.1.9-bookworm-r3`, which refreshes
  the Debian bookworm base. FFmpeg stays at 5.1.9.
- Update Go module dependencies, including the AWS SDK for Go v2 1.47 with its
  S3 GetObject deadlock fix.
- Stop showing defaults for `--segment-duration`, `--segment-format` and
  `--essence-storage` in help and the references: the required profile
  supplies them, and an explicit value overrides it. Describe
  `--progress plain` as the same as `auto`.
- List only the values TAMSin emits in the events reference. The diagnostic
  severities `debug`, `info` and `warning`, the cancellation reasons
  `output_closed` and `internal`, the `uploaded` Object disposition and the
  `media.failed` failure code were documented but never emitted.

## [8.2.0-in2] - 2026-09-08

- Measure each rendered Object's presentation bounds instead of extrapolating
  timestamps from the first Object or trusting FFmpeg segment-list durations.
  Use the same calculation for staged, rolling and streamed ingest, including
  reordered video and audio padding. Missing timing fails before registration.
- Correct signed TAMS timestamp parsing and formatting: minus 80 milliseconds
  is `-0:80000000`, not `-1:920000000`.
- Advance renderer identity epoch to 3. Re-ingest affected media into new Flows;
  existing Segment metadata is not repaired automatically. Packaging profiles
  are unchanged; use `mpegts-segments@1` for HLS-compatible browser media.

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
- Stage instead of failing in `auto` when an origin rejects the byte-range
  probe or no loopback address can be bound for the private media input.
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
  image inspection, and release the runner's local image between architectures,
  keeping live tests and release smoke checks compatible with the hosted Docker
  runner.
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
