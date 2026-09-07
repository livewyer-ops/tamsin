# Profiles and supported media

Choose a treatment explicitly: packaging changes CPU use, temporary space,
Object count, API traffic and downstream reads. Run `tamsin profiles` to see the
catalogue, or `tamsin --format json profiles` for its versioned report.

These local treatments are distinct from immutable, service-owned TAMS 8.2 Flow
Profiles. The latter constrain generated technical metadata; neither choice
is inferred from the other.

## Named profiles

| Profile | Essence storage | Segment target | Stored container | Intended use |
| --- | --- | --- | --- | --- |
| `preserve@1` | muxed | whole file | source bytes | compliance, archive, and source reprocessing |
| `demux@1` | independent | whole essence | source-family remux | transcription, analysis, and downstream proxy generation |
| `muxed-segments@1` | muxed | 10 seconds | source-family remux | time-range access when consumers normally need the complete multiplex |
| `essence-segments@1` | independent | 10 seconds | source-family remux | independent essence and time-range access for TAMS production workflows |
| `mpegts-segments@1` | independent | 2 seconds | MPEG-TS | downstream systems that explicitly require short MPEG-TS Objects |

Use `--profile NAME` or pin the contract as `--profile NAME@1`. An individual
media flag is applied after the profile. If it changes a profile setting, or if
`--ffmpeg-arg` is supplied, the resolved result is reported as `custom@1`. A
custom treatment using `--ffmpeg-arg` is limited to a single essence and
requires explicit output `codec` and complete `essence_parameters` in
`--flow-metadata`. Changing only a packaging setting such as Segment duration
does not impose that restriction. Multi-stream transcodes must be performed
before ingest because one override cannot describe each output essence
truthfully.

```sh
tamsin --profile preserve -i master.mxf -o https://tams.example.com
tamsin --profile essence-segments --segment-duration 5s -i edit.mov -o https://tams.example.com
```

`-o` is the short form of `--endpoint`. The first command reports
`preserve@1`; the second reports `custom@1`. Repeating
a profile's own value does not make it custom. Profile and version appear in
both JSON terminal events and human receipts, including dry runs and failures.

Named profiles and the catalogue policy have separate versions. `custom@1`
versions the identity recipe for overrides, not an equivalence between arbitrary
FFmpeg treatments.

## TAMS Flow Profiles

The assigned Profile must already exist on the service and match the generated
metadata; it does not choose packaging or perform a transcode.

Repeat `--tams-flow-profile FORMAT[:INDEX]=UUID`, using `video`, `audio`,
`image` or `data`. The optional index is zero-based within that format. A bare
UUID, or a format without an index, must identify exactly one eligible Flow.
Missing, duplicate and ambiguous assignments fail before mutation. See
[compatibility](compatibility.md) for exact matching and omission rules.

For a video stream and the first audio stream, substitute your service's
matching Profile UUIDs:

```sh
tamsin ingest --profile essence-segments \
  --tams-flow-profile video=60d9df18-6d9d-4b86-84bf-d1dcf14b3a28 \
  --tams-flow-profile audio:0=8d5a25eb-35cb-423b-8e80-72258195ac2c \
  --input ./programme.ts
```

## Processing and local-resource impact

Object counts below are nominal. Segment boundaries follow source keyframes,
so the actual count can be lower.

| Profile | FFmpeg work | Nominal Object pattern | Local and service impact |
| --- | --- | --- | --- |
| `preserve@1` | never | 1 per input | Lowest processing and Object overhead; partial time ranges require fetching the whole input |
| `demux@1` | only for a multi-essence multiplex | about 1 per essence | Few Objects, but complete demuxed outputs may coexist with the staged source |
| `muxed-segments@1` | always | about 360 per input-hour | Bounded rolling staging and fewer Objects; a reader fetches every essence |
| `essence-segments@1` | always | about 360 per essence-hour | Bounded rolling staging; Object, API, and verification work multiply by essence count |
| `mpegts-segments@1` | always | about 1800 per essence-hour | Highest Object and API overhead; MPEG-TS packet overhead; no manifests or adaptive renditions |

Segmentation does not inherently hold a complete rendered asset in temporary
storage. TAMSin commits and removes closed outputs through a bounded rolling
spool. Whole-essence demultiplexing has fewer Objects but can temporarily need
space for the source plus complete output essences. All profiles use FFprobe;
`doctor` conservatively requires FFmpeg for `demux@1` because it has no input
from which to prove that demultiplexing is unnecessary.

## Supported media and packaging

TAMSin supports these combinations:

| Output policy | Containers | Codecs | Storage | Segments |
| --- | --- | --- | --- | --- |
| source bytes (`preserve@1`) | any self-contained container on the FFmpeg demuxer allowlist (`internal/media/allowlist.go`) whose streams TAMSin can describe; known containers receive a registered media type and other listed containers use `application/octet-stream` with a warning; playlists, manifests, concatenations and image sequences are refused | no mux compatibility restriction because bytes are not rewritten | muxed | one Object containing the whole input |
| whole source-family essence (`demux@1`) | MP4/QuickTime/3GP/3G2, WebM/Matroska, MPEG-TS/PS, MXF, AVI, ASF, WAV, AIFF, FLAC, MP3, AAC/ADTS, AC-3/E-AC-3, Ogg, JPEG/JPEG 2000/PNG/GIF/WebP | stream copy; the source muxer must accept the source codec, otherwise FFmpeg fails before TAMS is mutated | independent | one complete Object per essence |
| source-family multiplex (`muxed-segments@1`) | same source-family set as `demux@1` | stream copy with source-muxer compatibility | muxed | keyframe-aligned, nominally 10 seconds |
| source-family essence (`essence-segments@1`) | same source-family set as `demux@1` | stream copy with source-muxer compatibility | independent | keyframe-aligned, nominally 10 seconds |
| MPEG-TS (`mpegts-segments@1`) | MPEG-TS written by TAMSin regardless of input container | video: H.264, HEVC, MPEG-2; audio: AAC, MPEG Layer II/III, AC-3, E-AC-3; attached pictures are ignored; other stream types/codecs are rejected before FFmpeg or TAMS mutation | independent | keyframe-aligned, nominally 2 seconds |

Segment duration is a target. Stream copy cannot invent random-access points,
so a GOP longer than the target produces longer Segments. The MPEG-TS profile
does not transcode and therefore does not promise a two-second decoder refresh
when the source has a longer GOP. It also does not create HLS/DASH manifests,
adaptive-bitrate renditions, encryption, or CDN packaging.

TAMSin owns the output muxer through `--segment-format`. An explicit FFmpeg
`-f`, `-format`, or `-segment_format` is accepted only when it repeats the
resolved policy; a conflicting value is rejected with guidance to select the
matching profile or `--segment-format`. This keeps stored bytes, filename,
Flow `container`, and generated identity in agreement.

## Media-toolchain identity

Every generated Flow identifier includes the resolved profile, semantic profile
version, and a project-controlled renderer epoch. Exact FFmpeg package and build
strings do not rotate identity. TAMSin still records `_tamsin_ingest_profile`,
`_tamsin_ingest_profile_version`, `_tamsin_ffmpeg_version`, and a hashed
`_tamsin_media_toolchain` fingerprint in Flow provenance, and returns the
concise version and fingerprint in JSON terminal events. This keeps routine
security upgrades usable across a worker fleet while preserving diagnostic
evidence. A deliberate output-policy change bumps the affected profile version
or renderer epoch.

`preserve@1` does not use FFmpeg, so its version is absent from identity and
provenance.

## Media metadata

Optional metadata is omitted without reliable probe evidence. Required fields
must be established or explicitly supplied before BBC schema preflight passes.
Video needs either a fixed `frame_rate` or `vfr: true` with no frame rate.

`avg_frame_rate` and `r_frame_rate` do not answer that question. TAMSin scans
presentation timestamps, across the whole input for local/staged media or
across each closed segment for streamed remote media. Video without
frame reordering can use packet timestamps without decoding the programme.
When FFprobe reports B-frames, TAMSin goes directly to decoded frame timestamps;
other missing, duplicated or non-monotonic packet evidence also requires that
fallback. Whole-input decoding can add substantial CPU use and startup time
before upload, even with `preserve`. It emits `vfr: true` when
presentation intervals vary, and the probed rational `frame_rate` when they form
one cadence. Adjacent tick counts may differ by one because a rational cadence
such as 30000/1001 has to be quantised onto a container time base; that alone is
not treated as VFR.
Both paths retain only the preceding timestamp and interval bounds, so memory
use does not grow with programme length. For local/staged media, a complete
`essence_parameters` override skips the input cadence scan. Streaming still
checks each segment against the declared metadata, including overrides.

A streamed Flow's initial cadence remains its declaration: a later variable
interval cannot silently change a fixed-rate Flow. A variable-rate declaration
does permit fixed-rate stretches. Boundary checks restore the input timeline
from segment manifests and presentation evidence, allowing one container tick
and the manifest's microsecond rounding.

If frames have missing or non-monotonic presentation timestamps, TAMSin makes
neither claim. TAMS has no representation for an unknown video rate, so the
final Flow fails schema preflight. Local/staged workflows can supply complete
`essence_parameters` through `--flow-metadata`; streaming does not bypass its
timestamp checks for an override.

### Video presentation

| TAMS field | FFprobe evidence | Automatic support | Uncertain case |
| --- | --- | --- | --- |
| `frame_width`, `frame_height` | `width`, `height` | Required positive values | Ingest is rejected when either is absent |
| `frame_rate`, `vfr` | packet `pts` or decoded `best_effort_timestamp`, then the declared rate for the fixed rational | Fixed and genuinely variable cadence | Missing/non-monotonic timestamps are not guessed |
| `interlace_mode` | stream `field_order` plus container family | non-MXF `progressive`, `tt` → `interlaced_tff`, `bb` → `interlaced_bff` | MXF `progressive`, `tb`/`bt`, and PsF are omitted because FFmpeg cannot distinguish them reliably |
| `pixel_aspect_ratio` | `sample_aspect_ratio` | Positive colon-separated ratios | Missing, `0:1`, and malformed values are omitted |
| `aspect_ratio` | `display_aspect_ratio` | Positive colon-separated ratios | Missing or malformed values are omitted |
| `colorspace` | `color_primaries`, with matrix coefficients only as a fallback | BT.601, BT.709, BT.2020; BT.2100 when BT.2020 primaries carry HLG/PQ | Unknown names are omitted |
| `transfer_characteristic` | `color_transfer` | SDR, HLG, PQ | Unknown names are omitted |
| `bit_depth` | `bits_per_raw_sample`, `bits_per_sample`, then pixel-format depth | Positive known depths | Omitted when the probe exposes none |

FFprobe does not reliably establish PsF, so TAMSin never synthesises
`interlaced_psf`. A workflow may supply it in a complete
`essence_parameters` override when that production fact is known.

### Other essences

| Flow format | TAMS fields | Automatic support |
| --- | --- | --- |
| Audio | `sample_rate`, `channels` | Both are required and ingest is rejected if FFprobe does not supply positive values |
| Audio | `bit_depth` | From raw/sample depth or other probed format evidence when present |
| PCM audio | `unc_parameters.unc_type` | Interleaved by default; planar for FFmpeg's `pcm_*_planar` family and `pcm_lxf` |
| Still image | `frame_width`, `frame_height` | Required positive values; a zero-duration single image stream is not described as moving video |
| Data | `essence_parameters` | Empty unless supplied by an operator; TAMSin does not invent a `data_type` URN |

### Codec media types

The table is keyed by FFprobe's `codec_name`, not a filename extension. These
are the automatic mappings currently promised:

| Essence | FFprobe names | TAMS `codec` |
| --- | --- | --- |
| Video | `h264`, `hevc`, `av1`, `ffv1` | `video/h264`, `video/h265`, `video/AV1`, `video/FFV1` |
| Video | `mpeg2video`, `mpeg4`, `vp8`, `vp9` | `video/mpeg`, `video/mp4v-es`, `video/VP8`, `video/VP9` |
| Video | `prores` | `video/quicktime` (the established compatibility mapping; IANA has no ProRes coding subtype) |
| Image coding | `mjpeg`, `jpeg2000`, `png`, `gif`, `webp` | `image/jpeg`, `image/jp2`, `image/png`, `image/gif`, `image/webp` |
| Audio | `aac`, `ac3`, `eac3`, `flac`, `mp2`, `mp3`, `opus`, `vorbis` | `audio/aac`, `audio/ac3`, `audio/eac3`, `audio/flac`, `audio/mpeg` (MP2/MP3), `audio/opus`, `audio/vorbis` |
| PCM audio | any `pcm_*` | `audio/x-raw-int` or `audio/x-raw-float`, the explicit vocabulary used by the pinned TAMS audio schema |
| Timed text/data | `webvtt`, `ttml`, `subrip`, `ass` | `text/vtt`, `application/ttml+xml`, `application/x-subrip`, `text/x-ssa` |

An FFmpeg codec outside this table produces no generated `codec`. TAMSin logs
the codec name, stream type and stream index; it never constructs
`video/x-<name>`, `audio/x-<name>`, or `application/x-<name>`. This is stricter
than container fallback because the pinned elemental Flow schemas require
`codec`: without a valid explicit override, final schema preflight rejects the
Flow before TAMS mutation.

`--flow-metadata` is most useful for a single-essence input whose workflow owns
a more specific registry mapping. A multi-essence file may contain different
unknown codecs, so one top-level override cannot safely describe every track;
use a supported ingest profile rather than applying one codec value to all of
them.

See [operations](operations.md) for staging, verification and recovery.
