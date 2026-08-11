# Profiles and supported media

TAMS describes the media a Flow owns, but it does not prescribe how an ingest
should package that media. TAMSin makes that product choice explicit with
small, versioned ingest profiles. A profile is a reproducible contract over
essence storage, Segment duration, Segment container, and compatibility checks;
it is not a claim that every combination FFmpeg can mux is interoperable.

## Named profiles

| Profile | Essence storage | Segment target | Stored container | Intended use |
| --- | --- | --- | --- | --- |
| `preserve@1` | muxed | whole file | source bytes | archive, interchange, and evidence preservation |
| `demux@1` | independent | whole essence | source-family remux | transcription, analysis, and downstream proxy generation |
| `muxed-segments@1` | muxed | 10 seconds | source-family remux | time-range access when consumers normally need the complete multiplex |
| `essence-segments@1` | independent | 10 seconds | source-family remux | TAMS-native essence access and general production work |
| `mpegts-segments@1` | independent | 2 seconds | MPEG-TS | downstream systems that explicitly require short MPEG-TS Objects |

Ingest requires an explicit profile. There is no universal default because
whole versus segmented and muxed versus independent storage materially change
CPU use, temporary space, Object count, API traffic, and downstream reads.
Run `tamsin profiles` to inspect the catalogue without loading configuration;
use `tamsin --format json profiles` for its strict, versioned machine report.

The source-family choices form a useful access matrix:

| Consumer access | Whole Object | Time-addressable Objects |
| --- | --- | --- |
| Complete multiplex | `preserve@1` | `muxed-segments@1` |
| Individual essence | `demux@1` | `essence-segments@1` |

`mpegts-segments@1` adds an explicit container and codec-compatibility policy;
it is not a synonym for a complete streaming workflow.

Use `--profile NAME` or pin the contract as `--profile NAME@1`. An individual
media flag is applied after the profile. If it changes a profile setting, or if
`--ffmpeg-arg` is supplied, the resolved result is reported as `custom@1`. A
custom treatment is limited to a single essence and requires explicit output
`codec` and complete `essence_parameters` in `--flow-metadata`. Multi-stream
transcodes must be performed before ingest because one override cannot
describe each output essence truthfully.

```sh
tamsin --profile preserve -i master.mxf -o https://tams.example.com
tamsin --profile essence-segments --segment-duration 5s -i edit.mov -o https://tams.example.com
```

The first command reports `preserve@1`; the second reports `custom@1`. Repeating
a profile's own value does not make it custom. Profile and version appear in
both JSON terminal events and human receipts, including dry runs and failures.

Each named profile has its own semantic version. The separate profile-policy
version describes the catalogue and discovery contract, so changing one
profile does not imply that every other profile changed. `custom@1` versions
the identity recipe for explicit overrides; it does not make arbitrary custom
treatments equivalent. Pre-release names `editorial` and `streaming-ts` are
rejected with their replacements rather than retained as ambiguous aliases.

## Processing and local-resource impact

Object counts below are nominal. Segment boundaries follow source keyframes,
so the actual count can be lower.

| Profile | FFmpeg work | Nominal Object pattern | Local and service impact |
| --- | --- | --- | --- |
| `preserve@1` | never | 1 per input | Lowest processing and Object overhead; partial time ranges require fetching the whole input |
| `demux@1` | only for a multi-essence multiplex | about 1 per essence | Few Objects, but complete demuxed outputs may coexist with the staged source |
| `muxed-segments@1` | always | about 360 per input-hour | Bounded rolling staging and fewer Objects; a reader fetches every essence |
| `essence-segments@1` | always | about 360 per essence-hour | Bounded rolling staging; Object, API, and verification work multiply by essence count |
| `mpegts-segments@1` | always | about 1,800 per essence-hour | Highest Object and API overhead; MPEG-TS packet overhead; no manifests or adaptive renditions |

Segmentation does not inherently hold a complete rendered asset in temporary
storage. TAMSin commits and removes closed outputs through a bounded rolling
spool. Whole-essence demultiplexing has fewer Objects but can temporarily need
space for the source plus complete output essences. All profiles use FFprobe;
`doctor` conservatively requires FFmpeg for `demux@1` because it has no input
from which to prove that demultiplexing is unnecessary.

## Supported container × codec × storage × Segment matrix

The following is the supported product boundary, not FFmpeg's much larger
capability list.

| Output policy | Containers | Codecs | Storage | Segments |
| --- | --- | --- | --- | --- |
| source bytes (`preserve@1`) | any input whose streams TAMSin can describe; known containers receive a registered media type and unknowns use `application/octet-stream` with a warning | no mux compatibility restriction because bytes are not rewritten | muxed | one Object containing the whole input |
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

When `preserve@1` uploads a whole multiplex, FFmpeg writes nothing. Its version
is deliberately absent from both identity and provenance, so upgrading an
unused tool does not manufacture a new Flow for identical source bytes.

## See also

- [Choose how essences are stored](../how-to/choose-how-essences-are-stored.md)
- [Prepare MPEG-TS Segments](../how-to/prepare-mpegts-segments.md)
- [Container media-type decision](../explanation/decisions/0005-container-media-types.md)
