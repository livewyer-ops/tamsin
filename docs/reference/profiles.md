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
| `editorial@1` | independent | 10 seconds | source-family remux | TAMS-native essence access and general production work |
| `streaming-ts@1` | independent | 2 seconds | MPEG-TS | independently accessible short-form delivery Segments |

Ingest requires an explicit profile. `editorial@1` is exactly the previous
default combination: `--essence-storage independent --segment-duration 10s
--segment-format source`, but operators now name that policy rather than
receiving it implicitly.

Use `--profile NAME` or pin the contract as `--profile NAME@1`. An individual
media flag is applied after the profile. If it changes a profile setting, or if
`--ffmpeg-arg` is supplied, the resolved result is reported as `custom@1`. A
custom treatment is limited to a single essence and requires explicit output
`codec` and complete `essence_parameters` in `--flow-metadata`. Multi-stream
transcodes must be performed before ingest because one override cannot
describe each output essence truthfully.

```sh
tamsin --profile preserve -i master.mxf -o https://tams.example.com
tamsin --profile editorial --segment-duration 5s -i edit.mov -o https://tams.example.com
```

The first command reports `preserve@1`; the second reports `custom@1`. Repeating
a profile's own value does not make it custom. Profile and version appear in
both JSON terminal events and human receipts, including dry runs and failures.

## Supported container × codec × storage × Segment matrix

The following is the supported product boundary, not FFmpeg's much larger
capability list.

| Output policy | Containers | Codecs | Storage | Segments |
| --- | --- | --- | --- | --- |
| source bytes (`preserve@1`) | any input whose streams TAMSin can describe; known containers receive a registered media type and unknowns use `application/octet-stream` with a warning | no mux compatibility restriction because bytes are not rewritten | muxed | one Object containing the whole input |
| source-family remux (`editorial@1`) | MP4/QuickTime/3GP/3G2, WebM/Matroska, MPEG-TS/PS, MXF, AVI, ASF, WAV, AIFF, FLAC, MP3, AAC/ADTS, AC-3/E-AC-3, Ogg, JPEG/JPEG 2000/PNG/GIF/WebP | stream copy; the source muxer must accept the source codec, otherwise FFmpeg fails before TAMS is mutated | independent | keyframe-aligned, nominally 10 seconds; a zero-duration custom profile extracts each essence whole |
| MPEG-TS (`streaming-ts@1`) | MPEG-TS written by TAMSin regardless of input container | video: H.264, HEVC, MPEG-2; audio: AAC, MPEG Layer II/III, AC-3, E-AC-3; attached pictures are ignored; other stream types/codecs are rejected before FFmpeg or TAMS mutation | independent | keyframe-aligned, nominally 2 seconds |

Segment duration is a target. Stream copy cannot invent random-access points,
so a GOP longer than the target produces longer Segments. The streaming profile
does not transcode and therefore does not promise a two-second decoder refresh
when the source has a longer GOP.

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
evidence. A deliberate output-policy change bumps the profile version or
renderer epoch.

When `preserve@1` uploads a whole multiplex, FFmpeg writes nothing. Its version
is deliberately absent from both identity and provenance, so upgrading an
unused tool does not manufacture a new Flow for identical source bytes.

## See also

- [Choose how essences are stored](../how-to/choose-how-essences-are-stored.md)
- [Segment media for streaming](../how-to/segment-media-for-streaming.md)
- [Container media-type decision](../explanation/decisions/0005-container-media-types.md)
