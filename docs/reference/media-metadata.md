# Media metadata support

TAMSin describes the essence it can establish from FFprobe evidence. It is not
a universal media classifier, and it does not turn an unfamiliar FFmpeg name
into a plausible-looking MIME type. Optional TAMS fields are omitted when the
probe does not support a defensible claim. Required fields must either be in
the supported profile or be supplied explicitly before the Flow passes the
pinned TAMS schema preflight.

The boundary matters most for video cadence. TAMS 8.1 requires exactly one of:

- `essence_parameters.frame_rate` for fixed-rate video; or
- `essence_parameters.vfr: true` for variable-rate video, with `frame_rate`
  absent.

`avg_frame_rate` and `r_frame_rate` do not answer that question. TAMSin makes a
second, streaming FFprobe pass over decoded presentation timestamps. It emits
`vfr: true` when presentation intervals vary, and emits the probed rational
`frame_rate` when they form one cadence. Adjacent tick counts may differ by one
because a rational cadence such as 30000/1001 has to be quantised onto a
container time base; that alone is not treated as VFR. The pass retains only
the preceding timestamp and interval bounds, so memory use does not grow with
programme length. When `--flow-metadata` supplies the complete
`essence_parameters` object, this decoded pass is skipped because its result
cannot affect the overridden Flow.

If frames have missing or non-monotonic presentation timestamps, TAMSin makes
neither claim. TAMS has no representation for an unknown video rate, so the
final Flow fails schema preflight unless a workflow supplies the complete
`essence_parameters` override through `--flow-metadata`.

## Video presentation matrix

| TAMS field | FFprobe evidence | Automatic support | Uncertain case |
| --- | --- | --- | --- |
| `frame_width`, `frame_height` | `width`, `height` | Required positive values | Ingest is rejected when either is absent |
| `frame_rate`, `vfr` | decoded `best_effort_timestamp` sequence, then `avg_frame_rate`/`r_frame_rate` for the fixed rational | Fixed and genuinely variable cadence | Missing/non-monotonic timestamps are not guessed |
| `interlace_mode` | stream `field_order` plus container family | non-MXF `progressive`, `tt` → `interlaced_tff`, `bb` → `interlaced_bff` | MXF `progressive`, `tb`/`bt`, and PsF are omitted because FFmpeg cannot distinguish them reliably |
| `pixel_aspect_ratio` | `sample_aspect_ratio` | Positive colon-separated ratios | Missing, `0:1`, and malformed values are omitted |
| `aspect_ratio` | `display_aspect_ratio` | Positive colon-separated ratios | Missing or malformed values are omitted |
| `colorspace` | `color_primaries`, with matrix coefficients only as a fallback | BT.601, BT.709, BT.2020; BT.2100 when BT.2020 primaries carry HLG/PQ | Unknown names are omitted |
| `transfer_characteristic` | `color_transfer` | SDR, HLG, PQ | Unknown names are omitted |
| `bit_depth` | `bits_per_raw_sample`, `bits_per_sample`, then pixel-format depth | Positive known depths | Omitted when the probe exposes none |

PsF is a temporal production fact: two fields were captured at the same
instant. FFprobe exposes coding/field order but no PsF property, and in some MXF
paths it reports segmented-frame layout as progressive. TAMSin therefore never
synthesises `interlaced_psf`. A workflow that knows the source is PsF may set
`essence_parameters.interlace_mode` to `interlaced_psf` in a complete metadata
override; the pinned schema accepts exactly `progressive`, `interlaced_tff`,
`interlaced_bff`, and `interlaced_psf`.

## Other essence parameters

| Flow format | TAMS fields | Automatic support |
| --- | --- | --- |
| Audio | `sample_rate`, `channels` | Both are required and ingest is rejected if FFprobe does not supply positive values |
| Audio | `bit_depth` | From raw/sample depth or other probed format evidence when present |
| PCM audio | `unc_parameters.unc_type` | Interleaved by default; planar for FFmpeg's `pcm_*_planar` family and `pcm_lxf` |
| Still image | `frame_width`, `frame_height` | Required positive values; a zero-duration single image stream is not described as moving video |
| Data | `essence_parameters` | Empty unless supplied by an operator; TAMSin does not invent a `data_type` URN |

## Codec media-type profile

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

## What the fixtures prove

The test suite generates actual media with FFmpeg and reads it back with
FFprobe. It covers 30000/1001 fixed cadence on a millisecond time base, a true
24-to-30 fps variable-cadence file, top- and bottom-field-first MPEG-2 video,
anamorphic standard-definition video, BT.2100 HLG signalling, and ISO container
brands. Table-only tests cover unknown and contradictory values so an FFmpeg
upgrade cannot silently turn missing evidence into confident metadata.

This profile targets the [pinned TAMS 8.1 video schema](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/api/schemas/flow-video.json)
and [ADR 0041](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/adr/0041-require-explicit-framerate.md).
