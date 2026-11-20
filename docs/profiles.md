# Profiles and supported media

Choose a treatment explicitly: packaging changes CPU use, temporary space,
Object count, API traffic and downstream reads. Run `tamsin profiles` to see the
catalogue, or `tamsin --format json profiles` for its versioned report.

These local treatments are distinct from immutable, service-owned TAMS 8.2 Flow
Profiles. The latter constrain generated technical metadata; neither choice
is inferred from the other.

## Named profiles

| Profile | Essence storage | Segment target | Stored container | Codecs |
| --- | --- | --- | --- | --- |
| `preserve@1` | muxed | whole file | source bytes: the original MP4 or MPEG-TS | existing describable codecs; no remux compatibility restriction |
| `demux@1` | independent | whole essence | MP4 or MPEG-TS, matching the input | stream copy with the [render rules](#supported-media-and-packaging) |
| `muxed-segments@1` | muxed | 10 seconds | MP4 or MPEG-TS, matching the input | stream copy with the render rules |
| `essence-segments@1` | independent | 10 seconds | MP4 or MPEG-TS, matching the input | stream copy with the render rules |
| `mpegts-segments@1` | independent | 2 seconds | MPEG-TS from either accepted input container | stream copy with the render rules |

`tamsin profiles` prints the intended use of each profile.

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
tamsin --profile preserve -i master.mp4 -o https://tams.example.com
tamsin --profile essence-segments --segment-duration 5s -i edit.mp4 -o https://tams.example.com
```

`-o` is the short form of `--endpoint`. The first command reports
`preserve@1`; the second reports `custom@1`. Repeating
a profile's own value does not make it custom. Profile and version appear in
both JSON terminal events and human receipts, including dry runs and failures.

Named profiles and the catalogue policy have [separate versions](compatibility.md#media-policy-exception).
`custom@1` versions the identity recipe for overrides, not an equivalence
between arbitrary FFmpeg treatments.

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

TAMSin accepts **MP4 and MPEG-TS only**, including audio-only MP4. It identifies
containers from content and ISO BMFF brands, not filenames. Renaming a MOV,
MXF, Matroska, WebM, WAV, raw stream or image does not make it supported.
QuickTime, 3GP/3G2 and image brands are refused even though FFmpeg shares their
demuxer with MP4. Playlists, manifests and nested resources remain refused.
Metadata overrides and custom FFmpeg arguments cannot bypass this boundary.

The [named profiles](#named-profiles) table gives each profile's container
and codec policy. The render rules follow.

MP4 retains the existing codec mappings below; this is not an H.264/AAC-only
policy. The output muxer must be able to carry each codec. MPEG-TS stream copy
accepts H.264, HEVC and MPEG-2 video, and AAC, MPEG Layer II/III, AC-3, E-AC-3
and SMPTE 302M audio. This check also applies when `source` resolves to MPEG-TS.

MP4 rendering supports `mov_text` subtitles. Other subtitle carriage and all
data or attachment tracks are unqualified for rendering and are rejected
before Flow mutation, including DVB subtitles, teletext and SCTE-35 in TS.
Use `preserve@1` to keep the original bytes, including ancillary tracks that
TAMSin cannot describe as Flows. Attached cover pictures remain excluded from
the essence graph. Sparse subtitle tracks keep their descriptors in Objects
with no cues; those empty tracks do not contribute invented timing bounds.
Missing audio or video packet timing still fails.

These are TAMSin product limits. BBC TAMS does not mandate a container, and
[AppNote 0006](https://github.com/bbc/tams/blob/34fb31b80cb8afb3194f28c8b787301379caacf8/docs/appnotes/0006-containers-and-mappings.md)
allows a sparse subtitle stream to be absent from some Objects.

Segment duration is a target. Stream copy cannot invent random-access points,
so a GOP longer than the target produces longer Segments. The MPEG-TS profile
does not transcode and therefore does not promise a two-second decoder refresh
when the source has a longer GOP. It also does not create HLS/DASH manifests,
adaptive-bitrate renditions, encryption, or CDN packaging.

Browser HLS playback requires compatible media Objects as well as a playlist.
For H.264/AAC playback through TAMOSS, select `mpegts-segments@1`. Source-family
MP4 segments are ordinary MP4 files, not fragmented MP4 HLS segments.
With `--essence-storage muxed`, each TS Object carries both picture and sound.
When the input MP4 trims the final AAC packet, the published Segment ends at the
programme boundary. A player that decodes the complete TS Object can include
AAC padding in its displayed duration.

TAMSin measures each rendered Object's presentation timestamps and sample
durations to register its Segment range and `ts_offset`; it does not infer
these from container duration. Each Segment is placed where FFmpeg cut it on
the source timeline and covers the span of the stream it was cut on (the first
video stream, otherwise the first stream), so a multiplex keeps the source
clock: interior cuts follow the video. A muxed programme includes audio that
starts before the first video key frame or continues after the last picture.
An independent video Flow starts at its first copied key frame. TAMSin reads
up to five minutes of the video to find that frame. Without one, MP4 output
is refused with `media.unsupported`, and MPEG-TS output is placed by its own
clock. Rendered
MPEG-TS timestamps follow the Flow clock modulo 2^33 at 90 kHz. Its
`ts_offset` identifies the rollover epoch. A Segment begins on its Object's
first sample and ends on the tick its stream's next sample is due on, which
the rate the stream's samples keep names. That keeps Segments together at
rates whose sample period the clock rounds, such as 23.976 fps and 44.1 kHz,
and a gap in the source stays a gap. At such a rate, until the stream has
kept its rate for a whole cycle, 15015 ticks at 23.976 fps and 102400 at
44.1 kHz, a gap of one tick can be mistaken for the clock's rounding, and its
size or position may be reported inaccurately. Where the samples keep a steady
step that is no rate the stream states, a Segment ends on the next Object's
first sample if that follows at the same step. Otherwise it ends where FFprobe
takes its last sample to end. An open GOP presents its leading pictures
before the frame it was cut on. In MPEG-TS they travel in the same Object and
the Segment begins with them, so long-GOP MPEG-2 and open-GOP HEVC place
without holes. A decoder that starts at such an Object needs the Segment
before it to show those pictures, which TAMS allows for open GOPs (ADR 0013);
use `preserve` where every Object must decode whole on its own.
H.264 copied from MP4 into MPEG-TS has the parameter sets of its sample
description written before every key frame, so each Object decodes from its
own first key frame. Those sets are the right ones only while the stream
does not replace one of them, so video that gives a parameter set new content
in the stream is refused with `media.unsupported` where the change arrives.
Sets with new identifiers are followed as long as every picture has them. An
MPEG-TS source is rendered without this automatic insertion and `--ffmpeg-arg`
bypasses it, and an Object that would hold a picture without the parameter
sets it refers to is refused in the same way. Both refusals keep the Segments
before them. Use `preserve` for that video. The check covers the AVC base
layer and auxiliary pictures. Scalable layers and additional views are not
checked, and `preserve` keeps them as supplied.
An MP4 Object does not show open-GOP leading pictures, which would leave a hole
before every such cut, so TAMSin refuses to cut MP4 Segments on an open GOP
with `media.unsupported`. Video whose cuts all land on closed GOPs is kept.
A multiplex of several video streams is cut on the first one's key frames. A
cut that falls between another video stream's key frames would leave that
stream undecodable in the Object, so it is refused in the same way, in MP4
and in MPEG-TS. Every cut of such a multiplex is checked before anything is
written, so a remote input is staged, and `--input-mode=stream` is refused
with `source.stream_unavailable`. A multiplex is cut exactly only on its
first video stream: sound and further pictures stored either side of a cut
stay in the Object they were stored in, so a few of their samples can present
just outside that Object's Segment. Continuous playback of a muxed MP4 Flow
can skip audio at a cut or end its audio early. When the video has B-frames,
the next Object can contain audio from just before the cut. A player may skip
those samples because their timestamps precede the Object's playback start.
Use `essence-segments` or `mpegts-segments` where each stream must be cut on
its own timeline, or `mpegts-segments --essence-storage muxed` to keep audio
and video together in MPEG-TS.
A staged input is refused before anything is written, unless it is rendered
with custom FFmpeg arguments. That render, like a streamed input, is refused
at the first such cut and keeps the Segments before it. Use `mpegts-segments`
or `preserve` for that video.
Adjacent Segments abut exactly; a gap in the source stays a gap on the Flow; a
cut whose pictures present before the previous Segment ends is refused. A fixed-rate video reference stream is
regularised to its nominal period when the container rounds its timestamps
(for example, a coarse time base may round a ten-second span short), following AppNote 0012; a span that differs from the
nominal by more than a quarter of a frame, such as a frame missing inside the
Object, is kept as measured. Each measured Segment also carries
`key_frame_count` (the reference stream's access points) and `last_duration`
(how long its last sample presents). A correction to placement changes the
renderer epoch, so a retry creates new Flows rather than reusing Flows written
under the earlier rule; existing Flows are never rewritten. The
[changelog](../CHANGELOG.md) records each such change. The upgrade from
8.2.0-in2 renews the renderer epoch to 6: generated Source IDs stay the same,
and a Flow ID given explicitly for an earlier render needs replacing with a
fresh one.

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
After cadence is established, a segment without usable timestamp evidence
retains the declaration. Other metadata is still checked, and boundary
comparisons resume between adjacent observed segments, not across the gap.

If initial evidence cannot establish cadence, TAMSin makes neither claim.
TAMS has no representation for an unknown video rate, so the final Flow fails
schema preflight. Non-monotonic timestamps fail streamed validation.
Local/staged workflows can supply complete `essence_parameters` through
`--flow-metadata`; streaming does not bypass its timestamp checks for an override.

### Video presentation

| TAMS field | FFprobe evidence | Automatic support | Uncertain case |
| --- | --- | --- | --- |
| `frame_width`, `frame_height` | `width`, `height` | Required positive values | Ingest is rejected when either is absent |
| `frame_rate`, `vfr` | packet `pts` or decoded `best_effort_timestamp`, then the declared rate for the fixed rational | Fixed and genuinely variable cadence | Missing/non-monotonic timestamps are not guessed |
| `interlace_mode` | stream `field_order` | `progressive`, `tt` → `interlaced_tff`, `bb` → `interlaced_bff` | `tb`/`bt` and PsF are omitted because FFmpeg cannot distinguish them reliably |
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
| Data without a codec | none | A data or attachment track whose codec has no media type below (such as `tmcd` timecode) is not described as a Flow. Preserve keeps the track and warns that no Flow describes it; rendering is refused. Video, audio and subtitle streams with unknown codecs still stop the ingest |

### Codec media types

The table is keyed by FFprobe's `codec_name`, not a filename extension. These
are the automatic mappings for codecs found inside accepted containers; a
mapping does not qualify a codec for rendering:

| Essence | FFprobe names | TAMS `codec` |
| --- | --- | --- |
| Video | `h264`, `hevc`, `av1`, `ffv1` | `video/h264`, `video/h265`, `video/AV1`, `video/FFV1` |
| Video | `mpeg1video`, `mpeg2video`, `mpeg4`, `vp8`, `vp9` | `video/mpeg` (MPEG-1/2), `video/mp4v-es`, `video/VP8`, `video/VP9` |
| Video | `prores` | `video/quicktime` (the established compatibility mapping; IANA has no ProRes coding subtype). `video/mpeg` for MPEG-1 and MPEG-2 video is likewise also the MPEG programme stream container type; on an elemental Flow it names the coding |
| Video | `dvvideo`, `dnxhd` | `video/DV`, `video/x-dnxhd` |
| Video | `jpeg2000` on a moving-picture stream | `video/jp2`, as the BBC reference Flows use |
| Image coding | `mjpeg`, `jpeg2000` (still image), `png`, `gif`, `webp` | `image/jpeg`, `image/jp2`, `image/png`, `image/gif`, `image/webp` |
| Audio | `aac`, `ac3`, `eac3`, `flac`, `mp2`, `mp3`, `opus`, `vorbis` | `audio/aac`, `audio/ac3`, `audio/eac3`, `audio/flac`, `audio/mpeg` (MP2/MP3), `audio/opus`, `audio/vorbis` |
| Audio | `dts`, `truehd`, `s302m` | `audio/vnd.dts`, `audio/vnd.dolby.mlp`, `audio/x-smpte302m` |
| Audio | `pcm_alaw`, `pcm_mulaw` | `audio/PCMA`, `audio/PCMU` (companded G.711 is a codec, so no `unc_parameters`) |
| PCM audio | other `pcm_*` | `audio/x-raw-int` or `audio/x-raw-float`, the explicit vocabulary used by the pinned TAMS audio schema; 16- and 24-bit float, which the schema cannot describe, are unmapped |
| Timed text/data | `webvtt`, `ttml`, `subrip`, `ass`, `mov_text` | `text/vtt`, `application/ttml+xml`, `application/x-subrip`, `text/x-ssa`, `text/x-quicktime-text` |
| Broadcast data | `dvb_subtitle`, `dvb_teletext`, `scte_35`, `eia_608` (QuickTime `c608` captions) | `application/x-dvb-subtitle`, `application/x-dvb-teletext`, `application/x-scte35`, `application/x-cea-608` |

Where IANA registers a coding media type it is used. The `x-` names above are
the only ones TAMSin writes: they are fixed values for codecs with no
registration, chosen once so that every TAMSin deployment names them the same
way, and they are listed here so a consumer can match them. An FFmpeg codec
outside this table produces no generated `codec`. TAMSin logs the codec name,
stream type and stream index; it never constructs a name from the FFmpeg codec
string. The pinned elemental Flow schemas require `codec`: without a valid explicit override, final schema
preflight rejects the Flow before TAMS mutation.

## Supply Flow metadata

For a label or other workflow-owned fields, write a JSON object and preview
the ingest before uploading:

```sh
printf '%s\n' '{"label":"Studio programme","description":"Ingested master"}' > flow.json
tamsin ingest --profile preserve --flow-metadata flow.json \
  --dry-run=exact --input ./programme.ts
tamsin ingest --profile preserve --flow-metadata flow.json --input ./programme.ts
```

`--flow-metadata` is most useful for a single-essence input whose workflow owns
a more specific registry mapping. In a multi-essence file each track may need a
different value, so `--collected-flow-metadata` takes a JSON object keyed by
collection role (`video`, `audio 1`, `data`) whose values are merged into that
one essence Flow; a role the input does not produce is an error rather than a
silently unused override. For example, if planning reports roles `video` and
`audio`, label each one with:

```json
{
  "video": {"label": "Programme video"},
  "audio": {"label": "Programme audio"}
}
```

Save that object as `collected.json` and add
`--collected-flow-metadata collected.json` to both the exact dry run and the
real command. Use the exact roles from your input's `flow.planned`
events, including numbered roles such as `audio 1` for multiple audio tracks.
Supply technical fields only when you know the output's properties; complete
`essence_parameters` replace that object rather than patching individual fields.

Overrides cannot directly set IDs. Technical overrides can change generated
IDs for streamed inputs; see [identity and resume](concepts.md#generated-identity-and-resume).
Resume keeps existing operator labels and descriptions unless explicitly
overridden for that Flow. In muxed storage, `--flow-metadata` targets the root
and `--collected-flow-metadata` targets the named roles. Independent essences
inherit `--flow-metadata`, with per-role overrides taking precedence.

See [operations](operations.md) for staging, verification and recovery.
