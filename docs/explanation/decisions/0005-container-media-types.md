# 0005: Own a supported container profile, not a universal classifier

- Status: accepted
- Date: 2026-08-08

## Context

An object-owning TAMS Flow declares the MIME type of its Segment containers.
The TAMS 8.1 schema prefers IANA registrations and asks for the closest type to
the Flow format where a family offers several choices, but it does not define a
complete format registry or an identification algorithm. The same `container`
property also signals that a Flow owns Media Objects, so omitting it is not a
safe way to express uncertainty.

FFprobe identifies demuxer families, codecs, streams and ISO Base Media File
Format tags, but does not report a file MIME type. Host extension databases are
not deterministic and an extension may lie. Content-sniffing libraries provide
useful evidence but do not cover the broadcast profile consistently and cannot
choose a type according to the Object's actual presentation.

Broadcast interchange normally obtains interoperability by constraining an
output profile, such as AS-11 or an agreed MPEG-TS workflow, rather than making
every ingest client classify every format a media framework can open.

## Decision

TAMSin delegates format identification to FFprobe and owns a small,
deterministic projection from supported inputs and outputs to TAMS `container`
values:

- An explicit output format wins. MPEG-TS Segments are `video/mp2t`, including
  when they carry only audio.
- Known source families are selected from FFprobe's `format_name`, the actual
  audio/video presentation, and `major_brand` for the ambiguous ISO family.
- MP4 containing video is `video/mp4`; audio-only MP4 is `audio/mp4`; only MP4
  with neither presentation is `application/mp4`.
- QuickTime remains `video/quicktime`; no unregistered `audio/quicktime` value
  is invented.
- Matroska uses the registered `video/matroska` and `audio/matroska` values.
  WebM is tested before its Matroska parent because FFprobe reports
  `matroska,webm`.
- File extensions and the host MIME database are never classification
  authority.
- An unsupported or ambiguous format is `application/octet-stream`. TAMSin
  emits a warning containing FFprobe's format and brand. The operator may set a
  precise value through `--flow-metadata` when a workflow has knowledge TAMSin
  does not.
- Source-format segmentation names the FFmpeg muxer from the same probe-derived
  profile, never from the input filename. If TAMSin can store a format whole but
  cannot safely name a source remuxer, that treatment is rejected before TAMS
  mutation; the operator can store it whole or select the explicit MPEG-TS
  profile where its codecs are compatible.

The table is covered by fixtures and standards-derived cases. No second runtime
media parser or test-only heuristic MIME database is added merely to populate
this property.

## Consequences

TAMSin can store a format FFmpeg understands whole without pretending to
identify it more precisely than the evidence allows. A format TAMSin cannot
safely remux is not accepted for source-format segmentation merely because
FFmpeg can open it. Expanding the supported profile is an intentional code,
test and documentation change. The same input produces the same metadata on
every supported host, and a misleading suffix cannot alter either the Flow or
the muxer used for its Objects.

`application/octet-stream` is less helpful to a reader than a precise type, so
the warning is part of the behaviour rather than a debug message. Workflows that
require a constrained interchange format should choose the explicit MPEG-TS
output profile or enforce their own input profile before invoking TAMSin.

## Sources

- [TAMS 8.1 Flow schema](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/api/schemas/flow-core.json)
- [TAMS AppNote 0002: Timing in MPEG-TS](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0002-Timing-in-MPEG-TS.md)
- [TAMS AppNote 0006: Containers and mappings](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0006-containers-and-mappings.md)
- [RFC 2046 section 4.5.1: application/octet-stream](https://www.rfc-editor.org/rfc/rfc2046.html#section-4.5.1)
- [RFC 4337: MPEG-4 media types](https://www.rfc-editor.org/rfc/rfc4337.html#section-2)
- [RFC 9559: Matroska media types](https://www.rfc-editor.org/rfc/rfc9559.html#section-27.18)
- [AMWA AS-11](https://www.amwa.tv/as-11)
