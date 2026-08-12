# Segmentation

A Flow Segment maps a Media Object onto a period of a Flow's timeline. Segmentation is the decision about how finely to cut that media up, and it is more consequential than it first appears — it determines what a reader can do with your content without decoding it.

## Why short Objects

[AppNote 0001](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0001-multi-mono-essence-flows-sources.md) describes Media Objects as "typically short (on the order of seconds) and independently decodable to allow for efficient random access". [AppNote 0005](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0005-indepentent-segments.md) sets out what that independence buys:

- A store-to-store transfer becomes a copy. Nothing has to decode the media to work out which pieces are needed, because each piece stands alone.
- Copy-by-reference clipping works. A new Flow can reference existing Segments as a metadata-only operation.
- A corrupted or missing Segment costs you that Segment, not its neighbours as well.
- HLS profiles that require independently decodable Segments can read the Flow.

Storing media as whole Objects is legal TAMS and is what a zero Segment duration
does. Muxed storage creates one whole-input Object; independent storage creates
one whole Object per essence. Both forfeit the four benefits above. A reader
wanting ten seconds from the middle must fetch the complete owning Object.

The versioned `preserve@1` and `demux@1` profiles select whole-file or
whole-essence treatments. `muxed-segments@1` and `essence-segments@1` select
ten-second source-family policies, while `mpegts-segments@1` selects the
conservative two-second MPEG-TS policy. Ingest requires one of these choices
explicitly.

Note that AppNote 0005 makes independence *recommended*, not mandatory, and is candid about the drawbacks: some codec configurations do not support short independently decodable groups of pictures, and waiting to close a Segment adds write latency.

## The duration is a target, not a promise

Segmentation cuts on keyframes, and stream copy cannot manufacture keyframes that are not already in the media. A long group of pictures therefore raises the floor on how short a Segment can be. Ask for two seconds from a source whose only interior keyframe is at 8.3 seconds and you get two Segments, not five.

This is not a TAMSin limitation but a property of cutting coded media without re-encoding it, and TAMS says the same thing in its own words: the `segment_duration` Flow property is documented as a value that "the duration for each Segment may vary around". TAMSin sets that property from your requested duration, so the store records the intent alongside the actual Segments.

FFmpeg publishes each closed Segment through a live manifest. TAMSin probes the
first generated Segment to establish the output container's timestamp base and
derives later starts and actual durations from that manifest. This keeps local
process use independent of the number of Segments. Closed outputs are hashed,
committed, journalled, and removed in batches of at most 64 Objects or 64 MiB
(lowered when the staging window is smaller). On Linux and macOS, FFmpeg is
paused at a closed-Segment boundary when staging reaches its high watermark and
resumed after completed outputs are reclaimed. The active Segment for each
output must be allowed to close before it can be uploaded, so it may temporarily
cross the rolling watermark; the global staging-capacity ledger cancels the
render if that unavoidable overshoot cannot fit. A late renderer failure
therefore leaves a named, resumable prefix.
FFprobe and FFmpeg still share a two-process local budget. Only one rolling
FFmpeg render runs at a time, leaving one slot for its first-Segment timestamp
probe; uploads and verification can continue concurrently. This makes local
process and memory use predictable, at the cost of serial rendering when a
batch contains several inputs that all need segmentation.

If you need Segments at a precise cadence, the source has to carry keyframes at that cadence — which means influencing the encoder, not the ingest.

## Segmenting is a container rewrite

Even under stream copy, cutting media rewrites containers. The coded pictures and samples are preserved bit-for-bit — you can verify this by comparing decoded frames — but the framing around them is not. Moving MPEG-TS into MP4 or the reverse changes how each frame is delimited, and TS additionally carries its own packetisation and per-frame access unit delimiters.

The practical consequence: a Segment is **not** a byte-for-byte slice of your source file. You can reassemble Segments and decode identical pictures; you cannot reassemble them and recover the original file's bytes. Where that matters, `-d 0` with muxed essence storage is the only combination that stores exactly what arrived.

## Choosing a container

`--segment-format` states what you want rather than how to get it. `source` keeps the input's own container; `mpegts` writes MPEG-TS. Because Flow metadata is built from the source probe *before* any segmentation runs, TAMSin corrects the Flow's `container` to describe what was actually written — otherwise a Flow would advertise the input's container while holding Segments in another.

With `source`, TAMSin maps the formats in its supported profile from FFprobe's
format family, the streams actually present, and the file's `major_brand` where
the ISO family is ambiguous. It passes the corresponding muxer to FFmpeg
explicitly and never trusts the filename extension. If the format can be stored
whole but is outside the safe source-segmentation profile, TAMSin fails before
changing TAMS; use `--segment-format mpegts` where the codecs are compatible, or
store it whole with `-d 0 --essence-storage muxed`.

For an unclassified whole-file Object, the Flow uses the honest generic value
`application/octet-stream` and stderr names the FFprobe evidence it could not
classify. A workflow that knows the correct type can override it explicitly:

```json
{"container":"application/vnd.example.media"}
```

```sh
tamsin --profile essence-segments -i programme.bin --flow-metadata container.json -o https://tams.example.com
```

This fallback keeps the required distinction between a Flow that owns Objects
and an empty collector Flow, which must omit `container`; it is not a claim that
the media bytes themselves have no more specific type.

There is deliberately no high-level fragmented-MP4 option in this release.
TAMS 8.2 can associate a separately allocated initialisation Object through
`init_object_id`, and TAMSin's typed Segment API preserves an explicitly
supplied value. Producing fMP4 safely still needs coordinated initialisation
Object allocation, verification, resume, retraction, and player-compatibility
tests. Exposing a renderer flag before that lifecycle exists would create
apparently valid Flows whose media may not be independently usable. MPEG-TS
carries decoder configuration in every Segment and remains the supported short
independently decodable representation.

## See also

- [Prepare MPEG-TS Segments](../how-to/prepare-mpegts-segments.md) — the task
- [Integrity](integrity.md) — what happens to Segments that fail verification
- [Container media-type decision](decisions/0005-container-media-types.md) — the supported-profile boundary and fallback
- [Ingest profiles and supported media](../reference/profiles.md) — versioned treatments and compatibility matrix
