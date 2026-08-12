# Prepare MPEG-TS Segments

Use this guide when a downstream system explicitly requires short MPEG-TS
Objects. Select the named profile:

```sh
tamsin --profile mpegts-segments -i programme.mov -o https://tams.example.com
```

`mpegts-segments@1` stores each essence independently, targets two-second
Segments, and rejects codecs outside its documented MPEG-TS compatibility set
before writing media. See the [supported media matrix](../reference/profiles.md).

This profile is a packaging policy, not an end-to-end streaming system. TAMSin
does not create HLS or DASH manifests, adaptive-bitrate renditions, encryption,
or CDN configuration. It also stream-copies: it cannot add keyframes or make an
incompatible codec compatible.

## Check the actual Segment cadence

The two-second value is a target, not a guarantee. Cuts land on source
keyframes, so a long group of pictures produces longer Segments. Inspect the
timeranges TAMSin registered:

```sh
tamsctl segment list "$FLOW_ID" --output json | jq '[.[].timerange]'
```

If a consumer requires a precise decoder-refresh cadence, encode the source
with keyframes at that cadence before ingest.

## Choose source-family Segments instead

If MPEG-TS is not a downstream requirement, use the source-family profile:

```sh
tamsin --profile essence-segments -i programme.mov -o https://tams.example.com
```

It keeps essences independent and targets ten seconds. To make an intentional
custom policy with another target:

```sh
tamsin --profile essence-segments -i programme.mov -d 2s -o https://tams.example.com
```

This reports `custom@1`, because the override changes the named profile.

## Store the input whole

To store the source as a single byte-for-byte Media Object and skip FFmpeg:

```sh
tamsin --profile preserve -i programme.mov -o https://tams.example.com
```

The result is suitable for source preservation, not short time-range reads.

## Apply a custom FFmpeg treatment

TAMSin never transcodes on its own. A custom treatment is supported for a
single-essence input when its output metadata is explicit. For example, to
re-encode a video-only Flow deliberately:

```sh
tamsin --profile essence-segments -i video-only.mov -d 2s \
  --ffmpeg-arg=-c:v --ffmpeg-arg=libx264 \
  --flow-metadata h264-output.json \
  -o https://tams.example.com
```

Each argument is a separate `--ffmpeg-arg`, and values beginning with `-` need
the `=` form so they are not parsed as flags. This resolves as `custom@1`.
TAMSin rejects `-f`, `-format`, or `-segment_format` values that contradict the
structured `--segment-format` policy. Once custom arguments transcode, no named
profile codec promise applies.

`--flow-metadata` must provide the output `codec` and complete
`essence_parameters`; input probe metadata is not reused as if it described the
transcode. Multi-stream custom treatments are rejected because one metadata
document cannot describe every output essence. Transcode those assets before
ingest, then ingest the resulting file under a named profile.

Because TAMSin cannot determine from an arbitrary argument list whether content
was re-encoded, it leaves `generation` unset. Set it when you know the lineage:

```sh
echo '{"codec":"video/h264","generation":1,"essence_parameters":{"frame_width":1920,"frame_height":1080,"frame_rate":{"numerator":25,"denominator":1}}}' > h264-output.json
tamsin --profile essence-segments -i video-only.mov -d 2s \
  --ffmpeg-arg=-c:v --ffmpeg-arg=libx264 \
  --flow-metadata h264-output.json -o https://tams.example.com
```

## See also

- [Segmentation](../explanation/segmentation.md) — why duration is a target and what a container rewrite costs
- [Profiles and supported media](../reference/profiles.md) — exact profile, codec, and resource contracts
