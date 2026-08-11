# Segment media for streaming

Readers that assemble HLS from TAMS need Segments they can decode individually. This guide covers getting media into that shape.

For the supported short-form MPEG-TS treatment, select the named profile:

```sh
tamsin --profile streaming-ts -i programme.mov -o https://tams.example.com
```

`streaming-ts@1` stores each essence independently, targets two-second
Segments, and rejects codecs outside its documented MPEG-TS compatibility set
before writing media. See the [supported media matrix](../reference/profiles.md).

## Set a Segment duration

The editorial profile selects a ten-second target:

```sh
tamsin --profile editorial -i programme.mov -o https://tams.example.com
```

To choose a different target:

```sh
tamsin --profile editorial -i programme.mov -d 2s -o https://tams.example.com
```

The value is a target, not a guarantee — cuts land on keyframes, so a long group of pictures produces longer Segments than you asked for. Check what you actually got:

```sh
tamsin api segment list "$FLOW_ID" --format json | jq '[.[].timerange]'
```

## Write MPEG-TS Segments

Some readers require a container that carries decoder configuration in every Segment:

```sh
tamsin --profile editorial -i programme.mov --segment-format mpegts -o https://tams.example.com
```

TAMSin sets the Flow's `container` to `video/mp2t` to match what it wrote, so the metadata describes the stored media rather than the input.

The individual flag changes the editorial profile into `custom@1`; prefer
`--profile streaming-ts` when its complete storage, duration, and codec policy
is the intended result.

## Store an input whole

To store the input as a single Media Object and skip segmentation entirely:

```sh
tamsin -i programme.mov --profile preserve -o https://tams.example.com
```

This performs no container rewrite, but the result cannot be read by an HLS consumer and forfeits partial fetches.

Because nothing rewrites the media on this path, `--ffmpeg-arg` and a changed
`--segment-format` cannot take effect and TAMSin refuses them rather than
ignoring them. Both feed the generated Flow identity, so an option that changed
nothing about the bytes would still change which Flow the ingest lands on. Set
`--segment-duration` if you want them honoured.

## Apply a custom FFmpeg treatment

TAMSin stream-copies by default and never transcodes on its own. A custom
treatment is supported for a single-essence input when its output metadata is
explicit. For example, to re-encode a video-only Flow deliberately:

```sh
tamsin --profile editorial -i video-only.mov -d 2s \
  --ffmpeg-arg=-c:v --ffmpeg-arg=libx264 \
  --flow-metadata h264-output.json \
  -o https://tams.example.com
```

Each argument is a separate `--ffmpeg-arg`, and values beginning with `-` need the `=` form so they are not parsed as flags.

This resolves as `custom@1`. TAMSin rejects `-f`, `-format`, or
`-segment_format` values that contradict the structured `--segment-format`
policy. If the arguments transcode, the named streaming codec guarantee no
longer applies. `--flow-metadata` must provide the output `codec` and complete
`essence_parameters`; input probe metadata is not reused as if it described the
transcode. Multi-stream custom treatments are rejected because one metadata
document cannot describe each output essence. Transcode those assets before
ingest, then ingest the resulting file under a named profile.

Because TAMSin cannot tell from an argument list whether a profile re-encodes, it leaves `generation` unset here rather than claiming the content came straight from its source. Set it yourself if you know:

```sh
echo '{"codec":"video/h264","generation":1,"essence_parameters":{"frame_width":1920,"frame_height":1080,"frame_rate":{"numerator":25,"denominator":1}}}' > h264-output.json
tamsin --profile editorial -i video-only.mov -d 2s \
  --ffmpeg-arg=-c:v --ffmpeg-arg=libx264 \
  --flow-metadata h264-output.json -o https://tams.example.com
```

## See also

- [Segmentation](../explanation/segmentation.md) — why duration is a target and what a container rewrite costs
- [Ingest profiles and supported media](../reference/profiles.md) — exact profile and codec contracts
