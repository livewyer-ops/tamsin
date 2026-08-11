# 0004: Use one FFmpeg pass for high-track-count demultiplexing

- Status: accepted
- Date: 2026-08-08

## Context

The `demux`, `essence-segments`, and `mpegts-segments` profiles use independent
essence storage, so a
multiplexed input becomes one Flow per essence. TAMSin produces those essences by invoking FFmpeg once per
stream: each invocation opens the input, maps a single stream, and writes that
essence's Segments.

The obvious objection is that this reads the input once per stream. A sixteen
track master would be opened sixteen times, and the natural fix is a single
invocation with sixteen mapped outputs. That change is not free: `Segmenter`
currently answers "give me the Segments for this one stream", and one-pass
demultiplexing means it has to answer "give me the Segments for all of these
streams", which changes the interface, the FFmpeg argument construction, the
pipeline code that schedules the work, and every test double.

The original implementation retained one process per essence because the
common two-track case saved only 0.08 seconds on the measured five-minute
fixture. Resource review later identified the other side of the threshold:
four or more passes multiply process startup, descriptors and page-cache
pressure enough to affect constrained workstations and containers.

## Decision

Keep the simple per-essence path for one to three essences. For four or more
essences using the built-in stream-copy treatment, open the input once and map
every essence to its own output in one FFmpeg process. Each output has a
dedicated live manifest pipe, so Segment records retain their stream identity.

Custom FFmpeg arguments remain on the per-essence path because arbitrary
output-scoped options cannot be duplicated safely. Those invocations run one
at a time under the shared media-process budget.

## Evidence

Both arrangements were measured on the same inputs, stream-copying to ten second
Segments, comparing N sequential single-stream invocations against one
invocation with N mapped outputs.

A 60 second, 21 MB input:

| Tracks | Per-stream | One pass | Speed-up |
| --- | --- | --- | --- |
| 2 | 0.17s | 0.11s | 1.59x |
| 4 | 0.32s | 0.15s | 2.19x |
| 8 | 0.64s | 0.24s | 2.62x |
| 16 | 1.45s | 0.40s | 3.64x |

A 5 minute, 720p input of roughly 240 MB:

| Tracks | Per-stream | One pass | Saving |
| --- | --- | --- | --- |
| 2 | 0.72s | 0.63s | 0.08s (1.13x) |
| 8 | 1.91s | 1.31s | 0.60s (1.46x) |

The speed-up *falls* as the input grows, which is the opposite of what the
"N reads of the input" argument predicts. The saving is dominated by fixed
per-process cost — starting FFmpeg, opening and probing the container — and that
cost is per invocation, not per byte. The repeated reads themselves are largely
absorbed by the page cache, so the second and later passes do not return to
disk.

On the common case of a video and an audio track in a 240 MB input, one-pass
demultiplexing saves 0.08 seconds, against an ingest that also hashes and
uploads 240 MB.

## Consequences

`Segmenter` accepts one or several stream indexes and emits closed Segment
records through a callback. The four-track threshold is pinned by tests: it
captures the measured 2.19x short-input improvement while retaining the simpler
and effectively equivalent common two-track path.

The measurement assumes the input fits in the page cache. Constrained hosts are
exactly where repeated reads are least reliable, so the one-pass high-track path
also provides the safer local-resource bound even when wall-clock savings vary.

## Sources

- AppNote 0005, which is why the segmented independent profiles use this
  storage model and the question arises at all.
