# 0002: Integrate FFmpeg through its command-line boundary

- Status: accepted
- Date: 2026-08-06

## Context

TAMSin must identify media in its supported ingest profile accurately and optionally create independently decodable Flow Segments. FFmpeg can open a much wider set of media than TAMSin promises to classify; an open-ended MIME taxonomy is deliberately outside this decision and is covered by [0005](0005-container-media-types.md). FFmpeg is the broadest maintained codec/container implementation, but its reusable libraries expose a C ABI whose compatibility, licensing, patching, and cross-compilation constraints would become part of every TAMSin build.

## Decision

Treat the FFmpeg suite as a runtime tool dependency. Invoke `ffprobe` with JSON output for deterministic metadata extraction. Invoke `ffmpeg` for optional segmentation/remuxing. Do not link libav libraries into the Go process.

The executable names are configurable for hermetic tests and specialist installations. TAMSin passes arguments directly through `os/exec`, never through a shell. Cancellation terminates the child process, stderr is bounded and attached to diagnostics, and the exact detected FFmpeg version is available through `tamsin doctor`. FFprobe and FFmpeg share a two-process local budget. A non-rolling custom FFmpeg invocation consumes the complete budget; a rolling custom invocation consumes one slot and leaves the other for its required first-Segment probe.

Segmenting FFmpeg processes publish live CSV manifests through inherited pipes. A closed Segment can therefore be discovered without scanning its output directory. TAMSin probes the first generated Segment to establish the container timestamp base, then derives later starts and durations from the manifest rather than spawning FFprobe for every Segment. It commits and removes closed outputs in bounded batches. On supported Unix targets, process-level stop/continue signals apply backpressure at closed-Segment high watermarks without terminating the encoder. Stopping only after an output closes is essential: stopping an oversized in-progress output would leave the sink nothing it could reclaim and deadlock the encoder. The global capacity ledger bounds the unavoidable active-output overshoot, and cancellation always resumes a paused child before asking it to exit.

When FFmpeg writes stored media, its detected version and build fingerprint are
recorded in implementation-prefixed provenance tags. Generated identity uses
the semantic ingest profile and a project-controlled renderer epoch instead of
the package report, so routine FFmpeg updates do not create duplicate Flow
graphs. A whole-file muxed ingest does not run FFmpeg, so the unused executable
is absent from provenance as well.

Whole-file ingest remains available because a complete media file is a valid TAMS Media Object. Segmentation is explicit and uses stream copy by default so TAMSin does not silently introduce a lossy generation. Users may supply a checked, explicit FFmpeg argument profile when transcoding is required.

## Evidence

- `ffprobe` JSON is a documented machine-readable interface designed to expose streams, containers, durations, rates, and codec metadata.
- The TAMS specification itself uses FFmpeg commands in its flow/media-timeline guidance.
- A process boundary lets distributions and container rebuilds consume security-patched FFmpeg packages without rebuilding or relinking Go code.
- Linking libav would require CGO and target-specific development packages, prevent simple cross-compilation, couple TAMSin to libav ABI versions, and make combined-binary licensing dependent on the selected FFmpeg build options.
- Go FFmpeg wrappers still ultimately expose either the process interface or the same C ABI; adding one would not remove the operational dependency.

## Consequences

Native users install FFmpeg separately. The supported OCI image pins and includes it. The process boundary has a measurable startup and local-I/O cost, especially for short media and high track counts; the shared process budget, one-probe timestamp anchoring, multi-output high-track path, and rolling staging window bound and amortise that cost without introducing long-lived native allocations into TAMSin. Upload throughput can overlap rendering until a closed Segment reaches the high watermark; slower storage then applies backpressure instead of allowing disk use to follow programme duration. One active Segment per output may exceed the rolling watermark before it closes; the global staging-capacity guard cancels a render whose active output cannot fit.

## Sources

- [FFprobe documentation](https://ffmpeg.org/ffprobe.html)
- [FFmpeg legal and licensing guidance](https://ffmpeg.org/legal.html)
- [TAMS](https://github.com/bbc/tams)
