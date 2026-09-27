# Operations and recovery

## Check readiness

Run doctor with the profile and endpoint intended for the job:

```sh
tamsin doctor --online --profile essence-segments --format json >doctor.json
```

Doctor is input-free and read-only: it never probes media, renders or mutates
TAMS. Without a profile, it checks the union of local dependencies and lists
the available choices; ingest still requires a selection. Local checks run by
default. `--online` adds service and storage-backend GETs, and may exchange
credentials at the configured OAuth token endpoint.

Checks appear in this order, even when an earlier failure makes later work
unsafe. Independent checks continue; dependent checks are marked `skipped`.

| Check | What it establishes |
| --- | --- |
| `configuration` | File, environment, flags and static values resolve safely |
| `profile` | The treatment is usable, or available profiles are listed |
| `staging` | The directory is writable and has space for the budget |
| `ffprobe` | The configured executable completes its version check |
| `ffmpeg` | Available when rendering may be needed; skipped for `preserve`, required conservatively for `demux` |
| `authentication` | Online transport policy and a successful read-only request |
| `service` | Service metadata is readable |
| `api_compatibility` | API version meets the 8.1 floor and is compared with the 8.2 target |
| `service_lifetimes` | Object and URL lifetime guarantees are valid |
| `storage_backends` | Storage discovery succeeds |
| `storage_selection` | The requested backend exists, or a usable default can be selected |

JSON output is one document with `schema_version: "1.0"`, not ingest NDJSON.
Checks have `name` and `status` (`pass`, `fail`, `skipped`); failures include
`error`, and skipped checks give `detail.reason`. The report includes tool,
build, platform and profile provenance. Endpoint and credential diagnostics
are redacted; peer bodies, storage IDs and executable paths/banners are omitted.
Tool checks run sequentially, each with a five-second deadline.

With multiple failures, doctor selects exit categories in order: configuration
(2), media dependency (6), authentication/policy (3), TAMS validation (7), then
local runtime (1). Cancellation exits 8; failure to write the report exits 1.

## Local resources and scheduling

All profiles use FFprobe. Local and staged video may require a complete decoded
timestamp scan before upload, even under `preserve`. Streamed inputs are
validated segment by segment; this reduces startup I/O but adds per-segment
probe work. It does not eliminate B-frame decoding or guarantee lower total CPU
use. Overlapping FFmpeg and validation can also increase peak RAM. Choose
`--input-mode=stage` when startup latency and temporary disk use matter less
than process overlap. See [profiles](profiles.md) for processing and Object-count
costs.

`--dry-run=exact` renders and validates using the selected input mode, without
TAMS mutation. `--dry-run=fast` skips rendering and cannot report exact Objects.
For streaming it performs only the initial probe and warns that cadence and
output metadata remain unverified; insufficient metadata still fails planning.

`--concurrency` bounds inputs, `--transfers` bounds uploads and verification
across the run, and `--probe-concurrency` bounds queued measurements; active
media processes remain capped at two in total. Segmentation uses a rolling disk
spool, not an in-memory copy of the input. Streamed independent essences share
one FFmpeg input reader. Whole-essence demultiplexing can need the staged source
and complete output essences at once.

`--staging-byte-budget` coordinates one process, not an entire filesystem.
Use a writable `--temp-dir` with sufficient headroom and a volume quota when
several processes share it.

The default `auto` budget uses 80% of free space in `--temp-dir` at startup.
A configured budget is capped by available space. Streaming reserves output
space only, with a target of at most 512 MiB per active input, reduced for
smaller budgets. Closed segments are removed after registration and verification.
Large active segments can exceed the target; the global budget remains the
limit. Staging needs the whole remote source plus its output allowance.

Updates to one root Flow are serialised within a process only. Schedule one
writer per explicit or deterministic root Flow across processes, or provide
external coordination. SIGINT/SIGTERM stop new work and allow bounded cleanup;
give workloads more than the shared 30-second cleanup deadline before a hard
kill. A second signal, crash or hard kill can leave `tamsin-input-*` or
`tamsin-segments-*` directories. Remove them only after confirming no process
uses them; age alone is not sufficient.

## Containers

The [Docker example](../README.md#using-docker) mounts one input read-only and
passes credentials by environment variable. The image runs as UID/GID 65532:

- Inputs must be readable by that user. Grant read access to the intended media
  file or volume; do not make a private media directory world-writable or run
  as root to bypass a permission error.
- The endpoint must be reachable from the container. `localhost` refers to the
  container itself, not the host or another service.
- `/tmp` is writable in a normal container. For larger jobs or a read-only root
  filesystem, provide a writable temporary volume and select it with
  `--temp-dir`. Size the volume above the staging budget and allow headroom for
  logs and concurrent processes. A memory-backed temporary volume consumes RAM.
- Use secret injection for credentials and retain both output streams. See
  [authentication](configuration.md#authentication) and [events](events.md).

## Verification and network cost

Default `--verify=auto` compares SHA-256 evidence from a successful upload's
response (`X-Amz-Checksum-Sha256`, `Repr-Digest` or `Digest`) with the digest
of the uploaded bytes. A response `Content-Digest` describes the response body
(RFC 9530), not the stored Object, so it is not evidence; nor are request-only
checksum headers or ETags. Malformed evidence or a mismatch fails before
Segment registration. An upload to an unsigned URL carries the Object's digest
as `Content-Digest`, so storage that checks digests refuses a corrupted
transfer; a presigned URL is sent exactly as issued. S3 returns
`x-amz-checksum-sha256` only when the service's `put_url` instruction includes
that header in the signed request, so without it readback remains the
verification.

Without storage evidence, TAMSin registers the Object and reads it back.
TAMS requires unregistered Objects to return 404, so download verification
cannot happen earlier. Storage-attested uploads can satisfy BBC
[ADR 0016's pre-registration recommendation](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/adr/0016-checksums-and-filesize.md).
Resumed Objects always use readback because the original upload response is
unavailable. New ingests therefore transfer from roughly one to two times the
Object bytes, depending on storage support.

`--verify=readback` requires an independent download. `--verify=none` skips
integrity checks explicitly and does not report the bytes as verified.
Readback shares the upload transfer budget and remains real storage/network I/O.
The `_tamsin_sha256` Flow tag hashes a local or staged input, not each rendered
Object. Streamed inputs omit it and record `_tamsin_input_revision` instead;
every output Object still has its own SHA-256. Only whole-file muxed `preserve`
keeps the original bytes unchanged.

Transfer slots are acquired before requesting URLs. Small batches are uploaded
and registered before more storage is allocated. Presigned expiry limits when
a request starts, not a healthy transfer already in flight. Non-presigned URLs
have no such start deadline. Scheduling respects advertised lifetimes but
cannot guarantee registration under arbitrary network delays.

API credentials travel only with unsigned media URLs on the API origin. A
presigned URL is requested exactly as issued, and other origins never receive
the API credentials. Uploads carry the store's header instructions; with no
`Content-Type` instruction the Flow's `container` is sent. Verification tries
each advertised download URL in turn, presigned first.

## Recovery

Streaming writes the complete Flow graph only after initial segments establish
its metadata and assigned Profiles match. It then commits the first valid
batch immediately. Every later segment is checked before storage allocation,
including cadence across segment boundaries. A later contradiction stops new
uploads and discards queued output. Already committed valid segments remain;
the Flow is not marked `closed_complete`. Use `--input-mode=stage` when the whole
input must pass media preflight before the first upload.

A Segment that already occupies an Object's exact timerange under another
identifier (an earlier run that fell back to a service-assigned identifier,
or another producer) is adopted only when its bytes and timing match, which
needs readback; otherwise, and for any partial overlap, the input stops with
`tams.segment_conflict` and nothing is uploaded. TAMSin never registers an
overlapping Segment.

A failed verification retracts only the exact Object/timerange Segment.
Cleanup is detached from cancellation and bounded by `--deletion-timeout`
(default five minutes), while each request inside it keeps `--timeout`. A 202
DELETE response is followed through its same-service deletion request to
`done`; a 202 without a usable `Location` (missing, on another origin, or
outside the API path) is logged and confirmed by absence instead, and a request
that ends in `error` is accepted when the Segment has nevertheless gone. Both
202 and 204 require the exact Segment to become observably absent. Ambiguous
transport errors and 404s are also reconciled by checking absence. Failed
cleanup is reported alongside the original failure, never as successful
retraction.

After the Flow graph is written, TAMSin reads each Source the store derived
and sets its label and description when they are empty, plus its own
`_tamsin_` provenance tags, so a Source lists under a readable name (AppNote
0007). A label or description already present, an operator's for instance, is
left alone. A Source that cannot be read or written is logged as a warning;
the ingest does not depend on it.

A partial registration response identifies failed Segments; TAMSin retracts
the registered complement. A lost response instead requires fresh readback:
visible Segments are verified and unresolved ones receive targeted retraction.
If readback fails, every Object in the batch is treated as possibly registered.
Recovery shares one deadline across the batch and reports every outcome;
ordinary verification cleanup shares one deadline too (the deletion timeout,
at least 30 seconds), not one per Object.

## Troubleshooting

If your shell reports `tamsin: command not found` after installation, add
`~/.local/bin` to `PATH`:

```sh
export PATH="$HOME/.local/bin:$PATH"
tamsin --version
```

Add the export to your shell's startup file to keep it in new sessions.

| Failure | Action |
| --- | --- |
| `media.tool_unavailable` | Check FFmpeg/FFprobe 5.1+ run as the workload user, from the same maintained build |
| `staging.capacity` | Check doctor staging details; reduce concurrency or provide more temporary space |
| `source.stream_unavailable` | The input cannot be streamed as requested; use `--input-mode=auto` to stage automatically, or `--input-mode=stage` |
| `tams.preflight_failed`, `tams.storage_unavailable` | Check endpoint, auth, API version, lifetimes and selected backend before retrying |
| `flow.plan_failed` | Inspect the JSON Pointer; correct source, metadata or assigned Profile rather than forcing a match |
| `verification.stranded`, `object.stranded`, `object.indeterminate`, `flow.indeterminate` | Keep exact UUIDs and inspect the store whenever `action_required` is true; do not retry blindly |

Preserve the [event stream](events.md), exit code, exact version/image digest
and redacted doctor report for investigation. EOF without `run.finished` is
incomplete. Do not attach credentials, private media, signed URLs or unreviewed
customer paths to issues.
