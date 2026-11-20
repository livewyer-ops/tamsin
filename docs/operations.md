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
timestamp scan before upload, even under `preserve`. In that scan each video
decoder uses up to four threads, capped by Go's `GOMAXPROCS` setting. Streamed
inputs are validated segment by segment; this reduces startup I/O but adds
per-segment probe work. It does not eliminate B-frame decoding or guarantee
lower total CPU use. Overlapping FFmpeg and validation can also increase peak
RAM. Choose `--input-mode=stage` when startup latency and temporary disk use
matter less than process overlap. See [profiles](profiles.md) for processing
and Object-count costs.

`--dry-run=exact` renders and validates using the selected input mode, without
TAMS mutation. `--dry-run=fast` skips rendering and cannot report exact Objects.
For streaming it performs only the initial probe and warns that cadence and
output metadata remain unverified; insufficient metadata still fails planning.

`--concurrency` bounds inputs, `--transfers` bounds uploads and verification
across the run, and `--probe-concurrency` bounds queued measurements; active
media processes remain capped at two in total. Sources are populated side by
side under a separate limit of the same size as `--transfers`. Segmentation
uses a rolling disk spool, not an in-memory copy of the input. Streamed
independent essences share one FFmpeg input reader. Whole-essence
demultiplexing can need the staged source and complete output essences at once.

`--transfers` follows `--concurrency` unless it is set, and `--concurrency`
follows the CPU count. A container limited to one or two CPUs therefore uploads
and verifies one or two Objects at a time, although that work waits on the
network and not on a CPU. Set `--transfers` there, for example `--transfers 8`.

`--staging-byte-budget` coordinates one process, not an entire filesystem.
Use a writable `--temp-dir` with sufficient headroom and a volume quota when
several processes share it.

The default `auto` budget uses 80% of free space in `--temp-dir` at startup.
A configured budget is capped by available space. Streaming reserves output
space only, with a target of at most 512 MiB per active input, reduced for
smaller budgets. Closed segments are removed after registration and verification.
Large active segments can exceed the target, as can the newest closed Segment
of each output, which waits on disk until the next one is listed or FFmpeg
exits. The global budget remains the limit. Staging needs the whole remote
source plus its output allowance.

Updates to one root Flow are serialised within a process only. Schedule one
writer per explicit or deterministic root Flow across processes, or provide
external coordination. SIGINT/SIGTERM stop new work and allow bounded cleanup.
Set the workload's termination grace period above `--deletion-timeout`
(default five minutes), with headroom for shutdown; cleanup has a minimum
allowance of 30 seconds. See [recovery](#recovery). A second signal, crash or
hard kill can leave `tamsin-input-*` or `tamsin-segments-*` directories.
Remove them only after confirming no process
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

### Resume an interrupted ingest

1. Wait for the original process to exit, including its cleanup. Keep its
   receipt or event stream and confirm no other worker is writing the same Flow.
2. Read the cause and hint of any result with `action_required: true`. The
   flag alone does not establish that the store was written to, and a
   transient cause can clear without a configuration change. Where
   a code or an Object's disposition is stranded or indeterminate, inspect the
   store and retain the exact Flow, Object and timerange identifiers. Resolve
   that state before retrying; do not delete a whole Flow merely to clear one
   failed Segment.
3. Restore the same input revision, binary, profile, start time, input mode,
   metadata and explicit IDs, if any. Check readiness and rerun the original
   command. For a local file previously ingested with `essence-segments`:

   ```sh
   tamsin doctor --online --profile essence-segments
   tamsin ingest --profile essence-segments --verbose --input ./programme.ts
   ```

4. Check the final receipt and exit status. A partial ingest can reuse verified
   Objects and upload the missing ones; a fully stored input reports
   `RESUMED AND VERIFIED`. A `tams.segment_conflict` needs investigation of the
   existing Segment's bytes and timing. Changing the Flow ID would create a
   separate ingest, not repair that conflict.

For a remote input, a renewed signed URL or changed validator can change its
generated identity. Check [identity rules](concepts.md#generated-identity-and-resume)
before treating it as the same retry. After an upgrade, check the
[changelog](../CHANGELOG.md) for deliberate identity renewals.

### Registration and cleanup behaviour

Streaming writes the complete Flow graph only after initial segments establish
its metadata and assigned Profiles match. It then commits the first valid
batch immediately. Every later segment is checked before storage allocation,
including cadence across segment boundaries. A later contradiction stops new
uploads and discards queued output. Already committed valid segments remain;
the Flow is not marked `closed_complete`. Use `--input-mode=stage` when the whole
input must pass media preflight before the first upload. Flow status is
written only to a store at TAMS 8.2 or later within the supported major: on
8.1 no Flow is marked, whatever the outcome.

A Segment that already occupies an Object's exact timerange under another
identifier (an earlier run that fell back to a service-assigned identifier,
or another producer) is adopted only when its bytes and timing match, which
needs readback; otherwise, and for any partial overlap, the input stops with
`tams.segment_conflict` and nothing is uploaded. TAMSin never registers an
overlapping Segment. A candidate that cannot be read back is not a conflict:
the input fails with the read error and a retry decides.

Failed verification of a new registration retracts only the exact
Object/timerange Segment. A download whose presigned URL expires during a long
readback is listed once more for a fresh URL before verification gives up.
During resume, a timeout, cancellation, unavailable download or partial HTTP
response leaves existing Segments in place and reports verification as
`not_reached`. Retry when the store can serve the complete Object.
A confirmed byte-length or checksum mismatch still retracts
the corrupt Segment and reports `failed_retracted`, even when other Segments
were left in place. Every resumed Segment must match the expected media timing,
even with verification disabled; a timing conflict leaves it unchanged.

On a store that takes Flow status, a failed ingest returns to
`awaiting_content` only the Flows it created or
moved to `ingesting`, the Flow that owns the media first. A retry that found
everything in place and then failed leaves `closed_complete` Flows, and a
status another client set, as it found them. The metadata write at the end of
a segmented ingest reads each Flow again first, so tags, labels and
descriptions another system added while it ran are kept.

`--timeout` bounds each metadata request attempt, so a slow first response
does not use up the time its retry needs. An OAuth token that expires during
cleanup after an interrupt is refreshed as usual. A reader of the event stream
or the log that exits first, such as a pager, makes the next write fail; the
run then cancels and cleans up as for any output failure rather than being
killed by the closed pipe.
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

A partial registration response must identify each failed Segment uniquely
within the batch. TAMSin retracts the registered complement. A missing or
ambiguous failure list requires the same fresh readback as a lost response.
TAMSin verifies visible Segments and attempts targeted retraction for unresolved ones.
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

The table is keyed by failure code. A human receipt prints the code with
`--verbose`; `--format json` always carries it.

| Failure | Action |
| --- | --- |
| `media.tool_unavailable` | Check FFmpeg/FFprobe 5.1+ run as the workload user, from the same maintained build |
| `staging.capacity` | Check doctor staging details; reduce concurrency or provide more temporary space |
| `source.stream_unavailable` | The input cannot be streamed as requested; use `--input-mode=auto` to stage automatically, or `--input-mode=stage` |
| `tams.preflight_failed`, `tams.storage_unavailable` | Check endpoint, auth, API version, lifetimes and selected backend before retrying |
| `flow.plan_failed` | Inspect the JSON Pointer; correct source, metadata or assigned Profile rather than forcing a match |
| `verification.stranded`, `object.stranded`, `object.indeterminate`, `flow.indeterminate` | Keep exact UUIDs and inspect the store; do not retry blindly |

Preserve the [event stream](events.md), exit code, exact version/image digest
and redacted doctor report for investigation. EOF without `run.finished` is
incomplete. Do not attach credentials, private media, signed URLs or unreviewed
customer paths to issues.
