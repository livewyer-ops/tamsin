# Ingest output protocol and durable journal

`tamsin ingest` has two stdout presentations:

- `--format human` (the default) writes a permanent receipt for an operator;
- `--format json` writes the versioned `tamsin.ingest.events` NDJSON process
  protocol as the ingest runs.

The human receipt is designed to read, not parse. In JSON mode every stdout
line is one complete JSON object and is flushed promptly. There is no leading
array, enclosing batch document, ANSI control sequence, carriage return, or
human footer.

This streaming contract applies only to `ingest`. Finite commands such as
`api`, `doctor`, `profiles`, and `config show --effective` continue to write one
command-specific JSON document when `--format json` is selected. In
particular, the [doctor report](doctor.md) has its own schema and is not an
ingest event stream.

## Human receipts and live progress

In human mode, transient progress uses stderr and the permanent receipt uses
stdout. The two are coordinated so the live region is closed before the
receipt begins.

The permanent human output groups all Flows for an input under one outcome.
An empty root Multi-Flow is labelled `collection`; it is not shown as
`root=true` with zero Objects. Full Flow UUIDs and every action-required Object
UUID remain copyable. The
concise view prints input checksum, resolved profile, verification state, and
ordinary counts once rather than repeating them on every row.

`--verbose` adds the sanitised full locator, Source IDs, mutation dispositions,
FFmpeg/toolchain provenance, logical staged/uploaded/verified byte counters,
and non-zero retry/recovery details. Clean per-Object terminal records remain
on the event stream or in the journal rather than being retained merely for a
larger receipt; action-required Object records are always retained and shown.
`--quiet` suppresses only clean successful or dry-run receipts and live
progress. It never hides a warning, failure, action-required identifier, or
non-zero exit status.

The terminal wording distinguishes `NOT VERIFIED`, `UNVERIFIED OBJECTS
RETRACTED`, `FLOW STATE INDETERMINATE`, `OBJECT STATE INDETERMINATE`, and
`OBJECTS STRANDED: ACTION REQUIRED`. Colour can reinforce those headings, but
words and exit codes carry all meaning. `--color auto`
honours `NO_COLOR` and `TERM=dumb`; `always` and `never` are explicit. Durable
results never use horizontal tabs or require cursor control.

Live progress owns one small terminal region, never an alternate screen. It
shows an indeterminate analysing/staging phase before exact totals exist, then
separate cumulative storage and verification rows for active work. Completed
rows are folded into one store/verify summary, so redraw cost follows configured
concurrency rather than the history of a large batch. It follows terminal resize;
at narrower widths it removes rate and decorative detail before semantic
counters, and falls back to a short phase line below 50 columns. Rates and ETA
are derived display values, not protocol fields. Warnings and errors suspend
and redraw the live region through the same serialiser. The region is cleared
and closed before the permanent stdout receipt, so neither diagnostics nor a
receipt heading can collide with progress.

Plain and non-terminal progress is deliberately sparse: it writes the initial
analysis state, sealed phase totals, completion, and at most one heartbeat per
active phase every 30 seconds. It does not append one line per Object.

## Event envelope

Every record has the same envelope:

```json
{
  "protocol": "tamsin.ingest.events",
  "protocol_version": "2.1",
  "type": "progress.snapshot",
  "seq": 17,
  "run_id": "0a853551-fb19-40d6-8f17-15dd3562e6d4",
  "emitted_at": "2026-08-09T08:35:48.123Z",
  "elapsed_ms": 1842,
  "scope": {"input_index": 0},
  "payload": {
    "revision": 4,
    "phase": "store",
    "totals_final": true,
    "completed_objects": 7,
    "total_objects": 13,
    "completed_bytes": 806912,
    "total_bytes": 1514152,
    "elapsed_ms": 1842
  }
}
```

The encoder writes that object on one physical line. It is expanded above only
for readability.

| Field | Meaning |
| --- | --- |
| `protocol` | Always `tamsin.ingest.events`. Do not confuse the stream with a finite-command result or the durable journal. |
| `protocol_version` | Wire-protocol major and minor version. Version `2.1` adds optional TAMS Flow Profile identity and is independent of media-profile policy versions. |
| `type` | Stable event name. Consumers branch on this value, not on a display message. |
| `seq` | Globally contiguous sequence, starting at zero, in actual emission order. It is authoritative when timestamps tie or clocks differ. |
| `run_id` | UUID shared by the whole stream, support diagnostics, and the optional journal. Together, `run_id` and `seq` identify one event. |
| `emitted_at` | Informational UTC timestamp. |
| `elapsed_ms` | Monotonic elapsed time from the event publisher's start. |
| `scope` | Optional stable input, Flow, and Object correlation. `input_index` is zero-based. |
| `payload` | Event-specific structured data. |

The first event is always `hello` at sequence zero. It advertises tool/build
provenance, terminal-result and profile-policy versions, capabilities, and the
maximum encoded event size. A parent should inspect it before depending on an
optional capability. Sequence numbers are assigned after progress coalescing,
so a deliberately omitted intermediate snapshot never creates a sequence gap.
The profile-policy version describes the built-in catalogue, not the selected
profile's independent semantic version.

In `run.started`, `dry_run_mode` is `off`, `fast`, or `exact`, and
`verification_mode` is `auto`, `readback`, or `none`. These resolved values are
strings rather than booleans so a wrapper can display the actual resource and
integrity policy without inferring it from later events.

## Event catalogue

| Event | Meaning |
| --- | --- |
| `hello` | Identifies the producer and protocol capabilities. It is sequence zero. |
| `run.started` | Gives safely resolved operation settings such as profile, verification, dry-run, and concurrency when known. |
| `input.declared` | Declares one resolved input and its stable `input_index`. Locators are sanitised. |
| `manifest.finished` | Seals the complete input count. Input selection remains atomic. |
| `input.started` | Marks dispatch of one input. |
| `flow.planned` | Declares a Flow, Source, role/kind, `root`/`parent_flow_id` hierarchy, and safe media description as soon as the graph is known. It deliberately has no exact Object totals. |
| `progress.snapshot` | Gives cumulative, phase-specific Object and logical-byte progress. |
| `retry.scheduled` | Gives a sanitised operation class, next and maximum attempts, delay, and closed status/error class. |
| `diagnostic` | Gives a stable code, severity, bounded safe message and hint, and `action_required`. |
| `run.cancellation_requested` | Records graceful cancellation and its closed reason. |
| `object.result` | Gives one terminal Object record as soon as its committed batch is durable, scoped to its input and Flow. Disposition and verification are separate. |
| `flow.result` | Closes one Flow with its mutation disposition and compact Object counters, scoped to its input. |
| `input.finished` | Gives the input outcome, verification state, root Flow, checksum, profile/toolchain provenance, counts, and a structured safe failure when applicable. |
| `run.finished` | Gives the final outcome, counts, logical byte metrics, retries, recovery totals, and intended process exit code. |

Concurrent inputs may interleave. Correlate with `scope.input_index`; do not
assume all records for one input are adjacent. The pair `(run_id, input_index)`
identifies one input. Object and Flow terminal records precede their owning
`input.finished`. On any graceful success, partial failure, or interruption,
every declared input receives exactly one `input.finished`, including work
cancelled before dispatch.

If input resolution fails before a profile can be resolved, synthetic terminal
input records use `unresolved@0`. That value is failure provenance, not a
selectable profile. Once resolution succeeds, terminal records carry the named
profile version or `custom@1`.

`run.finished` is the last graceful record and nothing follows it. Its outcome
is `succeeded`, `partial`, `failed`, or `interrupted`, and its `exit_code` must
match the process status. EOF without `run.finished` means the state is
abnormal or indeterminate; use the process status and, if configured, the
durable journal. Never infer success merely from cleanly decoded earlier
lines.

## Progress is cumulative and phase-specific

`progress.snapshot` values are cumulative, not deltas. A consumer may replace
an older snapshot for the same input and phase with a newer one. Only progress
snapshots may be throttled, coalesced, or skipped when a consumer is slow;
lifecycle, diagnostic, and terminal records are not disposable.

`revision` is one strictly increasing counter for an input, shared by its
`store` and `verify` snapshots. It is not a per-phase counter and has no meaning
across different inputs. Use it to reject stale snapshots after coalescing;
use the envelope `seq` to order the complete interleaved run.

`totals_final: false` means discovery or planning may still increase the
denominator. Show completed work if useful, but do not derive a percentage or
ETA. Once `totals_final` becomes true, the Object and byte totals do not
change.

Storage and verification are separate phases. Thirteen Media Objects may
therefore produce `13/13` stored and `13/13` verified snapshots; they are never
presented as 26 Objects. `completed_bytes` counts unique accepted payload bytes,
not attempted HTTP or network-interface bytes, and does not increase again for
retries.

## Terminal result semantics

The terminal events retain the same distinctions in the human receipt and
durable journal. A UI should preserve them rather than flattening every
non-success into `failed`.

Each Flow result has a mutation `disposition`:

When assigned, `tams_flow_profile_id` is the immutable TAMS 8.2 Flow Profile
UUID on both `flow.planned` and the matching terminal Flow result. Consumers
should retain it with the Flow ID: changing the assignment changes generated
identity, and an existing deterministic Flow is never silently repointed.

| Disposition | Meaning |
| --- | --- |
| `planned` | The final Flow was validated, but mutation was intentionally skipped, as in dry-run. |
| `unchanged` | TAMSin confirmed that the existing Flow required no PUT. |
| `written` | The Flow PUT returned successfully. |
| `indeterminate` | A Flow PUT was attempted but its commit state cannot be proved. Operator inspection is required before a blind retry. |
| `unattempted` | A preceding required Flow write failed, so this write was not attempted. |

`input.finished.status` describes ingest as `planned`, `ingested`, `resumed`,
or `failed`. Verification is separate:

| Verification | Meaning |
| --- | --- |
| `verified` | Every retained Object has matching SHA-256 evidence from storage or a matching readback. |
| `not_requested` | Verification was disabled. |
| `not_reached` | Verification was requested, but dry-run or an earlier failure prevented successful verification of the complete input. |
| `failed_retracted` | Verification failed or could not complete, and every affected registered Segment was withdrawn. |
| `failed_stranded` | At least one affected Segment could not be withdrawn and needs operator action. |

Each `object.result` separates what happened to registration from how integrity
was established. `disposition` is one of `planned`, `uploaded`,
`registration_indeterminate`, `registered`, `rejected`, `ingested`, `resumed`,
`retracted`, `stranded`, or `unattempted`. `verification_status` is `verified`,
`not_requested`, `not_reached`, or `failed`; `verification_method` is `storage`,
`readback`, or `none`. A verified Object always names `storage` or `readback`.
Keep exact Flow and Object UUIDs for indeterminate or stranded dispositions:
they are the operator's recovery handles.

`flow.result.object_summary` counts total Objects and bytes, terminal
dispositions, verified Objects, and the split between storage and readback
evidence. These counters must match all preceding `object.result` records for
that Flow. Intermediate uploaded/registered states and dry-run planned Objects
count under `unattempted` because they did not reach a retained terminal
disposition.

A failed batch result and failed durable-journal result contain a required,
closed failure object:

```json
{
  "failure": {
    "code": "tams.request_failed",
    "message": "A TAMS operation did not complete successfully.",
    "action_required": false
  }
}
```

`code` is the stable machine branch, `message` is a bounded safe display
fallback, and `action_required` tells an operator-facing wrapper whether the
state needs intervention rather than an ordinary retry. A non-failed result
never carries `failure`. Raw Go errors, HTTP response bodies, provider text,
and the former free-form `error` field are not part of either serialised
contract.

The event-stream projection places the safe code and message on the failed
`input.finished`; its immediately preceding `diagnostic` carries the same code,
message, and `action_required`. Consumers should branch on codes and terminal
state, not compare display messages.

The strict
[`batch-result-v2.json`](../../contracts/tamsin/batch-result-v2.json) is the
compact whole-batch terminal shape. Flow entries contain Object summaries, not
per-Object arrays, and carry an explicit `kind` (`essence`, `collection`, or
`muxed`) so consumers never infer ownership from role or counts. It is not the ingest stdout wire format; the live Object
records and optional journal carry recovery detail without growing one final
document. The v1 schema remains published for previously captured results.

## Forking TAMSin safely

A UI or service wrapper should start TAMSin with `ingest --format json`, create
separate pipes for stdout and stderr, and drain both concurrently. stdout is
the only machine state channel. stderr is an opaque, sanitised support and
crash stream: display or retain it for operators, but never require it to
determine the ingest outcome.

Use the public `github.com/livewyer-ops/tamsin/ingestevent` decoder and reducer
from the same TAMSin module version as the executable. They enforce the
advertised record bound, protocol major, global sequence, lifecycle ordering,
terminal counts, and incomplete-stream semantics. A default `bufio.Scanner`
rejects tokens over 64 KiB and is not suitable. The essential parent-process
shape is:

`NewReducer` retains input/Flow state and Object summaries, so its memory does
not grow with Segment count. A caller that needs every Object can use
`NewReducerWithOptions`/`ReduceWithOptions` with `RetainObjectResults`, or set
`ObjectObserver` to stream records directly into its own durable store. Opting
into retention deliberately makes memory proportional to Object count. The
observer runs in event order after the Object is committed to reducer state, so
it may call `Snapshot`; an observer error stops reduction but does not make that
sequence replayable. `Snapshot` clones all retained input and Flow state. A live
UI should therefore call it at a bounded display refresh cadence, not once per
Object or protocol event; the example below takes only the final snapshot.

```go
cmd := exec.Command("tamsin", "ingest", "--profile", "essence-segments", "--format", "json", "-i", input)
stdout, err := cmd.StdoutPipe()
if err != nil {
    return err
}
stderr, err := cmd.StderrPipe()
if err != nil {
    return err
}
if err := cmd.Start(); err != nil {
    return err
}

stderrDone := make(chan error, 1)
go func() { _, err := io.Copy(operatorLog, stderr); stderrDone <- err }()

decoder := ingestevent.NewDecoder(stdout)
reducer := ingestevent.NewReducer()
var decodeErr error
for {
    event, err := decoder.Decode()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        decodeErr = err
        _ = cmd.Process.Signal(os.Interrupt)
        go io.Copy(io.Discard, stdout) // keep the child unblocked during cleanup
        break
    }
    if err := reducer.Apply(event); err != nil {
        decodeErr = err
        _ = cmd.Process.Signal(os.Interrupt)
        go io.Copy(io.Discard, stdout)
        break
    }
}
waitErr := cmd.Wait()
stderrErr := <-stderrDone
if decodeErr != nil {
    return decodeErr
}
if err := reducer.Finalize(); err != nil {
    return err // includes ingestevent.ErrIncompleteStream
}
state := reducer.Snapshot()
actualExit := 0
if waitErr != nil {
    var exitErr *exec.ExitError
    if !errors.As(waitErr, &exitErr) {
        return waitErr
    }
    actualExit = exitErr.ExitCode()
}
if state.Finished.ExitCode != actualExit {
    return errors.New("tamsin event exit code disagrees with process status")
}
_ = stderrErr
```

Do not use stdin as a control pipe: it may be the media input. Send SIGINT or
SIGTERM to request graceful cancellation. The first signal produces
`run.cancellation_requested`, reconciles in-scope work, emits terminal input
records and `run.finished` with outcome `interrupted`, and exits 8 when output
remains available. A second signal may force termination without a terminal
event. Keep draining both pipes during cleanup and allow the advertised TAMS
registration/retraction deadlines before escalating.

The producer has no unbounded output queue. Progress is sampled by input and
phase and placed in a latest-value mailbox with at most one pending `store` and
one pending `verify` snapshot per active input. A slow but draining consumer
therefore skips stale intermediate progress without blocking transfer workers.
Lifecycle, diagnostic, and terminal records remain lossless and synchronously
acknowledged, so they apply backpressure. A parent which stops reading can
therefore still stop the ingest: no implementation can both discard no terminal
truth and write indefinitely to an unread pipe. If stdout closes or returns
EPIPE, TAMSin cancels and reconciles work, then exits nonzero; it cannot promise
a terminal event over the broken channel.

## Versioning and security

Protocol versions use `major.minor`. Reject an unsupported major. Within a
supported major, ignore unknown fields and non-terminal event types, while
continuing to validate sequence and terminal ordering. A minor version may add
either; a change to required fields, meaning, ordering, or terminal guarantees
requires a new major version. Terminal-result and profile-policy versions in
`hello` evolve independently. A UI may display an unknown event's bounded safe
fallback message, when present, but must not make a state decision from it.

Every v2 envelope conforms to
[`ingest-events-v2.json`](../../contracts/tamsin/ingest-events-v2.json). The v1
schema remains available only for previously captured streams; v2 consumers do
not accept v1 records as a compatible minor.

Structured output uses the same safe-locator policy as logs and journals:
userinfo, query values, and fragments are omitted. It never includes headers,
tokens, OAuth codes, signed storage URLs, response bodies, raw FFmpeg commands,
or arbitrary provider errors. Diagnostics have typed codes and bounded,
explicitly truncated messages.

That boundary is not anonymity. Local paths, S3 keys, UUIDs, checksums, safe
URL paths, and media metadata remain operationally sensitive. Protect captured
event streams and journals accordingly.

## Durable JSONL journal

The optional journal is not a copy of stdout and is not a replacement process
protocol. It is an append-only, filesystem-synced recovery record for a
supervisor which must retain terminal work across a crash or forced kill. It
intentionally does not contain transient progress, retry, or diagnostic events.

Use `--journal PATH` (or `ingest.journal`):

```sh
tamsin ingest --profile essence-segments --journal /var/lib/tamsin/run-results.jsonl \
  -i /incoming -o https://tams.example.com
```

The path must not already exist. TAMSin creates one mode-`0600` file with
exclusive creation and refuses existing files and symlinks; one journal belongs
to exactly one invocation.

Before scheduling any input, TAMSin writes and syncs a `record_type: "start"`
record containing `run_id`, the total count, and an indexed manifest of every
resolved input. Authenticated HTTP userinfo and query values are redacted.
After each committed Object batch, TAMSin appends one `record_type: "object"`
line per Object and performs one filesystem sync for that complete batch. This
makes an already registered prefix recoverable without multiplying sync calls
by Segment count. When an input becomes terminal it writes a compact
`record_type: "input"` containing Flow summaries. `index` is the input's
zero-based position; input records are in completion order because concurrent
inputs need not finish in input order. Finally it writes one
`record_type: "summary"` with counts and one of these outcomes:

- `completed`: batch execution finished, including a completed batch with
  individual failed inputs;
- `interrupted`: cancellation or a deadline ended the run cleanly;
- `failed`: a run-wide error ended the run.

The start, each Object batch, each input, and the summary are synced before the
corresponding unit is considered journalled; the new directory entry is also
synced before scheduling. TAMSin stops scheduling if a write or sync fails and
exits with the general-error code. It does not write a summary unless every
manifest index has a durable input record.

On graceful interruption, undispatched inputs receive terminal failed records
and the summary says `interrupted`. A hard kill cannot run cleanup or write a
summary; the synced start manifest still identifies the intended input set.
Missing input indexes and the absence of a summary mark the journal incomplete,
while every earlier Object batch or input record whose sync returned remains
recoverable.

Journal records conform to
[`ingest-journal-v2.json`](../../contracts/tamsin/ingest-journal-v2.json). The
v1 schema remains available for previously captured journals.
