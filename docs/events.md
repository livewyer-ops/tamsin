# Ingest event protocol

`tamsin ingest --format json` writes newline-delimited JSON to stdout while the
run is in progress, including progress and diagnostic events. Supporting logs
use stderr. Redirect stdout to retain the event stream:

```sh
tamsin ingest --format json --profile preserve --input programme.ts \
  > run.events.jsonl
```

## Envelope

Every line is one JSON object with these fields:

| Field | Meaning |
| --- | --- |
| `protocol` | `tamsin.ingest.events` |
| `protocol_version` | `2.1`; consumers should accept compatible `2.x` versions |
| `type` | Event type |
| `seq` | Zero-based, contiguous sequence number |
| `run_id` | UUID shared by the complete invocation |
| `emitted_at` | UTC timestamp |
| `elapsed_ms` | Milliseconds since the run began |
| `scope` | Optional input, Flow and Object identity |
| `payload` | Event-specific object |

The first record is always `hello` with sequence zero. A complete stream ends
with exactly one `run.finished`. All records belong to one `run_id`.

`hello.payload.max_event_bytes` is the upper bound for one encoded line
including its newline: `1048576` (1 MiB). `hello.payload.capabilities` lists
`graceful_cancel`, `live_object_results`, `progress`, `progress_coalescing`,
`retry_events` and `terminal_results`.

## Events

| Event | Scope | Purpose |
| --- | --- | --- |
| `hello` | run | Tool, schema and capability versions |
| `run.started` | run | Resolved run settings |
| `input.declared` | input | One atomically resolved input |
| `manifest.finished` | run | Final input count |
| `input.started` | input | Work began for an input |
| `flow.planned` | Flow | Planned graph and optional TAMS 8.2 Profile UUID |
| `progress.snapshot` | input | Cumulative store or verify progress |
| `retry.scheduled` | run | A safe operation will be retried |
| `diagnostic` | run or input | Stable code and redacted operator message |
| `object.result` | Object | Terminal Object disposition and verification |
| `flow.result` | Flow | Terminal Flow disposition and bounded Object totals |
| `input.finished` | input | Terminal input status |
| `run.cancellation_requested` | run | Graceful cancellation began |
| `run.finished` | run | Final outcome, exit code and totals |

Object detail is emitted once as `object.result`; `flow.result` carries totals
instead of repeating an unbounded Object array. Records from concurrent inputs
may interleave. Use `scope.input_index` when reconstructing a batch.

`flow.planned` records identify the Flow graph, format, container and assigned
Profile; they do not contain the complete technical metadata sent to TAMS.

For streamed inputs, initial segments are checked before `flow.planned`; these
records still precede mutations and Object events. `input.finished.payload.sha256` is
omitted because the whole source was not hashed. Object digests remain present,
and streamed source bytes do not increment `run.finished.payload.bytes_staged`. Input
mode and automatic fallback are reported in logs; protocol version stays `2.1`.

Progress snapshots are cumulative and may be coalesced. Do not display a
percentage until `totals_final` is true. Terminal Object, Flow, input and run
events are not progress and are never intentionally dropped.

## Payload fields

Names below are relative to `payload`. Fields are required unless marked
optional. Integers are non-negative JSON numbers unless stated otherwise;
consumers must preserve integer precision for byte counters and sequence
numbers. Timestamps are UTC RFC 3339 strings, UUIDs are strings, and `sha256`
is a hexadecimal string (empty when no digest was established).

`scope` is absent on run events. An input scope has a zero-based `input_index`
integer; a Flow scope adds `flow_id`; an Object scope also adds `object_id`.
These identifiers let consumers join records without parsing display text.

### Start and planning

| Event | Fields and types | Optional fields and types |
| --- | --- | --- |
| `hello` | `tool_version`, `tool_commit`, `result_schema_version`, `profile_policy_version`: string; `max_event_bytes`: integer; `capabilities`: array of strings | `tool_build_date`: string |
| `run.started` | `started_at`: timestamp | `profile`, `profile_version`, `dry_run_mode`, `verification_mode`: string; `concurrency`, `transfers`, `requested_inputs`: integer |
| `input.declared` | `input`: redacted locator string | none |
| `manifest.finished` | `total_inputs`: integer | none |
| `input.started` | `started_at`: timestamp | none |
| `flow.planned` | `flow_id`, `source_id`: UUID; `kind`: string; `root`: boolean | `role`, `format`, `container`: string; `parent_flow_id`, `tams_flow_profile_id`: UUID |

`requested_inputs` counts input arguments before expansion; `total_inputs`
counts resolved inputs. A root Flow has `root: true`; collected Flows identify
their parent and role. `format` is a format URN; `container` is a media-type
string. `tams_flow_profile_id` identifies a service-owned Flow Profile.

### Progress and diagnostics

| Event | Fields and types | Optional fields and types |
| --- | --- | --- |
| `progress.snapshot` | `revision`, `completed_objects`, `total_objects`, `completed_bytes`, `total_bytes`, `elapsed_ms`: integer; `phase`: string; `totals_final`: boolean | none |
| `retry.scheduled` | `operation`: string; `attempt`, `max_attempts`, `delay_ms`: integer | `status_class`, `error_class`: string |
| `diagnostic` | `severity`, `code`, `message`: string; `action_required`, `truncated`: boolean | `hint`: string |
| `run.cancellation_requested` | `reason`: string | none |

Progress `revision` identifies successive snapshots. Its `elapsed_ms` measures
time since the run began when the snapshot was taken; envelope `elapsed_ms`
measures time since the encoder started when the record was emitted.
Retry delay is in milliseconds. `truncated` indicates that diagnostic display
text was shortened or repaired; use the stable `code` for decisions.

### Terminal results

| Event | Fields and types | Optional fields and types |
| --- | --- | --- |
| `object.result` | `object_id`, `timerange`, `sha256`, `disposition`, `verification_status`, `verification_method`: string; `bytes`: integer | none |
| `flow.result` | `flow_id`, `source_id`: UUID; `kind`, `disposition`: string; `object_summary`: object described below | `role`: string; `tams_flow_profile_id`: UUID |
| `input.finished` | `input`, `profile`, `profile_version`, `status`, `verification`: string; `flow_count`, `object_count`: integer | `ffmpeg_version`, `media_toolchain`, `sha256`, `error_code`, `message`: string; `root_flow_id`: UUID; `bytes`: integer |
| `run.finished` | `outcome`: string; `exit_code`: integer; `total`, `succeeded`, `failed`, `elapsed_ms`, `bytes_staged`, `bytes_uploaded`, `bytes_verified`, `retries`, `objects_verified`, `objects_retracted`, `objects_stranded`: integer | none |

`object.result.payload.timerange` is a TAMS timerange string on the owning Flow.
`bytes` and `sha256` describe that Object. Input-level `bytes` and `sha256`
instead describe the local or staged source and may be absent, including for
streamed sources. `media_toolchain` is a fingerprint, not a tool path.

Every `flow.result.payload.object_summary` field is a required integer:
`total`, `bytes`, `ingested`, `resumed`, `rejected`, `retracted`, `stranded`,
`unattempted`, `verified`, `storage_verified`, `readback_verified`.
`total` and `bytes` cover that Flow's Objects; the remaining fields count
outcomes or verification methods and are not all mutually exclusive.

Run `total`, `succeeded` and `failed` count inputs. Byte counters report work
performed, so a resume can have zero `bytes_uploaded` and positive
`bytes_verified`. Run `elapsed_ms` is the terminal duration in milliseconds.
Use the [enumerations](#enumerations) and [exit codes](cli.md#exit-codes) to
interpret outcomes.

## Example records

These records illustrate a single-Object audio ingest. They are excerpts from
a stream, shown with indentation for readability; actual NDJSON uses one line
per record and includes every intervening sequence number.

```json
{
  "protocol": "tamsin.ingest.events",
  "protocol_version": "2.1",
  "type": "flow.planned",
  "seq": 5,
  "run_id": "183fb015-2bbd-4f1c-a8a7-4df061865216",
  "emitted_at": "2026-09-27T12:00:00Z",
  "elapsed_ms": 100,
  "scope": {"input_index": 0, "flow_id": "d521da0d-8b1c-5cd1-81e3-8d0f44c3c0ed"},
  "payload": {
    "flow_id": "d521da0d-8b1c-5cd1-81e3-8d0f44c3c0ed",
    "source_id": "639b43b9-072b-5671-8efe-dc6d35de9e38",
    "kind": "essence",
    "role": "audio",
    "root": true,
    "format": "urn:x-nmos:format:audio",
    "container": "audio/wav"
  }
}
```

```json
{
  "protocol": "tamsin.ingest.events",
  "protocol_version": "2.1",
  "type": "object.result",
  "seq": 8,
  "run_id": "183fb015-2bbd-4f1c-a8a7-4df061865216",
  "emitted_at": "2026-09-27T12:00:01Z",
  "elapsed_ms": 1100,
  "scope": {
    "input_index": 0,
    "flow_id": "d521da0d-8b1c-5cd1-81e3-8d0f44c3c0ed",
    "object_id": "c966a435-07a5-5e80-b806-6c471babda95"
  },
  "payload": {
    "object_id": "c966a435-07a5-5e80-b806-6c471babda95",
    "timerange": "[0:0_12:0)",
    "bytes": 1152044,
    "sha256": "998630e67a73c3992db3c5a3dcc2fb8a7919ea7522421f5a87afad24a47d0a68",
    "disposition": "ingested",
    "verification_status": "verified",
    "verification_method": "readback"
  }
}
```

```json
{
  "protocol": "tamsin.ingest.events",
  "protocol_version": "2.1",
  "type": "run.finished",
  "seq": 11,
  "run_id": "183fb015-2bbd-4f1c-a8a7-4df061865216",
  "emitted_at": "2026-09-27T12:00:01.100Z",
  "elapsed_ms": 1200,
  "payload": {
    "outcome": "succeeded",
    "exit_code": 0,
    "total": 1,
    "succeeded": 1,
    "failed": 0,
    "elapsed_ms": 1200,
    "bytes_staged": 1152044,
    "bytes_uploaded": 1152044,
    "bytes_verified": 1152044,
    "retries": 0,
    "objects_verified": 1,
    "objects_retracted": 0,
    "objects_stranded": 0
  }
}
```

## Consumer rules

- Drain stdout and stderr concurrently to avoid blocking the process.
- Decode one line at a time; do not wait for one final JSON document.
- Require contiguous `seq` values and one `run_id`.
- Ignore unknown event types within protocol major version 2.
- Treat EOF, malformed JSON, a sequence gap, or absence of `run.finished` as an
  incomplete run regardless of the last visible progress event.
- Use `run.finished.payload.exit_code` with the process exit status. Exit code `4`
  denotes a partial batch.
- Retain exact Flow and Object UUIDs when a failure reports stranded data.

Structured locators exclude URL user information, query strings and fragments.
Diagnostic messages are bounded and must not contain provider response bodies
or credentials. Treat captured event files as operational records nonetheless;
they still identify inputs and TAMS entities.

## Enumerations

| Field | Values |
| --- | --- |
| `flow.planned.payload.kind`, `flow.result.payload.kind` | `essence`, `collection`, `muxed` |
| `progress.snapshot.payload.phase` | `store`, `verify` |
| `diagnostic.payload.severity` | `error` |
| `object.result.payload.disposition` | `planned`, `registration_indeterminate`, `registered`, `rejected`, `ingested`, `resumed`, `retracted`, `stranded`, `unattempted` |
| `object.result.payload.verification_status` | `verified`, `not_requested`, `not_reached`, `failed` |
| `object.result.payload.verification_method` | `none`, `storage`, `readback` |
| `flow.result.payload.disposition` | `planned`, `unchanged`, `written`, `indeterminate`, `unattempted` |
| `input.finished.payload.status` | `planned`, `ingested`, `resumed`, `failed` |
| `input.finished.payload.verification` | `verified`, `not_requested`, `not_reached`, `failed_retracted`, `failed_stranded` |
| `run.cancellation_requested.payload.reason` | `signal`, `parent`, `deadline` |
| `run.finished.payload.outcome` | `succeeded`, `failed`, `partial`, `interrupted` |

## Failure codes

`input.finished.payload.error_code`, `diagnostic.payload.code` and human
receipts use these stable codes. Messages are fixed operator text and never
carry store-supplied content; a code with several messages emits the one that
matches the cause. `action_required` is true when the store may hold state an
operator must inspect.

| Code | Message |
| --- | --- |
| `config.invalid` | The command arguments or configuration are invalid. |
| `authentication.failed` | Authentication did not complete successfully. |
| `ingest.input_failures` | One or more inputs did not complete successfully. |
| `ingest.input_failed` | The input did not complete successfully. |
| `source.failed` | Input resolution or transfer did not complete successfully. |
| `source.transfer_failed` | The input could not be read completely. |
| `source.changed` | The input changed while it was being ingested. |
| `source.stream_unavailable` | The input cannot be streamed as requested; use --input-mode=auto or stage. |
| `staging.capacity` | The input could not reserve enough staging capacity. |
| `media.analysis_failed` | Media analysis did not complete successfully.<br>The input container could not be identified.<br>The input could not be described as a valid Flow.<br>The media interpretation identity could not be derived. |
| `media.unsupported` | The input codecs are not supported by the MPEG-TS segment policy. |
| `media.options_invalid` | The FFmpeg options conflict with the selected media treatment. |
| `media.options_ignored` | Media options cannot take effect when storing the source without segmentation. |
| `media.tool_unavailable` | The configured media toolchain is unavailable. |
| `media.prepare_failed` | Media Objects could not be prepared.<br>An elemental media stream could not be prepared. |
| `tams.failed` | The TAMS operation did not complete successfully. |
| `tams.request_failed` | A TAMS operation did not complete successfully. |
| `tams.preflight_failed` | The TAMS service preflight did not complete successfully.<br>The TAMS service is not compatible with this ingest.<br>The TAMS service did not advertise usable transfer lifetimes. |
| `tams.storage_unavailable` | No usable TAMS storage backend was selected. |
| `tams.registration_failed` | Media Object registration did not complete successfully. |
| `tams.segment_conflict` | Another Media Object occupies a Segment timerange in this Flow. |
| `flow.plan_failed` | The final Flow graph is not valid or could not be read. |
| `flow.write_failed` | The Flow graph could not be committed completely. |
| `flow.indeterminate` | A Flow update may have committed before its response was lost. |
| `object.stranded` | A registered media object could not be retracted. |
| `object.indeterminate` | A media object's registration state is indeterminate. |
| `verification.stranded` | Verification failed and registered media remains stranded. |
| `verification.retracted` | Verification failed; unverified media was retracted. |
| `verification.not_reached` | The input failed before verification completed. |
| `run.interrupted` | The ingest was interrupted while cleanup was in progress.<br>The input did not finish before the run was interrupted. |
| `run.failed` | The ingest run did not complete successfully. |
| `output.failed` | The process event stream could not be written. |

## Exit codes

The process exit codes are listed in the [CLI reference](cli.md#exit-codes).
Ingest reports media failures as `4` or `7`. For `--format json`, a complete
run repeats the code in `run.finished.payload.exit_code`; input and diagnostic
events carry the failure codes above.

The process code classifies the run; inspect terminal events for the affected
input, Flow and Object UUIDs. In particular, exit `4` can include both
successful and failed inputs.

SIGINT and SIGTERM request graceful cancellation. TAMSin stops scheduling new
work, completes one terminal event for each declared input where possible, and
then exits `8`. A second signal exits `130` at once; it and a hard kill can
prevent terminal output and cleanup.
