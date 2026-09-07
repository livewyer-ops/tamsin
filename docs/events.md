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
records still precede mutations and Object events. `input.finished.sha256` is
omitted because the whole source was not hashed. Object digests remain present,
and streamed source bytes do not increment `run.finished.bytes_staged`. Input
mode and automatic fallback are reported in logs; protocol version stays `2.1`.

Progress snapshots are cumulative and may be coalesced. Do not display a
percentage until `totals_final` is true. Terminal Object, Flow, input and run
events are not progress and are never intentionally dropped.

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
| `flow.planned.kind`, `flow.result.kind` | `essence`, `collection`, `muxed` |
| `progress.snapshot.phase` | `store`, `verify` |
| `diagnostic.severity` | `debug`, `info`, `warning`, `error` |
| `object.result.disposition` | `planned`, `uploaded`, `registration_indeterminate`, `registered`, `rejected`, `ingested`, `resumed`, `retracted`, `stranded`, `unattempted` |
| `object.result.verification_status` | `verified`, `not_requested`, `not_reached`, `failed` |
| `object.result.verification_method` | `none`, `storage`, `readback` |
| `flow.result.disposition` | `planned`, `unchanged`, `written`, `indeterminate`, `unattempted` |
| `input.finished.status` | `planned`, `ingested`, `resumed`, `failed` |
| `input.finished.verification` | `verified`, `not_requested`, `not_reached`, `failed_retracted`, `failed_stranded` |
| `run.cancellation_requested.reason` | `signal`, `parent`, `deadline`, `output_closed`, `internal` |
| `run.finished.outcome` | `succeeded`, `failed`, `partial`, `interrupted` |

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
| `media.failed` | Media analysis or transformation did not complete successfully. |
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

| Code | Meaning |
| ---: | --- |
| `0` | Every input succeeded, resumed, or completed its dry run |
| `1` | Internal or output failure |
| `2` | Invalid arguments, configuration, environment variable or value |
| `3` | Authentication failed or credentials are unsafe for the transport |
| `4` | A batch completed with at least one failed input |
| `5` | Input discovery or reading failed |
| `6` | `doctor` only: FFprobe or FFmpeg is missing, fails its version check or is unsupported |
| `7` | TAMS preflight, mutation, transfer or verification failed |
| `8` | The run was interrupted or its parent context ended |
| `130` | A second SIGINT or SIGTERM forced exit before cleanup finished |

Ingest reports media failures as `4` or `7`. For `--format json`, a complete
run repeats the code in `run.finished.payload.exit_code`; input and diagnostic
events carry the failure codes above.

The process code classifies the run; inspect terminal events for the affected
input, Flow and Object UUIDs. In particular, exit `4` can include both
successful and failed inputs. EOF without `run.finished` is incomplete and may
not have a trustworthy exit record.

SIGINT and SIGTERM request graceful cancellation. TAMSin stops scheduling new
work, completes one terminal event for each declared input where possible, and
then exits `8`. A second signal exits `130` at once; it and a hard kill can
prevent terminal output and cleanup.
