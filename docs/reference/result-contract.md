# Ingest event protocol

`tamsin ingest --format json` writes newline-delimited JSON to stdout while the
run is in progress, including progress and diagnostic events. Supporting logs
use stderr. A durable event record is therefore ordinary shell redirection:

```sh
tamsin ingest --format json --profile preserve --input programme.ts \
  > run.events.jsonl
```

The current schema is
[`ingest-events-v2.json`](../../contracts/tamsin/ingest-events-v2.json).

## Envelope

Every line is one JSON object with these fields:

| Field | Meaning |
| --- | --- |
| `protocol` | `tamsin.ingest.events` |
| `protocol_version` | Current `2.x` event version |
| `type` | Event type |
| `seq` | Zero-based, contiguous sequence number |
| `run_id` | UUID shared by the complete invocation |
| `emitted_at` | UTC timestamp |
| `elapsed_ms` | Milliseconds since the run began |
| `scope` | Optional input, Flow and Object identity |
| `payload` | Event-specific object |

The first record is always `hello` with sequence zero. A complete stream ends
with exactly one `run.finished`. All records belong to one `run_id`.

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
| `retry.scheduled` | run or input | A safe operation will be retried |
| `diagnostic` | run or input | Stable code and redacted operator message |
| `object.result` | Object | Terminal Object disposition and verification |
| `flow.result` | Flow | Terminal Flow disposition and bounded Object totals |
| `input.finished` | input | Terminal input status |
| `run.cancellation_requested` | run | Graceful cancellation began |
| `run.finished` | run | Final outcome, exit code and totals |

Object detail is emitted once as `object.result`; `flow.result` carries totals
instead of repeating an unbounded Object array. Array order is event order,
not input-completion order. Use `scope.input_index` when reconstructing a
batch.

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
- Use `run.finished.exit_code` with the process exit status. Exit code `4`
  denotes a partial batch.
- Retain exact Flow and Object UUIDs when a failure reports stranded data.

Structured locators exclude URL user information, query strings and fragments.
Diagnostic messages are bounded and must not contain provider response bodies
or credentials. Treat captured event files as operational records nonetheless;
they still identify inputs and TAMS entities.
