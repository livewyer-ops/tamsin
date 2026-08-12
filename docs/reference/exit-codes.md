# Exit codes

TAMSin exit codes are stable process API. In ingest JSON mode,
`run.finished.payload.exit_code` matches the actual process status whenever the
stdout channel remains usable. Earlier terminal events remain useful when the
process exits nonzero.

| Code | Name | Meaning |
| ---: | --- | --- |
| 0 | success | Every selected input completed or the requested read-only command succeeded. |
| 1 | general | Internal/output/runtime dependency failure not covered below. |
| 2 | usage | Invalid flags, arguments, configuration, metadata, or required option. |
| 3 | authentication | Authentication configuration or token acquisition failed before the TAMS operation. |
| 4 | partial | At least one batch item failed. Inspect every `input.finished` and its structured failure code. |
| 5 | source | Input resolution, manifest, directory, HTTP source, or S3 listing failed before batch execution. |
| 6 | media | FFprobe or FFmpeg is missing or fails a `doctor` runtime check. Batch media failures use code 4 with per-item structured failures. |
| 7 | remote | A TAMS preflight, ingest metadata request, or media transfer failed. |
| 8 | interrupted | Context deadline, SIGINT, or SIGTERM cancelled the operation. |

Wrappers should treat only `0` as success. Code `4` is intentionally distinct so an orchestrator can persist successful Flow IDs while routing failed items for retry.

`doctor` emits all safely independent checks before selecting one process exit
when several fail. Its precedence is usage/configuration (`2`), media tools
(`6`), authentication or credential-transport policy (`3`), remote TAMS
validation (`7`), then other local runtime failures (`1`). Cancellation uses
`8` after the report is emitted when possible, and failure to write the report
uses `1`. The [versioned doctor report](doctor.md) retains every individual
failure and skip reason regardless of that single process code.

EOF without `run.finished` is never evidence of success, even if earlier
events decoded cleanly. Treat the run as abnormal or indeterminate, combine the
process status with any durable journal, and retain exact Flow/Object IDs for
recovery. A mismatch between `run.finished.payload.exit_code` and the process
status is a protocol failure.

For long batches, `--journal` first persists the complete indexed input
manifest, then each terminal input independently from stdout. The path must be
new. A journal write or sync failure uses code `1` and stops new ingest work;
see the [ingest output protocol and durable journal](result-contract.md).
