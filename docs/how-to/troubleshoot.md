# Troubleshoot an ingest

Start with a redacted readiness report using the same profile and endpoint as
the failing job:

```sh
tamsin --format json --endpoint https://tams.example.com \
  doctor --online --profile essence-segments >doctor.json
```

Keep stdout and stderr separate. The JSON report and ingest event stream carry
stable check and failure codes; stderr is supporting operator detail. Do not
attach credentials, private media, path-sensitive customer data, or unreviewed
signed URLs to an issue.

## Common failures

`media.tool_unavailable` means FFprobe or FFmpeg could not run or is older than
5.1. Confirm both configured executables come from the same maintained build and
run `ffprobe -version` and `ffmpeg -version` as the workload user. The published
OCI image already contains the supported tools.

`staging.capacity` means the process could not reserve its conservative local
working-space bound. Check the `staging` doctor detail, reduce `--concurrency`,
or provision a larger `--temp-dir` and explicit `--staging-byte-budget`. The
budget coordinates one TAMSin process; a filesystem quota remains the hard
boundary.

`tams.preflight_failed` or `tams.storage_unavailable` should be investigated
before retrying. Confirm the endpoint, authentication mode, TAMS `api_version`,
advertised Object/URL lifetimes, and requested storage ID in the doctor report.

`flow.plan_failed` can indicate that supplied metadata, a TAMS Flow Profile, or
an existing explicit Flow does not describe the generated media. The mismatch
path is a JSON Pointer. Correct the contract or source; TAMSin does not coerce
metadata or overwrite a Flow whose `source_id` identifies different material.

`verification.retracted`, `verification.stranded`, `object.stranded`,
`object.indeterminate`, and `flow.indeterminate` are recovery states, not blind
retry instructions. Preserve the event stream and exact UUIDs. Inspect the store
before retrying whenever `action_required` is true.

## Temporary directories and concurrent writers

TAMSin removes per-run `tamsin-input-*` and `tamsin-segments-*` directories on
normal completion and bounded cancellation. A hard kill, host crash, or second
signal can leave one behind. TAMSin does not automatically delete an old
directory because age alone cannot prove that another process is not using it.
After confirming that no TAMSin process references the directory, an operator
may remove it using the surrounding workload or volume lifecycle.

Workers inside one process serialise updates that resolve to the same root Flow.
Separate TAMSin processes do not share that lock. Schedule only one writer for
a given explicit or deterministic root Flow at a time, or provide equivalent
external coordination. The staging budget is likewise per process, so several
processes sharing one volume need a volume-level quota and enough aggregate
headroom.

## Escalating a report

Include the exact TAMSin version, immutable image digest where applicable,
platform, profile, redacted doctor report, process exit code, relevant stable
failure codes, and the terminal part of the event stream. EOF without
`run.finished` is an incomplete run, even when earlier events decoded cleanly.
