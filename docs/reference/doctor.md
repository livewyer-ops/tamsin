# Doctor report

`tamsin doctor` is an input-free, read-only ingest readiness check. With a
profile it resolves the same configuration and treatment an ingest would use.
Without one it lists all built-in profiles and checks the union of their local
dependencies, while reporting that ingest still requires a selection. In that
case `profile.selection` is the empty string; it does not claim that a synthetic
profile was selected. It emits
every check in a stable order even when an earlier check fails or makes a later
one unsafe. It never probes an input, renders media, or mutates TAMS.

By default it checks local readiness only. Select a profile when checking a
specific planned treatment:

```sh
tamsin --format json doctor --profile essence-segments
```

The checks are, in order:

| Check | Meaning |
| --- | --- |
| `configuration` | The selected file, environment, flags, and static values resolved safely. |
| `profile` | The named profile and explicit treatment overrides form a usable media treatment, or all available profiles are listed when none was selected. |
| `staging` | The directory exists, has write permissions, accepts a temporary-file probe, and has enough currently free space for the configured budget. |
| `ffprobe` | The configured executable completes its version check. |
| `ffmpeg` | The executable completes its version check when the resolved treatment may write media. `preserve@1` skips it; `demux@1` checks conservatively because doctor has no input from which to prove separation is unnecessary. |
| `authentication` | Online only. Credential/transport policy succeeded and at least one read-only endpoint accepted the request; it is skipped as indeterminate when no response succeeds for a non-authentication reason. |
| `service` | Online only. `GET /service` succeeded. |
| `api_compatibility` | Online only. The service API meets the TAMS 8.1 compatibility floor and is compared with the 8.2 target. TAMS 8.0, missing/malformed versions, and different major versions fail before mutation; newer 8.x minors pass with their relationship reported. |
| `service_lifetimes` | Online only. Object and presigned-URL lifetime guarantees have valid TAMS timestamp syntax, ordering, and minimums. |
| `storage_backends` | Online only. `GET /service/storage-backends` succeeded. |
| `storage_selection` | Online only. The requested backend exists, or exactly one usable default backend can be selected. |

If configuration cannot be parsed or validated, all dependent checks are
reported as `skipped` and no subprocess or network request is started. A failed
check similarly skips only checks that cannot safely proceed; independent
checks continue so one report captures as much actionable state as possible.

## Online preflight

Add `--online` to perform the same read-only service compatibility, lifetime,
and storage selection preflight used before ingest mutations:

```sh
tamsin --format json doctor --online
```

This uses the normal authentication, HTTPS, certificate, timeout, and retry
policy. The only TAMS operations are `GET /service` and
`GET /service/storage-backends`. No input or Flow/Object identifier is needed.
Doctor never starts an interactive OAuth flow; authorization-code mode requires
a pre-obtained `--oauth-code`. Acquiring that code may be done separately, and
exchanging it can contact the configured OAuth token endpoint before the two
read-only TAMS requests.
The report contains the resolved authentication mode and a redacted endpoint;
credentials, URL query values, and peer response bodies are never copied into
the report or stderr. Storage backend identifiers are likewise omitted because
they are peer-controlled values; the report gives only the backend count and
whether selection was requested or used the default.

Configured media executables are also treated as untrusted report inputs. Their
path, stdout banner, and stderr are not reproduced; a successful check reports
only `available: true`, and a failure gives a fixed remediation message. Each
version check has a five-second execution deadline; checks remain sequential so
doctor consumes at most one local media-process slot at a time.

## Versioned JSON

With `--format json`, stdout is one document conforming to the published
[`doctor-report-v1.json`](../../contracts/tamsin/doctor-report-v1.json) schema.
Its `schema_version` is `1.0`; this finite-command contract is separate from the
`tamsin.ingest.events` NDJSON protocol. Top-level, profile, and check fields are
closed to unknown properties. Each check's `detail` object is intentionally
extensible so new non-secret diagnostics do not require a schema revision.

Every check has `name` and `status` (`pass`, `fail`, or `skipped`). Failed
checks also have `error`; skipped checks explain why in `detail.reason`. The
top-level `status` is `fail` when any check fails. Provenance includes the full
TAMSin version string, version/source commit/build date, Go version, operating
system, architecture, and resolved profile.

The default human output carries the same checks for operators; automation
should select JSON and validate `schema_version`. Do not apply ingest-stream
rules such as waiting for `run.finished` to a doctor report.

When more than one check fails, the process exit category is deterministic:
usage/configuration (2), media dependency (6), authentication/policy (3),
remote TAMS validation (7), then general local runtime failure (1). A cancelled
run exits 8 after emitting the report when output remains available; failure to
write the report itself exits 1. See [Exit codes](exit-codes.md).
