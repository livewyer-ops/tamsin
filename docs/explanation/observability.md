# Run and retry observability

An ingest has one UUID before TAMSin resolves its first source. The same
`run_id` appears on every `tamsin.ingest.events` record, correlated support
diagnostics, human receipt, and optional journal. Source expansion remains
atomic: if resolution fails, TAMSin does not create a journal or begin the
pipeline, but JSON mode still emits a structured diagnostic and terminal
`run.finished` when its stdout channel remains available.

The semantic event stream is the machine state contract. `--log-format` and
`--log-level` control a separate sanitised support stream on stderr; a wrapper
may retain or display it, but never needs to parse it to decide what happened.

## Retry records

In ingest JSON mode, one `retry.scheduled` event records each additional
attempt TAMSin actually permits. Human mode includes non-zero retry totals in
its permanent summary and expands their safe provenance with `--verbose`.
Debug support logs may also describe retries, but are not the process
contract. Each semantic retry event contains only:

- a fixed operation name;
- the one-based next attempt and maximum total attempts;
- a coarse HTTP status class or error class; and
- the backoff duration.

The initial attempt is not a retry. An attempt refused because its context was
cancelled or its presigned URL cannot remain valid through the backoff is not
reported as scheduled.

The logging API cannot accept a URL, userinfo, query value, header, response
reason phrase, response body, provider message, or raw error string as an
attribute. It reduces ordinary errors and numeric HTTP statuses to a closed set
of classes. This matters most for presigned object URLs and S3 errors, where a
generic request dump can itself be a usable credential.

The events cover HTTP source requests, resumptions of interrupted source
bodies, AWS SDK S3 retries, replay-safe TAMS metadata requests, Media Object
uploads, and verification downloads. Observation does not replace retry
policy. In particular, the S3 wrapper delegates to the exact retryer the AWS SDK
selected after reading its environment and shared configuration, preserving
standard/adaptive/custom policy, attempt limits, backoff, and quota state.

## Terminal metrics

After progress rendering closes, human mode prints non-zero terminal metrics
in its permanent receipt. JSON mode carries the full structured snapshot in
the final `run.finished` payload. Consequently, a machine consumer does not
depend on an info-level log record which an operator can suppress.

The counters describe logical, exactly-once lifecycle completions rather than
claiming to measure every byte placed on a network interface:

| Metric | Meaning |
| --- | --- |
| `elapsed_ms` | Wall time from the event publisher's start until terminal cleanup and result handling return. |
| `bytes_staged` | Bytes in each successfully staged resolved input, including a local file hashed in place. Partial failed attempts and a restarted/resumed prefix do not add bytes. |
| `bytes_uploaded` | Bytes in each Flow/Object payload whose upload returned success in this invocation. Internal PUT attempts and an Object found on resume do not add bytes. |
| `bytes_verified` | Bytes in each Object whose downloaded size and SHA-256 matched in this invocation. Failed download attempts do not add bytes; a resumed Object verified by the new invocation does. |
| `retries` | Additional attempts actually scheduled across the retry operations above; initial attempts are excluded. |
| `objects_verified` | Objects which reached successful byte verification in this invocation. |
| `objects_retracted` | Registered Objects whose Segment TAMSin confirmed withdrawn while resolving a verification or registration failure. |
| `objects_stranded` | Registered Objects whose required retraction could not be confirmed and need operator action. |

The metrics publisher receives only byte counts and terminal outcomes, not
source locators, Flow IDs, or Object IDs. Counters rely on the ingest lifecycle's
exactly-once completion points, so metrics memory remains constant as Segment
count grows.

Metrics use the same semantic publisher as progress and terminal outcomes, so
the human receipt, event stream, and recovery counts cannot independently
reinterpret them. They remain operational measurements, not TAMS media
metadata.

## See also

- [Ingest output protocol and durable journal](../reference/result-contract.md)
- [Integrity](integrity.md)
- [CLI reference](../reference/cli.md)
