# Compatibility and versioning

TAMSin follows semantic versioning. Within `1.x`, these automation interfaces
remain compatible:

- command and flag names, configuration keys and `TAMSIN_*` environment names;
- exit codes `0` through `8` and stable structured failure codes;
- the `tamsin.ingest.events` protocol major, ordering and terminal rules;
- published JSON Schema identifiers;
- named ingest profiles at profile major `@1`;
- deterministic identity inputs; and
- documented mutation, verification, rollback and redaction boundaries.

Compatible releases may add optional JSON fields, event types, configuration
keys or profiles. Consumers must ignore unknown event types and fields within a
supported event-protocol major. A change that would make an existing consumer
interpret the same value differently requires a new protocol, profile or
product major.

Human receipts, help text, diagnostic prose, progress frequency and exact
timings may improve without a major release. They are not parser interfaces.

There is no public Go library API. The executable, its documented wire formats
and the schemas in [`contracts/tamsin`](../../contracts/tamsin) are the product
surface.

## Updating from rc.3

The 1.0.0 release candidates are not covered by the final 1.x compatibility
promise. Changes after rc.3 remove the separate result journal, the public Go
event-consumer package, terminal redraw progress, and the local OAuth callback
listener. Redirect the NDJSON event stream for durable results; use plain
progress and obtain OAuth authorisation codes through your identity provider.
Configuration is YAML only, with unknown keys and extra documents rejected.
Dry-run and verification flags require explicit mode values.
The five treatments, FFmpeg pass-through, event protocol and exit codes remain.

## TAMS versions

TAMSin targets TAMS 8.2 and has a tested TAMS 8.1 compatibility floor. Each
release runs focused live TAMOSS tests against the revisions recorded in
[`tams-v8.1.json`](../../contracts/tams-v8.1.json) and
[`tams-v8.2.json`](../../contracts/tams-v8.2.json). TAMS 8.0 and different major
versions fail preflight before mutation.

Before updating production, read the changelog, pin the binary or image digest,
run `tamsin doctor --format json --online`, and test one exact dry run and one
real ingest in the target environment.
