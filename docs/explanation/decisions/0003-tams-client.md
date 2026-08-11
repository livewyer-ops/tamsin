# 0003: Maintain a narrow typed TAMS ingest client

- Status: accepted
- Date: 2026-08-06

## Context

BBC TAMS publishes an OpenAPI 3.1 contract and examples, but no maintained Go client module. Generic OpenAPI generation was considered. The upstream document uses many external JSON schema/example references and covers broad read, metadata, webhook, and deletion surfaces beyond TAMSin's upload/ingest responsibility. Generated output would be large, would require a preprocessing/generator toolchain, and would not implement TAMSin-specific cross-origin credential rules, streaming checksum verification, ambiguous POST recovery, or presigned Object transfer.

The adjacent BBC media-timestamp implementation is Python-only. Calling Python from the Go binary or porting its complete feature surface would increase runtime and maintenance costs for the two TAMS timestamp operations TAMSin needs.

## Decision

Use a small typed Go client over `net/http` for every TAMS 8.1 operation involved in upload and ingest. Keep the operation/authentication inventory machine-readable in `contracts/tams-v8.1.json`, compile behaviour tests against each typed method, and retain `tamsin api request` for vendor extensions.

Implement only TAMS-specific timeline formatting and checked nanosecond/rational conversion locally. Continue using maintained libraries for OAuth, S3, HTTP input retries, UUIDs, CLI/configuration, and media inspection.

Presigned transfers use a separate unauthenticated transport unless the URL has the TAMS API origin, as required by the contract. Media is streamed rather than represented in generated request types. Retry policy is method-aware: safe requests and byte-identical PUTs are bounded and replayable; Segment POST is recovered by a read rather than blind replay.

## Consequences

Upstream pin changes require an explicit conformance inventory review instead of automatic regeneration. The maintained client surface remains small and readable, and generated code cannot silently change wire behaviour. If TAMS publishes a maintained Go client with OpenAPI 3.1, external-reference, cross-origin, and streaming support, this decision should be revisited.

## Sources

- [BBC TAMS](https://github.com/bbc/tams)
- [TAMS 8.1 OpenAPI contract](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/api/TimeAddressableMediaStore.yaml)
- [BBC media timestamp library](https://github.com/bbc/rd-apmm-python-lib-mediatimestamp)
