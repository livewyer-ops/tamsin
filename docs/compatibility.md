# Compatibility

Release versions are `MAJOR.MINOR.PATCH-inN`: `MAJOR.MINOR.PATCH` is the BBC
TAMS API version the release targets and `-inN` counts TAMSin releases for
that API version, with `-rcM` marking a release candidate. Across `8.2.0-inN`
releases these interfaces remain compatible: commands, flags, configuration
and environment names; exit and structured failure codes; event-protocol
ordering and terminal rules; named profiles at `@1`; deterministic identity
inputs; and documented mutation, verification, rollback and redaction
boundaries. A release targeting a new TAMS API version may change them; the
[changelog](../CHANGELOG.md) lists every break.

Compatible releases may add optional JSON fields, event types, settings or
profiles. Consumers must ignore unknown fields and event types within a
supported protocol major. Changing the meaning of an existing value requires
a new protocol or profile major, or a release for a new TAMS API version.
Human receipts, help, diagnostic prose and progress frequency are not parser
interfaces. The executable and documented wire formats are the product
interfaces.

## Runtime

| Component | Supported |
| --- | --- |
| Go toolchain for source builds | 1.26.8, as pinned in `go.mod` |
| FFprobe and FFmpeg | 5.1 or newer, from the same maintained build |
| Release binaries | linux/amd64, linux/arm64, darwin/amd64 and darwin/arm64, without CGO |
| Container image | linux/amd64 and linux/arm64 on `tamsin-ffmpeg-runtime:5.1.9-bookworm-r2`, a Debian bookworm base with FFmpeg 5.1.9 |

`go install` is not a supported install path: release tags carry no `v`
prefix, so Go tooling cannot resolve them.

## TAMS support

TAMSin targets [BBC TAMS 8.2](https://github.com/bbc/tams/tree/34fb31b80cb8afb3194f28c8b787301379caacf8) and retains
a tested [TAMS 8.1](https://github.com/bbc/tams/tree/98d307b09b5ebf79278aa7d3aad53295154e2c17) compatibility floor.
TAMS 8.0, missing or malformed versions, and different major versions fail
preflight. Newer 8.x minors pass, with their relationship to the target
reported by `doctor`.

The [embedded BBC schemas](../internal/tamsschema/schemas) validate generated
Flows. The [live test script](../scripts/e2e-kind.sh) pins both BBC TAMS and
TAMOSS revisions and provisions each through TAMOSS `local-kind`. Both versions
must pass before release. It tests local, directory, manifest, HTTP, S3 and
stdin ingest, resume, storage verification, TLS and authentication. TAMS 8.2
also exercises Profile assignment. Verification-failure retraction and 202
deletion-request monitoring are covered by HTTP-level tests, not induced
against the live store.

TAMSin uses service/storage discovery, Flow reads and writes, Segment listing,
registration and retraction, Object allocation/readback, and assigned Profile
reads. Changes to the upstream pins require reviewing the affected BBC
specification, application notes and ADRs, updating focused tests and passing
both live jobs.

## Profile matching and metadata

Before the first mutation, TAMSin checks service version and lifetimes, storage,
initial media evidence, the complete planned Flow graph, existing Flow identity
and assigned TAMS Flow Profiles. Planning failures make no partial write.
Streaming validates later media incrementally; a later failure can retain a
valid committed prefix. Staged input receives whole-input media preflight.

A TAMS 8.2 Profile must match the generated technical metadata by strict JSON
structure and exact numeric value. Integers larger than 2^53 retain precision;
strings never equal numbers, array order matters, and object key order does
not. Missing, null and empty fields remain distinct. Omitted optional fields
stay omitted: a Profile can fail if it omits a field TAMSin generates, even
when a schema default would give it the same meaning. TAMSin does not rewrite
metadata to force a match.

Profile-backed Flows are written in compact `profile_id` form. `avg_bit_rate`
is the Profile's encoding target, not measured output; `max_bit_rate` remains
Flow-specific measured metadata. Mismatch diagnostics give a bounded JSON
Pointer; values and unrecognised extension keys are redacted.

Independent essences own their Objects and are grouped by a containerless
Multi-Flow that owns none. A muxed Flow owns the Objects and maps its tracks
through ordered collection items. Local and staged input identities derive
from content, treatment and technical graph, not the locator or tool patch
version. Streamed inputs use a separate revision-based recipe: source resource,
validator and length, then treatment, start, overrides and initial stream
interpretation. S3 resource identity includes endpoint, bucket and exact key.
HTTP uses the effective URL without user information or fragment; query values
remain part of the fingerprint but are never published. Changed signed queries
can therefore change generated IDs. `--source-id` supplies the caller's resource
identity instead, while the remote revision must still be pinned.

Switching between streaming and staging changes generated IDs. Explicit Flow
reuse with a different input revision or incompatible technical metadata fails
before mutation. Input revision and checksum tags cannot be overridden.
Segment timeranges do not overlap. TAMS 8.2 initial Object identity is
supported, but TAMSin does not manufacture fragmented-MP4 initialisation media.

TAMSin does not infer editorial purpose or source lineage generation.
`--flow-metadata` supplies workflow-owned metadata but cannot change identity,
format or collection ownership. Resume preserves operator labels, descriptions
and non-`_tamsin_` tags. See [profiles](profiles.md) for media limits and
[operations](operations.md) for verification and recovery.

Before updating production, read the [changelog](../CHANGELOG.md), pin the
binary or image digest, run `tamsin doctor --format json --online`, and test an
exact dry run and a real ingest in the target environment.
