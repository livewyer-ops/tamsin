# 0006: Derive generated Flow identity from content and resolved treatment

- Status: accepted
- Date: 2026-08-09

## Context

TAMSin uses deterministic identifiers so repeating an ingest can discover and
verify the Segments already present instead of uploading them again. The first
recipe included the resolved input URI in every generated Flow identifier. That
made a temporary access mechanism part of durable media identity: refreshing a
presigned HTTP URL changed its query values and created a new Flow graph for the
same bytes. It also put raw credential-bearing locator material on the input to
the UUID derivation.

Removing only known signing parameters is not defensible. Providers use
different parameter names, ordinary URLs may carry credentials in userinfo, and
new signing schemes would silently reintroduce churn. Location is useful
provenance, but it does not say whether two representations are different.

Content alone is also insufficient. FFmpeg can use a filename hint when probing
raw or otherwise ambiguous media. Identical bytes interpreted as audio and
video are different technical graphs and must not claim one Flow identifier.

## Decision

A generated root Flow UUID is derived from three versioned inputs:

1. the SHA-256 of the fully staged input;
2. the resolved media treatment (semantic profile and version, segmentation,
   output container, FFmpeg arguments, essence-storage arrangement, timeline
   placement, and a project-controlled renderer epoch); and
3. the normalised technical interpretation produced by probing and Flow
   construction, including stream mapping, offsets, duration, container, codec,
   and essence parameters.

Identifiers, display labels, descriptions, and provenance tags are removed
from the normalised technical interpretation. Thus a filename affects identity
only when it changes the media interpretation, not because its spelling differs.
Components are byte-length framed before UUIDv5 hashing, and the recipe and
root/child domains are explicitly versioned. This prevents delimiter ambiguity
and lets a future semantic change use a new domain rather than silently changing
the meaning of the current one.

The renderer epoch changes only when TAMSin deliberately changes output
semantics. The exact FFmpeg version and build fingerprint remain provenance,
but security patches, distribution rebuilds, and host-specific configuration
strings do not rotate Flow identity. This lets a heterogeneous worker fleet
resume one logical treatment while retaining enough evidence to diagnose a
byte-level difference. A renderer change that can intentionally alter output
must bump either the semantic profile version or this epoch.

Every generated child Flow derives from the root Flow UUID, its relationship,
and its collection position. `--flow-id` therefore anchors the entire graph;
`--source-id` continues to anchor the root Source. Source UUIDs remain derived
from staged content, and Object UUIDs remain derived from Flow, Segment bytes,
and timerange. Their existing encoding is deliberately unchanged so this
Flow-only migration does not rotate Sources or Objects beneath an explicit
Flow.

Locators are provenance only. TAMSin persists their scheme, authority, and path,
removing userinfo, the complete query, and the fragment. Standard input is
recorded as `stdin:`. The value of `--stdin-name` remains only a staging/probe
hint even though explicitly supplying that CLI flag also selects stdin when no
other effective input selector exists; `source.stdin_name` in configuration
remains hint-only. The canonical locators seen for a Flow are accumulated in a sorted
`_tamsin_sources` tag. The generated description is locator-neutral and the
generated label uses a short content-digest prefix, so worker scheduling cannot
choose durable display metadata. Operators can provide an editorial label or
description through `--flow-metadata`.

Workers in one Pipeline serialise on the derived root UUID. Two inputs that
converge on one graph therefore become one ingest and one resume rather than two
concurrent allocations of the same deterministic Objects.

## Consequences

Refreshed signatures, different manifests, mount points, S3 keys, and equivalent
stdin delivery converge when staged bytes, treatment, and technical
interpretation agree. Different content, treatment, or interpretation produces
a different generated graph. This intentionally coalesces byte-identical media
found at unrelated locators; all canonical locators remain visible as
provenance.

Generated Flow UUIDs change once when this recipe is adopted. The project has
not published a release, and the changelog already treats pre-release generated
identity as unstable. Existing Source UUIDs and Object UUIDs are not changed.

Canonical HTTP, file, and S3 paths are persisted. A workflow must not place a
credential in a path component; generic URL parsing cannot distinguish a secret
path from an object key. S3 ETags and version IDs protect a resumed transfer but
are not editorial identity and are not recorded as a retrievable historical
locator. The content digest remains the durable assertion about what was read.

The in-process lock does not coordinate separate TAMSin processes. That race
already existed when two processes ingested one URI and now also applies to
identical content reached through different locators. Allocation-conflict
reconciliation or an external ingest lease is a separate distributed
coordination concern.

Local files are hashed in place rather than copied into private staging. TAMSin
rechecks a local input after media processing and before the first TAMS write,
which rejects a persistent same-size rewrite, but that check cannot eliminate a
change-and-revert race or a rewrite between the final check and upload. Callers
must keep local inputs immutable for the run; a private snapshot is the complete
future fix.

## Sources

- [TAMS 8.1 Flow core schema](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/api/schemas/flow-core.json)
- [TAMS AppNote 0003: tag names](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0003-tag-names.md)
- [RFC 9562: UUIDs](https://www.rfc-editor.org/rfc/rfc9562.html)
