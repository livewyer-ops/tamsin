# TAMS ingest conformance

TAMSin targets BBC TAMS 8.2 at commit [`ebb18b09cc6effe70a3464fc0282e9663d0a583f`](https://github.com/bbc/tams/commit/ebb18b09cc6effe70a3464fc0282e9663d0a583f). This is the exact submodule revision used by the pinned TAMOSS 8.2 preview at commit [`5ec9df15f661a5db230577074e2e55d10444d4ea`](https://github.com/livewyer-ops/tamoss/commit/5ec9df15f661a5db230577074e2e55d10444d4ea). TAMS 8.1 remains the compatibility floor, pinned separately rather than being silently replaced by the target.

The target inventory is [`contracts/tams-v8.2.json`](../../contracts/tams-v8.2.json). It explicitly extends the complete [`TAMS 8.1 compatibility inventory`](../../contracts/tams-v8.1.json). Together they are the source of truth for both TAMS and TAMOSS revisions and for the live matrix. Tests fail if pins diverge, inherited review documents disappear, an authentication mechanism lacks an implementation mapping, or an ingest operation is removed from coverage.

## Authentication coverage

- HTTP Basic
- HTTP Bearer/JWT
- `access_token` URL query authentication
- OAuth2 client credentials acquisition for bearer tokens
- OAuth2 authorization-code acquisition for bearer tokens, using a pre-obtained code or validated localhost callback

## Upload and ingest operation coverage

| Operation | High-level use | Low-level command |
| --- | --- | --- |
| `GET_service` | Ingest and `doctor --online` compatibility/lifetime preflight | `api service` |
| `GET_storage-backends` | Ingest and `doctor --online` backend selection | `api storage-backends` |
| `GET_profiles` | Flow Profile discovery and filtered paging | `api flow-profile list` |
| `GET_profiles-profileId` | Profile-backed ingest and inspection | `api flow-profile get` |
| `POST_profiles-profileId` | Operator-managed immutable Profile creation | `api flow-profile create` |
| `PUT_flows-flowId` | Deterministic Flow creation | `api flow put` |
| `GET_flows-flowId` | Created Flow verification | `api flow get` |
| `POST_flows-flowId-storage` | Media Object allocation | `api storage allocate` |
| `POST_flows-flowId-segments` | Segment registration | `api segment register` |
| `GET_flows-flowId-segments` | Resume and checksum verification | `api segment list` |
| `DELETE_flows-flowId-segments` | Retraction of a Segment that failed verification | `api segment delete` |
| `GET_flow-delete-requests-request-id` | Monitor asynchronous Segment retraction | `api segment delete` |
| `GET_objects` | Registered Object verification | `api object get` |
| `POST_objects-instances` | Controlled/external instance registration | `api object instance register` |
| `DELETE_objects-instances` | Instance removal | `api object instance delete` |

`api request` remains available for vendor extensions without weakening typed behaviour for the pinned operations.

Segment listing implements the paging and `get_urls` controls on the pinned
operation rather than treating its first response as complete. Each `rel=next`
target is resolved against the exact URL of the page that supplied it, so
absolute, path-relative, and query-only references follow standard URL
semantics. All Link field-values are parsed as RFC 8288 lists, including commas
inside URI-references or quoted parameters and case-insensitive relation token
lists; malformed or multiple `rel=next` links fail rather than truncating the
result. Before another authenticated request is made, the resolved URL must
remain on the configured origin and beneath the configured API base path.
Encoded slash/backslash and recursively encoded dot-segment ambiguity is
rejected before credentials are attached; cycles are rejected. Collection is
atomic and bounded to 1,000 pages, 100,000 Segments, and 64 MiB of successful
JSON page bodies, so an endlessly novel or oversized cursor stream cannot
return a plausible partial result or grow memory without a fixed limit.
Listings are lean by default through an empty `accept_get_urls`;
`api segment list --include-download-urls` is the explicit presigned and
verbose-storage opt-in. Without that opt-in TAMSin removes any unexpected
`get_urls` from every response page, including pages whose cursor replaced the
original query. Ingest callers request fresh download URLs only at the points
that immediately verify media. Presigned response headers are canonicalized
before use; invalid or case-duplicate names are rejected, and the newer
`headers` object takes precedence over the legacy `content-type` member.

## Verification layers

- Unit/contract tests exercise every authentication transport, typed TAMS client operation, timeline conversion, Flow mapping, source resolver, retry rule, upload checksum, resume transition, partial failure, configuration precedence, and CLI exit behaviour.
- The published doctor-report v1 schema is validated against real offline,
  online, and failure reports. Online doctor tests prove its startup checks use
  only `GET /service` and `GET /service/storage-backends`, share ingest's API,
  lifetime, storage-selection, authentication, and TLS rules, and never proceed
  from an invalid partial configuration.
- Every final effective Flow is validated at runtime against the matching TAMS 8.1 or 8.2 JSON Schemas vendored in [`contracts/schemas/`](../../contracts/schemas/), after generated metadata, TAMS Flow Profile expansion, operator overrides, and preserved existing enrichment have been composed but before the first PUT. An 8.2 Profile-backed PUT is separately validated in its compact `profile_id` form. Cross-Flow checks then enforce media ownership, exact collection membership, and each parent Collection Item's `container_mapping`. The same revisioned schemas back offline contract tests.
- End-to-end CLI behaviour is covered by [`testscript`](https://github.com/rogpeppe/go-internal) archives in [`cmd/tamsin/testdata/script/`](../../cmd/tamsin/testdata/script/), the harness the Go team uses for the `go` command itself. Each archive is a readable transcript asserting real arguments, exit codes, and the separation of stdout presentation/event records from diagnostic stderr, with fixtures inlined as `txtar` members. Scripts run the CLI in-process, so coverage remains attributable to the packages under test. Run `go test ./cmd/tamsin -update` to refresh golden output after an intentional change.
- The race detector covers concurrent batch execution.
- `make test` passes `-coverpkg=./internal/...` so coverage is attributed to the package a statement lives in rather than to the package whose test ran it; without it the contract and CLI suites, which drive `internal/` from outside, report nothing. Read the aggregate with `make coverage`. The per-package lines are each test binary's share of the whole internal tree and are easy to misread as a regression.
- `make e2e` provisions both pinned TAMOSS contracts and exercises the native TAMS storage allocation/upload/registration/readback path from the OCI image. Every ingested Flow is read back and checked against that service revision rather than against TAMSin's own output. The 8.2 run additionally exercises the Flow status lifecycle; 8.1 proves those additive writes do not leak across the compatibility boundary. The matrix covers whole-file ingest, an explicit `--segment-format`, both essence-storage arrangements against a genuine multiplex, and exact Segment retraction against the live service.
- The live matrix covers local file plus deterministic resume, Object-instance registration/removal, directory, text manifest, HTTP, stdin, and S3 sources plus bearer, URL-token, and OAuth client-credential authentication. Basic and authorization-code behaviour use deterministic local identity/API servers because the TAMOSS local profile does not expose those grants as unattended test principals. Unit regressions additionally bind every credential-bearing request to the configured TAMS origin and reject cross-origin HTTPS redirects before token injection.

## Specification rules

The 8.1 inventory records the compatibility rules and the 8.2 delta records every added requirement or reviewed non-applicable document. Each rule names its source and asserting test. Contract tests evaluate their union: the 8.2 target cannot conceal an omitted base rule, and a changed pin becomes a reviewable checklist diff.

`not_applicable` records documents reviewed and found not to constrain ingest, with the reason. That distinction matters: an unlisted document is unread, not cleared.

The inventory covers every ADR and application note present at the pinned
commit, excluding their index READMEs. Rules name the documents that constrain
this ingest path; every other document is recorded under `not_applicable` with
its reason. Contract tests derive the reviewed-document counts from those
unique source references and require them to match the recorded upstream
totals, so matching `read` and `total` numbers cannot conceal an unclassified
document.

## Deliberate omissions

- The typed 8.2 Segment API accepts explicit `init_object_id`, and storage allocation accepts the corresponding content type. TAMSin does not yet manufacture fragmented-MP4 initialisation Objects: that high-level packaging mode needs a complete allocation, verification, resume, and playback contract rather than merely exposing the new field.
- `_tams_segmentation_rate` is not set. It is an implementation-specific tag superseded by the core `segment_duration` Flow property, which TAMSin populates, and AppNote 0003 directs implementers to prefer core API metadata over tags.
- Container MIME types are a supported-profile mapping, not an attempt to classify every format FFmpeg can read. Extensions and the host MIME database are not trusted; an unknown direct Object is declared as `application/octet-stream` with a warning, while an empty collector still omits `container` as AppNote 0006 requires. `--flow-metadata` is the explicit override for a workflow with more specific knowledge.
- Video `vfr` is established from decoded presentation timestamps rather than FFprobe's summary-rate fields; fixed 30000/1001 quantisation and true mixed cadence have real-media regressions. Interlace metadata is emitted only for FFprobe's unambiguous evidence. MXF `progressive` is omitted because FFmpeg collapses both full-frame and segmented-frame descriptors to that value; `tb`/`bt` and PsF are also omitted because FFprobe cannot distinguish them reliably. A workflow with production metadata may override the complete `essence_parameters` object. The exact field matrix is in [Media metadata support](../reference/media-metadata.md).
- Codec names follow a finite supported profile. An unknown FFprobe codec is omitted and warned about rather than converted into a fabricated `video/x-*`, `audio/x-*`, or `application/x-*` value. Because the pinned elemental Flow schemas require `codec`, a final Flow without a valid explicit override fails schema preflight before mutation.
- `generation` is operator-owned source-lineage metadata. TAMSin preserves an existing or explicitly supplied value but does not invent `0` from stream copy: local treatment cannot establish how many generations occurred before the input reached this process.
- Named ingest profiles are TAMSin product policy rather than TAMS conformance
  requirements. The resolved profile is recorded separately from core Flow
  fields; implementation-specific profile and media-toolchain provenance stays
  under the AppNote 0003 `_tamsin_` tag prefix.
- TAMS Flow Profiles are distinct immutable 8.2 technical contracts. Assignment
  is explicit, participates in generated identity, requires an exact technical
  match except for measured average bit rate, and is never inferred from the
  local treatment profile or silently changed on an existing Flow.
- Generated Flow identity contains staged content, resolved media treatment,
  and the normalised technical graph, never an input locator, display label, or
  credential. Child Flows derive from the root. Canonical credential-free
  locators are accumulated as `_tamsin_sources` provenance, while Source and
  Object identifier recipes remain stable.
- Every Flow in the graph is read before any is written, and TAMSin only replaces what it generates. Technical metadata — codec, container, essence parameters, bit rates — is refreshed, because it has to stay true of the media. `label` and `description` are written when the Flow is created and then left alone, since a person may have improved them. TAMSin uses a locator-neutral description and a short content-derived label; use `--flow-metadata` for an editorial name. Tags whose names begin with `_tamsin_` are TAMSin's and are rewritten, except that canonical `_tamsin_sources` provenance is accumulated; every other tag is somebody else's and is kept. When nothing TAMSin owns has changed, the Flow is not written at all. `--flow-metadata` can override descriptive and technical fields, but not `/id`, `/source_id`, `/format`, or collection ownership paths; those are rejected as usage errors with the JSON path and a specific alternative.

Changing either TAMS or TAMOSS pin requires updating the appropriate base or delta inventory, reviewing upstream OpenAPI, ADR, and Application Note changes, and extending both contract and live-matrix tests before the pin is accepted.
