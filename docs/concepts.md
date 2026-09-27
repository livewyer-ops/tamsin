# Media, identity and verification

## Stored media

TAMS separates the media's identity, description and bytes:

| Entity | Purpose |
| --- | --- |
| Source | Identifies media content that may have multiple representations |
| Flow | Describes one representation and its timeline, or a collection of related Flows |
| Object | Holds stored media bytes, such as a WAV file or an MPEG-TS segment |
| Segment | Associates an Object with a timerange on a Flow |

A muxed Flow owns Objects containing several tracks and maps those tracks to
collected essence Flows. With independent essence storage, each essence Flow
owns its Objects; a containerless Multi-Flow groups them. A treatment such as
`preserve` or `essence-segments` selects this packaging. Service-owned TAMS
Flow Profiles instead constrain the technical metadata a Flow may declare.
See the [profile catalogue](profiles.md#named-profiles).

The Segment's `timerange` locates media on the Flow timeline.
`object_timerange` describes the media available inside the Object, and
`ts_offset` relates the Object's timestamps to the Flow. Those ranges can
differ when audio leads or trails a video cut. The
[placement rules](profiles.md#supported-media-and-packaging) preserve the
source clock, including gaps, without registering overlapping Segments.

## Generated identity and resume

For local and staged inputs, the generated root Source ID follows the input's
content. Generated Flow IDs also include the treatment, timeline position and
technical graph. Moving an unchanged file does not change its generated IDs.
A streamed remote input uses a separate recipe
based on its resource, pinned revision and initial media interpretation.
Switching between streaming and staging therefore changes generated IDs.
The [compatibility reference](compatibility.md#profile-matching-and-metadata)
describes those inputs and the effect of signed URLs.

Metadata overrides cannot directly set identity fields. Technical overrides
participate in the streamed Flow identity recipe, so changing them can create
a new Flow. Local and staged inputs derive their graph identity before merging
those overrides; compatibility checks still prevent reusing a Flow to describe
different media. Compare the Flow IDs in exact dry runs before changing an
existing workflow's metadata, treatment or input mode.

A retry can reuse an existing Segment when its timerange, bytes and timing
match. TAMSin verifies the stored bytes before reporting a resume. It refuses
a conflicting or partially overlapping Segment instead of overwriting it.
Explicit `--flow-id` and `--source-id` select identities; they do not bypass
these checks. Follow the [recovery procedure](operations.md#resume-an-interrupted-ingest)
after an interrupted run.

Generated identities normally remain stable within the supported release
line. A deliberate renderer-epoch renewal is an exception: the
[changelog](../CHANGELOG.md) records the affected upgrade, which creates new
Flows without rewriting old ones. Routine FFmpeg package updates do not
rotate IDs. See [compatibility](compatibility.md) before upgrading workers.

## Verification and partial results

An upload completing does not by itself prove that storage holds the expected
bytes. With the default `--verify=auto`, TAMSin checks SHA-256 evidence from
storage or downloads the registered Object for readback. Resumes use readback.
`--verify=none` explicitly skips that assurance and does not report verification.
See [verification and network cost](operations.md#verification-and-network-cost)
for the accepted evidence and transfer overhead.

Ingest is incremental. A failed or cancelled run can leave a verified prefix
of the input in the store. Recovery retracts failed registrations when it can
confirm their absence; uncertain cleanup is reported for operator attention.
Human receipts summarise the input, while the [event protocol](events.md)
records each Object's outcome. A terminal `run.finished` event and the process
exit status establish whether the whole invocation completed.
