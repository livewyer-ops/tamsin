# 0008: Separate ingest from general TAMS control

- Status: accepted
- Date: 2026-08-12

## Context

TAMSin had accumulated a general-purpose `api` command beside its ingest
workflow. That surface mixed two products with different users, dependencies
and release pressures: a media process that probes, renders, transfers and
verifies content, and a lightweight client for inspecting and administering a
TAMS service.

Shipping both together made the ingest CLI harder to understand and required
operators who only needed API access to install the FFmpeg-based application
image. It also encouraged low-level operations to grow without a clear product
boundary.

## Decision

TAMSin remains a process-oriented ingest tool. It exposes ingest, treatment
profiles, diagnostics, configuration and shell completion, and retains only
the TAMS client operations required to plan, write, resume and verify ingest.

General TAMS discovery, inspection, mutation and extension requests move to a
separate `tamsctl` repository and executable. Its initial command grammar keeps
the established resource commands while dropping the intermediate `api` noun.
It shares no release lifecycle or media-tool dependency with TAMSin.

## Consequences

- The TAMSin command tree and compatibility inventory describe one finite
  product surface.
- `tamsctl` can develop independently towards a complete TAMS control client.
- Existing `tamsin api ...` scripts must migrate to a dedicated TAMS control
  client. The tombstone identifies `tamsctl` as the destination of the removed
  command surface, but TAMSin does not install or release it.
- A hidden non-functional tombstone rejects the old prefix with that migration
  instruction so it cannot fall through to TAMSin's implicit ingest syntax.
- Some authentication and HTTP foundations exist in both repositories until a
  stable shared package is justified by real cross-project maintenance costs.
