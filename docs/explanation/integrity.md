# Integrity

TAMSin requires SHA-256 integrity evidence for every Object by default. It uses
storage-side evidence when an upload exchange provides it and reads the Object
back otherwise. This page explains that choice, why readback happens after
registration, and what TAMSin does when verification fails.

## Why the API has no checksum field

You might expect TAMS to carry a checksum against each Media Object. [ADR 0016](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/adr/0016-checksums-and-filesize.md) considered exactly that and rejected it, on the grounds that duplicating metadata the storage layer already provides adds complexity for little benefit. It chose instead to rely on existing mechanisms in products like S3, and recommended that clients verify integrity themselves before registering Segments.

So the guarantee is the client's to provide. TAMSin compares either explicit
storage SHA-256 evidence or downloaded bytes with the digest calculated while
it uploaded the prepared Object.

## Why readback verification happens after registration

ADR 0016's recommendation is to verify *before* registering. Upload-side
storage checksum evidence satisfies that ordering. A download-based check
cannot, and the reason is worth stating precisely because it looks like a bug
otherwise.

The TAMS API specifies that `GET /objects/{objectId}` **must** return 404 for a Media Object that has been assigned through `/flows/{flowId}/storage` but is not yet registered against a Flow Segment. Between upload and registration, the Object is deliberately unreadable. There is no way to fetch the bytes back and check them in that window.

Readback verification therefore follows registration. That is not a choice
TAMSin makes; it is the only order the API permits for stores that do not attest
the upload itself.

## Retraction waits for a terminal result

Verifying after registration creates the obvious hazard: for a moment, the Flow references bytes that have not been checked. If the check then fails, the store is left pointing at media TAMSin knows is wrong.

So a failed verification retracts the Segment. TAMS also deletes any Media Object the removal leaves unreferenced, which withdraws both halves of the registration. TAMSin supplies both the Object ID and timerange, so cleanup cannot remove an overlapping Segment belonging to another writer. The retraction runs on a context detached from cancellation, so an interrupt landing between registering and verifying cannot strand a bad Segment.

Neither successful DELETE status is terminal on its own. A 202 points to a Flow Delete Request whose state can later become `error`; a 204 says the Segments "have been **or will be** deleted". TAMSin follows a same-service 202 `Location` through `created` and `started` to `done`, rejects mismatched or failed requests, and then handles both statuses the same way: it lists the exact Object/timerange until that tuple is absent. Only then is retraction reported as complete.

A transport failure after sending DELETE is ambiguous: the store may have committed the removal even though TAMSin did not receive its response. A retry can then legitimately return 404. TAMSin reconciles a transport/retryable failure or 404 through the same exact absence check; confirmed absence is success, while a failed confirmation reports both errors. Peer-provided asynchronous deletion details are omitted when response-body suppression is active.

If the retraction itself cannot be completed, that is reported alongside the original failure rather than replacing it, because that is the case needing manual intervention.

Bulk registration has the same hazard one transition earlier. A TAMS `200`
partial response is authoritative: it names the Segments that failed, so TAMSin
can roll back the registered complement without a racy read-after-write. A lost
HTTP response is different because the whole POST is indeterminate. TAMSin
reads the batch back on a context detached from cancellation, verifies every
visible Segment, and sends every missing or otherwise unresolved Segment for
targeted retraction. If that readback itself fails, every Object in the batch is
treated as maybe registered and retracted. Recovery has one deadline for the
whole batch and collects all outcomes, so one failure cannot abandon its
siblings or multiply a timeout by the number of Segments.

Ordinary verification cleanup follows the same resource bound. Its detached
30-second deadline starts with the first failed verification and is shared by
every retraction in that verification batch. TAMSin still attempts each exact
Object/timerange deletion after the deadline; those attempts fail immediately
and are reported as stranded rather than extending shutdown by another 30
seconds per Object.

## Verification modes and cost

The default `auto` mode avoids a second transfer when the successful upload
provides unambiguous SHA-256 evidence. TAMSin recognises SHA-256 values in
`X-Amz-Checksum-Sha256`, `Content-Digest`, or `Digest` in the provider response.
A checksum header supplied only in the signed allocation request is not
evidence: a successful PUT does not prove storage understood or validated it,
so TAMSin performs readback instead. ETags are never treated as content hashes. Malformed evidence fails the
upload, and a digest mismatch fails before Segment registration.

Many presigned URLs do not include checksum support. In that case `auto`
registers the Object and reads it back, because TAMS deliberately makes an
unregistered Object unreadable. Resumed Objects also use readback: their prior
upload response is not available to the new process. Thus network cost ranges
from upload bytes only when every new Object is storage-attested to roughly
twice the Object bytes when all Objects require readback.

`--verify=readback` forces the latter behaviour for policies that require an
independent round trip. `--verify=none` skips integrity verification explicitly;
receipts and events report that choice instead of implying success.

Readback verification draws on the same transfer budget as uploads. It can
overlap work from other inputs, but it remains real network and storage I/O and
should be budgeted as such.

The transfer slot is taken before TAMSin asks for a presigned URL. Storage is
allocated only for the uploads that can start, and a verification worker asks
for one Object's GET URL only after it is ready to read it. This ordering is
intentional: TAMS guarantees a URL for only `min_presigned_url_timeout` (as
little as thirty seconds), so generating every URL for a long Flow before a
bounded worker queue would spend later URLs' entire lifetime waiting. The same
small upload batches are registered before another batch is allocated, keeping
the Object-registration lifetime bounded as well. If an attempt fails, the
client retries only while the same URL can still start a request; it does not
turn that start window into an absolute deadline that cuts off a healthy media
body already in flight.

## What the checksum does and does not prove

The SHA-256 recorded in `_tamsin_sha256` is the digest of the **input**, not of any stored Object. When segmentation or demultiplexing is in play, no stored Object hashes to it, because the stored Objects are cut and re-framed versions of the input.

What the per-Object verification proves is narrower and more useful: the bytes that came back are the bytes that went out. The input digest is provenance — it records which source produced this Flow — rather than a checksum you can validate the store against.

Where you need the stored bytes to equal the source bytes, `-d 0` with muxed essence storage is the only combination that produces it.

See [TAMS conformance](conformance.md) for the complete verification boundary.
