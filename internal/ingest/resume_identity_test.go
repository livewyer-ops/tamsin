package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func ingestFixture(t *testing.T) source.Item {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media-object"), 0o600); err != nil {
		t.Fatal(err)
	}
	return localSource(filename)
}

func runOnce(t *testing.T, client *fakeClient, item source.Item) Result {
	t.Helper()
	pipeline, err := New(Config{VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Results) != 1 {
		t.Fatalf("unexpected batch: %#v", batch)
	}
	return batch.Results[0]
}

// TestRetryAfterLostRegistrationUsesFreshObjectIDs covers the retry that
// used to be impossible: an upload landed, its registration never did, and
// the service still holds the deterministic identifier until it collects the
// orphan. The batch continues under service-assigned identifiers instead.
func TestRetryAfterLostRegistrationUsesFreshObjectIDs(t *testing.T) {
	t.Parallel()
	item := ingestFixture(t)
	client := newFakeClient()
	client.rejectOccupiedIDs = true
	first := runOnce(t, client, item)
	if first.Status != ResultStatusIngested {
		t.Fatalf("unexpected first result: %#v", first)
	}
	flow := first.rootFlow()
	original := flow.Objects[0].ObjectID
	client.lock.Lock()
	delete(client.segments[flow.FlowID], original)
	client.lock.Unlock()

	second := runOnce(t, client, item)
	if second.Status != ResultStatusIngested {
		t.Fatalf("retry did not ingest: %#v", second)
	}
	replaced := second.rootFlow().Objects[0]
	if replaced.ObjectID == original || replaced.Disposition != ObjectDispositionIngested ||
		replaced.Verification != ObjectVerificationVerified {
		t.Fatalf("retry did not continue under a fresh identifier: %#v", replaced)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.freshAllocations != 1 || client.uploads != 2 {
		t.Fatalf("fresh allocations = %d, uploads = %d; want 1 and 2", client.freshAllocations, client.uploads)
	}
	if _, registered := client.segments[flow.FlowID][replaced.ObjectID]; !registered {
		t.Fatalf("the fresh Object %s was not registered", replaced.ObjectID)
	}
	client.lock.Unlock()

	// A third run computes the deterministic identifier again and must adopt
	// the Segment registered under the fresh one rather than register over it.
	third := runOnce(t, client, item)
	client.lock.Lock()
	if third.Status != ResultStatusResumed || third.rootFlow().Objects[0].ObjectID != replaced.ObjectID ||
		third.rootFlow().Objects[0].Verification != ObjectVerificationVerified {
		t.Fatalf("the fresh Segment was not adopted on the next run: %#v", third.rootFlow().Objects)
	}
	if client.uploads != 2 || client.freshAllocations != 1 || len(client.segments[flow.FlowID]) != 1 {
		t.Fatalf("adoption allocated or uploaded again: uploads=%d fresh=%d segments=%d",
			client.uploads, client.freshAllocations, len(client.segments[flow.FlowID]))
	}
}

// TestOccupiedTimerangeIsAConflictWithoutVerification pins the one case where
// adoption is refused for the right reason: without a readback there is no way
// to know the other Object holds the same bytes.
func TestOccupiedTimerangeIsAConflictWithoutVerification(t *testing.T) {
	t.Parallel()
	item := ingestFixture(t)
	client := newFakeClient()
	first := runOnce(t, client, item)
	flow := first.rootFlow()
	original := flow.Objects[0].ObjectID
	// Re-register the same bytes under another identifier at the same timerange.
	client.lock.Lock()
	segment := client.segments[flow.FlowID][original]
	delete(client.segments[flow.FlowID], original)
	segment.ObjectID = "other-producer"
	segment.GetURLs = []tams.PresignedURL{{URL: "mem://other-producer"}}
	client.segments[flow.FlowID][segment.ObjectID] = segment
	client.objects["other-producer"] = client.objects[original]
	client.lock.Unlock()

	pipeline, err := New(Config{VerificationMode: VerificationNone}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || batch.Results[0].Failure == nil || batch.Results[0].Failure.Code != FailureCodeSegmentConflict ||
		!strings.Contains(batch.Results[0].Error, "--verify readback") {
		t.Fatalf("unverified adoption was not refused as a conflict: %#v / %s", batch.Results[0].Failure, batch.Results[0].Error)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.uploads != 1 {
		t.Fatalf("a conflict must not upload; uploads=%d", client.uploads)
	}

	// With readback the same Segment is adopted.
	client.lock.Unlock()
	adopted := runOnce(t, client, item)
	client.lock.Lock()
	if adopted.Status != ResultStatusResumed || adopted.rootFlow().Objects[0].ObjectID != "other-producer" {
		t.Fatalf("matching bytes were not adopted under readback: %#v", adopted.rootFlow().Objects)
	}
}

// TestAllocationRejectionsOtherThanOccupiedIDsAreReported keeps the fallback
// narrow: only a 400, the status TAMS uses for an identifier already in use,
// leads to a fresh allocation.
func TestAllocationRejectionsOtherThanOccupiedIDsAreReported(t *testing.T) {
	t.Parallel()
	pipeline, err := New(Config{}, newFakeClient(), fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := &tams.HTTPError{Method: "POST", URL: "flows/flow/storage", StatusCode: 403, Status: "403 Forbidden"}
	_, err = pipeline.allocateFreshObjects(context.Background(), "flow", []preparedObject{{id: "object"}}, nil, "storage", forbidden)
	if !errors.Is(err, forbidden) {
		t.Fatalf("a 403 was not reported as it was: %v", err)
	}
}

// TestUploadsCarryTheFlowContainerWhenUninstructed pins the type sent for an
// Object when the store returns no Content-Type instruction: the Flow's
// container, which a registered Object must match, rather than a generic
// octet-stream the store would then record.
func TestUploadsCarryTheFlowContainerWhenUninstructed(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	result := runOnce(t, client, ingestFixture(t))
	flow := result.rootFlow()
	client.lock.Lock()
	defer client.lock.Unlock()
	container, _ := client.flows[flow.FlowID]["container"].(string)
	if container == "" {
		t.Fatalf("the written Flow declares no container: %#v", client.flows[flow.FlowID])
	}
	if got := client.uploadContentTypes[flow.Objects[0].ObjectID]; got != container {
		t.Fatalf("upload Content-Type = %q, want the Flow container %q", got, container)
	}
}

func TestIdentifiersAreCanonicalizedBeforeUse(t *testing.T) {
	t.Parallel()
	pipeline, err := New(Config{
		FlowID:    "6BA7B810-9DAD-11D1-80B4-00C04FD430C8",
		SourceID:  "{6ba7b811-9dad-11d1-80b4-00c04fd430c8}",
		StorageID: "urn:uuid:6BA7B812-9DAD-11D1-80B4-00C04FD430C8",
	}, newFakeClient(), fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline.config.FlowID != "6ba7b810-9dad-11d1-80b4-00c04fd430c8" ||
		pipeline.config.SourceID != "6ba7b811-9dad-11d1-80b4-00c04fd430c8" ||
		pipeline.config.StorageID != "6ba7b812-9dad-11d1-80b4-00c04fd430c8" {
		t.Fatalf("identifiers were not canonicalised: %q %q %q",
			pipeline.config.FlowID, pipeline.config.SourceID, pipeline.config.StorageID)
	}
	_, err = New(Config{FlowID: "6ba7b810-9dad-71d1-80b4-00c04fd430c8"}, newFakeClient(), fakeProber{}, nil, discardLogger(), nil)
	if err == nil || !strings.Contains(err.Error(), "flow ID") {
		t.Fatalf("a UUID version outside the schema was accepted: %v", err)
	}
}

func TestMatchingSegmentAcceptsEquivalentTimeranges(t *testing.T) {
	t.Parallel()
	listed := []tams.Segment{
		{ObjectID: "instant", Timerange: "[5:0_5:0]"},
		{ObjectID: "range", Timerange: "[00:0_1:0)"},
		{ObjectID: "other", Timerange: "[0:0_1:0)"},
	}
	if matchingSegment(listed, "instant", "[5:0]") == nil {
		t.Error("the two-Timestamp spelling of an instant was not matched")
	}
	if matchingSegment(listed, "range", "[0:0_1:0)") == nil {
		t.Error("a leading zero defeated the match")
	}
	if matchingSegment(listed, "range", "[0:0_1:0]") != nil {
		t.Error("a different end inclusivity was matched")
	}
	if matchingSegment(listed, "other", "[0:0_2:0)") != nil {
		t.Error("a different end was matched")
	}
}

// TestVerificationTriesEveryAdvertisedURL covers a service that lists more
// than one route to an Object. A route that cannot be read is skipped for the
// next; only bytes that disagree end verification.
func TestVerificationTriesEveryAdvertisedURL(t *testing.T) {
	t.Parallel()
	item := ingestFixture(t)
	client := newFakeClient()
	first := runOnce(t, client, item)
	flow := first.rootFlow()
	object := flow.Objects[0]
	client.lock.Lock()
	client.downloadErrors["mem://unreachable"] = errors.New("403 Forbidden")
	client.listSegmentsOverride = []tams.Segment{{
		ObjectID: object.ObjectID, Timerange: object.Timerange,
		GetURLs: []tams.PresignedURL{
			{URL: "mem://unreachable", Presigned: true},
			{URL: "mem://" + object.ObjectID},
		},
	}}
	client.hasListingOverride = true
	client.lock.Unlock()

	second := runOnce(t, client, item)
	resumed := second.rootFlow().Objects[0]
	if second.Status != ResultStatusResumed || resumed.Verification != ObjectVerificationVerified {
		t.Fatalf("an alternative readable URL was not used: %#v", second)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.uploads != 1 {
		t.Fatalf("uploads = %d, want the original only", client.uploads)
	}

	// A readable copy with different bytes is an integrity failure, whatever
	// other routes remain.
	client.objects["corrupt"] = []byte("wrong bytes")
	client.listSegmentsOverride[0].GetURLs = []tams.PresignedURL{
		{URL: "mem://corrupt", Presigned: true},
		{URL: "mem://" + object.ObjectID},
	}
	client.lock.Unlock()
	third := runOnce(t, client, item)
	client.lock.Lock()
	if third.Status == ResultStatusResumed || third.rootFlow().Objects[0].Verification == ObjectVerificationVerified {
		t.Fatalf("a corrupt copy was tolerated: %#v", third)
	}
}
