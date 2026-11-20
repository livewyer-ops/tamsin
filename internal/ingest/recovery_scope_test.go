package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func writeFixture(t *testing.T, name, content string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return filename
}

// A retry that finds every Segment in place and then fails, for instance on
// a listing timeout, changed nothing. Its Flows were closed_complete and must
// stay so; recovery restores only Flows the run created or opened.
func TestFailedResumeLeavesCompletedFlowsAlone(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.ts", "muxed-media")
	client := newFakeClient()
	client.serviceDocument = map[string]any{"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"}
	config := Config{Concurrency: 1, VerificationMode: VerificationReadback}
	run := func() Result {
		pipeline := mustNew(t, config, client, muxedProber{}, nil, discardLogger(), nil)
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		return batch.Results[0]
	}
	first := run()
	if first.Status != ResultStatusIngested {
		t.Fatalf("setup: %#v", first.Failure)
	}
	client.lock.Lock()
	for _, flow := range first.Flows {
		if status := stringField(client.flows[flow.FlowID], "status"); status != flowStatusClosedComplete {
			t.Fatalf("setup: %s is %q", flow.Role, status)
		}
	}
	client.flowStatusWrites = nil
	client.listSegmentsErr = context.DeadlineExceeded
	client.lock.Unlock()

	if second := run(); second.Status != ResultStatusFailed {
		t.Fatalf("second run should fail on the listing: %#v", second)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.flowStatusWrites) != 0 {
		t.Fatalf("a failed resume that changed nothing rewrote status: %v", client.flowStatusWrites)
	}
	for _, flow := range first.Flows {
		if status := stringField(client.flows[flow.FlowID], "status"); status != flowStatusClosedComplete {
			t.Fatalf("%s left %q", flow.Role, status)
		}
	}
}

// Different paths can resolve to the same graph within a batch.
type failSecondListing struct{ *fakeClient }

func (c failSecondListing) ListSegments(ctx context.Context, id string, opts tams.SegmentListOptions) ([]tams.Segment, error) {
	if index, ok := inputIndexFromContext(ctx); ok && index == 1 {
		return nil, context.DeadlineExceeded
	}
	return c.fakeClient.ListSegments(ctx, id, opts)
}

func TestFailedDuplicateInputLeavesCompletedGraphAlone(t *testing.T) {
	t.Parallel()
	client := failSecondListing{newFakeClient()}
	client.serviceDocument = map[string]any{"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"}
	pipeline := mustNew(t, Config{Concurrency: 1}, client, muxedProber{}, nil, discardLogger(), nil)
	inputs := []source.Item{localSource(writeFixture(t, "first.ts", "same bytes")), localSource(writeFixture(t, "second.ts", "same bytes"))}
	batch, err := pipeline.Run(t.Context(), inputs)
	if err != nil {
		t.Fatal(err)
	}
	first, second := batch.Results[0], batch.Results[1]
	if first.Status != ResultStatusIngested || second.Status != ResultStatusFailed || first.RootFlowID != second.RootFlowID || client.uploads != 1 {
		t.Fatalf("duplicate input: first=%s second=%s roots=%s,%s uploads=%d", first.Status, second.Status, first.RootFlowID, second.RootFlowID, client.uploads)
	}
	for _, flow := range first.Flows {
		if status := stringField(client.flows[flow.FlowID], "status"); status != flowStatusClosedComplete {
			t.Errorf("Flow %s left %q", flow.FlowID, status)
		}
	}
}

// Corrupt media that was found and withdrawn outranks Segments a run merely
// could not read: alerting on the verification status must see it.
func TestRetractedCorruptionOutranksPreservedSegments(t *testing.T) {
	t.Parallel()
	pipeline := mustNew(t, Config{VerificationMode: VerificationReadback}, newFakeClient(), fakeProber{}, nil, discardLogger(), nil)
	for _, testCase := range []struct {
		err  *VerificationError
		want VerificationStatus
	}{
		{&VerificationError{Retracted: 1, Preserved: 1}, VerificationFailedRetracted},
		{&VerificationError{Preserved: 2}, VerificationNotReached},
		{&VerificationError{Stranded: 1, Retracted: 1, Preserved: 1}, VerificationFailedStranded},
	} {
		if got := pipeline.verificationFailureStatus(testCase.err); got != testCase.want {
			t.Errorf("%+v -> %s, want %s", *testCase.err, got, testCase.want)
		}
	}
}

// A Segment adopted under another identifier must not be declared a
// conflict because its readback failed; only bytes proven different are.
func TestAdoptionReadFailureIsNotASegmentConflict(t *testing.T) {
	t.Parallel()
	item := ingestFixture(t)
	client := newFakeClient()
	client.rejectOccupiedIDs = true
	first := runOnce(t, client, item)
	flow := first.rootFlow()
	client.lock.Lock()
	delete(client.segments[flow.FlowID], flow.Objects[0].ObjectID)
	client.lock.Unlock()
	second := runOnce(t, client, item)
	fresh := second.rootFlow().Objects[0].ObjectID
	client.lock.Lock()
	client.downloadErrors["mem://"+fresh] = errors.New("connection reset by peer")
	uploads := client.uploads
	client.lock.Unlock()

	third := runOnce(t, client, item)
	if third.Status != ResultStatusFailed || third.Failure == nil {
		t.Fatalf("an unreadable adoption candidate should fail the run: %#v", third)
	}
	if third.Failure.Code == FailureCodeSegmentConflict {
		t.Fatalf("a read failure was reported as a Segment conflict: %s", third.Error)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.segments[flow.FlowID]) != 1 || client.uploads != uploads {
		t.Fatalf("the adoption candidate was disturbed: segments=%d uploads=%d", len(client.segments[flow.FlowID]), client.uploads)
	}
}

// On the pinned minor every essence parameter is Tamsin's, so one an earlier
// version stored and this one no longer generates is replaced on resume.
func TestResumeReplacesStaleGeneratedEssenceParameters(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.mp4", "media")
	client := newFakeClient()
	config := Config{Concurrency: 1, VerificationMode: VerificationReadback}
	run := func() Result {
		pipeline := mustNew(t, config, client, fakeProber{}, nil, discardLogger(), nil)
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil || batch.Succeeded != 1 {
			t.Fatalf("run: %#v, %v", batch, err)
		}
		return batch.Results[0]
	}
	first := run()
	client.lock.Lock()
	client.flows[first.RootFlowID]["essence_parameters"].(map[string]any)["bit_depth"] = 12
	client.lock.Unlock()
	if second := run(); second.Status != ResultStatusResumed {
		t.Fatalf("resume: %#v", second)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if _, present := client.flows[first.RootFlowID]["essence_parameters"].(map[string]any)["bit_depth"]; present {
		t.Fatal("a stale generated essence parameter survived the resume")
	}
}

// A large readback can outlive its presigned URL. The Segment is listed again
// for a fresh URL rather than a correct upload being retracted.
func TestVerificationRefreshesAnExpiredDownloadURL(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.mp4", "media")
	client := newFakeClient()
	client.expireFirstDownloads = true
	pipeline := mustNew(t, Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	result := batch.Results[0]
	if result.Status != ResultStatusIngested || result.Verification != VerificationVerified {
		t.Fatalf("expired URL was not refreshed: %s/%s %s", result.Status, result.Verification, result.Error)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.segments[result.RootFlowID]) != 1 {
		t.Fatal("the verified Segment was retracted")
	}
}

// A PUT replaces the whole Flow. The write at the end of a rolling ingest
// must keep what another system added while the ingest ran.
func TestRollingIngestKeepsEditsMadeWhileItRan(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.mp4", "media")
	client := newFakeClient()
	client.onRegisterSegments = func() {
		client.lock.Lock()
		defer client.lock.Unlock()
		for _, flow := range client.flows {
			if tags, ok := flow["tags"].(map[string]any); ok {
				tags["mam_asset"] = "A-1234"
			}
			flow["label"] = "Curated during ingest"
		}
	}
	pipeline := mustNew(t, Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 4}, discardLogger(), nil)
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("run: %#v, %v", batch, err)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	flow := client.flows[batch.Results[0].RootFlowID]
	tags, _ := flow["tags"].(map[string]any)
	if tags["mam_asset"] != "A-1234" || flow["label"] != "Curated during ingest" {
		t.Fatalf("the final write reverted edits made during the ingest: label=%v tags=%v", flow["label"], tags)
	}
	if flow["avg_bit_rate"] == nil {
		t.Fatal("the final write lost the measured bit rate")
	}
}
