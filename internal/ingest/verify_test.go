package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

// strandedSegments counts Segments left registered in the fake store. Any
// Segment still present after a failed ingest is one the Flow references and
// nobody checked.
func strandedSegments(client *fakeClient) int {
	client.lock.Lock()
	defer client.lock.Unlock()
	total := 0
	for _, segments := range client.segments {
		total += len(segments)
	}
	return total
}

// A verification failure stops later allocation. Every Segment already
// registered in the upload group must still be verified or retracted.
func TestNoCorruptSegmentSurvivesVerification(t *testing.T) {
	t.Parallel()
	for _, transfers := range []int{1, 4, 16} {
		t.Run("transfers="+strconv.Itoa(transfers), func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			client.corruptOnUpload = true

			pipeline, err := New(Config{
				Concurrency: 1, Transfers: transfers, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
				EssenceStorage: media.EssenceStorageMuxed,
			}, client, fakeProber{}, countingSegmenter{objects: 16}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if batch.Failed != 1 {
				t.Fatalf("a corrupted upload must fail the ingest: %#v", batch)
			}
			if stranded := strandedSegments(client); stranded != 0 {
				t.Fatalf("%d corrupt Segments remain registered; every registered Object must end verified or retracted", stranded)
			}
			client.lock.Lock()
			retractions := len(client.deletedTimeranges)
			client.lock.Unlock()
			wantRetractions := min(transfers, 16)
			if retractions != wantRetractions {
				t.Fatalf("retracted %d Objects, want the %d that entered the ready upload group",
					retractions, wantRetractions)
			}
		})
	}
}

func TestRollingResultRetainsActionRequiredObjectWithoutOptIn(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.corruptOnUpload = true
	client.deleteSegmentsErr = errors.New("flow is read-only")
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	root := batch.Results[0].rootFlow()
	if root == nil || root.ObjectSummary.Stranded != 1 || len(root.Objects) != 1 ||
		root.Objects[0].Disposition != ObjectDispositionStranded || root.Objects[0].ObjectID == "" {
		t.Fatalf("rolling action-required result = %#v; recovery Object must remain copyable", root)
	}
}

// TestCancellationRetractsRegisteredSegments covers the other way an Object can
// end up registered and unchecked: the run is cancelled after registration.
// Cancellation must not be a route to leaving media in the store unverified.
func TestCancellationRetractsRegisteredSegments(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	// The uploads are corrupt, so any Segment still registered at the end is one
	// nobody verified. With healthy bytes a surviving Segment would be correct,
	// and the assertion would prove nothing.
	client.corruptOnUpload = true
	// Cancel once verification has started, so Segments are registered and some
	// checks are still queued behind the single transfer slot.
	ctx, cancel := context.WithCancel(context.Background())
	client.onDownload = func() { cancel() }

	pipeline, err := New(Config{
		RetainObjectResults: true,
		Concurrency:         1, Transfers: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 8}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pipeline.Run(ctx, []source.Item{localSource(filename)})

	if stranded := strandedSegments(client); stranded != 0 {
		t.Fatalf("%d Segments left registered after cancellation; retraction must not depend on the cancelled context", stranded)
	}
}

func TestVerificationCleanupUsesOneSharedDeadline(t *testing.T) {
	client := newFakeClient()
	client.blockDeleteUntilDone = true
	pipeline, objects, _ := registrationFixture(t, client, 8, 1, true)
	pipeline.verificationRecoveryTimeout = 50 * time.Millisecond
	tasks := make([]verificationTask, len(objects))
	for index, object := range objects {
		tasks[index] = verificationTask{object: object}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	outcomes, err := pipeline.verifyAllWithOutcomes(ctx, "flow", tasks)
	if err == nil {
		t.Fatal("deadline-bound cleanup unexpectedly succeeded")
	}
	var verificationErr *VerificationError
	if !errors.As(err, &verificationErr) || verificationErr.Stranded != len(tasks) {
		t.Fatalf("verification error = %#v, want %d stranded Objects", err, len(tasks))
	}
	client.lock.Lock()
	deletions := len(client.deletedTimeranges)
	deadlines := append([]time.Time(nil), client.deleteDeadlines...)
	client.lock.Unlock()
	if deletions != len(tasks) {
		t.Fatalf("attempted %d of %d cleanups under the shared deadline", deletions, len(tasks))
	}
	for index, deadline := range deadlines {
		if deadline.IsZero() || !deadline.Equal(deadlines[0]) {
			t.Fatalf("cleanup %d deadline = %s, want one shared deadline %s", index, deadline, deadlines[0])
		}
	}
	for index, outcome := range outcomes {
		if outcome != outcomeRetractionFailed {
			t.Fatalf("outcome %d = %d, want retraction failure", index, outcome)
		}
	}
}

// TestResumeVerificationUsesTransferBudget covers the second half of the same
// scheduler. Resumed Objects were verified serially in the caller's loop,
// bypassing the budget entirely: a resume could not use more than one
// connection however --transfers was set, and several inputs resuming at once
// could collectively exceed it.
func TestResumeVerificationUsesTransferBudget(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Latency makes overlap observable; without it a check can finish before the
	// next begins and a peak of one would be legitimate.
	client := newCountingClient(2 * time.Millisecond)
	config := Config{
		RetainObjectResults: true,
		Concurrency:         1, Transfers: 4, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	pipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 16}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	if _, err := pipeline.Run(context.Background(), []source.Item{item}); err != nil {
		t.Fatal(err)
	}

	// Everything is stored now, so the second run is a pure resume.
	client.peakTransfers.Store(0)
	resumePipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 16}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := resumePipeline.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Results[0].rootFlow().Objects[0].Disposition != ObjectDispositionResumed {
		t.Fatalf("expected a resume, got %q", batch.Results[0].rootFlow().Objects[0].Disposition)
	}
	assertBatchResultSchema(t, batch)

	peak := client.peakTransfers.Load()
	if peak < 2 {
		t.Fatalf("peak in-flight transfers during resume = %d: resumed verification is still serial", peak)
	}
	if peak > int64(config.Transfers) {
		t.Fatalf("peak in-flight transfers during resume = %d, exceeding the budget of %d", peak, config.Transfers)
	}
}

func TestFailedResumeVerificationUpdatesObjectTerminalState(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	config := Config{Concurrency: 1, Transfers: 1, VerificationMode: VerificationReadback}
	item := localSource(filename)
	first, err := New(config, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if batch, runErr := first.Run(context.Background(), []source.Item{item}); runErr != nil || batch.Failed != 0 {
		t.Fatalf("initial ingest: batch=%#v error=%v", batch, runErr)
	}

	client.lock.Lock()
	for objectID, data := range client.objects {
		client.objects[objectID] = append(append([]byte(nil), data...), byte('!'))
	}
	client.lock.Unlock()

	resume, err := New(config, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, runErr := resume.Run(context.Background(), []source.Item{item})
	if runErr != nil {
		t.Fatal(runErr)
	}
	result := batch.Results[0]
	if batch.Failed != 1 || result.Verification != VerificationFailedRetracted {
		t.Fatalf("corrupt resumed Object was not classified as a verification failure: %#v", result)
	}
	root := result.rootFlow()
	if root == nil || len(root.Objects) != 1 || root.Objects[0].Disposition != ObjectDispositionRetracted {
		t.Fatalf("resumed Object retained a stale status after retraction: %#v", root)
	}
	assertBatchResultSchema(t, batch)
}

// TestFailedRetractionIsReportedDistinctly keeps the operator-actionable case
// separable: a Segment that could not be withdrawn needs manual cleanup, and
// must not read the same as one that was.
func TestFailedRetractionIsReportedDistinctly(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.corruptOnUpload = true
	client.deleteSegmentsErr = errors.New("flow is read-only")

	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 4}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	message := batch.Results[0].Error
	if !strings.Contains(message, "could not be retracted") {
		t.Fatalf("a failed retraction must be reported as such, got %q", message)
	}
	if !strings.Contains(message, "SHA-256 mismatch") {
		t.Fatalf("the integrity failure must survive a failed retraction, got %q", message)
	}
}

// A partial bulk response names failed Segments despite its success status.
// Every committed Segment still needs verification or retraction.
func TestPartialBulkRegistrationLeavesNothingUnchecked(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		committed int
		verify    VerificationMode
	}{
		{name: "one of the batch landed", committed: 1, verify: VerificationReadback},
		{name: "several landed", committed: 5, verify: VerificationReadback},
		{name: "none landed", committed: 0, verify: VerificationReadback},
		// Without verification there is nothing to distinguish good bytes from
		// bad, and the pipeline's rule is that unchecked means corrupt.
		{name: "several landed with verification off", committed: 5, verify: VerificationNone},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			client.registerSegmentsErr = errors.New("2 of 8 segments failed to register")
			client.registerSegmentsCommit = testCase.committed

			pipeline, err := New(Config{
				Concurrency: 1, Transfers: 8, VerificationMode: testCase.verify, SegmentDuration: time.Second,
				EssenceStorage: media.EssenceStorageMuxed,
			}, client, fakeProber{}, countingSegmenter{objects: 8}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if batch.Failed != 1 {
				t.Fatalf("a failed bulk registration must fail the ingest: %#v", batch)
			}
			if !strings.Contains(batch.Results[0].Error, "register segments") {
				t.Fatalf("the registration failure must be reported: %q", batch.Results[0].Error)
			}
			wantVerification := VerificationNotRequested
			if testCase.verify != VerificationNone {
				wantVerification = VerificationNotReached
			}
			if batch.Results[0].Verification != wantVerification {
				t.Fatalf("registration cleanup verification = %q, want %q", batch.Results[0].Verification, wantVerification)
			}
			assertBatchResultSchema(t, batch)

			client.lock.Lock()
			remaining := 0
			for _, segments := range client.segments {
				remaining += len(segments)
			}
			verified, retracted := len(client.verified), len(client.deletedTimeranges)
			client.lock.Unlock()

			if testCase.verify != VerificationNone {
				// Everything that landed was read back and checked, and having
				// passed, it stays: the next run resumes rather than repeating
				// work that is already correct.
				if verified != testCase.committed {
					t.Fatalf("%d of the %d registered Segments were verified", verified, testCase.committed)
				}
				if remaining != testCase.committed {
					t.Fatalf("%d Segments remain of %d verified", remaining, testCase.committed)
				}
				return
			}
			// Nothing can vouch for these bytes, so none may stay.
			if remaining != 0 {
				t.Fatalf("%d Segments remain registered but unchecked", remaining)
			}
			// The generic error is ambiguous, so readback-visible Segments and
			// every missing/maybe-landed Segment receive an exact retraction. A
			// missing Segment is not proof that a just-completed write did not
			// land behind an eventually consistent listing.
			if retracted != 8 {
				t.Fatalf("sent %d of 8 ambiguous Segments for retraction", retracted)
			}
		})
	}
}

// TestFullBulkCommitWithALostResponseIsNotTreatedAsFailure keeps the ambiguous
// case working. When every Segment reached the store and only the answer was
// lost, the read-back finds them all and the ingest carries on rather than
// discarding correct work.
func TestFullBulkCommitWithALostResponseIsNotTreatedAsFailure(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.registerSegmentsErr = errors.New("connection reset before the response arrived")
	client.registerSegmentsCommit = -1 // the whole batch commits

	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 4, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 4}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 {
		t.Fatalf("a committed batch whose response was lost should succeed: %#v", batch.Results[0].Error)
	}
	assertBatchResultSchema(t, batch)
	if stranded := strandedSegments(client); stranded != 4 {
		t.Fatalf("%d Segments in the store, want the 4 that committed", stranded)
	}
}

// Count all goroutines, including those waiting for a transfer slot. Do not run
// in parallel: the count includes other tests in this process.
func TestVerificationGoroutinesFollowTheBudgetNotTheWork(t *testing.T) {
	const (
		objects   = 2000
		transfers = 4
		// Allow scheduling noise while rejecting one goroutine per Object.
		headroom = 250
	)
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}

	client := newFakeClient()
	var peak atomic.Int64
	client.onDownload = func() {
		if current := int64(runtime.NumGoroutine()); current > peak.Load() {
			peak.Store(current)
		}
	}

	baseline := runtime.NumGoroutine()
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: transfers, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)}); err != nil {
		t.Fatal(err)
	}

	if grew := int(peak.Load()) - baseline; grew > headroom {
		t.Fatalf("verifying %d Objects with a budget of %d grew the goroutine count by %d; "+
			"they are being created per Object rather than per worker", objects, transfers, grew)
	}
}
