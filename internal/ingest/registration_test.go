package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

func registrationFixture(t *testing.T, client *fakeClient, objects, transfers int, verify bool) (*Pipeline, []preparedObject, []ObjectResult) {
	t.Helper()
	verification := VerificationNone
	if verify {
		verification = VerificationReadback
	}
	pipeline, err := New(Config{Concurrency: 1, Transfers: transfers, VerificationMode: verification},
		client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	prepared := make([]preparedObject, objects)
	results := make([]ObjectResult, objects)
	for index := range objects {
		id := fmt.Sprintf("object-%d", index)
		data := []byte("media-" + id)
		path := filepath.Join(directory, id+".bin")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		timerange := fmt.Sprintf("[%d:0_%d:0)", index, index+1)
		prepared[index] = preparedObject{
			id: id, path: path, size: int64(len(data)), sha256: hex.EncodeToString(digest[:]),
			start: int64(index) * int64(time.Second), duration: int64(time.Second), timerange: timerange,
		}
		results[index] = ObjectResult{ObjectID: id, Timerange: timerange, Disposition: ObjectDispositionPlanned}
	}
	return pipeline, prepared, results
}

func commitRegistrationFixture(ctx context.Context, pipeline *Pipeline, objects []preparedObject, results []ObjectResult) error {
	_, _, err := pipeline.commitChunk(ctx, "flow", objects, results, "storage", 0)
	return err
}

type uploadDeadlineClient struct {
	*fakeClient
	presigned    *bool
	wantDeadline bool
}

func (c uploadDeadlineClient) AllocateStorage(ctx context.Context, flowID string, request tams.StorageRequest) (tams.StorageResponse, error) {
	response, err := c.fakeClient.AllocateStorage(ctx, flowID, request)
	for index := range response.MediaObjects {
		response.MediaObjects[index].Presigned = c.presigned
	}
	return response, err
}

func (c uploadDeadlineClient) UploadFile(ctx context.Context, destination tams.PresignedURL, filename string) (tams.UploadReceipt, error) {
	if destination.StartBefore.IsZero() == c.wantDeadline {
		return tams.UploadReceipt{}, fmt.Errorf("upload start deadline = %s, want deadline=%t", destination.StartBefore, c.wantDeadline)
	}
	return c.fakeClient.UploadFile(ctx, destination, filename)
}

func TestUploadDeadlineFollowsTheAllocationPresignedFlag(t *testing.T) {
	t.Parallel()
	yes, no := true, false
	for _, test := range []struct {
		name         string
		minor        int
		presigned    *bool
		wantDeadline bool
	}{
		{"8.2 presigned", 2, &yes, true},
		{"8.2 non-presigned", 2, &no, false},
		{"8.2 omitted", 2, nil, false},
		{"8.1 fallback", 1, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			pipeline, objects, results := registrationFixture(t, client, 1, 1, false)
			pipeline.client = uploadDeadlineClient{client, test.presigned, test.wantDeadline}
			pipeline.apiVersion = tams.APIVersion{Major: 8, Minor: test.minor}
			pipeline.limits = tams.ServiceLimits{ObjectRegistration: 300 * time.Second, PresignedURL: 30 * time.Second}
			if err := commitRegistrationFixture(context.Background(), pipeline, objects, results); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAutomaticVerificationUsesStorageEvidenceAndFallsBackToReadback(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name          string
		attested      bool
		wantDownloads int
	}{
		{name: "storage attested", attested: true},
		{name: "readback fallback", wantDownloads: 3},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeClient()
			client.storageChecksum = testCase.attested
			pipeline, objects, results := registrationFixture(t, client, 3, 2, true)
			pipeline.config.VerificationMode = VerificationAuto

			if err := commitRegistrationFixture(context.Background(), pipeline, objects, results); err != nil {
				t.Fatal(err)
			}
			client.lock.Lock()
			downloads := len(client.verified)
			client.lock.Unlock()
			if downloads != testCase.wantDownloads {
				t.Fatalf("verification downloads = %d, want %d", downloads, testCase.wantDownloads)
			}
		})
	}
}

func TestStorageChecksumMismatchStopsBeforeRegistration(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	client.storageChecksumValue = strings.Repeat("0", sha256.Size*2)
	pipeline, objects, results := registrationFixture(t, client, 1, 1, true)
	pipeline.config.VerificationMode = VerificationAuto

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil || !strings.Contains(err.Error(), "storage checksum") {
		t.Fatalf("commit error = %v, want storage checksum mismatch", err)
	}
	client.lock.Lock()
	registered := len(client.segments["flow"])
	client.lock.Unlock()
	if registered != 0 {
		t.Fatalf("registered segments = %d, want 0", registered)
	}
}

func TestRegistrationResponseLossAndReadbackFailureRetractsEveryObject(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	client.registerSegmentsErr = errors.New("connection reset after request body")
	client.registerSegmentsCommit = -1
	client.listSegmentsErr = errors.New("segment listing unavailable")
	pipeline, objects, results := registrationFixture(t, client, 4, 2, true)

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil || !strings.Contains(err.Error(), "connection reset") || !strings.Contains(err.Error(), "listing unavailable") {
		t.Fatalf("commit error = %v, want the original POST and readback failures", err)
	}
	client.lock.Lock()
	deletions := append([]string(nil), client.deletedTimeranges...)
	exactDeletions := append([]tams.SegmentDeleteOptions(nil), client.deletedSegments...)
	remaining := len(client.segments["flow"])
	client.lock.Unlock()
	if len(deletions) != len(objects) || remaining != 0 {
		t.Fatalf("retractions = %v, remaining = %d; every maybe-landed Object must be resolved", deletions, remaining)
	}
	deleted := make(map[string]string, len(exactDeletions))
	for _, options := range exactDeletions {
		deleted[options.ObjectID] = options.Timerange
	}
	for _, object := range objects {
		if deleted[object.id] != object.timerange {
			t.Fatalf("Object %s exact retraction = %q, want %q", object.id, deleted[object.id], object.timerange)
		}
	}
	for _, result := range results {
		if result.Disposition != ObjectDispositionRetracted {
			t.Fatalf("object %s status = %q, want retracted", result.ObjectID, result.Disposition)
		}
	}
}

func TestSuccessfulRegistrationWithFreshListingFailureRollsBack(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	client.listSegmentsErr = errors.New("cannot issue fresh URLs")
	pipeline, objects, results := registrationFixture(t, client, 3, 3, true)
	pipeline.limits = tams.ServiceLimits{
		ObjectRegistration: tams.MinimumObjectRegistration,
		PresignedURL:       tams.MinimumPresignedURL,
	}

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil || !strings.Contains(err.Error(), "cannot issue fresh URLs") {
		t.Fatalf("commit error = %v", err)
	}
	client.lock.Lock()
	deletions, remaining := len(client.deletedTimeranges), len(client.segments["flow"])
	client.lock.Unlock()
	if deletions != 3 || remaining != 0 {
		t.Fatalf("deletions = %d, remaining = %d; known-registered Objects were left unchecked", deletions, remaining)
	}
}

func TestIncompleteFreshListingVerifiesVisibleAndRetractsEveryMissingObject(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	pipeline, objects, results := registrationFixture(t, client, 5, 5, true)
	pipeline.limits = tams.ServiceLimits{
		ObjectRegistration: tams.MinimumObjectRegistration,
		PresignedURL:       tams.MinimumPresignedURL,
	}
	client.hasListingOverride = true
	for _, object := range objects[:2] {
		client.listSegmentsOverride = append(client.listSegmentsOverride, tams.Segment{
			ObjectID: object.id, Timerange: object.timerange,
			GetURLs: []tams.PresignedURL{{URL: "mem://" + object.id}},
		})
	}

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil {
		t.Fatal("an incomplete post-registration listing unexpectedly succeeded")
	}
	for _, object := range objects[2:] {
		if !strings.Contains(err.Error(), object.id) {
			t.Fatalf("error does not name omitted Object %s: %v", object.id, err)
		}
	}
	client.lock.Lock()
	verified := len(client.verified)
	deletions := len(client.deletedTimeranges)
	remaining := len(client.segments["flow"])
	client.lock.Unlock()
	if verified != 2 || deletions != 3 || remaining != 2 {
		t.Fatalf("verified = %d, deletions = %d, remaining = %d", verified, deletions, remaining)
	}
	for index, result := range results {
		want := ObjectDispositionRegistered
		if index >= 2 {
			want = ObjectDispositionRetracted
		}
		if result.Disposition != want {
			t.Fatalf("object %s status = %q, want %q", result.ObjectID, result.Disposition, want)
		}
		if index < 2 && (result.Verification != ObjectVerificationVerified || result.VerificationMethod != VerificationMethodReadback) {
			t.Fatalf("visible object was not verified by readback: %#v", result)
		}
	}
}

func TestTypedPartialRegistrationDoesNotReadAfterWrite(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	pipeline, objects, results := registrationFixture(t, client, 4, 2, true)
	registered := []tams.SegmentRequest{
		{ObjectID: objects[0].id, Timerange: objects[0].timerange},
		{ObjectID: objects[1].id, Timerange: objects[1].timerange},
	}
	client.registerSegmentsCommit = 2
	client.registerSegmentsErr = &tams.PartialSegmentRegistrationError{
		Total: 4, RegisteredSegments: registered,
		FailedSegments: []tams.FailedSegment{
			{ObjectID: objects[2].id, Timerange: objects[2].timerange},
			{ObjectID: objects[3].id, Timerange: objects[3].timerange},
		},
	}

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil || !strings.Contains(err.Error(), objects[2].id) || !strings.Contains(err.Error(), objects[3].id) {
		t.Fatalf("commit error = %v", err)
	}
	client.lock.Lock()
	listings := client.listSegmentsCalls
	deletions := len(client.deletedTimeranges)
	remaining := len(client.segments["flow"])
	client.lock.Unlock()
	if listings != 0 {
		t.Fatalf("typed partial result caused %d read-after-write listing(s)", listings)
	}
	if deletions != 2 || remaining != 0 {
		t.Fatalf("deletions = %d, remaining = %d; authoritative registered complement was not rolled back", deletions, remaining)
	}
	for index, result := range results {
		want := ObjectDispositionRetracted
		if index >= 2 {
			want = ObjectDispositionRejected
		}
		if result.Disposition != want {
			t.Fatalf("object %s status = %q, want %q", result.ObjectID, result.Disposition, want)
		}
	}
}

func TestCancellationImmediatelyAfterRegistrationCannotSkipRollback(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	pipeline, objects, results := registrationFixture(t, client, 4, 2, true)
	ctx, cancel := context.WithCancel(context.Background())
	client.onRegisterSegments = cancel

	err := commitRegistrationFixture(ctx, pipeline, objects, results)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("commit error = %v, want cancellation", err)
	}
	client.lock.Lock()
	deletions, remaining := len(client.deletedTimeranges), len(client.segments["flow"])
	client.lock.Unlock()
	if deletions != 4 || remaining != 0 {
		t.Fatalf("deletions = %d, remaining = %d; parent cancellation skipped detached recovery", deletions, remaining)
	}
}

func TestRegistrationCleanupReportsEveryStrandedObject(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	client.registerSegmentsErr = errors.New("response lost")
	client.registerSegmentsCommit = -1
	client.listSegmentsErr = errors.New("readback lost")
	pipeline, objects, results := registrationFixture(t, client, 6, 2, true)
	client.deleteSegmentsErrors = make(map[string]error, len(objects))
	for _, object := range objects {
		client.deleteSegmentsErrors[object.timerange] = errors.New("flow is read-only")
	}

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil {
		t.Fatal("cleanup failures unexpectedly succeeded")
	}
	for _, object := range objects {
		if !strings.Contains(err.Error(), object.id) {
			t.Fatalf("stranded Object %s is absent from error: %v", object.id, err)
		}
	}
	client.lock.Lock()
	deletions := len(client.deletedTimeranges)
	client.lock.Unlock()
	if deletions != len(objects) {
		t.Fatalf("attempted %d of %d retractions", deletions, len(objects))
	}
	for _, result := range results {
		if result.Disposition != ObjectDispositionStranded {
			t.Fatalf("object %s status = %q, want stranded", result.ObjectID, result.Disposition)
		}
	}
}

func TestRegistrationCleanupUsesOneSharedDeadline(t *testing.T) {
	client := newFakeClient()
	client.registerSegmentsErr = errors.New("response lost")
	client.registerSegmentsCommit = -1
	client.listSegmentsErr = errors.New("readback lost")
	client.blockDeleteUntilDone = true
	pipeline, objects, results := registrationFixture(t, client, 5, 1, true)
	pipeline.registrationRecoveryTimeout = 40 * time.Millisecond

	err := commitRegistrationFixture(context.Background(), pipeline, objects, results)
	if err == nil {
		t.Fatal("deadline-bound cleanup unexpectedly succeeded")
	}
	client.lock.Lock()
	deletions := len(client.deletedTimeranges)
	deadlines := append([]time.Time(nil), client.deleteDeadlines...)
	client.lock.Unlock()
	if deletions != len(objects) {
		t.Fatalf("attempted %d of %d cleanups under the shared deadline", deletions, len(objects))
	}
	for index, deadline := range deadlines {
		if deadline.IsZero() || !deadline.Equal(deadlines[0]) {
			t.Fatalf("cleanup %d deadline = %s, want one shared deadline %s", index, deadline, deadlines[0])
		}
	}
}
