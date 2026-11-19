package ingest

import (
	"bytes"
	"context"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// TestNewerMinorStoreFieldsDoNotFailPinnedSchemas covers a store one or more
// minor revisions ahead of the pinned schemas. Minor revisions add fields, so
// a Flow read back may carry keys the closed 8.2 schemas reject; only what
// Tamsin generates is held to them, and the store's additions are sent back
// untouched.
func TestNewerMinorStoreFieldsDoNotFailPinnedSchemas(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.serviceDocument = map[string]any{"api_version": "8.7", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"}
	client.decorateFlowReads = func(flow tams.Flow) tams.Flow {
		// A real store returns fresh JSON; the fake must not alias the maps
		// the pipeline generated.
		flow["future_top_level"] = "added by an 8.7 store"
		if parameters, ok := flow["essence_parameters"].(map[string]any); ok {
			parameters = maps.Clone(parameters)
			parameters["future_parameter"] = true
			flow["essence_parameters"] = parameters
		}
		return flow
	}
	config := Config{Concurrency: 1, Transfers: 2, SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed}
	for run, want := range []ResultStatus{ResultStatusIngested, ResultStatusResumed} {
		pipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if batch.Succeeded != 1 || batch.Results[0].Status != want {
			t.Fatalf("run %d = %#v / %s, want %s", run, batch.Results[0].Failure, batch.Results[0].Error, want)
		}
		client.lock.Lock()
		flow := client.flows[batch.Results[0].RootFlowID]
		client.lock.Unlock()
		if flow["status"] != "closed_complete" {
			t.Fatalf("run %d left status %v", run, flow["status"])
		}
		if run == 1 && flow["future_top_level"] != "added by an 8.7 store" {
			t.Fatalf("the store's own field was not sent back on resume: %#v", flow)
		}
	}
}

// TestPresignedLifetimeIsClampedToTheObjectLifetime pins the tolerant reading
// of a service whose URL lifetime exceeds its Object lifetime, as the
// specification's own example does: warn, schedule against the shorter one,
// and carry on.
func TestPresignedLifetimeIsClampedToTheObjectLifetime(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.serviceDocument = map[string]any{"api_version": "8.1", "min_object_timeout": "300:0", "min_presigned_url_timeout": "301:0"}
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 {
		t.Fatalf("ingest failed: %#v / %s", batch.Results[0].Failure, batch.Results[0].Error)
	}
	if pipeline.limits.PresignedURL != 5*time.Minute || pipeline.limits.ObjectRegistration != 5*time.Minute {
		t.Fatalf("limits = %#v", pipeline.limits)
	}
	if !strings.Contains(logged.String(), "scheduled against the Object lifetime") {
		t.Fatalf("no warning about the clamped lifetime: %s", logged.String())
	}
}

// TestFreshAllocationFollowsTheServiceCap covers a service that hands out
// fewer service-assigned identifiers than asked for: the batch asks again for
// the remainder rather than failing.
func TestFreshAllocationFollowsTheServiceCap(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.rejectOccupiedIDs = true
	client.allocationCap = 1
	config := Config{Concurrency: 1, Transfers: 2, SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed, VerificationMode: VerificationReadback}
	run := func() Result {
		pipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 3}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		return batch.Results[0]
	}
	first := run()
	if first.Status != ResultStatusIngested {
		t.Fatalf("first run: %#v", first)
	}
	// Every registration is lost while the identifiers stay occupied.
	client.lock.Lock()
	delete(client.segments, first.RootFlowID)
	client.lock.Unlock()
	second := run()
	if second.Status != ResultStatusIngested {
		t.Fatalf("retry under a capped allocation failed: %#v / %s", second.Failure, second.Error)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.freshAllocations != 3 || len(client.segments[first.RootFlowID]) != 3 {
		t.Fatalf("fresh allocations = %d, segments = %d; want 3 and 3", client.freshAllocations, len(client.segments[first.RootFlowID]))
	}
	for _, object := range second.rootFlow().Objects {
		if !strings.HasPrefix(object.ObjectID, "fresh-") || object.Disposition != ObjectDispositionIngested {
			t.Fatalf("object not under a fresh identifier: %#v", object)
		}
	}
}

func TestRecoveryDeadlineFollowsTheClientDeletionTimeout(t *testing.T) {
	t.Parallel()
	if got := recoveryDeadline(newFakeClient()); got != retractionTimeout {
		t.Fatalf("fake client deadline = %v", got)
	}
	client, err := tams.New(tams.Config{Endpoint: "https://tams.example.test", DeletionTimeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if got := recoveryDeadline(client); got != 2*time.Minute {
		t.Fatalf("client deadline = %v, want the deletion timeout", got)
	}
	if client, err = tams.New(tams.Config{Endpoint: "https://tams.example.test", DeletionTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if got := recoveryDeadline(client); got != retractionTimeout {
		t.Fatalf("short deletion timeout should keep the cleanup floor: %v", got)
	}
}
