package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

// The startup phases share mutable state, so the order they run in is
// load-bearing rather than stylistic. This drives the whole sequence: if storage
// selection ran before the backends request was checked, it would resolve
// against a nil list and report "no default storage backend", blaming the
// operator's configuration for what was actually a transport failure.
func TestStartupPreflightBlamesTheBackendsRequestNotTheSelection(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	client.backendsErr = errors.New("connection refused")
	pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = pipeline.runStartupPreflight(context.Background())
	if err == nil {
		t.Fatal("a failed storage backends request did not fail the preflight")
	}
	var classified *classifiedFailure
	if !errors.As(err, &classified) || classified.Code != FailureCodePreflightFailed {
		t.Fatalf("backends transport failure was not classified as a preflight failure: %#v", err)
	}
	if classified.Code == FailureCodeStorageUnavailable {
		t.Fatal("the transport failure was reported as an unusable storage backend")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("the underlying transport cause was dropped: %v", err)
	}
}

// Both startup requests finish before either response is interpreted. A
// storage transport failure must therefore remain visible even if the service
// response is independently incompatible or malformed.
func TestStartupPreflightChecksRequestFailuresBeforeResponseSemantics(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name    string
		service map[string]any
	}{
		{name: "incompatible service", service: map[string]any{"api_version": "7.0"}},
		{name: "missing lifetimes", service: map[string]any{"api_version": "8.1"}},
		{name: "valid service", service: map[string]any{
			"api_version": "8.1", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0",
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeClient()
			client.serviceDocument = testCase.service
			client.backendsErr = errors.New("backend connection refused")
			pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = pipeline.runStartupPreflight(context.Background())
			var classified *classifiedFailure
			if err == nil || !errors.As(err, &classified) || classified.Code != FailureCodePreflightFailed {
				t.Fatalf("backend request failure classification = %#v", err)
			}
			if !strings.Contains(err.Error(), "backend connection refused") {
				t.Fatalf("backend request failure was hidden by response validation: %v", err)
			}
		})
	}
}

// Selection over a nil backend list is what the phase above would produce if the
// two ran the other way round, and it is also the genuine classification when
// the request succeeded but returned nothing usable.
func TestStartupStorageSelectionReportsAnUnusableBackendList(t *testing.T) {
	t.Parallel()

	var classified *classifiedFailure
	err := selectStorage(&startupState{})
	if err == nil {
		t.Fatal("selection over a nil backend list unexpectedly succeeded")
	}
	if !errors.As(err, &classified) || classified.Code != FailureCodeStorageUnavailable {
		t.Fatalf("storage selection failure code = %#v", err)
	}
}

// Compatibility is checked before lifetimes so an unreadable or wrong-major
// service is reported as such, rather than as a missing lifetime field.
func TestStartupPreflightReportsServiceFailureBeforeLifetimes(t *testing.T) {
	t.Parallel()

	pipeline := &Pipeline{logger: discardLogger()}
	state := &startupState{serviceErr: errors.New("503 Service Unavailable")}
	err := checkServiceRequest(state)
	var classified *classifiedFailure
	if err == nil || !errors.As(err, &classified) || classified.Message != FailureMessagePreflightFailed {
		t.Fatalf("unreadable service was not classified as a preflight failure: %#v", err)
	}

	incompatible := &startupState{service: map[string]any{"api_version": "7.0"}}
	err = checkServiceCompatibility(pipeline, incompatible)
	if err == nil || !errors.As(err, &classified) || classified.Message != FailureMessagePreflightIncompatible {
		t.Fatalf("a differing major version was not classified as incompatible: %#v", err)
	}
}

// The selected backend ID is what every later Object registration is addressed
// to, so the phase must write the resolved ID back into the run's state.
func TestStartupPreflightAdoptsTheResolvedDefaultBackend(t *testing.T) {
	t.Parallel()

	state := &startupState{backends: []tams.StorageBackend{
		{ID: "other"},
		{ID: "chosen", DefaultStorage: true},
	}}
	if err := selectStorage(state); err != nil {
		t.Fatal(err)
	}
	if state.storageID != "chosen" {
		t.Fatalf("resolved storage ID = %q, want %q", state.storageID, "chosen")
	}
}

// A cancelled run must not be reported as a TAMS failure: the store did nothing
// wrong, and the operator needs the two causes kept apart.
func TestStartupRequestsReportParentCancellation(t *testing.T) {
	t.Parallel()

	pipeline := &Pipeline{client: newFakeClient(), logger: discardLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := issueStartupRequests(ctx, pipeline, &startupState{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled startup requests returned %v, want context.Canceled", err)
	}
	var classified *classifiedFailure
	if errors.As(err, &classified) {
		t.Fatalf("run cancellation was mislabeled as a typed TAMS failure: %#v", classified)
	}
}
