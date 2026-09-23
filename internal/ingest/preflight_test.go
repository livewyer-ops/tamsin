package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

// Check request errors before backend selection, so transport failures do not
// become misleading "no default storage backend" errors.
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

// Selection over a nil backend list is what the preflight would see if request
// errors were checked the other way round, and it is also the genuine
// classification when the request succeeded but returned nothing usable.
func TestStartupStorageSelectionReportsAnUnusableBackendList(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	client.backends = nil
	pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pipeline.runStartupPreflight(context.Background())
	if err == nil {
		t.Fatal("selection over a nil backend list unexpectedly succeeded")
	}
	var classified *classifiedFailure
	if !errors.As(err, &classified) || classified.Code != FailureCodeStorageUnavailable {
		t.Fatalf("storage selection failure code = %#v", err)
	}
}

// Compatibility is checked before lifetimes so an unreadable or wrong-major
// service is reported as such, rather than as a missing lifetime field.
func TestStartupPreflightReportsServiceFailureBeforeLifetimes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name        string
		serviceErr  error
		service     map[string]any
		wantMessage string
	}{
		{name: "unreadable service", serviceErr: errors.New("503 Service Unavailable"), wantMessage: FailureMessagePreflightFailed},
		{name: "differing major version", service: map[string]any{"api_version": "7.0"}, wantMessage: FailureMessagePreflightIncompatible},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeClient()
			client.serviceErr = testCase.serviceErr
			client.serviceDocument = testCase.service
			pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pipeline.runStartupPreflight(context.Background())
			var classified *classifiedFailure
			if err == nil || !errors.As(err, &classified) || classified.Message != testCase.wantMessage {
				t.Fatalf("preflight failure = %#v, want message %q", err, testCase.wantMessage)
			}
		})
	}
}

// The selected backend ID is what every later Object registration is addressed
// to, so the preflight must return the resolved ID.
func TestStartupPreflightAdoptsTheResolvedDefaultBackend(t *testing.T) {
	t.Parallel()

	client := newFakeClient()
	client.backends = []tams.StorageBackend{
		{ID: "other"},
		{ID: "chosen", DefaultStorage: true},
	}
	pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	storageID, err := pipeline.runStartupPreflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if storageID != "chosen" {
		t.Fatalf("resolved storage ID = %q, want %q", storageID, "chosen")
	}
}

// A cancelled run must not be reported as a TAMS failure: the store did nothing
// wrong, and the operator needs the two causes kept apart.
func TestStartupRequestsReportParentCancellation(t *testing.T) {
	t.Parallel()

	pipeline, err := New(Config{Concurrency: 1}, newFakeClient(), fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pipeline.runStartupPreflight(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled startup requests returned %v, want context.Canceled", err)
	}
	var classified *classifiedFailure
	if errors.As(err, &classified) {
		t.Fatalf("run cancellation was mislabeled as a typed TAMS failure: %#v", classified)
	}
}
