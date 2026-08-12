package ingest

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/source"
)

func TestTAMS82FlowStatusWrapsMutationAndAvoidsResumeChurn(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.serviceDocument = map[string]any{
		"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0",
	}
	pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	items := []source.Item{localSource(filename)}
	first, err := pipeline.Run(context.Background(), items)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.flowStatusWrites, []string{flowStatusIngesting, flowStatusClosedComplete}) {
		t.Fatalf("status writes = %v", client.flowStatusWrites)
	}
	if !reflect.DeepEqual(client.allocationStatuses, []string{flowStatusIngesting}) {
		t.Fatalf("status at allocation = %v", client.allocationStatuses)
	}
	if status := stringField(client.flows[first.Results[0].RootFlowID], "status"); status != flowStatusClosedComplete {
		t.Fatalf("terminal Flow status = %q", status)
	}

	client.putFlowCalls = 0
	client.flowStatusWrites = nil
	client.allocationStatuses = nil
	second, err := pipeline.Run(context.Background(), items)
	if err != nil {
		t.Fatal(err)
	}
	if second.Results[0].Status != ResultStatusResumed {
		t.Fatalf("second result = %#v", second.Results[0])
	}
	if client.putFlowCalls != 0 || len(client.flowStatusWrites) != 0 || len(client.allocationStatuses) != 0 {
		t.Fatalf("complete resume churned Flow lifecycle: puts=%d statuses=%v allocations=%v",
			client.putFlowCalls, client.flowStatusWrites, client.allocationStatuses)
	}
}

func TestTAMS82FailedWrittenGraphReturnsToAwaitingContent(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.serviceDocument = map[string]any{
		"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0",
	}
	client.listSegmentsErr = context.DeadlineExceeded
	pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil || batch.Failed != 1 {
		t.Fatalf("failed batch = %#v, %v", batch, err)
	}
	flowID := batch.Results[0].RootFlowID
	if status := stringField(client.flows[flowID], "status"); status != flowStatusAwaitingContent {
		t.Fatalf("failed Flow status = %q, writes=%v", status, client.flowStatusWrites)
	}
}

func TestTAMS82FailedCloseReturnsCompletedMediaToAwaitingContent(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.serviceDocument = map[string]any{
		"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0",
	}
	// Initial Flow creation and the ingesting transition succeed; closing fails.
	client.putFlowErrAt = 3
	pipeline, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil || batch.Failed != 1 {
		t.Fatalf("failed batch = %#v, %v", batch, err)
	}
	flowID := batch.Results[0].RootFlowID
	if status := stringField(client.flows[flowID], "status"); status != flowStatusAwaitingContent {
		t.Fatalf("Flow status after failed close = %q, writes=%v", status, client.flowStatusWrites)
	}
}
