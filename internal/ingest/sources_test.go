package ingest

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

// TestSourcesReceiveLabelDescriptionAndProvenance covers AppNote 0007: the
// Sources the store derives for the root and each essence get a readable
// identity from their Flows, and an operator's later edits survive a resume.
func TestSourcesReceiveLabelDescriptionAndProvenance(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	config := Config{Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second}
	pipeline, err := New(config, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 {
		t.Fatalf("ingest failed: %#v", batch.Results[0].Failure)
	}
	client.lock.Lock()
	if len(client.sources) != 3 {
		t.Fatalf("sources = %d, want the root and two essences", len(client.sources))
	}
	for _, flowResult := range batch.Results[0].Flows {
		flow := client.flows[flowResult.FlowID]
		src := client.sources[flowResult.SourceID]
		tags, _ := src["tags"].(map[string]any)
		if src["label"] != flow["label"] || src["description"] != flow["description"] || tags[media.ProvenanceSourcesTag] == nil {
			t.Fatalf("source %s = %#v, flow %#v", flowResult.SourceID, src, flow)
		}
	}
	root := batch.Results[0].rootFlow()
	client.sources[root.SourceID]["label"] = "Operator's title"
	writes := client.sourceWrites
	client.lock.Unlock()

	resume, err := New(config, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resume.Run(context.Background(), []source.Item{localSource(filename)}); err != nil {
		t.Fatal(err)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.sources[root.SourceID]["label"] != "Operator's title" {
		t.Fatalf("resume overwrote the operator's label: %#v", client.sources[root.SourceID])
	}
	if client.sourceWrites != writes {
		t.Fatalf("resume rewrote source metadata that was already right: %d writes, was %d", client.sourceWrites, writes)
	}
}

func TestSourcePopulationFailureIsAWarningNotAFailure(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, configure := range map[string]func(*fakeClient){
		"forbidden writes": func(client *fakeClient) { client.sourceWriteErr = errors.New("403 Forbidden") },
		"unreadable":       func(client *fakeClient) { client.sourceReadErr = errors.New("503 Service Unavailable") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := newFakeClient()
			configure(client)
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
				t.Fatalf("source trouble failed the ingest: %#v", batch.Results[0].Failure)
			}
			if !strings.Contains(logged.String(), "source metadata was not populated") {
				t.Fatalf("no warning: %s", logged.String())
			}
		})
	}
}
