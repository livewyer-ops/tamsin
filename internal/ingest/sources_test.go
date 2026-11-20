package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// TestSourcesReceiveLabelDescriptionAndProvenance covers AppNote 0007: the
// Sources the store derives for the root and each essence get a readable
// identity from their Flows, and an operator's later edits survive a resume.
func TestSourcesReceiveLabelDescriptionAndProvenance(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.ts", "muxed-media")
	client := newFakeClient()
	config := Config{Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second}
	pipeline := mustNew(t, config, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
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

	resume := mustNew(t, config, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
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

// A store derives a Source from its Flow in its own time. One that is not
// there on the first read is asked for once more before its metadata is
// given up.
func TestSourceNotYetDerivedIsReadOnceMore(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.mp4", "media")
	client := newFakeClient()
	client.sourceMissingReads = 1
	pipeline := mustNew(t, Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 {
		t.Fatalf("ingest failed: %#v", batch.Results[0].Failure)
	}
	root := batch.Results[0].rootFlow()
	client.lock.Lock()
	defer client.lock.Unlock()
	if label := client.sources[root.SourceID]["label"]; label == nil || label != client.flows[root.FlowID]["label"] {
		t.Fatalf("source label = %v, want the Flow's %v", label, client.flows[root.FlowID]["label"])
	}
}

func TestSourcePopulationFailureIsAWarningNotAFailure(t *testing.T) {
	t.Parallel()
	filename := writeFixture(t, "fixture.mp4", "media")
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
			pipeline := mustNew(t, Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, logger, nil)
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

// sourceBarrierClient holds every Source read until the test lets it go, and
// says when each one arrives.
type sourceBarrierClient struct {
	*fakeClient
	arrived chan struct{}
	proceed chan struct{}
}

func (c *sourceBarrierClient) Source(ctx context.Context, id string) (tams.Source, error) {
	c.arrived <- struct{}{}
	<-c.proceed
	return c.fakeClient.Source(ctx, id)
}

// Sources are populated side by side, and no more of them at once across the
// run than the transfer budget is large.
func TestSourcesArePopulatedWithinTheTransferBudget(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		inputs    int
		transfers int
		together  int
	}{
		{"three at once", 1, 3, 3},
		{"one at a time", 1, 1, 1},
		{"two inputs share the budget", 2, 2, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var items []source.Item
			for input := range test.inputs {
				items = append(items, localSource(writeFixture(t, fmt.Sprintf("fixture-%d.ts", input), fmt.Sprintf("muxed-media-%d", input))))
			}
			// Each input has a root Source and one for each of two essences.
			sources := 3 * test.inputs
			client := &sourceBarrierClient{fakeClient: newFakeClient(), arrived: make(chan struct{}, sources), proceed: make(chan struct{})}
			pipeline := mustNew(t, Config{Concurrency: test.inputs, Transfers: test.transfers, SegmentDuration: time.Second},
				client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
			done := make(chan error, 1)
			go func() {
				_, err := pipeline.Run(context.Background(), items)
				done <- err
			}()
			for read := range test.together {
				select {
				case <-client.arrived:
				case <-time.After(10 * time.Second):
					t.Fatalf("%d Source reads were in flight, want %d", read, test.together)
				}
			}
			if test.together < sources {
				select {
				case <-client.arrived:
					t.Fatalf("more than %d Source reads were in flight", test.together)
				case <-time.After(100 * time.Millisecond):
				}
			}
			close(client.proceed)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A cancelled input does not wait for a Source slot that another input holds.
func TestCancelledInputDoesNotWaitForASourceSlot(t *testing.T) {
	t.Parallel()
	pipeline := mustNew(t, Config{Concurrency: 1, Transfers: 1}, newFakeClient(), fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
	pipeline.sourceSlots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		pipeline.populateSources(ctx, []plannedFlowWrite{{effective: tams.Flow{"source_id": "source"}}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled input waited for a Source slot held elsewhere")
	}
}
