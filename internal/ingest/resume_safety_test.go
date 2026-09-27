package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func TestResumePreservesNewerMinorEssenceFields(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.serviceDocument = map[string]any{"api_version": "8.7", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"}
	config := Config{Concurrency: 1, VerificationMode: VerificationReadback}
	run := func() Result {
		p, err := New(config, client, fakeProber{}, nil, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := p.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		if b.Succeeded != 1 {
			t.Fatalf("ingest failed: %s", b.Results[0].Error)
		}
		return b.Results[0]
	}
	first := run()
	client.flows[first.RootFlowID]["essence_parameters"].(map[string]any)["future_parameter"] = true
	client.flows[first.RootFlowID]["future_top_level"] = true
	config.FlowMetadata = tams.Flow{"label": "Updated root"}
	second := run()
	if second.Status != ResultStatusResumed {
		t.Fatalf("not a resume: %v", second.Status)
	}
	got := client.flows[first.RootFlowID]
	if got["future_top_level"] != true {
		t.Fatal("control: top-level extension was lost")
	}
	if got["essence_parameters"].(map[string]any)["future_parameter"] != true {
		t.Fatal("resume deleted the store-owned essence parameter")
	}
}

func TestMetadataOverridesRespectCollectedRoles(t *testing.T) {
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		for _, mode := range []string{"root-only", "collected-only", "both"} {
			t.Run(string(storage)+"/"+mode, func(t *testing.T) {
				filename := filepath.Join(t.TempDir(), "fixture.mp4")
				if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
					t.Fatal(err)
				}
				client := newFakeClient()
				config := Config{Concurrency: 1, EssenceStorage: storage, VerificationMode: VerificationReadback, SegmentDuration: time.Second}
				run := func() Result {
					p, err := New(config, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
					if err != nil {
						t.Fatal(err)
					}
					batch, err := p.Run(t.Context(), []source.Item{localSource(filename)})
					if err != nil {
						t.Fatal(err)
					}
					if batch.Succeeded != 1 {
						t.Fatalf("ingest failed: %s", batch.Results[0].Error)
					}
					assertBatchResultSchema(t, batch)
					return batch.Results[0]
				}
				first := run()
				for _, flow := range first.Flows {
					for _, field := range []string{"label", "description"} {
						client.flows[flow.FlowID][field] = "Operator " + flow.Role
					}
				}
				if mode != "collected-only" {
					config.FlowMetadata = tams.Flow{"label": "New root", "description": "New root"}
				}
				if mode != "root-only" {
					config.CollectedFlowMetadata = map[string]tams.Flow{"audio": {"label": "New audio", "description": "New audio"}}
				}
				second := run()
				if second.RootFlowID != first.RootFlowID {
					t.Fatal("unexpected identity change")
				}
				for _, flow := range second.Flows {
					want := "Operator " + flow.Role
					if mode != "collected-only" && (flow.FlowID == second.RootFlowID || storage == media.EssenceStorageIndependent) {
						want = "New root"
					}
					if mode != "root-only" && flow.Role == "audio" {
						want = "New audio"
					}
					for _, field := range []string{"label", "description"} {
						if got := client.flows[flow.FlowID][field]; got != want {
							t.Errorf("%s %s = %v, want %s", flow.Role, field, got, want)
						}
					}
				}
			})
		}
	}
}

func TestTransientResumeReadFailureKeepsStoredMedia(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	run := func() Result {
		p, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := p.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		assertBatchResultSchema(t, b)
		return b.Results[0]
	}
	first := run()
	if first.Status != ResultStatusIngested || first.Verification != VerificationVerified {
		t.Fatalf("setup failed: %#v", first)
	}
	for _, segment := range client.segments[first.RootFlowID] {
		for _, url := range segment.GetURLs {
			client.downloadErrors[url.URL] = context.DeadlineExceeded
		}
	}
	second := run()
	if second.Status != ResultStatusFailed || second.Verification != VerificationNotReached {
		t.Fatalf("unreadable resume = %s/%s", second.Status, second.Verification)
	}
	if object := second.rootFlow().Objects[0]; object.Disposition != ObjectDispositionResumed || object.Verification != ObjectVerificationNotReached {
		t.Fatalf("unreadable Object = %#v", object)
	}
	if len(client.segments[first.RootFlowID]) != 1 {
		t.Fatalf("temporary read failure deleted previously verified Segment; result=%s/%s", second.Status, second.Verification)
	}
}

func TestResumeChecksExistingTiming(t *testing.T) {
	for _, mode := range []VerificationMode{VerificationNone, VerificationReadback} {
		for _, mismatch := range []string{"offset", "object-range", "equivalent"} {
			t.Run(string(mode)+"/"+mismatch, func(t *testing.T) {
				filename := filepath.Join(t.TempDir(), "fixture.mp4")
				if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
					t.Fatal(err)
				}
				client := newFakeClient()
				run := func() Result {
					p, err := New(Config{Concurrency: 1, VerificationMode: mode}, client, fakeProber{}, nil, discardLogger(), nil)
					if err != nil {
						t.Fatal(err)
					}
					batch, err := p.Run(t.Context(), []source.Item{localSource(filename)})
					if err != nil {
						t.Fatal(err)
					}
					assertBatchResultSchema(t, batch)
					return batch.Results[0]
				}
				first := run()
				if first.Status != ResultStatusIngested {
					t.Fatalf("setup: %+v", first)
				}
				for id, segment := range client.segments[first.RootFlowID] {
					switch mismatch {
					case "offset":
						segment.TSOffset = "1:0"
						segment.ObjectTimerange = "[-1:0_1:0)"
					case "object-range":
						segment.ObjectTimerange = "[-1:0_1:0)"
					case "equivalent":
						segment.TSOffset = "-0:0"
						segment.ObjectTimerange = "0:0_1:0"
					}
					client.segments[first.RootFlowID][id] = segment
				}
				second := run()
				if mismatch == "equivalent" {
					if second.Status != ResultStatusResumed {
						t.Fatalf("equivalent timing refused: %+v", second)
					}
				} else if second.Status != ResultStatusFailed || second.Failure == nil || second.Failure.Code != FailureCodeSegmentConflict {
					t.Fatalf("incompatible timing accepted: %+v", second)
				}
				if len(client.segments[first.RootFlowID]) != 1 || client.uploads != 1 || len(client.deletedTimeranges) != 0 {
					t.Fatal("existing Segment was changed")
				}
			})
		}
	}
}

func TestResumeVerificationPreservesUnreadableSegments(t *testing.T) {
	for _, failure := range []string{"cancelled", "listing"} {
		t.Run(failure, func(t *testing.T) {
			client := newFakeClient()
			p, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			objects := []preparedObject{{id: "first", timerange: "[0:0_1:0)"}, {id: "second", timerange: "[1:0_2:0)"}}
			results := []ObjectResult{{ObjectID: "first", Disposition: ObjectDispositionResumed}, {ObjectID: "second", Disposition: ObjectDispositionResumed}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			} else {
				client.listSegmentsErr = context.DeadlineExceeded
			}
			err = p.verifyAll(ctx, "flow", objects, results, false)
			if err == nil || p.verificationFailureStatus(err) != VerificationNotReached {
				t.Fatalf("unreadable verification: %v", err)
			}
			if len(client.deletedTimeranges) != 0 {
				t.Fatal("unreadable existing Segments were retracted")
			}
			for _, object := range results {
				if object.Disposition != ObjectDispositionResumed || object.Verification != ObjectVerificationNotReached {
					t.Fatalf("result: %#v", object)
				}
			}
		})
	}
}
