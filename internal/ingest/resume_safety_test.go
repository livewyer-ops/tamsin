package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
			results := []ObjectResult{{ObjectID: "first", Timerange: objects[0].timerange, Disposition: ObjectDispositionResumed}, {ObjectID: "second", Timerange: objects[1].timerange, Disposition: ObjectDispositionResumed}}
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

// TestUnusableResumeListingWritesNothing covers the listing that decides
// what already exists: when it cannot be read, nothing is uploaded, registered
// or retracted on its strength.
func TestUnusableResumeListingWritesNothing(t *testing.T) {
	path := writeFixture(t, "clip.mp4", "media")
	client := newFakeClient()
	client.listSegmentsErr = errors.New("listing returned a response that is not a JSON array")
	p, err := New(Config{Concurrency: 1}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := p.Run(t.Context(), []source.Item{localSource(path)})
	if err != nil {
		t.Fatal(err)
	}
	result := batch.Results[0]
	if result.Status != ResultStatusFailed || result.Failure == nil {
		t.Fatalf("result = %+v, want a failed input", result)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.uploads != 0 || len(client.segments[result.RootFlowID]) != 0 || len(client.deletedTimeranges) != 0 {
		t.Fatalf("uploads = %d, segments = %d, retractions = %d; an unreadable listing authorised a write",
			client.uploads, len(client.segments[result.RootFlowID]), len(client.deletedTimeranges))
	}
}

func TestExplicitFlowRejectsChangedStagedContentBeforeWrites(t *testing.T) {
	path := writeFixture(t, "clip.mp4", "media one")
	client := newFakeClient()
	run := func() Result {
		p, err := New(Config{FlowID: "69aa64e4-0d55-4690-a7a7-07050d4ca493", SourceID: "046f3837-aaab-4335-83c7-a41f15ce522d"}, client, fakeProber{}, nil, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := p.Run(t.Context(), []source.Item{localSource(path)})
		if err != nil {
			t.Fatal(err)
		}
		return b.Results[0]
	}
	first := run()
	if first.Status != ResultStatusIngested {
		t.Fatal(first.Error)
	}
	before := client.flows[first.RootFlowID]["tags"].(map[string]any)[media.TagPrefix+"sha256"]
	if err := os.WriteFile(path, []byte("media two"), 0600); err != nil {
		t.Fatal(err)
	}
	second := run()
	if second.Status != ResultStatusFailed || client.uploads != 1 || client.flows[first.RootFlowID]["tags"].(map[string]any)[media.TagPrefix+"sha256"] != before {
		t.Fatalf("changed source was adopted: %+v", second)
	}
}

func TestReadOnlyResumePreservesReplicationStatus(t *testing.T) {
	path := writeFixture(t, "clip.mp4", "media")
	client := newFakeClient()
	client.serviceDocument = map[string]any{"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"}
	run := func() Result {
		p, err := New(Config{}, client, fakeProber{}, nil, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := p.Run(t.Context(), []source.Item{localSource(path)})
		if err != nil {
			t.Fatal(err)
		}
		return b.Results[0]
	}
	first := run()
	if first.Status != ResultStatusIngested {
		t.Fatal(first.Error)
	}
	client.flows[first.RootFlowID]["status"] = "replication_in_progress"
	client.flowStatusWrites = nil
	second := run()
	if second.Status != ResultStatusResumed || client.flows[first.RootFlowID]["status"] != "replication_in_progress" || len(client.flowStatusWrites) > 0 {
		t.Fatalf("read-only resume changed replication: %v %v", second.Status, client.flowStatusWrites)
	}
}

func TestSharedObjectDispositionsFollowItsTimerange(t *testing.T) {
	_, objects, results := registrationFixture(t, newFakeClient(), 2, 1, false)
	renameObjectResult(results, objects[1].key(), objects[0].id)
	objects[1].id = objects[0].id
	for _, object := range objects {
		setObjectDisposition(results, object.key(), ObjectDispositionResumed)
		setObjectVerification(results, object.key(), ObjectVerificationVerified, VerificationMethodReadback)
	}
	for _, item := range results {
		if item.Disposition != ObjectDispositionResumed || item.Verification != ObjectVerificationVerified {
			t.Fatalf("shared Object result: %+v", item)
		}
	}
}

// TestRetryAfterFailedRenderMatchesAnUninterruptedRun stops a render part of
// the way through. FFmpeg closes its outputs when it fails, so its manifest
// then lists a last Segment that ends short of the next cut. That Segment
// must not be kept: a retry writes the whole Segment at the same place, which
// a store refuses as an overlap. The retry reuses the Segments that were kept
// and ends with the Flows, Segments and Objects of a run that never stopped.
func TestRetryAfterFailedRenderMatchesAnUninterruptedRun(t *testing.T) {
	requireEncoders(t, "aac", "libx264")
	input := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=96x64:rate=25:duration=14",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=14",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:scenecut=0:bframes=2:open-gop=0", "-c:a", "aac")
	// The tool lets FFmpeg render 7.25 s and close its outputs, then reports a failure.
	tool := filepath.Join(t.TempDir(), "ffmpeg")
	script := "#!/bin/sh\ncase \" $* \" in\n*' -segment_list '*) ffmpeg -t 7.25 \"$@\" || exit $?; exit 23 ;;\n*) exec ffmpeg \"$@\" ;;\nesac\n"
	if err := os.WriteFile(tool, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		t.Run(string(storage), func(t *testing.T) {
			run := func(client *fakeClient, executable string) BatchResult {
				t.Helper()
				pipeline, err := New(Config{InputMode: InputStage, SegmentDuration: 2 * time.Second, SegmentFormat: media.SegmentFormatSource,
					EssenceStorage: storage, VerificationMode: VerificationReadback, TempDirectory: t.TempDir()},
					client, media.FFprobe{}, media.FFmpeg{Executable: executable}, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := pipeline.Run(t.Context(), []source.Item{localSource(input)})
				if err != nil {
					t.Fatal(err)
				}
				return batch
			}
			reference, client := newFakeClient(), newFakeClient()
			if batch := run(reference, ""); batch.Succeeded != 1 {
				t.Fatalf("uninterrupted run failed: %s", batch.Results[0].Error)
			}
			if batch := run(client, tool); batch.Failed != 1 {
				t.Fatalf("run with a failing render = %+v, want it failed", batch.Results[0])
			}
			kept := 0
			for flow, segments := range client.segments {
				for timerange, segment := range segments {
					if want, written := reference.segments[flow][timerange]; !written || want.ObjectID != segment.ObjectID {
						t.Errorf("the failed run kept Segment %s of Flow %s as Object %s, which an uninterrupted run does not write", timerange, flow, segment.ObjectID)
					}
					kept++
				}
			}
			if uploads := client.uploads; kept == 0 || uploads != kept {
				t.Fatalf("the failed run kept %d Segments from %d uploads, want a prefix and nothing else uploaded", kept, uploads)
			}
			if batch := run(client, ""); batch.Succeeded != 1 {
				t.Fatalf("retry failed: %+v: %s", batch.Results[0].Failure, batch.Results[0].Error)
			}
			if client.uploads != reference.uploads || !reflect.DeepEqual(client.segments, reference.segments) || !reflect.DeepEqual(client.flows, reference.flows) {
				t.Fatalf("after the retry the store holds Segments %v from %d uploads, want those of an uninterrupted run, %v from %d, and the same Flows",
					client.segments, client.uploads, reference.segments, reference.uploads)
			}
		})
	}
}
