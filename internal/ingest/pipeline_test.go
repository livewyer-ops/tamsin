package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// TestPipelineRetractsSegmentWhenVerificationFails covers the ordering TAMS
// forces on us: GET /objects/{objectId} MUST answer 404 until the Object is
// registered against a Flow Segment, so uploaded bytes cannot be read back
// before registration. Verification therefore runs afterwards, and a mismatch
// has to be retracted or the Flow keeps a Segment pointing at bad bytes.
func TestPipelineRetractsSegmentWhenVerificationFails(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media-object"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.corruptOnUpload = true
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Run records per-input outcomes in the batch; the CLI turns a non-zero
	// Failed count into its exit status.
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if batch.Succeeded != 0 || batch.Failed != 1 {
		t.Fatalf("corrupted upload should fail the ingest: %#v", batch)
	}
	if !strings.Contains(batch.Results[0].Error, "SHA-256 mismatch") {
		t.Fatalf("error should name the integrity failure, got %q", batch.Results[0].Error)
	}
	if !strings.Contains(batch.Results[0].Error, "retraction") {
		t.Fatalf("error should report the retraction, got %q", batch.Results[0].Error)
	}
	if batch.Results[0].Verification != VerificationFailedRetracted {
		t.Fatalf("verification = %q, want %q", batch.Results[0].Verification, VerificationFailedRetracted)
	}
	assertBatchResultSchema(t, batch)

	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.deletedTimeranges) != 1 {
		t.Fatalf("expected exactly one retraction, got %#v", client.deletedTimeranges)
	}
	for flowID, segments := range client.segments {
		if len(segments) != 0 {
			t.Fatalf("flow %s still references %d unverified segment(s)", flowID, len(segments))
		}
	}
}

func TestPipelineDoesNotRegisterBytesThatChangedBeforeUpload(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media-object"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newFakeClient()
	client := &changedUploadClient{fakeClient: store}
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationNone}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || batch.Results[0].Failure == nil || batch.Results[0].Failure.Code != FailureCodeSourceChanged {
		t.Fatalf("changed upload result = %#v", batch)
	}
	store.lock.Lock()
	defer store.lock.Unlock()
	for flowID, segments := range store.segments {
		if len(segments) != 0 {
			t.Fatalf("flow %s registered changed bytes: %#v", flowID, segments)
		}
	}
}

// TestPipelineRetractionDoesNotDeleteAnOverlappingSegment protects the
// multi-writer case. Timerange-only cleanup can delete a Segment another
// producer registered at the same point on the Flow; integrity cleanup must
// identify the Object it uploaded as well as its timerange.
func TestPipelineRetractionDoesNotDeleteAnOverlappingSegment(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media-object"), 0o600); err != nil {
		t.Fatal(err)
	}
	const (
		flowID           = "00000000-0000-4000-8000-000000000001"
		collateralObject = "00000000-0000-4000-8000-000000000002"
		timerange        = "[0:0_1:0)"
	)
	client := newFakeClient()
	client.segments[flowID] = map[string]tams.Segment{
		collateralObject: {ObjectID: collateralObject, Timerange: timerange},
	}
	client.objects[collateralObject] = []byte("somebody else's media")
	client.corruptOnUpload = true
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback, FlowID: flowID}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 {
		t.Fatalf("corrupted upload should fail the ingest: %#v", batch)
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	segments := client.segments[flowID]
	if len(segments) != 1 || segments[collateralObject].ObjectID != collateralObject {
		t.Fatalf("overlapping third-party Segment was changed by retraction: %#v", segments)
	}
	if _, exists := client.objects[collateralObject]; !exists {
		t.Fatal("overlapping third-party Media Object was deleted by retraction")
	}
}

// TestPipelineReportsFailedRetraction keeps the integrity failure visible when
// the retraction itself cannot be completed, since that is the case an operator
// must clean up by hand.
func TestPipelineReportsFailedRetraction(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media-object"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.corruptOnUpload = true
	client.deleteSegmentsErr = errors.New("flow is read-only")
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if batch.Failed != 1 {
		t.Fatalf("corrupted upload should fail the ingest: %#v", batch)
	}
	message := batch.Results[0].Error
	if !strings.Contains(message, "SHA-256 mismatch") {
		t.Fatalf("integrity failure should survive a failed retraction, got %q", message)
	}
	if !strings.Contains(message, "could not be retracted") {
		t.Fatalf("failed retraction should be reported, got %q", message)
	}
	if batch.Results[0].Verification != VerificationFailedStranded {
		t.Fatalf("verification = %q, want %q", batch.Results[0].Verification, VerificationFailedStranded)
	}
	assertBatchResultSchema(t, batch)
}

// TestPipelineRegistersCollectedFlowsBeforeCollector covers the TAMS rule that
// a Collection Item may only reference a Flow already registered in the
// service. The mono-essence Flows must therefore be created before the
// multi-essence Flow whose flow_collection names them.
func TestPipelineRegistersCollectedFlowsBeforeCollector(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, muxedProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 {
		t.Fatalf("muxed ingest should succeed: %#v", batch)
	}
	if batch.Results[0].Verification != VerificationVerified {
		t.Fatalf("verification = %q, want %q", batch.Results[0].Verification, VerificationVerified)
	}
	if len(batch.Results[0].Flows) != 3 {
		t.Fatalf("muxed result must list two collected Flows and its root once: %#v", batch.Results[0].Flows)
	}
	assertBatchResultSchema(t, batch)
	rootResult := batch.Results[0].rootFlow()
	if rootResult == nil || len(rootResult.Objects) == 0 {
		t.Fatalf("root Flow should own the multiplex Objects: %#v", batch.Results[0])
	}
	seen := make(map[string]bool, len(batch.Results[0].Flows))
	for _, flow := range batch.Results[0].Flows {
		if flow.Disposition != FlowWritten {
			t.Fatalf("new Flow %s disposition = %q, want written", flow.FlowID, flow.Disposition)
		}
		if seen[flow.FlowID] {
			t.Fatalf("Flow %s was reported more than once", flow.FlowID)
		}
		seen[flow.FlowID] = true
		if flow.FlowID != batch.Results[0].RootFlowID && (flow.Role == "" || len(flow.Objects) != 0) {
			t.Fatalf("collected Flow must have a role and no Objects: %#v", flow)
		}
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	collector := client.flows[batch.Results[0].RootFlowID]
	items, ok := collector["flow_collection"].([]map[string]any)
	if !ok || len(items) != 2 {
		t.Fatalf("multi-essence Flow should collect two Flows, got %#v", collector["flow_collection"])
	}
	for _, item := range items {
		id, _ := item["id"].(string)
		position, registered := client.flowOrder[id]
		if !registered {
			t.Fatalf("collected Flow %s was never registered", id)
		}
		if position > client.flowOrder[batch.Results[0].RootFlowID] {
			t.Fatalf("collected Flow %s was registered after the Flow collecting it", id)
		}
		if _, present := client.flows[id]["container"]; present {
			t.Fatalf("collected Flow %s must not set container", id)
		}
		if _, present := client.flows[id]["container_mapping"]; present {
			t.Fatalf("collected Flow %s carries its parent Collection Item's mapping", id)
		}
		if _, present := item["container_mapping"]; !present {
			t.Fatalf("Collection Item for %s has no container_mapping", id)
		}
	}
	// Only the multi-essence Flow owns Media Objects.
	if len(client.segments[batch.Results[0].RootFlowID]) == 0 {
		t.Fatal("multi-essence Flow should own the Flow Segments")
	}
	for _, item := range items {
		id, _ := item["id"].(string)
		if len(client.segments[id]) != 0 {
			t.Fatalf("collected Flow %s must not own Flow Segments", id)
		}
	}
	firstAllocation := -1
	for index, call := range client.callLog {
		if strings.HasPrefix(call, "allocate:") {
			firstAllocation = index
			break
		}
	}
	if firstAllocation < 0 {
		t.Fatal("muxed ingest made no storage allocation")
	}
	for id := range client.flows {
		position := -1
		for index, call := range client.callLog {
			if call == "flowWrite:"+id {
				position = index
				break
			}
		}
		if position < 0 || position > firstAllocation {
			t.Fatalf("Flow %s was not written as part of the empty graph before allocation: %v", id, client.callLog)
		}
	}
}

// TestCollectionItemsCarryExactMultiAudioTrackIndices covers the placement
// AppNote 0006 specifies. The mapping belongs to each parent Collection Item;
// track_index follows the container, while format_track_index restarts for each
// essence format and increments across multiple audio tracks.
func TestCollectionItemsCarryExactMultiAudioTrackIndices(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("video plus two audio tracks"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{Concurrency: 1}, client, multiAudioProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil || batch.Failed != 0 {
		t.Fatalf("ingest: batch=%#v error=%v", batch, err)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	items := client.flows[batch.Results[0].RootFlowID]["flow_collection"].([]map[string]any)
	want := []struct {
		role        string
		track       int
		formatTrack int
	}{{"video", 0, 0}, {"audio 0", 1, 0}, {"audio 1", 2, 1}}
	if len(items) != len(want) {
		t.Fatalf("collection has %d items, want %d: %#v", len(items), len(want), items)
	}
	for index, expected := range want {
		mapping, ok := items[index]["container_mapping"].(map[string]any)
		if !ok {
			t.Fatalf("/flow_collection/%d/container_mapping is missing: %#v", index, items[index])
		}
		if items[index]["role"] != expected.role || mapping["track_index"] != expected.track ||
			mapping["format_track_index"] != expected.formatTrack {
			t.Fatalf("/flow_collection/%d = %#v, want role=%q track=%d format_track=%d",
				index, items[index], expected.role, expected.track, expected.formatTrack)
		}
	}
}

// TestPartialFlowGraphMutationIsExplicit covers the only non-atomic boundary:
// TAMS offers individual Flow PUTs but no graph transaction. If a later PUT
// fails, the operator is told exactly that metadata may be partial and that no
// Media Objects were allocated behind that incomplete graph.
func TestPartialFlowGraphMutationIsExplicit(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.putFlowErrAt = 2
	pipeline, err := New(Config{Concurrency: 1}, client, muxedProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	message := batch.Results[0].Error
	if batch.Failed != 1 || !strings.Contains(message, "flow graph may be partially written") ||
		!strings.Contains(message, "no Media Objects were allocated") {
		t.Fatalf("partial graph failure was not actionable: %#v", batch)
	}
	if len(batch.Results[0].Flows) != 3 ||
		batch.Results[0].Flows[0].Disposition != FlowWritten ||
		batch.Results[0].Flows[1].Disposition != FlowIndeterminate ||
		batch.Results[0].Flows[2].Disposition != FlowUnattempted {
		t.Fatalf("partial graph dispositions are ambiguous: %#v", batch.Results[0].Flows)
	}
	assertBatchResultSchema(t, batch)
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.flows) != 1 || client.allocations != 0 || client.uploads != 0 || len(client.segments) != 0 {
		t.Fatalf("partial graph boundary = flows:%d allocations:%d uploads:%d segments:%d",
			len(client.flows), client.allocations, client.uploads, len(client.segments))
	}
}

// TestPipelineDoesNotInventGeneration keeps this editorial lineage field
// operator-owned. Stream copy does not prove that the input is camera-original
// generation zero; it only proves this ingest did not add another generation.
func TestPipelineDoesNotInventGeneration(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		ffmpegArgs []string
	}{
		{name: "stream-copy"},
		{name: "explicit-ffmpeg-profile", ffmpegArgs: []string{"-c:v", "libx264"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			config := Config{Concurrency: 1, VerificationMode: VerificationReadback, FFmpegArgs: testCase.ffmpegArgs}
			var segmenter media.Segmenter
			if len(testCase.ffmpegArgs) > 0 {
				config.SegmentDuration = time.Second
				config.FlowMetadata = tams.Flow{
					"codec": "video/h264",
					"essence_parameters": map[string]any{
						"frame_width": 64, "frame_height": 64,
						"frame_rate": map[string]any{"numerator": 25, "denominator": 1},
					},
				}
				segmenter = fakeSegmenter{}
			}
			pipeline, err := New(config, client, fakeProber{}, segmenter, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			if batch.Succeeded != 1 {
				t.Fatalf("ingest should succeed: %#v", batch)
			}
			client.lock.Lock()
			defer client.lock.Unlock()
			generation, present := client.flows[batch.Results[0].RootFlowID]["generation"]
			if present {
				t.Fatalf("ingest must not assert editorial generation, got %v", generation)
			}
		})
	}
}

func TestCustomTreatmentRequiresTruthfulOutputMetadata(t *testing.T) {
	t.Parallel()
	single := media.Probe{Streams: []media.Stream{{CodecType: "video", CodecName: "h264"}}}
	multi := media.Probe{Streams: []media.Stream{
		{CodecType: "video", CodecName: "h264"},
		{CodecType: "audio", CodecName: "aac"},
	}}
	valid := tams.Flow{
		"codec":              "video/h264",
		"essence_parameters": map[string]any{"frame_width": 1920, "frame_height": 1080},
	}
	for _, testCase := range []struct {
		name     string
		probe    media.Probe
		metadata tams.Flow
		wantErr  string
	}{
		{name: "single essence explicit output", probe: single, metadata: valid},
		{name: "missing output metadata", probe: single, wantErr: "output codec"},
		{name: "codec alone is incomplete", probe: single, metadata: tams.Flow{"codec": "video/h264"}, wantErr: "essence_parameters"},
		{name: "multi essence cannot share an override", probe: multi, metadata: valid, wantErr: "exactly one essence"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := validateCustomOutputMetadata(testCase.probe, []string{"-c:v", "libx264"}, testCase.metadata)
			if testCase.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("validation error = %v, want %q", err, testCase.wantErr)
			}
		})
	}
}

func TestInvalidCustomTreatmentStopsBeforeRendererOrTAMSMutation(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	segmenter := &recordingSegmenter{}
	pipeline, err := New(Config{
		Concurrency: 1, SegmentDuration: time.Second,
		FFmpegArgs: []string{"-c:v", "libx264"},
		FlowMetadata: tams.Flow{
			"codec": "video/h264", "essence_parameters": map[string]any{"frame_width": 64, "frame_height": 64},
		},
	}, client, muxedProber{}, segmenter, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || !strings.Contains(batch.Results[0].Error, "exactly one essence") {
		t.Fatalf("custom treatment result = %#v", batch)
	}
	segmenter.lock.Lock()
	renders := len(segmenter.requests)
	segmenter.lock.Unlock()
	client.lock.Lock()
	flows, allocations, uploads, segments := len(client.flows), client.allocations, client.uploads, len(client.segments)
	client.lock.Unlock()
	if renders != 0 || flows != 0 || allocations != 0 || uploads != 0 || segments != 0 {
		t.Fatalf("invalid custom treatment crossed mutation boundary: renders=%d flows=%d allocations=%d uploads=%d segments=%d",
			renders, flows, allocations, uploads, segments)
	}
}

func TestExplicitEssenceParametersSkipUnusedPresentationDecode(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		metadata tams.Flow
		want     int64
	}{
		{name: "automatic metadata scans cadence", want: 1},
		{
			name: "complete override replaces cadence evidence",
			metadata: tams.Flow{"essence_parameters": map[string]any{
				"frame_width": 64, "frame_height": 64,
				"frame_rate": map[string]any{"numerator": 25, "denominator": 1},
			}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			prober := &presentationCountingProber{}
			pipeline, err := New(Config{Concurrency: 1, FlowMetadata: testCase.metadata},
				newFakeClient(), prober, nil, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pipeline.probeInput(context.Background(), "input"); err != nil {
				t.Fatal(err)
			}
			if got := prober.presentations.Load(); got != testCase.want {
				t.Fatalf("presentation scans = %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestPipelineContainerFollowsSegmentFormat guards the one thing that makes an
// explicit format safe. Flow metadata is built from the source probe before any
// segmentation runs, so without this the Flow would declare the input's
// container while the Segments were written in another, and the metadata would
// simply be wrong.
func TestPipelineContainerFollowsSegmentFormat(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name          string
		format        media.SegmentFormat
		wantContainer string
	}{
		{name: "source-keeps-probed-container", format: media.SegmentFormatSource, wantContainer: "video/mp4"},
		{name: "mpegts-declares-what-was-written", format: media.SegmentFormatMPEGTS, wantContainer: "video/mp2t"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			pipeline, err := New(Config{
				Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second, SegmentFormat: testCase.format,
			}, client, fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			if batch.Succeeded != 1 {
				t.Fatalf("ingest should succeed: %#v", batch)
			}
			client.lock.Lock()
			defer client.lock.Unlock()
			container := client.flows[batch.Results[0].RootFlowID]["container"]
			if container != testCase.wantContainer {
				t.Fatalf("container = %v, want %s", container, testCase.wantContainer)
			}
		})
	}
}

func TestFlowMetadataRejectsIdentityAndOwnershipPathsAsUsage(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		path   string
		value  any
		action string
	}{
		{path: "id", value: "3f79b0f7-43d2-47ac-b75e-785f4ca25b96", action: "--flow-id"},
		{path: "source_id", value: "f9a291db-c8d1-48df-ac12-51078c29445f", action: "--source-id"},
		{path: "format", value: "urn:x-nmos:format:audio", action: "derived for each essence"},
		{path: "flow_collection", value: []any{}, action: "resolved Flow graph"},
		{path: "container_mapping", value: map[string]any{"track_index": 9}, action: "input track map"},
	} {
		t.Run(testCase.path, func(t *testing.T) {
			_, err := New(Config{FlowMetadata: tams.Flow{testCase.path: testCase.value}, DryRunMode: DryRunExact},
				nil, fakeProber{}, nil, discardLogger(), nil)
			if err == nil || !strings.Contains(err.Error(), "/"+testCase.path) || !strings.Contains(err.Error(), testCase.action) {
				t.Fatalf("New() error = %v, want JSON path /%s and action %q", err, testCase.path, testCase.action)
			}
		})
	}
}

// TestInvalidFinalFlowMetadataCausesZeroMutation validates the value after all
// generated fields and operator overrides have been composed. Structural
// errors must be found before a Flow PUT or Object allocation.
func TestInvalidFinalFlowMetadataCausesZeroMutation(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1, FlowMetadata: tams.Flow{"generation": "not-an-integer"},
	}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || !strings.Contains(batch.Results[0].Error, "/generation") {
		t.Fatalf("invalid final metadata did not report /generation: %#v", batch)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.flows) != 0 || client.allocations != 0 || client.uploads != 0 || len(client.segments) != 0 {
		t.Fatalf("invalid metadata mutated TAMS: flows=%d allocations=%d uploads=%d segments=%d",
			len(client.flows), client.allocations, client.uploads, len(client.segments))
	}
}

// TestUnsupportedContainerIsHonestAndActionable covers the product boundary:
// opening a file is FFmpeg's job, but Tamsin only promises MIME mappings for a
// supported profile. An unknown format must not inherit a lying suffix or be
// guessed into a neighbouring family, and the operator must be told how to
// supply domain knowledge that Tamsin does not have.
func TestUnsupportedContainerIsHonestAndActionable(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name          string
		metadata      tams.Flow
		wantContainer string
		wantWarning   bool
	}{
		{name: "fallback warns", wantContainer: "application/octet-stream", wantWarning: true},
		{name: "operator override", metadata: tams.Flow{"container": "application/vnd.example.media"}, wantContainer: "application/vnd.example.media"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "misleading.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			var logged bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
			pipeline, err := New(Config{
				Concurrency: 1, EssenceStorage: media.EssenceStorageMuxed,
				FlowMetadata: testCase.metadata,
			}, client, unknownContainerProber{}, nil, logger, nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			flow := client.flows[batch.Results[0].RootFlowID]
			if flow["container"] != testCase.wantContainer {
				t.Fatalf("container = %v, want %s", flow["container"], testCase.wantContainer)
			}
			warned := strings.Contains(logged.String(), "outside Tamsin's supported profile")
			if warned != testCase.wantWarning {
				t.Fatalf("warning present = %t, want %t; log: %s", warned, testCase.wantWarning, logged.String())
			}
			if testCase.wantWarning && !strings.Contains(logged.String(), "--flow-metadata") {
				t.Fatalf("warning gives no override action: %s", logged.String())
			}
		})
	}
}

func TestUnsupportedCodecIsHonestAndActionable(t *testing.T) {
	t.Parallel()
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pipeline := &Pipeline{logger: logger}
	pipeline.warnUnsupportedCodecs(source.Item{URI: "file:///unknown-codec.mp4"}, []media.UnsupportedCodec{{
		Name: "future_picture_codec", StreamType: "video", StreamIndex: 4,
	}})
	message := logged.String()
	for _, evidence := range []string{"outside Tamsin's supported profile", "future_picture_codec", "stream_index=4", "--flow-metadata"} {
		if !strings.Contains(message, evidence) {
			t.Fatalf("codec warning %q lacks %q", message, evidence)
		}
	}
}

// TestPipelineRejectsUnknownSegmentFormat fails before any media is staged,
// rather than surfacing as an opaque FFmpeg error part-way through an ingest.
func TestPipelineRejectsUnknownSegmentFormat(t *testing.T) {
	t.Parallel()
	_, err := New(Config{SegmentFormat: "quicktime"}, newFakeClient(), fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported segment format") {
		t.Fatalf("New() error = %v, want unsupported segment format", err)
	}
}

// TestPipelineStoresEssencesIndependently covers the arrangement AppNote 0001
// leads with: each essence is demultiplexed into its own Media Objects and
// ingested as a Flow in its own right, so a consumer can fetch one without the
// others.
//
// AppNote 0001 has such an ingest create three Flows for a two-stream input:
// one per essence, and a Multi-Flow collecting them so the store records that
// they came from one input. The collector owns no Media Objects and declares no
// container, because its media is reached through the essences it collects.
func TestPipelineStoresEssencesIndependently(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageIndependent,
	}, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Failed != 0 {
		t.Fatalf("independent ingest should succeed: %#v", batch)
	}

	// One result per input, with the Flows nested rather than flattened: two
	// essences and the Multi-Flow that collects them.
	result := batch.Results[0]
	assertBatchResultSchema(t, batch)
	if len(result.Flows) != 3 {
		t.Fatalf("expected one Flow per essence plus a collector, got %#v", result.Flows)
	}
	// The collector is the input's Flow, so it is what the result names.
	root := result.rootFlow()
	if result.RootFlowID == "" || root == nil || root.SourceID == "" {
		t.Fatalf("the collector should identify the input: %#v", result)
	}
	if len(root.Objects) != 0 {
		t.Fatalf("the collector must own no Objects: %#v", result)
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	sources := make(map[string]bool, len(result.Flows))
	for _, flowResult := range result.Flows {
		if flowResult.Disposition != FlowWritten {
			t.Fatalf("new Flow %s disposition = %q, want written", flowResult.FlowID, flowResult.Disposition)
		}
		if flowResult.FlowID == result.RootFlowID {
			// Checked below as a collection rather than as an essence.
			if flowResult.Role != "" {
				t.Fatalf("the root Flow must not invent a collection-item role: %#v", flowResult)
			}
			continue
		}
		if flowResult.Role == "" {
			t.Fatalf("essence Flow %s has no role", flowResult.FlowID)
		}
		// Each essence owns Media Objects of its own; that is the whole point.
		if len(client.segments[flowResult.FlowID]) == 0 {
			t.Fatalf("essence Flow %s (%s) owns no Segments", flowResult.FlowID, flowResult.Role)
		}
		if flowResult.ObjectSummary.Total == 0 {
			t.Fatalf("essence Flow %s reported no Objects", flowResult.FlowID)
		}
		stored := client.flows[flowResult.FlowID]
		if _, present := stored["flow_collection"]; present {
			t.Fatalf("independent storage creates no collection, but %s has one", flowResult.FlowID)
		}
		if _, present := stored["container_mapping"]; present {
			t.Fatalf("independently stored Flow %s must not carry container_mapping", flowResult.FlowID)
		}
		if _, present := stored["container"]; !present {
			t.Fatalf("independently stored Flow %s must declare a container", flowResult.FlowID)
		}
		// Distinct essences are not editorially equivalent, so they do not share a Source.
		if sources[flowResult.SourceID] {
			t.Fatalf("essences must not share a Source: %s repeated", flowResult.SourceID)
		}
		sources[flowResult.SourceID] = true
	}
	// Exactly one Multi-Flow, collecting every essence and owning nothing.
	collectors := make(map[string]tams.Flow)
	for id, flow := range client.flows {
		if flow["format"] == "urn:x-nmos:format:multi" {
			collectors[id] = flow
		}
	}
	if len(collectors) != 1 {
		t.Fatalf("expected one multi-essence Flow recording the association, found %d", len(collectors))
	}
	for id, collector := range collectors {
		if _, present := collector["container"]; present {
			t.Fatalf("collector %s declares a container; it owns no Media Objects", id)
		}
		items, ok := collector["flow_collection"].([]map[string]any)
		if !ok || len(items) != len(batch.Results[0].Flows)-1 {
			t.Fatalf("collector %s does not collect every essence: %#v", id, collector["flow_collection"])
		}
		collected := make(map[string]bool, len(items))
		for _, entry := range items {
			identifier, _ := entry["id"].(string)
			if role, _ := entry["role"].(string); role == "" || identifier == "" {
				t.Fatalf("collection item needs both id and role, got %#v", entry)
			}
			collected[identifier] = true
			// A Collection Item may only reference a Flow the service holds, so
			// the essences must have been registered before the collector.
			if client.flowOrder[identifier] > client.flowOrder[id] {
				t.Fatalf("collector %s was registered before the essence %s it references", id, identifier)
			}
		}
		for _, flowResult := range batch.Results[0].Flows {
			if flowResult.FlowID == batch.Results[0].RootFlowID {
				continue
			}
			if !collected[flowResult.FlowID] {
				t.Fatalf("essence %s (%s) is not in the collection", flowResult.FlowID, flowResult.Role)
			}
		}
	}
}

// TestTutorialIndependentDryRunShape is the executable contract behind the
// first-ingest tutorial. It deliberately uses fakes at the media boundary: the
// product promise is the planned Flow graph, and proving it must not depend on
// a host FFmpeg build or a checked-in binary fixture.
func TestTutorialIndependentDryRunShape(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "first-ingest.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := New(Config{
		Profile: ProfileEssenceSegments, ProfileVersion: "1",
		Concurrency: 1, Transfers: 1, ProbeConcurrency: 1, DryRunMode: DryRunExact,
		SegmentDuration: 10 * time.Second, SegmentFormat: media.SegmentFormatSource,
		EssenceStorage: media.EssenceStorageIndependent,
	}, nil, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Failed != 0 || len(batch.Results) != 1 {
		t.Fatalf("tutorial dry run failed: %#v", batch)
	}
	result := batch.Results[0]
	if len(result.Flows) != 3 {
		t.Fatalf("tutorial A/V dry run planned %d Flows, want video, audio, and collector: %#v", len(result.Flows), result.Flows)
	}
	roles := make(map[string]bool)
	for _, flow := range result.Flows {
		if flow.Disposition != FlowPlanned {
			t.Fatalf("dry-run Flow %s disposition = %q, want planned", flow.FlowID, flow.Disposition)
		}
		if flow.FlowID == result.RootFlowID {
			if flow.Role != "" || len(flow.Objects) != 0 {
				t.Fatalf("tutorial root is not an empty collector: %#v", flow)
			}
			continue
		}
		roles[flow.Role] = true
	}
	if !roles["video"] || !roles["audio"] || len(roles) != 2 {
		t.Fatalf("tutorial essence roles = %v, want exactly video and audio", roles)
	}
}

func TestDryRunFastSkipsRendererWhileExactBuildsObjects(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "dry-run.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name        string
		mode        DryRunMode
		wantRenders int
		wantObjects int
	}{
		{name: "fast", mode: DryRunFast},
		{name: "exact", mode: DryRunExact, wantRenders: 2, wantObjects: 4},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			segmenter := &recordingSegmenter{}
			pipeline, err := New(Config{
				Profile: ProfileEssenceSegments, ProfileVersion: "1",
				Concurrency: 1, Transfers: 1, ProbeConcurrency: 1, DryRunMode: testCase.mode,
				SegmentDuration: 10 * time.Second, SegmentFormat: media.SegmentFormatSource,
				EssenceStorage: media.EssenceStorageIndependent,
			}, nil, muxedProber{}, segmenter, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			objects := 0
			for _, flow := range batch.Results[0].Flows {
				objects += flow.ObjectSummary.Total
			}
			segmenter.lock.Lock()
			renders := len(segmenter.requests)
			segmenter.lock.Unlock()
			if renders != testCase.wantRenders || objects != testCase.wantObjects {
				t.Fatalf("mode %s rendered %d times and planned %d Objects, want %d and %d",
					testCase.mode, renders, objects, testCase.wantRenders, testCase.wantObjects)
			}
		})
	}
}

// TestLateEssenceFailureKeepsAResumableRegisteredPrefix proves the rolling
// boundary. The complete Flow graph exists before streaming starts, so a later
// renderer failure can retain already registered video without leaving an
// unassociated owner or pretending the audio completed.
func TestLateEssenceFailureKeepsAResumableRegisteredPrefix(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1, SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageIndependent,
	}, client, muxedProber{}, &failSecondEssenceSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || !strings.Contains(batch.Results[0].Error, "audio demultiplex failed") {
		t.Fatalf("second essence failure was not reported: %#v", batch)
	}
	var video, audio FlowResult
	for _, flow := range batch.Results[0].Flows {
		switch flow.Role {
		case "video":
			video = flow
		case "audio":
			audio = flow
		}
	}
	if video.ObjectSummary.Ingested != 2 || audio.ObjectSummary.Total != 0 {
		t.Fatalf("late failure did not preserve exactly the completed video prefix: video=%#v audio=%#v", video, audio)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.flows) != 3 || client.allocations != 2 || client.uploads != 2 || len(client.segments[video.FlowID]) != 2 {
		t.Fatalf("resumable prefix = flows=%d allocations=%d uploads=%d video_segments=%d, want 3/2/2/2",
			len(client.flows), client.allocations, client.uploads, len(client.segments[video.FlowID]))
	}
}

func TestPipelineIngestsVerifiesAndResumes(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	content := []byte("media-object")
	if err := os.WriteFile(filename, content, 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{Concurrency: 2, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	batch, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Failed != 0 || batch.Results[0].Status != ResultStatusIngested {
		t.Fatalf("unexpected first result: %#v", batch)
	}
	if len(batch.Results[0].rootFlow().Objects) != 1 || batch.Results[0].rootFlow().Objects[0].Status != ObjectStatusIngested {
		t.Fatalf("unexpected Object result: %#v", batch.Results[0].rootFlow().Objects)
	}
	client.lock.Lock()
	uploads := client.uploads
	client.lock.Unlock()
	if uploads != 1 {
		t.Fatalf("uploads = %d, want 1", uploads)
	}

	pipeline, err = New(Config{Concurrency: 2, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Results[0].Status != ResultStatusResumed || resumed.Results[0].rootFlow().Objects[0].Status != ObjectStatusResumed {
		t.Fatalf("unexpected resumed result: %#v", resumed)
	}
	client.lock.Lock()
	uploads = client.uploads
	client.lock.Unlock()
	if uploads != 1 {
		t.Fatalf("resume performed another upload; uploads = %d", uploads)
	}
}
func TestPipelineAllocatesAndUploadsEverySegment(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{SegmentDuration: time.Second, VerificationMode: VerificationReadback}, client, fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Results[0].rootFlow().ObjectSummary.Total != 2 {
		t.Fatalf("unexpected segmented result: %#v", batch)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	// Every Media Object is uploaded. With one transfer worker, each URL is
	// allocated only when that worker can begin it; issuing both together would
	// leave the second URL ageing behind the first upload.
	if client.uploads != 2 {
		t.Fatalf("uploads = %d, want one per Media Object", client.uploads)
	}
	if client.allocations != 2 || client.maxAllocationObjects != 1 {
		t.Fatalf("allocations = %d covering at most %d objects; want one ready-worker allocation each",
			client.allocations, client.maxAllocationObjects)
	}
}

func TestPipelineProgressKeepsStoreAndVerificationSeparate(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	var (
		progressLock sync.Mutex
		snapshots    []progress.Snapshot
	)
	reporter := progress.Observer(func(snapshot progress.Snapshot) {
		progressLock.Lock()
		defer progressLock.Unlock()
		snapshots = append(snapshots, snapshot)
	})
	client := newFakeClient()
	pipeline, err := New(Config{SegmentDuration: time.Second, VerificationMode: VerificationReadback},
		client, fakeProber{}, fakeSegmenter{}, discardLogger(), reporter)
	if err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	batch, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("run = %#v, %v", batch, err)
	}

	progressLock.Lock()
	defer progressLock.Unlock()
	var storeFinal, verifyFinal progress.Snapshot
	for _, snapshot := range snapshots {
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("pipeline published invalid progress %+v: %v", snapshot, err)
		}
		if snapshot.Scope.InputIndex != 0 || snapshot.Scope.Input != item.URI {
			t.Fatalf("snapshot lost resolved-input scope: %+v", snapshot.Scope)
		}
		if snapshot.TotalObjects > 2 || snapshot.TotalBytes > 2 {
			t.Fatalf("pipeline doubled work across phases: %+v", snapshot)
		}
		if snapshot.TotalsFinal && snapshot.CompletedObjects == snapshot.TotalObjects &&
			snapshot.CompletedBytes == snapshot.TotalBytes {
			switch snapshot.Phase {
			case progress.PhaseStore:
				storeFinal = snapshot
			case progress.PhaseVerify:
				verifyFinal = snapshot
			}
		}
	}
	for phase, snapshot := range map[progress.Phase]progress.Snapshot{
		progress.PhaseStore: storeFinal, progress.PhaseVerify: verifyFinal,
	} {
		if snapshot.CompletedObjects != 2 || snapshot.TotalObjects != 2 ||
			snapshot.CompletedBytes != 2 || snapshot.TotalBytes != 2 {
			t.Fatalf("final %s progress = %+v, want two logical Objects and two bytes", phase, snapshot)
		}
	}
}

func TestPipelineMetricsDescribeEachInvocationAcrossResume(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	firstRun := observability.New("aee7d238-c8da-4d44-a0d1-70ece72f0999", nil)
	pipeline, err := New(Config{Observability: firstRun, SegmentDuration: time.Second, VerificationMode: VerificationReadback},
		client, fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	first, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil || first.Succeeded != 1 || first.RunID != firstRun.RunID() {
		t.Fatalf("first run = %#v, %v", first, err)
	}
	metrics := firstRun.Snapshot()
	if metrics.BytesStaged != 5 || metrics.BytesUploaded != 2 || metrics.BytesVerified != 2 || metrics.Verified != 2 {
		t.Fatalf("first metrics = %+v", metrics)
	}

	resumeRun := observability.New("bc012fa0-1c9b-4a1e-9d4a-ad872f246bd1", nil)
	resume, err := New(Config{Observability: resumeRun, SegmentDuration: time.Second, VerificationMode: VerificationReadback},
		client, fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resume.Run(context.Background(), []source.Item{item})
	if err != nil || second.Succeeded != 1 || second.Results[0].Status != ResultStatusResumed {
		t.Fatalf("resume = %#v, %v", second, err)
	}
	resumedMetrics := resumeRun.Snapshot()
	if resumedMetrics.BytesStaged != 5 || resumedMetrics.BytesUploaded != 0 ||
		resumedMetrics.BytesVerified != 2 || resumedMetrics.Verified != 2 {
		t.Fatalf("resume invocation metrics = %+v", resumedMetrics)
	}
}

func TestPipelineDryRunDoesNotRequireClient(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := New(Config{DryRunMode: DryRunExact}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.SchemaVersion != ResultSchemaVersion || batch.ToolVersion == "" || batch.ProfileVersion != "1" ||
		batch.Results[0].Status != ResultStatusPlanned || batch.Results[0].RootFlowID == "" ||
		batch.Results[0].Verification != VerificationNotRequested {
		t.Fatalf("unexpected dry-run result: %#v", batch)
	}
}
func TestPipelineRequiresDefaultOrExplicitStorage(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.backends = []tams.StorageBackend{{ID: "storage"}}
	pipeline, err := New(Config{}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)}); err == nil ||
		!strings.Contains(err.Error(), "no default storage backend") {
		t.Fatalf("Run() error = %v, want missing default storage error", err)
	}
}

func TestPipelineValidatesExplicitStorageBeforeMutation(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{StorageID: "6ba7b811-9dad-11d1-80b4-00c04fd430c8"}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)}); err == nil ||
		!strings.Contains(err.Error(), "not available") {
		t.Fatalf("Run() error = %v, want unavailable explicit storage error", err)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.flows) != 0 || len(client.segments) != 0 || len(client.objects) != 0 {
		t.Fatalf("invalid storage selection mutated TAMS: flows=%d segments=%d objects=%d",
			len(client.flows), len(client.segments), len(client.objects))
	}
}

func TestPipelinePreservesBatchOrderAndReportsPartialFailure(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	good := filepath.Join(directory, "a.mp4")
	bad := filepath.Join(directory, "b.mp4")
	if err := os.WriteFile(good, []byte("good"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := New(Config{Concurrency: 2, DryRunMode: DryRunExact}, nil, fakeProber{failSuffix: "b.mp4"}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(good), localSource(bad)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Failed != 1 {
		t.Fatalf("unexpected counts: %#v", batch)
	}
	if !strings.HasSuffix(batch.Results[0].Input, "/a.mp4") || !strings.HasSuffix(batch.Results[1].Input, "/b.mp4") || batch.Results[1].Status != ResultStatusFailed {
		t.Fatalf("batch order/status changed: %#v", batch.Results)
	}
}

func TestIndependentHighTrackInputUsesOneRenderProcess(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("four essences"), 0o600); err != nil {
		t.Fatal(err)
	}
	segmenter := &recordingSegmenter{}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationNone, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageIndependent,
	}, newFakeClient(), fourEssenceProber{}, segmenter, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("Run() = %#v, %v", batch, err)
	}
	segmenter.lock.Lock()
	defer segmenter.lock.Unlock()
	if len(segmenter.requests) != 1 || !reflect.DeepEqual(segmenter.requests[0].StreamIndices, []int{0, 1, 2, 3}) {
		t.Fatalf("render requests = %#v, want one four-output invocation", segmenter.requests)
	}
}

// TestListingsOnlyRequestDownloadURLsWhenRead pins the cost of the change that
// made resume a single whole-Flow listing.
//
// Filtering by object_id returned at most one Segment, so asking for presigned
// URLs alongside it was free. Listing the whole Flow is not: the service signs
// a URL per Segment, and the listing that decides what to resume never reads
// the media. The spec offers accept_get_urls for exactly this, noting the
// response "could be substantially faster" without them.
func TestListingsOnlyRequestDownloadURLsWhenRead(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	config := Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationNone, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	pipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 8}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	if _, err := pipeline.Run(context.Background(), []source.Item{item}); err != nil {
		t.Fatal(err)
	}
	// A resume lists a Flow that is now full, which is the case where signing a
	// URL per Segment costs the most.
	resumePipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 8}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumePipeline.Run(context.Background(), []source.Item{item}); err != nil {
		t.Fatal(err)
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.listings) == 0 {
		t.Fatal("no listings recorded")
	}
	for index, options := range client.listings {
		if options.IncludeDownloadURLs {
			t.Fatalf("listing %d asked for download URLs with verification off; nothing reads them", index)
		}
	}
}

// TestStagedDigestIsReusedForOwnedFilesOnly pins which inputs may skip the
// second read of a whole input.
//
// Without segmentation the only Media Object is the staged file itself, and
// staging has already read it end to end to hash it. Reading it again is a
// second full pass over the input for a digest that cannot have changed --
// provided the file is one this run created. A local path is not: it belongs to
// whoever invoked Tamsin and may be rewritten at any moment, so it keeps the
// later digest and refuses to attach it to the earlier staged identity when
// they differ.
//
// A deliberately wrong staged digest makes the distinction observable: reuse
// returns it verbatim, while a fresh local read detects the mismatch.
func TestStagedDigestIsReusedForOwnedFilesOnly(t *testing.T) {
	t.Parallel()
	const fabricated = "0000000000000000000000000000000000000000000000000000000000000000"
	for _, testCase := range []struct {
		name  string
		owned bool
	}{
		{name: "owned staged copy reuses the digest", owned: true},
		{name: "local path is read again", owned: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "input.ts")
			contents := []byte("media bytes")
			if err := os.WriteFile(filename, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			pipeline, err := New(Config{
				Concurrency: 1, Transfers: 1, SegmentDuration: 0,
				EssenceStorage: media.EssenceStorageMuxed,
			}, newFakeClient(), fakeProber{}, nil, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			staged := stagedFile{
				path: filename, size: int64(len(contents)), sha256: fabricated,
				owned: testCase.owned, cleanup: func() {},
			}
			objects, cleanup, err := pipeline.prepareObjects(context.Background(),
				"flow", staged, media.FlowInfo{Duration: int64(time.Second)})
			if !testCase.owned {
				if err == nil || !strings.Contains(err.Error(), "changed after staging") {
					t.Fatalf("local mismatch error = %v, want changed-after-staging failure", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()

			if len(objects) != 1 {
				t.Fatalf("expected one object, got %d", len(objects))
			}
			want := fabricated
			if objects[0].sha256 != want {
				t.Fatalf("object digest = %s, want %s", objects[0].sha256, want)
			}
		})
	}
}

// TestStartupCallsOverlap covers the handshake before any media moves. The two
// answers are independent, so asking for them in sequence spent two round trips
// where one would do -- paid once per run, and most visible on the short ingests
// where fixed cost dominates.
func TestStartupCallsOverlap(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &concurrentStartupClient{
		fakeClient:      newFakeClient(),
		serviceEntered:  make(chan struct{}),
		backendsEntered: make(chan struct{}),
		timeout:         10 * time.Second,
	}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestFlowIsReadBeforeWritingAndNotAfter pins both halves of how a Flow is
// written.
//
// It is read first, because a PUT replaces the Flow and would otherwise discard
// metadata another system added. It is not read afterwards: a successful PUT
// already establishes that the Flow exists, so a read-back would assert nothing
// the write had not, at the cost of a round trip per Flow.
func TestFlowIsReadBeforeWritingAndNotAfter(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 4}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}

	flowID := batch.Results[0].RootFlowID
	client.lock.Lock()
	defer client.lock.Unlock()
	reads, writes := -1, -1
	for index, call := range client.callLog {
		switch call {
		case "flowRead:" + flowID:
			if reads < 0 {
				reads = index
			}
		case "flowWrite:" + flowID:
			if writes < 0 {
				writes = index
			}
		}
	}
	if reads < 0 || writes < 0 {
		t.Fatalf("expected the Flow to be read and then written: %v", client.callLog)
	}
	if reads > writes {
		t.Fatalf("the Flow was written before it was read, so anything already on it was replaced unseen: %v",
			client.callLog)
	}
	for _, call := range client.callLog[writes:] {
		if call == "flowRead:"+flowID {
			t.Fatalf("the Flow was read back after writing it; the PUT already proves it exists: %v", client.callLog)
		}
	}
}

// TestFlowCarriesSegmentBitRates covers AppNote 0013, which defines
// avg_bit_rate and max_bit_rate as Segment bit rates rather than essence ones.
//
// The distinction is not academic. A Segment carries container overhead the
// essence figure excludes, so a reader sizing a buffer from the essence rate
// sizes it too small -- and max_bit_rate, which is the property the buffer
// calculation actually uses, was not being set at all.
func TestFlowCarriesSegmentBitRates(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, essenceBitRateProber{}, sizedSegmenter{objects: 4, bytes: 1_000_000}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}

	flow := client.flows[batch.Results[0].RootFlowID]
	// Four Segments of a megabyte, each a second long: eight megabits a second.
	const wantRate = int64(8000)
	average, present := flow["avg_bit_rate"]
	if !present {
		t.Fatal("avg_bit_rate is not set on the Flow")
	}
	if average != wantRate {
		t.Fatalf("avg_bit_rate = %v, want %d; 12000 would mean the essence rate was written instead of the Segment rate",
			average, wantRate)
	}
	peak, present := flow["max_bit_rate"]
	if !present {
		t.Fatal("max_bit_rate is not set on the Flow; it is what a receiver sizes its buffer from")
	}
	if peak != wantRate {
		t.Fatalf("max_bit_rate = %v, want %d", peak, wantRate)
	}
}

// TestIngestChecksTheStoreAPIVersion covers the compatibility signal the
// service document carries. Tamsin fetches that document anyway to prove the
// store is reachable and the credentials work, so the version costs nothing to
// read -- and an incompatible store is better named at once than discovered
// partway through an ingest, when a rejected request looks like a bug.
func TestIngestChecksTheStoreAPIVersion(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		document map[string]any
		wantErr  string
	}{
		{
			name:     "a differing major version is refused",
			document: map[string]any{"api_version": "9.0"},
			wantErr:  "not compatible",
		},
		{
			// Minor revisions add to the API rather than change it, so a client
			// that refused them would obstruct every service upgrade.
			name:     "a newer minor version is accepted",
			document: map[string]any{"api_version": "8.7"},
		},
		{
			name:     "a version below the compatibility floor is refused",
			document: map[string]any{"api_version": "8.0"},
			wantErr:  "supports TAMS 8.1",
		},
		{
			// Without the required capability boundary the client cannot safely
			// choose between the 8.1 and 8.2 write representations.
			name:     "a missing version is refused before mutation",
			document: map[string]any{"name": "store"},
			wantErr:  "did not report api_version",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			client.serviceDocument = make(map[string]any, len(testCase.document)+2)
			for key, value := range testCase.document {
				client.serviceDocument[key] = value
			}
			client.serviceDocument["min_object_timeout"] = "300:0"
			client.serviceDocument["min_presigned_url_timeout"] = "30:0"

			pipeline, err := New(Config{
				Concurrency: 1, Transfers: 2, SegmentDuration: time.Second,
				EssenceStorage: media.EssenceStorageMuxed,
			}, client, fakeProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			switch {
			case testCase.wantErr == "" && err != nil:
				t.Fatalf("run: %v", err)
			case testCase.wantErr != "" && err == nil:
				t.Fatalf("an incompatible store must be refused")
			case testCase.wantErr != "" && !strings.Contains(err.Error(), testCase.wantErr):
				t.Fatalf("error = %q, want it to mention %q", err, testCase.wantErr)
			}
		})
	}
}

func TestIngestRejectsUnsupportedFFprobeBeforeTAMSMutation(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, oldVersionProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err == nil || !strings.Contains(err.Error(), "version 5.1 or newer") {
		t.Fatalf("Run() error = %v, want unsupported FFprobe rejection", err)
	}
	if batch.Failed != 1 || batch.Results[0].Failure == nil ||
		batch.Results[0].Failure.Code != FailureCodeMediaToolUnavailable {
		t.Fatalf("unsupported FFprobe result = %#v", batch)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if len(client.flows) != 0 || client.allocations != 0 || client.uploads != 0 || len(client.segments) != 0 {
		t.Fatalf("unsupported FFprobe crossed mutation boundary: flows=%d allocations=%d uploads=%d segments=%d",
			len(client.flows), client.allocations, client.uploads, len(client.segments))
	}
}

// flowIDsFrom runs an ingest and reports every Flow identifier it created.
func flowIDsFrom(t *testing.T, config Config, prober media.Prober, filename string) map[string]bool {
	return flowIDsFromSegmenter(t, config, prober, countingSegmenter{objects: 2}, filename)
}

func flowIDsFromSegmenter(t *testing.T, config Config, prober media.Prober, segmenter media.Segmenter, filename string) map[string]bool {
	t.Helper()
	client := newFakeClient()
	pipeline, err := New(config, client, prober, segmenter, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)}); err != nil {
		t.Fatal(err)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	ids := make(map[string]bool, len(client.flows))
	for id := range client.flows {
		ids[id] = true
	}
	return ids
}

// TestGeneratedIdentityCoversWhatChangesTheResult pins what a derived Flow
// identifier has to account for.
//
// The identifier is a function of the input and how it is treated, and anything
// that changes what ends up in the store has to be part of it. Essence storage
// decides whether one Flow or several are written and what shape the parent
// takes: two arrangements sharing an identifier would mean a muxed
// multi-essence Flow and a demultiplexed collector claiming the same one. The
// start decides where the media sits on the timeline, so without it a second
// ingest at a different --start silently appends to the first Flow instead of
// describing the placement that was asked for.
func TestGeneratedIdentityCoversWhatChangesTheResult(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := Config{
		Concurrency: 1, Transfers: 2, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}

	// Asserted against the derivation rather than a run, because independent
	// storage writes no parent Flow yet: comparing the Flows two ingests
	// actually create would pass whether or not the arrangement is accounted
	// for, and would only start failing once a collector exists to collide.
	t.Run("storage arrangement", func(t *testing.T) {
		t.Parallel()
		muxed := base
		muxed.EssenceStorage = media.EssenceStorageMuxed
		independent := base
		independent.EssenceStorage = media.EssenceStorageIndependent
		if flowProfile("digest", muxed) == flowProfile("digest", independent) {
			t.Fatal("both storage arrangements derive one identity; a collector and a " +
				"multi-essence Flow would claim the same Flow ID for the same input")
		}
	})

	t.Run("timeline placement", func(t *testing.T) {
		t.Parallel()
		atZero := flowIDsFrom(t, base, fakeProber{}, filename)
		shiftedConfig := base
		shiftedConfig.Start = int64(90 * time.Second)
		shifted := flowIDsFrom(t, shiftedConfig, fakeProber{}, filename)

		for id := range atZero {
			if shifted[id] {
				t.Fatalf("Flow %s is reused across two different --start values; "+
					"the second ingest would append to the first instead of placing the media", id)
			}
		}
	})

	// Determinism is what makes a resume work at all, so the same input treated
	// the same way has to keep landing on the same Flow.
	t.Run("an unchanged ingest keeps its identity", func(t *testing.T) {
		t.Parallel()
		first := flowIDsFrom(t, base, muxedProber{}, filename)
		second := flowIDsFrom(t, base, muxedProber{}, filename)
		if len(first) == 0 {
			t.Fatal("no Flows were created")
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("identical ingests produced different Flows:\n%v\n%v", first, second)
		}
	})

	t.Run("FFmpeg package versions do not rotate stable identity", func(t *testing.T) {
		first := flowIDsFromSegmenter(t, base, fakeProber{}, versionedCountingSegmenter{
			countingSegmenter: countingSegmenter{objects: 2}, version: "ffmpeg version 6.1",
		}, filename)
		second := flowIDsFromSegmenter(t, base, fakeProber{}, versionedCountingSegmenter{
			countingSegmenter: countingSegmenter{objects: 2}, version: "ffmpeg version 7.0",
		}, filename)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("FFmpeg package versions rotated Flow identity:\n%v\n%v", first, second)
		}
	})

	t.Run("whole-file muxed ingest does not inspect unused FFmpeg", func(t *testing.T) {
		whole := base
		whole.SegmentDuration = 0
		whole.EssenceStorage = media.EssenceStorageMuxed
		ids := flowIDsFromSegmenter(t, whole, muxedProber{}, versionedCountingSegmenter{
			err: errors.New("FFmpeg must not be inspected"),
		}, filename)
		if len(ids) == 0 {
			t.Fatal("whole-file ingest produced no Flow")
		}
	})
}

// TestObjectsAreCommittedWithinTheAdvertisedLifetime covers the scheduling the
// service document asks for.
//
// A store collects a Media Object that is not registered against a Flow Segment
// in time, and promises only five minutes. Allocating storage for a whole
// programme, uploading for an hour and registering at the end relies on a
// guarantee that was never given: the earliest Objects can be gone before the
// last upload finishes, and the registration then fails for Objects that were
// uploaded perfectly well.
//
// What makes that safe is not the size of a batch but the order: each batch is
// registered before the next is allocated, so an Object's unregistered life is
// the length of one batch rather than the whole ingest.
func TestObjectsAreCommittedWithinTheAdvertisedLifetime(t *testing.T) {
	t.Parallel()
	// Use virtual sizes so the test can exercise the pinned five-minute minimum
	// without creating hundreds of megabytes of fixture media. At the assumed
	// 1 MiB/s, half the lifetime is a 150 MiB scheduling budget.
	pipeline := &Pipeline{limits: tams.ServiceLimits{ObjectRegistration: 5 * time.Minute}}
	objects := []preparedObject{{size: 80 << 20}, {size: 80 << 20}, {size: 10 << 20}}
	if got := pipeline.chunkSize(objects, 0); got != 1 {
		t.Fatalf("chunkSize() = %d, want 1 Object within the conservative lifetime budget", got)
	}
	if got := pipeline.chunkSize(objects[1:], 10<<20); got != 2 {
		t.Fatalf("chunkSize() with measured throughput = %d, want the remaining 2 Objects", got)
	}
}

// TestInvalidServiceLifetimesFailBeforeMutation guards the preflight boundary:
// a bad guarantee must never become an unlimited allocation batch, and no Flow
// or Object may be written before the service document is rejected.
func TestInvalidServiceLifetimesFailBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		document map[string]any
		want     string
	}{
		{name: "missing", document: map[string]any{"api_version": "8.1"}, want: "/min_object_timeout"},
		{name: "malformed", document: map[string]any{
			"api_version": "8.1", "min_object_timeout": "five minutes", "min_presigned_url_timeout": "30:0",
		}, want: "/min_object_timeout"},
		{name: "below minimum", document: map[string]any{
			"api_version": "8.1", "min_object_timeout": "299:0", "min_presigned_url_timeout": "30:0",
		}, want: "requires at least 300:0"},
		{name: "wrong ordering", document: map[string]any{
			"api_version": "8.1", "min_object_timeout": "300:0", "min_presigned_url_timeout": "301:0",
		}, want: "exceeds /min_object_timeout"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "fixture.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			client.serviceDocument = testCase.document
			pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback}, client, fakeProber{}, nil, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Run() error = %v, want containing %q", err, testCase.want)
			}
			client.lock.Lock()
			defer client.lock.Unlock()
			if client.allocations != 0 || client.uploads != 0 || len(client.flows) != 0 || len(client.segments) != 0 {
				t.Fatalf("invalid lifetime mutated TAMS: flows=%d allocations=%d uploads=%d segments=%d",
					len(client.flows), client.allocations, client.uploads, len(client.segments))
			}
		})
	}
}

// TestResumedObjectsAreVerifiedBeforeUploadsBegin covers the other half of the
// lifetime problem. The listing that decides what to resume also hands back the
// download URLs for what is already there, and a store promises those last only
// thirty seconds. Carrying them through however long the missing Objects take
// to upload spends that budget on waiting, and the verification then fails on
// URLs that were valid when they were issued.
func TestResumedObjectsAreVerifiedBeforeUploadsBegin(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	config := Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	first, err := New(config, client, fakeProber{}, countingSegmenter{objects: 4}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	if _, err := first.Run(context.Background(), []source.Item{item}); err != nil {
		t.Fatal(err)
	}

	// Drop half the Segments so the next run resumes some and uploads the rest.
	client.lock.Lock()
	for flowID, segments := range client.segments {
		dropped := 0
		for objectID := range segments {
			if dropped == 2 {
				break
			}
			delete(client.segments[flowID], objectID)
			dropped++
		}
	}
	client.callLog = nil
	client.lock.Unlock()

	second, err := New(config, client, fakeProber{}, countingSegmenter{objects: 4}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Run(context.Background(), []source.Item{item}); err != nil {
		t.Fatal(err)
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	firstAllocate, firstVerify := -1, -1
	for index, call := range client.callLog {
		if firstAllocate < 0 && strings.HasPrefix(call, "allocate:") {
			firstAllocate = index
		}
		if firstVerify < 0 && call == "verify" {
			firstVerify = index
		}
	}
	if firstVerify < 0 || firstAllocate < 0 {
		t.Fatalf("expected both a resume check and an upload: %v", client.callLog)
	}
	if firstVerify > firstAllocate {
		t.Fatalf("resumed Objects were verified only after uploads started, so their URLs aged "+
			"through the transfer: %v", client.callLog)
	}
}

// TestQueuedUploadsRequestURLsOnlyAfterWorkerIsReady proves that storage
// allocation follows the global worker budget. The fake clock advances by the
// complete advertised lifetime on every transfer; allocating all four URLs up
// front would therefore make the second PUT deterministically expire.
func TestQueuedUploadsRequestURLsOnlyAfterWorkerIsReady(t *testing.T) {
	t.Parallel()
	const objects = 4
	lifetime := 30 * time.Second
	client := newExpiringURLClient(lifetime)
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 1, VerificationMode: VerificationNone, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{benchFixture(t)}); err != nil {
		t.Fatal(err)
	}

	client.lock.Lock()
	ages := append([]time.Duration(nil), client.uploadAges...)
	client.lock.Unlock()
	assertFreshURLAges(t, "upload", ages, objects, lifetime)
	client.fakeClient.lock.Lock()
	maxAllocated := client.maxAllocationObjects
	client.fakeClient.lock.Unlock()
	if maxAllocated != 1 {
		t.Fatalf("one upload worker received %d URLs at once; queued URLs can expire", maxAllocated)
	}
}

// TestQueuedVerificationRefreshesURLsOnlyAfterWorkerIsReady proves the GET
// half independently on a pure resume. With one worker and a transfer taking a
// full lifetime, a whole-Flow listing would leave every URL after the first
// expired in the verification queue.
func TestQueuedVerificationRefreshesURLsOnlyAfterWorkerIsReady(t *testing.T) {
	t.Parallel()
	const objects = 4
	lifetime := 30 * time.Second
	client := newExpiringURLClient(lifetime)
	item := benchFixture(t)
	base := Config{
		Concurrency: 1, Transfers: 1, VerificationMode: VerificationNone, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	first, err := New(base, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Run(context.Background(), []source.Item{item}); err != nil {
		t.Fatal(err)
	}

	base.VerificationMode = VerificationReadback
	resume, err := New(base, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := resume.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Results[0].Status != ResultStatusResumed {
		t.Fatalf("unexpected resume result: %#v", batch)
	}

	client.lock.Lock()
	ages := append([]time.Duration(nil), client.downloadAges...)
	client.lock.Unlock()
	assertFreshURLAges(t, "verification", ages, objects, lifetime)
}

// TestOutlastsRegistration pins the comparison itself, including when it declines to
// answer. A warning an operator can do nothing with is worse than silence, so
// an unmeasured rate or an unadvertised lifetime produces none.
func TestOutlastsRegistration(t *testing.T) {
	t.Parallel()
	const megabytePerSecond = 1 << 20
	for _, testCase := range []struct {
		name       string
		size       int64
		throughput float64
		lifetime   time.Duration
		want       bool
	}{
		{
			// 60 MiB at a megabyte a second is a minute, against thirty seconds.
			name: "an object too large for the url", size: 60 << 20,
			throughput: megabytePerSecond, lifetime: 30 * time.Second, want: true,
		},
		{
			name: "an object that fits", size: 10 << 20,
			throughput: megabytePerSecond, lifetime: 30 * time.Second,
		},
		{
			name: "no rate has been measured yet", size: 60 << 20,
			throughput: 0, lifetime: 30 * time.Second,
		},
		{
			name: "the store advertised no lifetime", size: 60 << 20,
			throughput: megabytePerSecond, lifetime: 0,
		},
		{
			name: "an empty object", size: 0,
			throughput: megabytePerSecond, lifetime: 30 * time.Second,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, oversized := outlastsRegistration(testCase.size, testCase.throughput, testCase.lifetime)
			if oversized != testCase.want {
				t.Fatalf("outlastsRegistration(%d, %v, %v) = %v, want %v",
					testCase.size, testCase.throughput, testCase.lifetime, oversized, testCase.want)
			}
		})
	}
}

// TestWholeFileIngestRefusesOptionsItCannotHonour covers options that quietly
// did nothing.
//
// Without segmentation the staged file is uploaded as it stands and FFmpeg is
// never invoked, so an argument list or a chosen Segment container could only
// be honoured by remuxing the whole file -- which this path does not do. Both
// nevertheless fed the derived Flow identity, so two ingests differing only in
// an argument that had no effect landed on different Flows, and an argument
// list also left generation unset, implying a transcode that never happened.
func TestWholeFileIngestRefusesOptionsItCannotHonour(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	contents := []byte("media that is stored exactly as it arrived")
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	base := Config{Concurrency: 1, Transfers: 2, SegmentDuration: 0}

	for _, testCase := range []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "ffmpeg arguments",
			mutate:  func(c *Config) { c.FFmpegArgs = []string{"-c:v", "libx264"} },
			wantErr: "--ffmpeg-arg",
		},
		{
			name:    "a chosen segment container",
			mutate:  func(c *Config) { c.SegmentFormat = media.SegmentFormatMPEGTS },
			wantErr: "--segment-format",
		},
	} {
		for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
			storage := storage
			t.Run(testCase.name+"/"+string(storage), func(t *testing.T) {
				t.Parallel()
				config := base
				config.EssenceStorage = storage
				testCase.mutate(&config)
				pipeline, err := New(config, newFakeClient(), fakeProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
				if err != nil {
					t.Fatalf("run: %v", err)
				}
				if batch.Failed != 1 {
					t.Fatalf("an option that cannot take effect must fail rather than be ignored: %#v", batch)
				}
				message := batch.Results[0].Error
				if !strings.Contains(message, testCase.wantErr) || !strings.Contains(message, "without segmentation") {
					t.Fatalf("error = %q, want it to name %s and say why", message, testCase.wantErr)
				}
			})
		}
	}

	// The same path without those options stores the input untouched, which is
	// the behaviour the rejection exists to keep honest.
	t.Run("the bytes are the input", func(t *testing.T) {
		t.Parallel()
		config := base
		config.EssenceStorage = media.EssenceStorageMuxed
		client := newFakeClient()
		pipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		if batch.Succeeded != 1 || len(batch.Results[0].rootFlow().Objects) != 1 {
			t.Fatalf("expected the whole input as one Media Object: %#v", batch.Results[0])
		}
		digest := sha256.Sum256(contents)
		if batch.Results[0].rootFlow().Objects[0].SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal("the stored Object is not the input verbatim")
		}
		client.lock.Lock()
		defer client.lock.Unlock()
		for _, stored := range client.objects {
			if !bytes.Equal(stored, contents) {
				t.Fatalf("uploaded %d bytes that are not the input", len(stored))
			}
		}
	})
}

// sourcesByRole reports the Source each Flow the run created belongs to, keyed
// by the role its collection entry gives it, or "" for a Flow that stands alone.
func sourcesByRole(t *testing.T, config Config, prober media.Prober, filename string) map[string]string {
	t.Helper()
	client := newFakeClient()
	pipeline, err := New(config, client, prober, countingSegmenter{objects: 2}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)}); err != nil {
		t.Fatal(err)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	roles := make(map[string]string, len(client.flows))
	for _, flow := range client.flows {
		role := ""
		if label, ok := flow["label"].(string); ok {
			if open := strings.LastIndex(label, "("); open >= 0 {
				role = strings.TrimSuffix(label[open+1:], ")")
			}
		}
		sourceID, _ := flow["source_id"].(string)
		roles[role] = sourceID
	}
	return roles
}

// TestSourceIdentityFollowsContentNotLocation covers what a Source is for.
//
// A Source is the content; Flows are representations of it. Deriving the
// identifier from the location meant reusing a path or an S3 key for an
// unrelated programme handed the new content the old Source, asserting the two
// were one thing in different renditions. Nothing about a filename supports
// that, and a store cannot tell afterwards that the claim was wrong.
func TestSourceIdentityFollowsContentNotLocation(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	filename := filepath.Join(directory, "programme.ts")
	base := Config{
		Concurrency: 1, Transfers: 2, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}

	t.Run("reusing a path for different content does not reuse the Source", func(t *testing.T) {
		if err := os.WriteFile(filename, []byte("the first programme"), 0o600); err != nil {
			t.Fatal(err)
		}
		first := sourcesByRole(t, base, fakeProber{}, filename)
		if err := os.WriteFile(filename, []byte("an entirely different programme"), 0o600); err != nil {
			t.Fatal(err)
		}
		second := sourcesByRole(t, base, fakeProber{}, filename)
		if first[""] == "" || first[""] == second[""] {
			t.Fatalf("both programmes were given Source %s, which says they are the same content", first[""])
		}
	})

	t.Run("the same content segmented differently shares one Source", func(t *testing.T) {
		shared := filepath.Join(t.TempDir(), "programme.ts")
		if err := os.WriteFile(shared, []byte("one programme, two renditions"), 0o600); err != nil {
			t.Fatal(err)
		}
		tenSeconds := base
		tenSeconds.SegmentDuration = 10 * time.Second
		if a, b := sourcesByRole(t, base, fakeProber{}, shared), sourcesByRole(t, tenSeconds, fakeProber{}, shared); a[""] != b[""] {
			t.Fatalf("two segmentations of one input got Sources %s and %s; they are renditions of the same content", a[""], b[""])
		}
	})

	// A video track is the same content whether it was left inside the
	// multiplex or demultiplexed out of it, so it keeps one Source across both.
	t.Run("an essence keeps its Source across storage arrangements", func(t *testing.T) {
		muxedFile := filepath.Join(t.TempDir(), "muxed.ts")
		if err := os.WriteFile(muxedFile, []byte("video and audio together"), 0o600); err != nil {
			t.Fatal(err)
		}
		independent := base
		independent.EssenceStorage = media.EssenceStorageIndependent
		muxed := sourcesByRole(t, base, muxedProber{}, muxedFile)
		demuxed := sourcesByRole(t, independent, muxedProber{}, muxedFile)
		for _, role := range []string{"video", "audio"} {
			if muxed[role] == "" {
				t.Fatalf("no %s Flow was created in the muxed arrangement: %v", role, muxed)
			}
			if muxed[role] != demuxed[role] {
				t.Fatalf("the %s essence got Sources %s and %s across the two arrangements",
					role, muxed[role], demuxed[role])
			}
		}
	})
}

// TestFlowMetadataWrittenElsewhereSurvivesAnIngest covers what a PUT does to a
// Flow somebody else has been curating.
//
// A PUT replaces the Flow, and the schema is explicit that tags are replaced
// rather than merged, so writing generated metadata unconditionally deleted
// whatever had been added between runs. The loss is silent, and the system that
// wrote the metadata is not the one running the ingest, so nobody present sees
// it go.
func TestFlowMetadataWrittenElsewhereSurvivesAnIngest(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	config := Config{
		Concurrency: 1, Transfers: 2, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	run := func(t *testing.T, config Config) Result {
		t.Helper()
		pipeline, err := New(config, client, fakeProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		return batch.Results[0]
	}

	firstResult := run(t, config)
	flowID := firstResult.RootFlowID
	if firstResult.rootFlow().Disposition != FlowWritten {
		t.Fatalf("new Flow disposition = %q, want written", firstResult.rootFlow().Disposition)
	}

	// Another system curates the Flow between runs.
	client.lock.Lock()
	tags := client.flows[flowID]["tags"].(map[string]any)
	tags["programme_id"] = "CRID/1234"
	client.flows[flowID]["description"] = "Edited by the archive team"
	// A property Tamsin never generates, so nothing but the copy of what was
	// already there can keep it.
	client.flows[flowID]["read_only"] = true
	client.callLog = nil
	client.lock.Unlock()

	// A second run over unchanged input has nothing of its own to change.
	resumeResult := run(t, config)
	if resumeResult.rootFlow().Disposition != FlowUnchanged {
		t.Fatalf("confirmed equivalent Flow disposition = %q, want unchanged", resumeResult.rootFlow().Disposition)
	}

	client.lock.Lock()
	stored := client.flows[flowID]
	storedTags := stored["tags"].(map[string]any)
	wrote := false
	for _, call := range client.callLog {
		if call == "flowWrite:"+flowID {
			wrote = true
		}
	}
	client.lock.Unlock()

	if wrote {
		t.Fatal("nothing Tamsin owns changed, so the Flow should not have been written at all")
	}
	if storedTags["programme_id"] != "CRID/1234" {
		t.Fatalf("a tag written elsewhere was discarded: %#v", storedTags)
	}
	if stored["description"] != "Edited by the archive team" {
		t.Fatalf("a description written elsewhere was discarded: %v", stored["description"])
	}
	if stored["read_only"] != true {
		t.Fatalf("a property Tamsin does not generate was discarded: %#v", stored)
	}

	// When Tamsin does have something to write, the same rules apply: its own
	// fields are updated, a tag of its own that it no longer writes goes, and
	// the rest is left alone.
	client.lock.Lock()
	client.flows[flowID]["tags"].(map[string]any)[media.TagPrefix+"retired"] = "from an older build"
	client.lock.Unlock()
	enriched := config
	enriched.FlowMetadata = tams.Flow{"label": "Relabelled by the operator"}
	_ = run(t, enriched)

	client.lock.Lock()
	defer client.lock.Unlock()
	stored = client.flows[flowID]
	storedTags = stored["tags"].(map[string]any)
	if stored["label"] != "Relabelled by the operator" {
		t.Fatalf("the run's own metadata was not applied: %v", stored["label"])
	}
	if storedTags["programme_id"] != "CRID/1234" {
		t.Fatalf("an update discarded a tag written elsewhere: %#v", storedTags)
	}
	if stored["description"] != "Edited by the archive team" {
		t.Fatalf("an update discarded a description written elsewhere: %v", stored["description"])
	}
	if stored["read_only"] != true {
		t.Fatalf("an update discarded a property Tamsin does not generate: %#v", stored)
	}
	if _, present := storedTags[media.TagPrefix+"retired"]; present {
		t.Fatalf("a Tamsin tag this run no longer writes was kept: %#v", storedTags)
	}
	if storedTags[media.TagPrefix+"sha256"] == nil {
		t.Fatalf("the run's own provenance tags are missing: %#v", storedTags)
	}
}

// TestCuratedMetadataSurvivesAcrossTheWholeFlowGraph covers the paths that used
// to bypass the ownership-aware writer: muxed children and the independent
// collector. A resume must preserve enrichment on every member and avoid a PUT
// when Tamsin's part of the graph is unchanged.
func TestCuratedMetadataSurvivesAcrossTheWholeFlowGraph(t *testing.T) {
	t.Parallel()
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		storage := storage
		t.Run(string(storage), func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.ts")
			if err := os.WriteFile(filename, []byte("muxed media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			run := func() {
				pipeline, err := New(Config{
					Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second, EssenceStorage: storage,
				}, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				batch, runErr := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
				if runErr != nil || batch.Failed != 0 {
					t.Fatalf("run: batch=%#v error=%v", batch, runErr)
				}
			}
			run()

			client.lock.Lock()
			flowCount := len(client.flows)
			for _, flow := range client.flows {
				flow["archive_annotation"] = "curated outside Tamsin"
				flow["description"] = "archive description"
				flow["tags"].(map[string]any)["programme_id"] = "CRID/graph"
			}
			client.callLog = nil
			client.lock.Unlock()

			run()

			client.lock.Lock()
			defer client.lock.Unlock()
			if len(client.flows) != flowCount {
				t.Fatalf("resume changed graph size from %d to %d", flowCount, len(client.flows))
			}
			for id, flow := range client.flows {
				if flow["archive_annotation"] != "curated outside Tamsin" || flow["description"] != "archive description" ||
					flow["tags"].(map[string]any)["programme_id"] != "CRID/graph" {
					t.Fatalf("Flow %s lost curated metadata: %#v", id, flow)
				}
			}
			for _, call := range client.callLog {
				if strings.HasPrefix(call, "flowWrite:") {
					t.Fatalf("unchanged enriched graph was rewritten on resume: %v", client.callLog)
				}
			}
		})
	}
}

// TestFlowIsNotOverwrittenWhenItCannotBeRead covers the unreadable case. Writing
// anyway would replace metadata that is there but could not be seen, and that
// cannot be undone; failing can be retried, and by this point the client has
// already spent its own retries.
func TestFlowIsNotOverwrittenWhenItCannotBeRead(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	client.flowReadErr = &tams.HTTPError{
		Method: http.MethodGet, URL: "flows/x", StatusCode: http.StatusServiceUnavailable,
		Status: "503 Service Unavailable",
	}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if batch.Failed != 1 {
		t.Fatalf("an unreadable Flow must fail rather than be replaced unseen: %#v", batch)
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	for _, call := range client.callLog {
		if strings.HasPrefix(call, "flowWrite:") {
			t.Fatalf("the Flow was written despite not being readable: %v", client.callLog)
		}
	}
}
