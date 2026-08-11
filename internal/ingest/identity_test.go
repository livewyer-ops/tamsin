package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func TestRefreshedSignedURLResumesTheSameGeneratedGraph(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "programme.ts")
	if err := os.WriteFile(filename, []byte("stable muxed media"), 0o600); err != nil {
		t.Fatal(err)
	}
	firstURL := "https://alice:first-secret@media.example.test/library/programme.ts?" +
		"X-Amz-Credential=first-credential&X-Amz-Signature=first-signature#first-fragment"
	secondURL := "https://bob:second-secret@media.example.test/library/programme.ts?" +
		"X-Amz-Signature=second-signature&X-Amz-Credential=second-credential#second-fragment"

	client := newFakeClient()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	config := Config{
		Concurrency: 1, Transfers: 2, Verify: true, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	run := func(rawURL string) Result {
		t.Helper()
		item := localSource(filename)
		item.URI, item.Name = rawURL, "programme.ts"
		pipeline, err := New(config, client, muxedProber{}, countingSegmenter{objects: 2}, logger, nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{item})
		if err != nil {
			t.Fatal(err)
		}
		if batch.Succeeded != 1 {
			t.Fatalf("unexpected batch: %#v", batch)
		}
		return batch.Results[0]
	}

	first := run(firstURL)
	firstGraph := storedFlowIDs(client)
	client.lock.Lock()
	firstUploads, firstAllocations := client.uploads, client.allocations
	client.lock.Unlock()
	second := run(secondURL)
	secondGraph := storedFlowIDs(client)

	firstRoot := rootFlowResult(t, first)
	secondRoot := rootFlowResult(t, second)
	if first.RootFlowID != second.RootFlowID || firstRoot.SourceID != secondRoot.SourceID {
		t.Fatalf("refreshed signature changed root identity: first=%#v second=%#v", first, second)
	}
	if !reflect.DeepEqual(firstGraph, secondGraph) || len(firstGraph) != 3 {
		t.Fatalf("refreshed signature changed the generated graph:\n%v\n%v", firstGraph, secondGraph)
	}
	if second.Status != ResultStatusResumed {
		t.Fatalf("second status = %q, want resumed", second.Status)
	}
	client.lock.Lock()
	if client.uploads != firstUploads || client.allocations != firstAllocations {
		t.Fatalf("resume uploaded or allocated again: uploads %d->%d allocations %d->%d",
			firstUploads, client.uploads, firstAllocations, client.allocations)
	}
	state, err := json.Marshal(struct {
		First  Result
		Second Result
		Flows  any
		Logs   string
	}{First: first, Second: second, Flows: client.flows, Logs: logs.String()})
	client.lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{
		"alice", "first-secret", "first-credential", "first-signature", "first-fragment",
		"bob", "second-secret", "second-credential", "second-signature", "second-fragment",
	} {
		if bytes.Contains(state, []byte(secret)) {
			t.Fatalf("generated state leaked %q: %s", secret, state)
		}
	}
	if first.Input != "https://media.example.test/library/programme.ts" || second.Input != first.Input {
		t.Fatalf("canonical result inputs = %q and %q", first.Input, second.Input)
	}
}

func TestDuplicateContentIsSerializedAndProvenanceIsDeterministic(t *testing.T) {
	run := func(concurrency int) (string, []Result, int, int) {
		t.Helper()
		filename := filepath.Join(t.TempDir(), "shared.bin")
		if err := os.WriteFile(filename, []byte("one representation"), 0o600); err != nil {
			t.Fatal(err)
		}
		first, second := localSource(filename), localSource(filename)
		first.URI, first.Name = "https://origin-b.example.test/b/programme.bin?token=one", "b.bin"
		second.URI, second.Name = "https://origin-a.example.test/a/programme.bin?token=two", "a.bin"
		client := newFakeClient()
		pipeline, err := New(Config{
			Concurrency: concurrency, Transfers: 2, SegmentDuration: time.Second,
			EssenceStorage: media.EssenceStorageMuxed,
		}, client, fakeProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{first, second})
		if err != nil {
			t.Fatal(err)
		}
		if batch.Succeeded != 2 || batch.Results[0].RootFlowID != batch.Results[1].RootFlowID {
			t.Fatalf("duplicate content did not converge on one graph: %#v", batch)
		}
		statuses := []string{string(batch.Results[0].Status), string(batch.Results[1].Status)}
		sort.Strings(statuses)
		if !reflect.DeepEqual(statuses, []string{string(ResultStatusIngested), string(ResultStatusResumed)}) {
			t.Fatalf("statuses = %v, want one ingest and one resume", statuses)
		}

		client.lock.Lock()
		flow := client.flows[batch.Results[0].RootFlowID]
		encoded, err := json.Marshal(flow)
		allocations, uploads := client.allocations, client.uploads
		client.lock.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded), batch.Results, allocations, uploads
	}

	serialFlow, _, serialAllocations, serialUploads := run(1)
	parallelFlow, _, parallelAllocations, parallelUploads := run(2)
	if serialFlow != parallelFlow {
		t.Fatalf("worker scheduling changed Flow metadata:\nserial:   %s\nparallel: %s", serialFlow, parallelFlow)
	}
	for label, count := range map[string]int{
		"serial allocations": serialAllocations, "serial uploads": serialUploads,
		"parallel allocations": parallelAllocations, "parallel uploads": parallelUploads,
	} {
		if count != 1 {
			t.Fatalf("%s = %d, want exactly one", label, count)
		}
	}
	for _, source := range []string{
		"https://origin-a.example.test/a/programme.bin",
		"https://origin-b.example.test/b/programme.bin",
	} {
		if !strings.Contains(serialFlow, source) {
			t.Fatalf("accumulated provenance omits %s: %s", source, serialFlow)
		}
	}
	if strings.Contains(serialFlow, "token") || strings.Contains(serialFlow, "one") || strings.Contains(serialFlow, "two") {
		t.Fatalf("provenance retained query material: %s", serialFlow)
	}
}

func TestGeneratedIdentityIncludesEffectiveMediaInterpretation(t *testing.T) {
	directory := t.TempDir()
	video := filepath.Join(directory, "same.video")
	audio := filepath.Join(directory, "same.audio")
	contents := []byte("identical raw bytes")
	for _, filename := range []string{video, audio} {
		if err := os.WriteFile(filename, contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(filename string) Result {
		t.Helper()
		pipeline, err := New(Config{
			DryRun: true, SegmentDuration: 0, EssenceStorage: media.EssenceStorageMuxed,
		}, nil, filenameSensitiveProber{}, nil, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if err != nil {
			t.Fatal(err)
		}
		return batch.Results[0]
	}
	videoResult, audioResult := run(video), run(audio)
	videoRoot := rootFlowResult(t, videoResult)
	audioRoot := rootFlowResult(t, audioResult)
	if videoRoot.SourceID != audioRoot.SourceID {
		t.Fatalf("identical input bytes should retain one Source: %s != %s", videoRoot.SourceID, audioRoot.SourceID)
	}
	if videoResult.RootFlowID == audioResult.RootFlowID {
		t.Fatalf("different effective media graphs claimed Flow %s", videoResult.RootFlowID)
	}
}

func TestGeneratedIdentityCoversEveryMediaTreatmentField(t *testing.T) {
	base := Config{
		SegmentDuration: time.Second, SegmentFormat: media.SegmentFormatSource,
		EssenceStorage: media.EssenceStorageMuxed,
	}
	root := func(digest, mediaKey string, config Config) string {
		return generatedRootFlowID(flowProfile(digest, config), mediaKey)
	}
	want := root("bytes-a", "media-a", base)
	variations := map[string]func(*Config){
		"profile":          func(config *Config) { config.Profile = "streaming-ts" },
		"profile version":  func(config *Config) { config.ProfileVersion = "2" },
		"segment duration": func(config *Config) { config.SegmentDuration = 2 * time.Second },
		"segment format":   func(config *Config) { config.SegmentFormat = media.SegmentFormatMPEGTS },
		"essence storage":  func(config *Config) { config.EssenceStorage = media.EssenceStorageIndependent },
		"timeline start":   func(config *Config) { config.Start = int64(time.Second) },
		"FFmpeg arguments": func(config *Config) { config.FFmpegArgs = []string{"-c:v", "libx264"} },
	}
	for label, mutate := range variations {
		config := base
		mutate(&config)
		if got := root("bytes-a", "media-a", config); got == want {
			t.Fatalf("%s did not change Flow %s", label, got)
		}
	}
	if root("bytes-b", "media-a", base) == want {
		t.Fatal("different staged content reused the same Flow")
	}
	if root("bytes-a", "media-b", base) == want {
		t.Fatal("different media interpretation reused the same Flow")
	}

	// Argument boundaries are framed as data. These two vectors had the same
	// NUL-joined byte representation in the old recipe.
	left, right := base, base
	left.FFmpegArgs = []string{"a\x00b", "c"}
	right.FFmpegArgs = []string{"a", "b\x00c"}
	if flowProfile("bytes-a", left) == flowProfile("bytes-a", right) {
		t.Fatal("FFmpeg argument boundaries collide in the media-treatment fingerprint")
	}
	if flowProfileForRendererEpoch("bytes-a", base, "1") == flowProfileForRendererEpoch("bytes-a", base, "2") {
		t.Fatal("different renderer policy epochs reused the same treatment fingerprint")
	}
}

func TestMediaInterpretationIdentityIncludesPhase2GraphFacts(t *testing.T) {
	flow := tams.Flow{"format": "urn:x-nmos:format:multi", "container": "video/mp2t"}
	info := media.FlowInfo{
		Format: "urn:x-nmos:format:multi", Container: "video/mp2t",
		UnsupportedCodecs: []media.UnsupportedCodec{{Name: "codec-a", StreamType: "video", StreamIndex: 3}},
		Collected: []media.CollectedFlow{{
			Role: "video", Flow: tams.Flow{"format": "urn:x-nmos:format:video"},
			ContainerMapping: map[string]any{"track_index": 3}, StreamIndex: 3,
		}},
	}
	base, err := mediaInterpretationFingerprint(flow, info)
	if err != nil {
		t.Fatal(err)
	}

	differentCodec := info
	differentCodec.UnsupportedCodecs = []media.UnsupportedCodec{{Name: "codec-b", StreamType: "video", StreamIndex: 3}}
	differentCodecKey, err := mediaInterpretationFingerprint(flow, differentCodec)
	if err != nil {
		t.Fatal(err)
	}
	if differentCodecKey == base {
		t.Fatal("an unknown codec interpretation did not change generated identity")
	}

	differentMapping := info
	differentMapping.Collected = append([]media.CollectedFlow(nil), info.Collected...)
	differentMapping.Collected[0].ContainerMapping = map[string]any{"track_index": 4}
	differentMappingKey, err := mediaInterpretationFingerprint(flow, differentMapping)
	if err != nil {
		t.Fatal(err)
	}
	if differentMappingKey == base {
		t.Fatal("a parent container mapping did not change generated identity")
	}
}

func TestExplicitRootStabilizesChildrenWithoutRotatingObjects(t *testing.T) {
	const (
		rootID   = "00000000-0000-4000-8000-000000000001"
		sourceID = "00000000-0000-4000-8000-000000000002"
	)
	filename := filepath.Join(t.TempDir(), "programme.ts")
	if err := os.WriteFile(filename, []byte("explicit graph"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(rawURL string) ([]string, Result) {
		t.Helper()
		item := localSource(filename)
		item.URI = rawURL
		client := newFakeClient()
		pipeline, err := New(Config{
			RetainObjectResults: true,
			Concurrency:         1, Transfers: 2, SegmentDuration: time.Second,
			EssenceStorage: media.EssenceStorageMuxed, FlowID: rootID, SourceID: sourceID,
		}, client, muxedProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(context.Background(), []source.Item{item})
		if err != nil {
			t.Fatal(err)
		}
		return storedFlowIDs(client), batch.Results[0]
	}
	firstGraph, first := run("https://user:secret@media.test/programme.ts?sig=one")
	secondGraph, second := run("https://media.test/programme.ts?sig=two")
	if !reflect.DeepEqual(firstGraph, secondGraph) || first.RootFlowID != rootID || second.RootFlowID != rootID {
		t.Fatalf("explicit root did not anchor children: %v vs %v", firstGraph, secondGraph)
	}
	firstRoot := rootFlowResult(t, first)
	secondRoot := rootFlowResult(t, second)
	if firstRoot.SourceID != sourceID || secondRoot.SourceID != sourceID ||
		!sameObjectIdentities(firstRoot.Objects, secondRoot.Objects) {
		t.Fatalf("explicit identities changed across locator refresh: %#v vs %#v", first, second)
	}
	if generatedChildFlowID(rootID, "collected", "0") == generatedChildFlowID(
		"00000000-0000-4000-8000-000000000003", "collected", "0") {
		t.Fatal("children of two explicit roots collided")
	}

	// Golden values pin the legacy encoding that Source and Object resume rely
	// on. This Flow-only migration must not rotate either namespace.
	if got := sourceIdentity("abc"); got != "51d68e09-61df-503d-bfbf-3c6c9840b2ba" {
		t.Fatalf("legacy Source ID rotated to %s", got)
	}
	if got := namedID("object", rootID, "deadbeef", "[0:0_1:0)"); got != "79f57cb9-346b-5200-b8a1-476e189f7b2a" {
		t.Fatalf("legacy Object ID rotated to %s", got)
	}
}

func rootFlowResult(t *testing.T, result Result) FlowResult {
	t.Helper()
	for _, flow := range result.Flows {
		if flow.FlowID == result.RootFlowID {
			return flow
		}
	}
	t.Fatalf("result has no root Flow %s: %#v", result.RootFlowID, result)
	return FlowResult{}
}

func sameObjectIdentities(left, right []ObjectResult) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].ObjectID != right[index].ObjectID ||
			left[index].Timerange != right[index].Timerange ||
			left[index].Bytes != right[index].Bytes ||
			left[index].SHA256 != right[index].SHA256 {
			return false
		}
	}
	return true
}

func TestCanonicalProvenanceDropsCredentialBearingURLParts(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		raw  string
		want string
	}{
		{
			raw:  "https://user:password@example.test/media.ts?signature=secret#token",
			want: "https://example.test/media.ts",
		},
		{
			raw:  "s3://key:secret@archive/programme.mxf?versionId=credential#state",
			want: "s3://archive/programme.mxf",
		},
		{raw: "file:///archive/programme.mxf", want: "file:///archive/programme.mxf"},
		{raw: "stdin:", want: "stdin:"},
	} {
		if got := safeURI(testCase.raw); got != testCase.want {
			t.Errorf("safeURI(%q) = %q, want %q", testCase.raw, got, testCase.want)
		}
	}
}

func TestLocalMutationFailsBeforeAnyTAMSMutation(t *testing.T) {
	for _, segmented := range []bool{false, true} {
		for _, verify := range []bool{false, true} {
			name := "whole"
			if segmented {
				name = "segmented"
			}
			if verify {
				name += "-verified"
			}
			t.Run(name, func(t *testing.T) {
				filename := filepath.Join(t.TempDir(), "mutable.bin")
				if err := os.WriteFile(filename, []byte("media-A"), 0o600); err != nil {
					t.Fatal(err)
				}
				client := newFakeClient()
				duration := time.Duration(0)
				var segmenter media.Segmenter
				if segmented {
					duration = time.Second
					segmenter = countingSegmenter{objects: 1}
				}
				prober := &mutatingProber{target: filename, replacement: []byte("media-B")}
				pipeline, err := New(Config{
					Concurrency: 1, Transfers: 1, Verify: verify, SegmentDuration: duration,
					EssenceStorage: media.EssenceStorageMuxed,
				}, client, prober, segmenter, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
				if err != nil {
					t.Fatal(err)
				}
				if batch.Failed != 1 || !strings.Contains(batch.Results[0].Error, "changed after staging") {
					t.Fatalf("mutation result = %#v", batch)
				}
				client.lock.Lock()
				defer client.lock.Unlock()
				if len(client.flows) != 0 || client.allocations != 0 || client.uploads != 0 {
					t.Fatalf("local mutation reached TAMS: flows=%d allocations=%d uploads=%d",
						len(client.flows), client.allocations, client.uploads)
				}
			})
		}
	}
}

func storedFlowIDs(client *fakeClient) []string {
	client.lock.Lock()
	defer client.lock.Unlock()
	ids := make([]string, 0, len(client.flows))
	for id := range client.flows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

type filenameSensitiveProber struct{}

func (filenameSensitiveProber) Probe(_ context.Context, filename string) (media.Probe, error) {
	probe := media.Probe{Format: media.Format{Name: "raw", Duration: "1.0", StartTime: "0.0"}}
	if strings.HasSuffix(filename, ".audio") {
		probe.Streams = []media.Stream{{
			CodecName: "pcm_s16le", CodecType: "audio", SampleRate: "48000", Channels: 2,
		}}
	} else {
		probe.Streams = []media.Stream{{
			CodecName: "rawvideo", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1",
		}}
	}
	return probe, nil
}

func (filenameSensitiveProber) Version(context.Context) (string, error) {
	return "filename-sensitive test prober", nil
}

type mutatingProber struct {
	target      string
	replacement []byte
	once        sync.Once
	err         error
}

func (p *mutatingProber) Probe(ctx context.Context, filename string) (media.Probe, error) {
	if filename == p.target {
		p.once.Do(func() { p.err = os.WriteFile(p.target, p.replacement, 0o600) })
		if p.err != nil {
			return media.Probe{}, p.err
		}
	}
	return (fakeProber{}).Probe(ctx, filename)
}

func (*mutatingProber) Version(context.Context) (string, error) {
	return "mutating test prober", nil
}
