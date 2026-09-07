package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func streamFixture(tb testing.TB, extension string, args ...string) string {
	tb.Helper()
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			tb.Skip("requires ffmpeg and ffprobe")
		}
	}
	filename := filepath.Join(tb.TempDir(), "media"+extension)
	command := exec.CommandContext(tb.Context(), "ffmpeg", append([]string{"-v", "error", "-nostdin", "-y"}, append(args, filename)...)...)
	if output, err := command.CombinedOutput(); err != nil {
		tb.Fatalf("create fixture: %v: %s", err, output)
	}
	return filename
}

type countedSourceReader struct {
	file  *os.File
	bytes *atomic.Int64
	delay time.Duration
}

func (r countedSourceReader) Read(buffer []byte) (int, error) {
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	n, err := r.file.Read(buffer[:min(len(buffer), 32<<10)])
	r.bytes.Add(int64(n))
	return n, err
}

func (r countedSourceReader) Seek(offset int64, whence int) (int64, error) {
	return r.file.Seek(offset, whence)
}

func serveStreamFixture(tb testing.TB, filename string, delay time.Duration) (source.Item, *atomic.Int64) {
	tb.Helper()
	var read atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		file, err := os.Open(filename)
		if err != nil {
			tb.Error(err)
			w.WriteHeader(500)
			return
		}
		defer file.Close()
		w.Header().Set("ETag", `"fixture-revision"`)
		http.ServeContent(w, r, filename, time.Time{}, countedSourceReader{file, &read, delay})
	}))
	tb.Cleanup(server.Close)
	items, err := source.New(source.Config{}).Resolve(tb.Context(), []string{server.URL + "/media" + filepath.Ext(filename)})
	if err != nil {
		tb.Fatal(err)
	}
	return items[0], &read
}

func TestStreamedContainersAndEssenceLayouts(t *testing.T) {
	for _, tc := range []struct {
		name, extension string
		args            []string
		storage         media.EssenceStorage
		format          media.SegmentFormat
	}{
		{"mp4 tail moov and B-frames", ".mp4", []string{"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=6", "-c:v", "libx264", "-g", "50", "-bf", "3"}, media.EssenceStorageMuxed, media.SegmentFormatSource},
		{"mpegts", ".ts", []string{"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=6", "-c:v", "libx264", "-g", "50", "-bf", "3"}, media.EssenceStorageMuxed, media.SegmentFormatMPEGTS},
		{"mxf", ".mxf", []string{"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=6", "-c:v", "mpeg2video", "-g", "25", "-pix_fmt", "yuv422p"}, media.EssenceStorageMuxed, media.SegmentFormatSource},
		{"audio", ".wav", []string{"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=6", "-c:a", "pcm_s16le"}, media.EssenceStorageIndependent, media.SegmentFormatSource},
		{"two independent streams", ".mp4", []string{"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=6", "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=6", "-c:v", "libx264", "-g", "50", "-c:a", "aac"}, media.EssenceStorageIndependent, media.SegmentFormatSource},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := streamFixture(t, tc.extension, tc.args...)
			item, _ := serveStreamFixture(t, filename, 0)
			client := newFakeClient()
			config := Config{InputMode: InputStream, SegmentDuration: 2 * time.Second, SegmentFormat: tc.format, EssenceStorage: tc.storage, TempDirectory: t.TempDir(), VerificationMode: VerificationReadback}
			pipeline, err := New(config, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(t.Context(), []source.Item{item})
			if err != nil || batch.Succeeded != 1 {
				t.Fatalf("run: %v, results: %+v", err, batch.Results)
			}
			if batch.Results[0].SHA256 != "" || pipeline.observability.Snapshot().BytesStaged != 0 {
				t.Fatal("streamed input claimed a whole-file checksum or staged source bytes")
			}
			for _, flow := range client.flows {
				tags := flow["tags"].(map[string]any)
				if tags[media.TagPrefix+"input_revision"] == nil || tags[media.TagPrefix+"sha256"] != nil {
					t.Fatalf("incorrect streamed provenance: %v", tags)
				}
			}
			firstID := batch.Results[0].RootFlowID
			batch, err = pipeline.Run(t.Context(), []source.Item{item})
			if err != nil || batch.Succeeded != 1 || batch.Results[0].RootFlowID != firstID || batch.Results[0].Status != ResultStatusResumed {
				t.Fatalf("repeat: %v, results: %+v", err, batch.Results)
			}
			entries, err := os.ReadDir(config.TempDirectory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging files survived cleanup: %v, %v", entries, err)
			}
		})
	}
}

func TestStreamingRegistersBeforeReadingWholeInputLargerThanBudget(t *testing.T) {
	filename := streamFixture(t, ".wav", "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=180", "-c:a", "pcm_s16le")
	for _, scheme := range []string{"http", "s3"} {
		t.Run(scheme, func(t *testing.T) {
			item, read := serveStreamFixture(t, filename, time.Millisecond)
			if scheme == "s3" {
				location, _ := url.Parse(item.URI)
				endpoint := location.Scheme + "://" + location.Host
				client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, func(options *s3.Options) {
					options.BaseEndpoint = aws.String(endpoint)
					options.UsePathStyle = true
				})
				items, err := source.New(source.Config{S3Client: client, S3: source.S3Config{Endpoint: endpoint}}).Resolve(t.Context(), []string{"s3://bucket/media.wav"})
				if err != nil {
					t.Fatal(err)
				}
				item = items[0]
			}
			info, err := os.Stat(filename)
			if err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			var firstRead atomic.Int64
			client.onRegisterSegments = func() { firstRead.CompareAndSwap(0, read.Load()) }
			config := Config{InputMode: InputStream, SegmentDuration: 10 * time.Second, EssenceStorage: media.EssenceStorageMuxed,
				StagingByteBudget: 4 << 20, TempDirectory: t.TempDir()}
			pipeline, err := New(config, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(t.Context(), []source.Item{item})
			if err != nil || batch.Succeeded != 1 {
				t.Fatalf("run: %v, results: %+v", err, batch.Results)
			}
			if info.Size() <= config.StagingByteBudget || firstRead.Load() == 0 || firstRead.Load() >= info.Size() {
				t.Fatalf("input=%d budget=%d source bytes at first registration=%d", info.Size(), config.StagingByteBudget, firstRead.Load())
			}
		})
	}
}

func TestStreamModeFallbackAndPassthrough(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "media") }))
	defer server.Close()
	items, err := source.New(source.Config{}).Resolve(t.Context(), []string{server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []InputMode{InputAuto, InputStream, InputStage} {
		client := newFakeClient()
		pipeline, err := New(Config{InputMode: mode, SegmentDuration: time.Second}, client, fakeProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(t.Context(), items)
		if err != nil || (batch.Failed == 1) != (mode == InputStream) {
			t.Fatalf("mode=%s: %v, %+v", mode, err, batch.Results)
		}
		if mode == InputStream && client.putFlowCalls != 0 {
			t.Fatal("required streaming failure mutated TAMS")
		}
	}
	if _, err := New(Config{InputMode: InputStream, FFmpegArgs: []string{"-c", "copy"}}, newFakeClient(), fakeProber{}, countingSegmenter{objects: 1}, discardLogger(), nil); err == nil || !strings.Contains(err.Error(), "FFmpeg arguments") {
		t.Fatalf("passthrough conflict: %v", err)
	}
}

func TestStreamedVariableCadenceKeepsOnlyAValidPrefix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		frame   int
		fail    bool
		uploads int
	}{
		{"variable in first segment", 12, false, 0},
		{"variable later", 75, true, 1},
		{"variable at segment boundary", 50, true, 1},
		// The second segment was validated and pending when the third
		// contradicted the plan; it is committed rather than discarded.
		{"variable after a validated pending segment", 125, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := streamFixture(t, ".mkv", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=6",
				"-vf", fmt.Sprintf("setpts=PTS+if(gte(N\\,%d)\\,0.2/TB\\,0)", tc.frame),
				"-fps_mode", "vfr", "-c:v", "libx264", "-bf", "0", "-g", "50")
			item, _ := serveStreamFixture(t, filename, 0)
			client := newFakeClient()
			client.serviceDocument = map[string]any{"api_version": "8.2", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"}
			pipeline, err := New(Config{InputMode: InputStream, SegmentDuration: 2 * time.Second, EssenceStorage: media.EssenceStorageMuxed, TempDirectory: t.TempDir()},
				client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(t.Context(), []source.Item{item})
			if err != nil || (batch.Failed == 1) != tc.fail {
				t.Fatalf("run=%v, results=%+v", err, batch.Results)
			}
			if tc.fail {
				if client.uploads != tc.uploads || client.allocations != tc.uploads || !strings.Contains(batch.Results[0].Error, "contradicts Flow") {
					t.Fatalf("late cadence mutation: uploads=%d allocations=%d error=%s", client.uploads, client.allocations, batch.Results[0].Error)
				}
				for _, status := range client.flowStatusWrites {
					if status == flowStatusClosedComplete {
						t.Fatal("failed stream closed_complete")
					}
				}
			}
		})
	}
}

func TestStreamRevisionAndMetadataGuardExplicitFlowID(t *testing.T) {
	t.Parallel()
	const flowID = "2233e4e2-796e-4c40-990a-2b23bb7dce32"
	client := newFakeClient()
	client.flows[flowID] = map[string]any{"source_id": "source", "tags": map[string]any{media.TagPrefix + "input_revision": "old"}}
	pipeline := &Pipeline{client: client, config: Config{DryRunMode: DryRunOff}}
	_, err := pipeline.planFlowWrite(context.Background(), graphFlow{id: flowID, flow: map[string]any{"source_id": "source", "tags": map[string]any{media.TagPrefix + "input_revision": "new"}}})
	if err == nil || !strings.Contains(err.Error(), "different input revision") || client.putFlowCalls != 0 {
		t.Fatalf("revision guard: %v, writes=%d", err, client.putFlowCalls)
	}
	// A Flow written from staged bytes lets auto mode stage again instead of
	// failing; a Flow written from a streamed revision names its remedy.
	client.flows[flowID] = map[string]any{"source_id": "source", "tags": map[string]any{}}
	_, err = pipeline.planFlowWrite(context.Background(), graphFlow{id: flowID, flow: map[string]any{"source_id": "source", "tags": map[string]any{media.TagPrefix + "input_revision": "new"}}})
	var unavailable *source.StreamUnavailableError
	if !errors.As(err, &unavailable) || client.putFlowCalls != 0 {
		t.Fatalf("staged Flow streamed: %v, writes=%d", err, client.putFlowCalls)
	}
	client.flows[flowID] = map[string]any{"source_id": "source", "tags": map[string]any{media.TagPrefix + "input_revision": "old"}}
	_, err = pipeline.planFlowWrite(context.Background(), graphFlow{id: flowID, flow: map[string]any{"source_id": "source", "tags": map[string]any{}}})
	if err == nil || !strings.Contains(err.Error(), "--input-mode=stream") || client.putFlowCalls != 0 {
		t.Fatalf("streamed Flow staged: %v, writes=%d", err, client.putFlowCalls)
	}
	flow := tams.Flow{"source_id": "source", "tags": map[string]any{media.TagPrefix + "input_revision": "new"},
		"essence_parameters": map[string]any{"frame_rate": map[string]any{"numerator": 25, "denominator": 1}}}
	existing := maps.Clone(flow)
	existing["essence_parameters"] = map[string]any{"frame_rate": map[string]any{"numerator": 25, "denominator": 1}, "vfr": false}
	client.flows[flowID] = existing
	if _, err := pipeline.planFlowWrite(t.Context(), graphFlow{id: flowID, flow: flow}); err != nil {
		t.Fatalf("normal Flow GET defaults prevented resume: %v", err)
	}
	existing["essence_parameters"] = map[string]any{"frame_rate": map[string]any{"numerator": 30, "denominator": 1}}
	if _, err := pipeline.planFlowWrite(t.Context(), graphFlow{id: flowID, flow: flow}); err == nil || !strings.Contains(err.Error(), "/essence_parameters") {
		t.Fatalf("technical metadata guard: %v", err)
	}
}

func TestStreamingFallsBackBeforeMutationWhenInitialCadenceIsUnknown(t *testing.T) {
	filename := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=0.04", "-c:v", "libx264")
	item, _ := serveStreamFixture(t, filename, 0)
	for _, mode := range []InputMode{InputStream, InputAuto} {
		client := newFakeClient()
		config := Config{InputMode: mode, SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed, TempDirectory: t.TempDir()}
		pipeline, err := New(config, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(t.Context(), []source.Item{item})
		if err != nil || (batch.Failed == 1) != (mode == InputStream) {
			t.Fatalf("mode=%s run=%v results=%+v", mode, err, batch.Results)
		}
		if mode == InputStream && client.putFlowCalls != 0 {
			t.Fatal("unknown cadence mutated TAMS")
		}
		if mode == InputAuto && batch.Results[0].SHA256 == "" {
			t.Fatal("auto did not fall back to content-based staging")
		}
		entries, err := os.ReadDir(config.TempDirectory)
		if err != nil || len(entries) != 0 {
			t.Fatalf("fallback left temporary files: %v, %v", entries, err)
		}
	}
}

type unavailableAfterReadProber struct{ fakeProber }

func (unavailableAfterReadProber) Probe(_ context.Context, path string) (media.Probe, error) {
	if response, err := http.Get(path); err == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	return media.Probe{}, insufficientStreamEvidence()
}

func TestStreamFallbackCannotHideAnUpstreamFailure(t *testing.T) {
	t.Parallel()
	var staged atomic.Bool
	item := source.Item{URI: "https://media.example.test/input", Size: 10,
		Snapshot: func(context.Context) (*source.Snapshot, error) {
			return &source.Snapshot{Resource: "input", Revision: "one", Size: 10,
				OpenAt: func(context.Context, int64) (io.ReadCloser, error) { return nil, source.ErrSnapshotChanged }}, nil
		},
		Open: func(context.Context) (io.ReadCloser, error) {
			staged.Store(true)
			return io.NopCloser(strings.NewReader("changed input")), nil
		},
	}
	client := newFakeClient()
	pipeline, err := New(Config{InputMode: InputAuto, SegmentDuration: time.Second, TempDirectory: t.TempDir()},
		client, unavailableAfterReadProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(t.Context(), []source.Item{item})
	if err != nil || batch.Failed != 1 || staged.Load() || client.putFlowCalls != 0 {
		t.Fatalf("run=%v staged=%v writes=%d results=%+v", err, staged.Load(), client.putFlowCalls, batch.Results)
	}
}

// Explicit stream mode reports a refusal with its own stable code; auto mode
// stages the same input instead.
func TestExplicitStreamRefusalReportsStableCode(t *testing.T) {
	t.Parallel()
	for _, mode := range []InputMode{InputStream, InputAuto} {
		var staged atomic.Bool
		item := source.Item{URI: "https://media.example.test/input", Size: 10,
			Snapshot: func(context.Context) (*source.Snapshot, error) {
				return nil, &source.StreamUnavailableError{Reason: "HTTP input does not provide a strong ETag"}
			},
			Open: func(context.Context) (io.ReadCloser, error) {
				staged.Store(true)
				return io.NopCloser(strings.NewReader("0123456789")), nil
			},
		}
		client := newFakeClient()
		pipeline, err := New(Config{InputMode: mode, SegmentDuration: time.Second, TempDirectory: t.TempDir()},
			client, fakeProber{}, countingSegmenter{objects: 1}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := pipeline.Run(t.Context(), []source.Item{item})
		if err != nil {
			t.Fatal(err)
		}
		result := batch.Results[0]
		if mode == InputAuto {
			if batch.Failed != 0 || !staged.Load() {
				t.Fatalf("auto did not stage: %+v", result)
			}
			continue
		}
		if batch.Failed != 1 || staged.Load() || client.putFlowCalls != 0 || result.Failure == nil ||
			result.Failure.Code != FailureCodeStreamUnavailable || !result.Failure.ActionRequired {
			t.Fatalf("explicit stream refusal: staged=%v writes=%d result=%+v", staged.Load(), client.putFlowCalls, result)
		}
	}
}

// cadenceGapProber gives every segment fixed-cadence evidence except one,
// which carries no usable timestamps at all.
type cadenceGapProber struct {
	fakeProber
	gap string
}

func (p cadenceGapProber) Probe(ctx context.Context, filename string) (media.Probe, error) {
	probe, err := p.fakeProber.Probe(ctx, filename)
	if err != nil || !strings.HasPrefix(filepath.Base(filename), "segment-") {
		return probe, err
	}
	stream := &probe.Streams[0]
	stream.TimeBase = "1/25"
	stream.Presentation = media.PresentationSpan{Frames: 25, First: 0, Last: 24, LastDuration: 1, MinimumStep: 1, MaximumStep: 1}
	if strings.HasSuffix(filename, p.gap) {
		stream.Presentation = media.PresentationSpan{}
	}
	return probe, nil
}

// A later segment without cadence evidence is not a contradiction of the
// written plan: the declaration stands and the run completes.
func TestStreamedSegmentWithoutCadenceEvidenceKeepsTheDeclaredPlan(t *testing.T) {
	t.Parallel()
	item := source.Item{URI: "https://media.example.test/input", Size: 10,
		Snapshot: func(context.Context) (*source.Snapshot, error) {
			return &source.Snapshot{Resource: "input", Revision: "one", Size: 10,
				OpenAt: func(_ context.Context, offset int64) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("0123456789"[offset:])), nil
				}}, nil
		},
	}
	client := newFakeClient()
	pipeline, err := New(Config{InputMode: InputStream, SegmentDuration: time.Second, TempDirectory: t.TempDir()},
		client, cadenceGapProber{gap: "segment-00000002.mp4"}, countingSegmenter{objects: 4}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(t.Context(), []source.Item{item})
	if err != nil || batch.Failed != 0 || client.uploads != 4 {
		t.Fatalf("run=%v uploads=%d results=%+v", err, client.uploads, batch.Results)
	}
}

func TestStagedFlowStreamRefusalAndAutomaticFallback(t *testing.T) {
	filename := streamFixture(t, ".wav", "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=3", "-c:a", "pcm_s16le")
	item, _ := serveStreamFixture(t, filename, 0)
	client := newFakeClient()
	for _, mode := range []InputMode{InputStage, InputStream, InputAuto} {
		pipeline, err := New(Config{FlowID: "2233e4e2-796e-4c40-990a-2b23bb7dce32", InputMode: mode,
			SegmentDuration: time.Second, TempDirectory: t.TempDir()},
			client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		writes, uploads := client.putFlowCalls, client.uploads
		batch, err := pipeline.Run(t.Context(), []source.Item{item})
		if err != nil {
			t.Fatal(err)
		}
		result := batch.Results[0]
		if mode == InputStream {
			if batch.Failed != 1 || result.Failure == nil || result.Failure.Code != FailureCodeStreamUnavailable ||
				client.putFlowCalls != writes || client.uploads != uploads {
				t.Fatalf("stream refusal: failure=%+v result=%+v", result.Failure, result)
			}
		} else if batch.Failed != 0 || (mode == InputAuto && (result.Status != ResultStatusResumed || client.uploads != uploads)) {
			t.Fatalf("mode=%s result=%+v", mode, result)
		}
	}
}

// unsupportedCodecProber reports a codec TAMSin cannot map, so the operator
// override is the only source of the Flow's codec.
type unsupportedCodecProber struct{ fakeProber }

func (p unsupportedCodecProber) Probe(ctx context.Context, filename string) (media.Probe, error) {
	probe, err := p.fakeProber.Probe(ctx, filename)
	if err == nil {
		probe.Streams[0].CodecName = "dnxhd"
		if strings.HasPrefix(filepath.Base(filename), "segment-") {
			probe.Streams[0].TimeBase = "1/25"
			probe.Streams[0].Presentation = media.PresentationSpan{Frames: 25, First: 0, Last: 24, LastDuration: 1, MinimumStep: 1, MaximumStep: 1}
		}
	}
	return probe, nil
}

// An operator override that supplies what the media tools cannot derive is
// not contradicted by segments that also cannot derive it.
func TestStreamedSegmentsAcceptOperatorSuppliedCodec(t *testing.T) {
	t.Parallel()
	item := source.Item{URI: "https://media.example.test/input", Size: 10,
		Snapshot: func(context.Context) (*source.Snapshot, error) {
			return &source.Snapshot{Resource: "input", Revision: "one", Size: 10,
				OpenAt: func(_ context.Context, offset int64) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("0123456789"[offset:])), nil
				}}, nil
		},
	}
	client := newFakeClient()
	pipeline, err := New(Config{InputMode: InputStream, SegmentDuration: time.Second, TempDirectory: t.TempDir(),
		FlowMetadata: map[string]any{"codec": "video/x-dnxhd"}},
		client, unsupportedCodecProber{}, countingSegmenter{objects: 2}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(t.Context(), []source.Item{item})
	if err != nil || batch.Failed != 0 || client.uploads != 2 {
		t.Fatalf("run=%v uploads=%d results=%+v", err, client.uploads, batch.Results)
	}
}

func TestGeneratedLabelUsesTheSameDigestFormForEveryInputKind(t *testing.T) {
	t.Parallel()
	staged := generatedLabel("1a2b3c4d5e6f7a8b9c0d")
	streamed := generatedLabel("sha256:1a2b3c4d5e6f7a8b9c0d")
	if staged != "TAMSin 1a2b3c4d5e6f" || streamed != staged {
		t.Fatalf("labels: staged=%q streamed=%q", staged, streamed)
	}
}
