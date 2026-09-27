package ingest

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// measuredProber probes rendered Objects the way a packet measurement does:
// per-stream timing, sample counts and access points.
type measuredProber struct{ muxedProber }

func (p measuredProber) ProbeObject(ctx context.Context, filename string) (media.Probe, error) {
	probe, err := p.Probe(ctx, filename)
	if err != nil {
		return probe, err
	}
	probe.Format.StartTime, probe.Format.Duration = "", ""
	probe.Streams[0].StartTime, probe.Streams[0].Duration = "0", "1"
	probe.Streams[0].LastSampleTime, probe.Streams[0].SampleCount, probe.Streams[0].KeyFrames = "24/25", 25, 1
	probe.Streams[1].StartTime, probe.Streams[1].Duration = "0", "1"
	probe.Streams[1].LastSampleTime, probe.Streams[1].SampleCount, probe.Streams[1].KeyFrames = "46/47", 47, 47
	return probe, nil
}

func TestSegmentRegistrationCarriesKeyFramesAndLastDuration(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second},
		client, measuredProber{}, fakeSegmenter{}, discardLogger(), nil)
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
	defer client.lock.Unlock()
	segments := client.segments[batch.Results[0].RootFlowID]
	if len(segments) != 2 {
		t.Fatalf("segments = %#v", segments)
	}
	for _, segment := range segments {
		if segment.KeyFrameCount == nil || *segment.KeyFrameCount != 1 || segment.LastDuration != "0:40000000" {
			t.Fatalf("segment hints = %#v", segment)
		}
	}
}

func TestCollectedFlowMetadataTargetsOneEssence(t *testing.T) {
	t.Parallel()
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		t.Run(string(storage), func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "fixture.ts")
			if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			pipeline, err := New(Config{
				Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second, EssenceStorage: storage,
				CollectedFlowMetadata: map[string]tams.Flow{
					"audio": {"codec": "audio/x-smpte302m", "tags": map[string]any{"note": "operator"}},
				},
			}, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
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
			defer client.lock.Unlock()
			for _, flowResult := range batch.Results[0].Flows {
				flow := client.flows[flowResult.FlowID]
				tags, _ := flow["tags"].(map[string]any)
				switch flowResult.Role {
				case "audio":
					if flow["codec"] != "audio/x-smpte302m" || tags["note"] != "operator" || tags[media.ProvenanceSourcesTag] == nil {
						t.Fatalf("audio override not applied or provenance lost: %#v", flow)
					}
				case "video":
					if flow["codec"] != "video/h264" || tags["note"] != nil {
						t.Fatalf("video Flow changed by the audio override: %#v", flow)
					}
				default:
					if tags["note"] != nil {
						t.Fatalf("collector changed by a collected override: %#v", flow)
					}
				}
			}
		})
	}
}

func TestCollectedFlowMetadataRejectsUnknownRolesAndIdentityFields(t *testing.T) {
	t.Parallel()
	_, err := New(Config{CollectedFlowMetadata: map[string]tams.Flow{"audio": {"id": "x"}}, DryRunMode: DryRunExact},
		nil, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err == nil || !strings.Contains(err.Error(), "--collected-flow-metadata /audio /id") {
		t.Fatalf("identity override accepted: %v", err)
	}
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1, SegmentDuration: time.Second,
		CollectedFlowMetadata: map[string]tams.Flow{"audio 1": {"codec": "audio/x-smpte302m"}},
	}, client, muxedProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	result := batch.Results[0]
	if batch.Failed != 1 || result.Failure == nil || result.Failure.Code != FailureCodeMediaOptionsInvalid ||
		!strings.Contains(result.Error, `"audio 1"`) || !strings.Contains(result.Error, "video, audio") {
		t.Fatalf("unknown role should fail before mutation: %#v / %s", result.Failure, result.Error)
	}
	if len(client.flows) != 0 {
		t.Fatalf("Flows were written despite the invalid override: %#v", client.flows)
	}
}

// timecodeProber reports a QuickTime file whose last track is timecode.
type timecodeProber struct{}

func (timecodeProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{
		Format: media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "1.0", StartTime: "0.0", Tags: map[string]string{"major_brand": "qt  "}},
		Streams: []media.Stream{
			{Index: 0, CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{Index: 1, CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
			{Index: 2, CodecName: "tmcd", CodecType: "data"},
		},
	}, nil
}

func (timecodeProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

func (p timecodeProber) ProbeObject(ctx context.Context, filename string) (media.Probe, error) {
	// The rendered Object no longer holds the timecode track.
	probe, err := p.Probe(ctx, filename)
	probe.Streams = probe.Streams[:2]
	return probe, err
}

func (timecodeProber) ProbePresentation(context.Context, string, *media.Probe) error { return nil }

func TestTimecodeTrackIsLeftOutOfRendersAndReported(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mov")
	if err := os.WriteFile(filename, []byte("quicktime"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	client := newFakeClient()
	segmenter := &recordingSegmenter{}
	pipeline, err := New(Config{Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second},
		client, timecodeProber{}, segmenter, logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 {
		t.Fatalf("a timecode track must not sink the ingest: %#v", batch.Results[0].Failure)
	}
	roles := make([]string, 0, 3)
	for _, flowResult := range batch.Results[0].Flows {
		if flowResult.Kind == FlowKindEssence {
			roles = append(roles, flowResult.Role)
		}
	}
	if strings.Join(roles, ",") != "video,audio" {
		t.Fatalf("essence roles = %v", roles)
	}
	if len(segmenter.requests) != 1 || len(segmenter.requests[0].OmitStreams) != 1 || segmenter.requests[0].OmitStreams[0] != 2 {
		t.Fatalf("render did not leave the timecode track out: %#v", segmenter.requests)
	}
	message := logged.String()
	for _, evidence := range []string{"data track has no coding media type", "tmcd", "stream_index=2", "left out of the rendered"} {
		if !strings.Contains(message, evidence) {
			t.Fatalf("warning %q lacks %q", message, evidence)
		}
	}
	if strings.Contains(message, "outside Tamsin's supported profile") {
		t.Fatalf("a dropped data track is not an unsupported codec: %s", message)
	}
}

func TestEssenceCountIgnoresUndescribableDataTracks(t *testing.T) {
	t.Parallel()
	probe, err := timecodeProber{}.Probe(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got := essenceCount(probe); got != 2 {
		t.Fatalf("essence count = %d, want the video and audio only", got)
	}
	// A single essence beside a timecode track is a whole-file ingest under
	// independent storage, not a demultiplex of one stream.
	probe.Streams = []media.Stream{probe.Streams[0], probe.Streams[2]}
	if ffmpegWritesOutput(Config{EssenceStorage: media.EssenceStorageIndependent}, probe) {
		t.Fatal("a lone essence with a timecode track should not be demultiplexed")
	}
}
