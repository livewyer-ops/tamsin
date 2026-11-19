package ingest

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func TestContainerPolicyAcrossInputTransports(t *testing.T) {
	for _, extension := range []string{".mp4", ".ts", ".mov", ".mkv", ".wav"} {
		filename := streamFixture(t, extension, "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=1", "-c:a", "aac")
		contents, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		// The suffix must neither admit an excluded container nor reject MP4/TS.
		misnamed := filepath.Join(t.TempDir(), "input.bin")
		if err := os.WriteFile(misnamed, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		for _, transport := range []string{"local", "stdin", "http-stage", "http-stream", "s3-stream"} {
			t.Run(extension+"/"+transport, func(t *testing.T) {
				item := localSource(misnamed)
				mode := InputStage
				if transport == "stdin" {
					items, err := newResolver(source.Config{Stdin: bytes.NewReader(contents), StdinName: "pretend.mp4"}).Resolve(t.Context(), []string{"-"})
					if err != nil {
						t.Fatal(err)
					}
					item = items[0]
				} else if strings.HasPrefix(transport, "http") || transport == "s3-stream" {
					item, _ = serveStreamFixture(t, misnamed, 0)
					if strings.HasSuffix(transport, "stream") {
						mode = InputStream
					}
					if transport == "s3-stream" {
						location, _ := url.Parse(item.URI)
						endpoint := location.Scheme + "://" + location.Host
						client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, func(options *s3.Options) {
							options.BaseEndpoint = aws.String(endpoint)
							options.UsePathStyle = true
						})
						items, err := newResolver(source.Config{S3Client: client, S3: source.S3Config{Endpoint: endpoint}}).Resolve(t.Context(), []string{"s3://bucket/pretend.mp4"})
						if err != nil {
							t.Fatal(err)
						}
						item = items[0]
					}
				}
				client := newFakeClient()
				pipeline, err := New(Config{InputMode: mode, SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed, TempDirectory: t.TempDir()}, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := pipeline.Run(t.Context(), []source.Item{item})
				if err != nil {
					t.Fatal(err)
				}
				if extension == ".mp4" || extension == ".ts" {
					if batch.Succeeded != 1 {
						t.Fatalf("supported container: %+v", batch.Results)
					}
				} else {
					if batch.Failed != 1 || batch.Results[0].Failure.Code != FailureCodeMediaUnsupported {
						t.Fatalf("excluded container: %+v", batch.Results)
					}
					if len(client.flows) != 0 || client.allocations != 0 || client.uploads != 0 {
						t.Fatal("excluded container mutated TAMS")
					}
				}
			})
		}
	}
}

func TestContainerOverridesMustMatchOutput(t *testing.T) {
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		for _, override := range []tams.Flow{{"container": "video/mp2t"}, {"container": "application/mxf"}, {"container": nil}} {
			filename := filepath.Join(t.TempDir(), "fixture.ts")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := newFakeClient()
			pipeline, err := New(Config{EssenceStorage: storage, SegmentDuration: time.Second, FlowMetadata: override}, client, muxedProber{}, &recordingSegmenter{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(t.Context(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			if override["container"] == "video/mp2t" {
				if batch.Succeeded != 1 {
					t.Fatalf("matching override: %+v", batch.Results)
				}
			} else if batch.Failed != 1 || len(client.flows) != 0 || client.uploads != 0 {
				t.Fatalf("conflicting override: %+v", batch.Results)
			}
		}
	}
}

func TestContainerOverridePrecedence(t *testing.T) {
	probe, _ := (muxedProber{}).Probe(t.Context(), "")
	for _, test := range []struct {
		name    string
		storage media.EssenceStorage
		root    tams.Flow
		role    tams.Flow
		wantErr bool
	}{
		{"absent", media.EssenceStorageIndependent, nil, nil, false},
		{"inherit", media.EssenceStorageIndependent, tams.Flow{"container": "video/mp2t"}, tams.Flow{"label": "Essence"}, false},
		{"inherit conflict", media.EssenceStorageIndependent, tams.Flow{"container": "audio/mp4"}, tams.Flow{"label": "Essence"}, true},
		{"role wins", media.EssenceStorageIndependent, tams.Flow{"container": "audio/mp4"}, tams.Flow{"container": "video/mp2t"}, false},
		{"role conflicts", media.EssenceStorageIndependent, tams.Flow{"container": "video/mp2t"}, tams.Flow{"container": "audio/mp4"}, true},
		{"role null", media.EssenceStorageIndependent, tams.Flow{"container": "video/mp2t"}, tams.Flow{"container": nil}, true},
		{"root null", media.EssenceStorageIndependent, tams.Flow{"container": nil}, nil, true},
		{"muxed root stays at root", media.EssenceStorageMuxed, tams.Flow{"container": "video/mp2t"}, tams.Flow{"label": "Essence"}, false},
		{"muxed role cannot own media", media.EssenceStorageMuxed, nil, tams.Flow{"container": "video/mp2t"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			flow, info, err := media.BuildFlow(probe, media.Identity{}, "video/mp2t", test.storage)
			if err != nil {
				t.Fatal(err)
			}
			pipeline := Pipeline{config: Config{
				EssenceStorage: test.storage, FlowMetadata: test.root,
				CollectedFlowMetadata: map[string]tams.Flow{"video": test.role, "audio": test.role},
			}}
			if err := pipeline.validateContainerOverrides(flow, info); (err != nil) != test.wantErr {
				t.Fatalf("container overrides: %v, want error=%t", err, test.wantErr)
			}
		})
	}
}

func TestSparseMP4SubtitlesSurviveRendering(t *testing.T) {
	subtitles := filepath.Join(t.TempDir(), "sparse.srt")
	if err := os.WriteFile(subtitles, []byte("1\n00:00:00,100 --> 00:00:00,700\nFirst cue\n\n2\n00:00:05,100 --> 00:00:05,700\nLast cue\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	filename := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25:duration=7", "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=7", "-i", subtitles,
		"-map", "0:v", "-map", "1:a", "-map", "2:s", "-c:v", "libx264", "-g", "50", "-bf", "2", "-c:a", "aac", "-c:s", "mov_text")
	for _, mode := range []InputMode{InputStage, InputStream} {
		for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
			t.Run(string(mode)+"/"+string(storage), func(t *testing.T) {
				item := localSource(filename)
				if mode == InputStream {
					item, _ = serveStreamFixture(t, filename, 0)
				}
				client := newFakeClient()
				pipeline, err := New(Config{InputMode: mode, SegmentDuration: 2 * time.Second, EssenceStorage: storage, TempDirectory: t.TempDir(), VerificationMode: VerificationReadback}, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				batch, err := pipeline.Run(t.Context(), []source.Item{item})
				if err != nil || batch.Succeeded != 1 {
					t.Fatalf("sparse subtitles: %v, %+v", err, batch.Results)
				}
				found, empty := false, false
				firstCue, lastCue := false, false
				for id, contents := range client.objects {
					firstCue = firstCue || bytes.Contains(contents, []byte("First cue"))
					lastCue = lastCue || bytes.Contains(contents, []byte("Last cue"))
					path := filepath.Join(t.TempDir(), id+".mp4")
					if err := os.WriteFile(path, contents, 0o600); err != nil {
						t.Fatal(err)
					}
					probe, err := (media.FFprobe{}).ProbeObject(t.Context(), path)
					if err != nil {
						t.Fatal(err)
					}
					for _, stream := range probe.Streams {
						if stream.CodecName == "mov_text" {
							found = true
							empty = empty || stream.Empty
						}
					}
				}
				if !firstCue || !lastCue {
					t.Fatal("subtitle cue payload was lost")
				}
				if !found || (storage == media.EssenceStorageMuxed && !empty) {
					t.Fatalf("subtitle descriptors retained=%t; empty subtitle Object exercised=%t", found, empty)
				}
			})
		}
	}
}
