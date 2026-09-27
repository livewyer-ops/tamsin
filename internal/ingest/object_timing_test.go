package ingest

import (
	"context"
	"math"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
)

func TestEmittedObjectPresentationTiming(t *testing.T) {
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	ctx := context.Background()
	input := filepath.Join(t.TempDir(), "synthetic.mp4")
	command := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i",
		"testsrc2=size=128x96:rate=25:duration=24", "-f", "lavfi", "-i",
		"sine=frequency=440:sample_rate=48000:duration=24", "-c:v", "libx264",
		"-preset", "veryfast", "-g", "75", "-bf", "2", "-sc_threshold", "0",
		"-c:a", "aac", "-movflags", "+faststart", input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v: %s", err, output)
	}
	for _, format := range []media.SegmentFormat{media.SegmentFormatSource, media.SegmentFormatMPEGTS} {
		for _, stream := range []int{0, 1, media.AllStreams} {
			name := string(format) + "/video"
			switch stream {
			case 1:
				name = string(format) + "/audio"
			case media.AllStreams:
				name = string(format) + "/muxed"
			}
			t.Run(name, func(t *testing.T) {
				var records []media.SegmentRecord
				if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
					Input: input, Duration: 3 * time.Second, Format: format,
					SourceContainer: media.SegmentContainer{Muxer: "mp4", Extension: ".mp4"},
					StreamIndices:   []int{stream}, Directory: t.TempDir(),
				}, func(record media.SegmentRecord) error {
					records = append(records, record)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				const start = int64(5 * time.Second)
				pipeline, err := New(Config{Concurrency: 1, SegmentDuration: 3 * time.Second, Start: start},
					newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
				if err != nil {
					t.Fatal(err)
				}
				staged, cleanup, err := pipeline.prepareObjectsForStream(ctx, "test-flow",
					stagedFile{path: input}, media.FlowInfo{}, stream, start, records)
				defer cleanup()
				if err != nil {
					t.Fatal(err)
				}
				rolling := newRollingObjectPreparer(pipeline, "test-flow", stream, start, start)
				streamed := newRollingObjectPreparer(pipeline, "test-flow", stream, start, start)
				var previous *preparedObject
				for index, record := range records {
					object, err := rolling.prepare(ctx, record, nil)
					if err != nil {
						t.Fatal(err)
					}
					measured, err := pipeline.probeStreamSegment(ctx, record.Path)
					if err != nil {
						t.Fatal(err)
					}
					cached, err := streamed.prepare(ctx, record, &measured)
					if err != nil {
						t.Fatal(err)
					}
					for _, candidate := range []preparedObject{staged[index], cached} {
						if candidate.timerange != object.timerange || candidate.tsOffset != object.tsOffset ||
							candidate.objectTimerange != object.objectTimerange {
							t.Fatalf("timing differs across ingest paths: %#v / %#v", candidate, object)
						}
					}
					// ts_offset maps the reference stream's first presentation
					// timestamp onto the Segment start exactly; the Object's own
					// range may begin earlier when audio leads the cut.
					referenceStart, _, err := media.ProbeReference(measured)
					if err != nil {
						t.Fatal(err)
					}
					objectStart, _, err := media.ProbeTiming(measured)
					if err != nil {
						t.Fatal(err)
					}
					offset := int64(0)
					if object.tsOffset != "" {
						offset, err = media.ParseTimestamp(object.tsOffset)
						if err != nil {
							t.Fatal(err)
						}
					}
					if referenceStart+offset != object.start {
						t.Fatalf("object %d does not map to its Flow timeline", index)
					}
					if objectStart > referenceStart {
						t.Fatalf("object %d range starts after its reference stream", index)
					}
					// Segments follow one another exactly: a source without gaps
					// must not gain or lose time at any boundary.
					if previous != nil && object.start != previous.start+previous.duration {
						t.Fatalf("object %d starts at %d, previous ended at %d", index, object.start, previous.start+previous.duration)
					}
					if stream != 1 && (object.duration != int64(3*time.Second) || object.start != start+int64(index)*int64(3*time.Second)) {
						t.Fatalf("video-anchored object %d: start=%d duration=%d", index, object.start, object.duration)
					}
					previous = &object
				}
				// The Flow spans exactly the source: 24 s of video, or 24 s of
				// audio plus the one AAC frame the encoder appends.
				want := 24.0
				if stream == 1 {
					want += 1024.0 / 48000
				}
				if got := float64(rolling.flowPosition-start) / 1e9; math.Abs(got-want) > 0.000002 {
					t.Fatalf("duration = %.9f, want %.9f", got, want)
				}
			})
		}
	}
}
