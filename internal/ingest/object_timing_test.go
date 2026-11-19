package ingest

import (
	"context"
	"math"
	"os/exec"
	"path/filepath"
	"strings"
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
					reference, err := media.ProbeReference(measured)
					if err != nil {
						t.Fatal(err)
					}
					referenceStart := reference.Start
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

// TestMP4SegmentsFollowTheNominalRate requires 72-frame Objects at 24 fps
// to produce Segments of exactly three seconds that abut. Coarse timestamp
// regularisation is covered by the packet-level reference timing tests.
func TestMP4SegmentsFollowTheNominalRate(t *testing.T) {
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	ctx := context.Background()
	input := filepath.Join(t.TempDir(), "synthetic.mp4")
	command := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i",
		"testsrc2=size=128x96:rate=24:duration=12", "-f", "lavfi", "-i",
		"sine=frequency=440:sample_rate=48000:duration=12", "-c:v", "libx264",
		"-preset", "veryfast", "-g", "72", "-bf", "2", "-sc_threshold", "0",
		"-c:a", "aac", input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v: %s", err, output)
	}
	for _, stream := range []int{0, media.AllStreams} {
		name := "video"
		if stream == media.AllStreams {
			name = "muxed"
		}
		t.Run(name, func(t *testing.T) {
			var records []media.SegmentRecord
			if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
				Input: input, Duration: 3 * time.Second, Format: media.SegmentFormatSource, BitExact: true,
				SourceContainer: media.SegmentContainer{Muxer: "mp4", Extension: ".mp4"},
				StreamIndices:   []int{stream}, Directory: t.TempDir(),
			}, func(record media.SegmentRecord) error {
				records = append(records, record)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(records) != 4 {
				t.Fatalf("expected four 3 s segments, got %d", len(records))
			}
			const start = int64(5 * time.Second)
			pipeline, err := New(Config{Concurrency: 1, SegmentDuration: 3 * time.Second, Start: start},
				newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			objects, cleanup, err := pipeline.prepareObjectsForStream(ctx, "test-flow",
				stagedFile{path: input}, media.FlowInfo{}, stream, start, records)
			defer cleanup()
			if err != nil {
				t.Fatal(err)
			}
			const period = int64(41666667)
			for index, object := range objects {
				want := start + int64(index)*int64(3*time.Second)
				if object.start != want || object.duration != int64(3*time.Second) {
					t.Fatalf("object %d: start=%d duration=%d, want %d and 3 s", index, object.start, object.duration, want)
				}
				if object.keyFrames != 1 || object.lastDuration != media.Timestamp(period) {
					t.Fatalf("object %d hints: key frames %d, last duration %q", index, object.keyFrames, object.lastDuration)
				}
				// The Object's own range holds the regularised Segment.
				objectStart, err := media.ParseTimestamp(strings.TrimPrefix(strings.SplitN(object.objectTimerange, "_", 2)[0], "["))
				if err != nil {
					t.Fatal(err)
				}
				objectEnd, err := media.ParseTimestamp(strings.TrimSuffix(strings.SplitN(object.objectTimerange, "_", 2)[1], ")"))
				if err != nil {
					t.Fatal(err)
				}
				offset := int64(0)
				if object.tsOffset != "" {
					if offset, err = media.ParseTimestamp(object.tsOffset); err != nil {
						t.Fatal(err)
					}
				}
				if objectStart+offset > object.start || objectEnd+offset < object.start+object.duration {
					t.Fatalf("object %d range %s%s does not contain its segment %s", index, object.objectTimerange, object.tsOffset, object.timerange)
				}
			}
		})
	}
}

// TestOpenGOPSegmentsAbut renders an MPEG-2 long-GOP transport stream with
// two B-frames between anchors. FFmpeg cuts on the I-frame but the leading
// B-frames that present before it travel in the same Object, so the manifest
// shows an 80 ms gap at every boundary that is not a gap in the source. The
// Segments must abut and the Flow must span exactly the source.
func TestOpenGOPSegmentsAbut(t *testing.T) {
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	ctx := context.Background()
	input := filepath.Join(t.TempDir(), "opengop.ts")
	command := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i",
		"testsrc2=size=128x96:rate=25:duration=8", "-f", "lavfi", "-i",
		"sine=frequency=440:sample_rate=48000:duration=8",
		"-c:v", "mpeg2video", "-g", "12", "-bf", "2", "-c:a", "mp2", "-f", "mpegts", input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v: %s", err, output)
	}
	for _, stream := range []int{0, media.AllStreams} {
		name := "video"
		if stream == media.AllStreams {
			name = "muxed"
		}
		t.Run(name, func(t *testing.T) {
			var records []media.SegmentRecord
			if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
				Input: input, Duration: 2 * time.Second, Format: media.SegmentFormatSource, BitExact: true,
				SourceContainer: media.SegmentContainer{Muxer: "mpegts", Extension: ".ts"},
				StreamIndices:   []int{stream}, Directory: t.TempDir(),
			}, func(record media.SegmentRecord) error {
				records = append(records, record)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(records) < 3 {
				t.Fatalf("expected several segments, got %d", len(records))
			}
			// The scenario is real only if the manifest itself shows the
			// leading-frame gap between consecutive entries.
			manifestGaps := 0
			for index := 1; index < len(records); index++ {
				if records[index].Start > records[index-1].End {
					manifestGaps++
				}
			}
			if manifestGaps == 0 {
				t.Skip("this FFmpeg build wrote no leading-frame gaps into the manifest")
			}
			const start = int64(time.Minute)
			pipeline, err := New(Config{Concurrency: 1, SegmentDuration: 2 * time.Second, Start: start},
				newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			probe, err := pipeline.probePreparedInput(ctx, stagedFile{path: input})
			if err != nil {
				t.Fatal(err)
			}
			_, flowInfo, err := media.BuildFlow(probe, media.Identity{}, "video/mp2t", media.EssenceStorageMuxed)
			if err != nil {
				t.Fatal(err)
			}
			objects, cleanup, err := pipeline.prepareObjectsForStream(ctx, "test-flow",
				stagedFile{path: input}, flowInfo, stream, start, records)
			defer cleanup()
			if err != nil {
				t.Fatal(err)
			}
			var previous *preparedObject
			for index, object := range objects {
				if previous != nil && object.start != previous.start+previous.duration {
					t.Fatalf("object %d starts at %s, previous ended at %s (gap %s)", index, media.Timestamp(object.start),
						media.Timestamp(previous.start+previous.duration), media.Timestamp(object.start-previous.start-previous.duration))
				}
				previous = &object
			}
			last := objects[len(objects)-1]
			if span := last.start + last.duration - objects[0].start; span != int64(8*time.Second) {
				t.Fatalf("flow spans %s, want 8 s", media.Timestamp(span))
			}
		})
	}
}

// TestMidGOPCaptureIsAnchoredOnItsFirstKeyFrame builds a transport stream
// whose video begins with the tail of a GOP. Stream copy drops those frames,
// so the rendered video begins at the first key frame; the Flow must be
// anchored there, not at the first packet the probe reports.
func TestMidGOPCaptureIsAnchoredOnItsFirstKeyFrame(t *testing.T) {
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	ctx := context.Background()
	directory := t.TempDir()
	full := filepath.Join(directory, "full.ts")
	if output, err := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i",
		"testsrc2=size=128x96:rate=25:duration=6", "-f", "lavfi", "-i",
		"sine=frequency=440:sample_rate=48000:duration=6", "-c:v", "libx264", "-preset", "veryfast",
		"-g", "25", "-bf", "2", "-c:a", "aac", "-f", "mpegts", full).CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v: %s", err, output)
	}
	input := filepath.Join(directory, "midgop.ts")
	if output, err := exec.Command("ffmpeg", "-v", "error", "-ss", "1.4", "-i", full, "-c", "copy", "-copyinkf",
		"-f", "mpegts", input).CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v: %s", err, output)
	}
	keyFrame, found, err := (media.FFprobe{}).ProbeFirstKeyFrame(ctx, input, 0)
	if err != nil || !found {
		t.Fatalf("first key frame: %d, %t, %v", keyFrame, found, err)
	}
	const start = int64(time.Minute)
	pipeline, err := New(Config{Concurrency: 1, SegmentDuration: 2 * time.Second, Start: start},
		newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := pipeline.probePreparedInput(ctx, stagedFile{path: input})
	if err != nil {
		t.Fatal(err)
	}
	_, flowInfo, err := media.BuildFlow(probe, media.Identity{}, "video/mp2t", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	firstPacketOffset := flowInfo.ReferenceOffset
	if err := pipeline.anchorOnKeyFrames(ctx, input, probe, &flowInfo); err != nil {
		t.Fatal(err)
	}
	if flowInfo.ReferenceOffset != keyFrame-flowInfo.Start || flowInfo.ReferenceOffset <= firstPacketOffset {
		t.Fatalf("reference offset = %s, first packet %s, key frame %s from container start %s",
			media.Timestamp(flowInfo.ReferenceOffset), media.Timestamp(firstPacketOffset),
			media.Timestamp(keyFrame), media.Timestamp(flowInfo.Start))
	}
	for _, essence := range flowInfo.Collected {
		if essence.Flow["format"] == "urn:x-nmos:format:video" && essence.Offset != flowInfo.ReferenceOffset {
			t.Fatalf("video essence offset %s differs from the reference offset %s", media.Timestamp(essence.Offset), media.Timestamp(flowInfo.ReferenceOffset))
		}
	}
	var records []media.SegmentRecord
	if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
		Input: input, Duration: 2 * time.Second, Format: media.SegmentFormatSource, BitExact: true,
		SourceContainer: media.SegmentContainer{Muxer: "mpegts", Extension: ".ts"},
		StreamIndices:   []int{media.AllStreams}, Directory: t.TempDir(),
	}, func(record media.SegmentRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	objects, cleanup, err := pipeline.prepareObjectsForStream(ctx, "test-flow", stagedFile{path: input}, flowInfo, media.AllStreams, start, records)
	defer cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if objects[0].start != start+flowInfo.ReferenceOffset {
		t.Fatalf("first segment at %s, want the key frame at %s", media.Timestamp(objects[0].start), media.Timestamp(start+flowInfo.ReferenceOffset))
	}
}
