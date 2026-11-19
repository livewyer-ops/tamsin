package ingest

import (
	"bytes"
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
				const start = int64(5 * time.Second)
				var records []media.SegmentRecord
				if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
					Input: input, FlowStart: start, Duration: 3 * time.Second, Format: format,
					SourceContainer: media.SegmentContainer{Muxer: "mp4", Extension: ".mp4"},
					StreamIndices:   []int{stream}, Directory: t.TempDir(),
				}, func(record media.SegmentRecord) error {
					records = append(records, record)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
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
					if referenceStart+offset != object.start && !(format == media.SegmentFormatMPEGTS && stream == 1 && referenceStart+offset < object.start && object.start-(referenceStart+offset) <= int64(22*time.Millisecond)) {
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
				// The declared programme excludes the AAC encoder preroll.
				want := 24.0

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
				Input: input, FlowStart: int64(time.Minute), Duration: 2 * time.Second, Format: media.SegmentFormatSource, BitExact: true,
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
			videoStart := objects[0].start
			if stream == media.AllStreams {
				videoStart = start + flowInfo.ReferenceOffset
			}
			if span := last.start + last.duration - videoStart; absInt64(span-int64(8*time.Second)) > manifestTolerance {
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
		Input: input, FlowStart: start, Duration: 2 * time.Second, Format: media.SegmentFormatSource, BitExact: true,
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
	if objects[0].start != start {
		t.Fatalf("muxed programme starts at %d, want leading audio at %d", objects[0].start, start)
	}
	firstObject, err := pipeline.probeObject(ctx, records[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := media.ProbeReference(firstObject)
	if err != nil || absInt64(reference.Start-start-flowInfo.ReferenceOffset) > manifestTolerance {
		t.Fatalf("video begins at %d, want first key frame at %d: %v", reference.Start, start+flowInfo.ReferenceOffset, err)
	}
}

func TestMuxedPresentationIncludesAudioTailAndPreservesPriming(t *testing.T) {
	path := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25:duration=1", "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=3", "-c:v", "libx264", "-g", "25", "-bf", "0", "-c:a", "aac")
	var records []media.SegmentRecord
	err := (media.FFmpeg{}).Segment(t.Context(), media.SegmentRequest{Input: path, Duration: 2 * time.Second, SourceContainer: media.SegmentContainer{Muxer: "mp4", Extension: ".mp4"}, StreamIndices: []int{media.AllStreams}, Muxed: true, Directory: t.TempDir()}, func(r media.SegmentRecord) error { records = append(records, r); return nil })
	if err != nil || len(records) != 1 || !records[0].Final {
		t.Fatalf("render: %v %+v", err, records)
	}
	decoded := func(path string) []byte {
		cmd := exec.Command("ffmpeg", "-v", "error", "-i", path, "-map", "0:a:0", "-f", "s16le", "-")
		data, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if !bytes.Equal(decoded(path), decoded(records[0].Path)) {
		t.Fatal("decoded audio changed during stream copy")
	}
	probe, err := (media.FFprobe{}).ProbeObject(t.Context(), records[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	m, err := measureObject(probe, 1, "hash")
	if err != nil {
		t.Fatal(err)
	}
	o, err := newTimelineCursor(0, 0, nil).object("flow", records[0].Path, records[0], m)
	if err != nil || o.timerange != "[0:0_3:0)" || o.lastDuration != "0:20833" {
		t.Fatalf("audio tail: %+v %v", o, err)
	}
}

func TestTransportClockAcrossRollover(t *testing.T) {
	path := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25:duration=4", "-c:v", "libx264", "-g", "25", "-bf", "0")
	epoch, err := media.ParseSeconds("8589934592/90000")
	if err != nil {
		t.Fatal(err)
	}
	start := epoch - int64(1500*time.Millisecond)
	cursor := newTimelineCursor(start, start, nil)
	count := 0
	err = (media.FFmpeg{}).Segment(t.Context(), media.SegmentRequest{Input: path, Format: media.SegmentFormatMPEGTS, FlowStart: start, Duration: time.Second, StreamIndices: []int{media.AllStreams}, Directory: t.TempDir()}, func(r media.SegmentRecord) error {
		probe, err := (media.FFprobe{}).ProbeObject(t.Context(), r.Path)
		if err != nil {
			return err
		}
		m, err := measureObject(probe, 1, "hash")
		if err != nil {
			return err
		}
		o, err := cursor.object("flow", r.Path, r, m)
		if err != nil {
			return err
		}
		wantStart := start + int64(count)*int64(time.Second)
		wantOffset := ""
		if count >= 2 {
			wantOffset = media.Timestamp(epoch)
		}
		if absInt64(o.start-wantStart) > 11112 || o.tsOffset != wantOffset || o.duration != int64(time.Second) {
			t.Fatalf("segment %d clock: %+v, want start %d and offset %s", count, o, wantStart, wantOffset)
		}
		count++
		return nil
	})
	if err != nil || count != 4 {
		t.Fatalf("rollover render produced %d Objects: %v", count, err)
	}
}

func TestTransportAudioPrerollPrecedesFirstPicture(t *testing.T) {
	path := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25:duration=1",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=1",
		"-c:v", "libx264", "-bf", "0", "-c:a", "aac")
	var records []media.SegmentRecord
	err := (media.FFmpeg{}).Segment(t.Context(), media.SegmentRequest{
		Input: path, Format: media.SegmentFormatMPEGTS, Duration: 2 * time.Second,
		StreamIndices: []int{media.AllStreams}, Muxed: true, Directory: t.TempDir(),
	}, func(r media.SegmentRecord) error {
		records = append(records, r)
		return nil
	})
	if err != nil || len(records) != 1 {
		t.Fatalf("rendered %d Objects: %v", len(records), err)
	}
	// Check a reader's original timestamps before TAMSin unwraps the PES clock.
	// Delayed AAC interleaving can make preroll appear one clock rollover late.
	probe, err := (media.FFprobe{}).Probe(t.Context(), records[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(probe.Streams) != 2 {
		t.Fatalf("got %d streams, want video and audio", len(probe.Streams))
	}
	video, audio := probe.Streams[0].StartTime, probe.Streams[1].StartTime
	if video != "0.000000" || audio != "-0.021333" {
		t.Fatalf("video starts at %s and audio at %s, want zero and 1024 samples of AAC preroll at 48 kHz", video, audio)
	}
}
