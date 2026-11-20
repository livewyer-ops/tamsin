package ingest

import (
	"bytes"
	"context"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// placeRendered places rendered Segments as a segmented ingest does: through
// the rolling preparer, with a muxed render anchored where its reference
// stream begins.
func placeRendered(ctx context.Context, pipeline *Pipeline, flowInfo media.FlowInfo, stream int, start int64,
	records []media.SegmentRecord) ([]preparedObject, error) {
	reference := start
	if stream == media.AllStreams {
		reference += flowInfo.ReferenceOffset
	}
	preparer := newRollingObjectPreparer(pipeline, "test-flow", stream, start, reference)
	objects := make([]preparedObject, 0, len(records))
	for _, record := range records {
		object, err := preparer.prepare(ctx, record, nil)
		if err != nil {
			return objects, err
		}
		objects = append(objects, object)
	}
	return objects, nil
}

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
				pipeline := mustNew(t, Config{Concurrency: 1, SegmentDuration: 3 * time.Second, Start: start},
					newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
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
					if cached.timerange != object.timerange || cached.tsOffset != object.tsOffset ||
						cached.objectTimerange != object.objectTimerange {
						t.Fatalf("timing differs across ingest paths: %#v / %#v", cached, object)
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

				if got := float64(rolling.cursor.position-start) / 1e9; math.Abs(got-want) > 0.000002 {
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
				Input: input, Duration: 3 * time.Second, Format: media.SegmentFormatSource,
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
			pipeline := mustNew(t, Config{Concurrency: 1, SegmentDuration: 3 * time.Second, Start: start},
				newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			objects, err := placeRendered(ctx, pipeline, media.FlowInfo{}, stream, start, records)
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
// Segments must abut and the Flow must span exactly the source. The same
// video rendered as MP4 does not show those B-frames, so its second cut is
// refused instead of placed after a hole.
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
				Input: input, FlowStart: int64(time.Minute), Duration: 2 * time.Second, Format: media.SegmentFormatSource,
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
			pipeline := mustNew(t, Config{Concurrency: 1, SegmentDuration: 2 * time.Second, Start: start},
				newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			probe, err := pipeline.probePreparedInput(ctx, stagedFile{path: input}, "")
			if err != nil {
				t.Fatal(err)
			}
			_, flowInfo, err := media.BuildFlow(probe, media.Identity{}, "video/mp2t", media.EssenceStorageMuxed)
			if err != nil {
				t.Fatal(err)
			}
			objects, err := placeRendered(ctx, pipeline, flowInfo, stream, start, records)
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
		t.Run(name+" as MP4", func(t *testing.T) {
			mp4 := filepath.Join(t.TempDir(), "opengop.mp4")
			if output, err := exec.Command("ffmpeg", "-v", "error", "-i", input, "-c:v", "copy", "-c:a", "aac", mp4).CombinedOutput(); err != nil {
				t.Fatalf("fixture: %v: %s", err, output)
			}
			var records []media.SegmentRecord
			if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
				Input: mp4, Duration: 2 * time.Second, Format: media.SegmentFormatSource,
				SourceContainer: media.SegmentContainer{Muxer: "mp4", Extension: ".mp4"},
				StreamIndices:   []int{stream}, Directory: t.TempDir(),
			}, func(record media.SegmentRecord) error {
				records = append(records, record)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			pipeline := mustNew(t, Config{Concurrency: 1, SegmentDuration: 2 * time.Second},
				newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			probe, err := pipeline.probePreparedInput(ctx, stagedFile{path: mp4}, "")
			if err != nil {
				t.Fatal(err)
			}
			_, flowInfo, err := media.BuildFlow(probe, media.Identity{}, "video/mp4", media.EssenceStorageMuxed)
			if err != nil {
				t.Fatal(err)
			}
			objects, err := placeRendered(ctx, pipeline, flowInfo, stream, 0, records)
			failure := DescribeFailure(Result{}, err)
			if err == nil || failure.Code != FailureCodeMediaUnsupported || failure.Message != FailureMessageMediaOpenGOP ||
				!strings.Contains(err.Error(), "leading pictures of an open GOP") {
				t.Fatalf("placed %d MP4 Objects with %v (%+v), want the open-GOP refusal", len(objects), err, failure)
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
	const start = int64(time.Minute)
	pipeline := mustNew(t, Config{Concurrency: 1, SegmentDuration: 2 * time.Second, Start: start},
		newFakeClient(), media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
	probe, err := pipeline.probePreparedInput(ctx, stagedFile{path: input}, "")
	if err != nil {
		t.Fatal(err)
	}
	_, flowInfo, err := media.BuildFlow(probe, media.Identity{}, "video/mp2t", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	video := slices.IndexFunc(probe.Streams, func(stream media.Stream) bool { return stream.CodecType == "video" })
	at, found, err := (media.FFprobe{}).ProbeFirstKeyFrame(ctx, input, probe.Streams[video])
	if err != nil || !found {
		t.Fatalf("first key frame: %v, %t, %v", at, found, err)
	}
	keyFrame, err := media.PresentationOffset(probe, flowInfo.Start, at)
	if err != nil {
		t.Fatal(err)
	}
	firstPacketOffset := flowInfo.ReferenceOffset
	if err := pipeline.anchorOnKeyFrames(ctx, input, "mpegts", probe, &flowInfo); err != nil {
		t.Fatal(err)
	}
	if flowInfo.ReferenceOffset != keyFrame || flowInfo.ReferenceOffset <= firstPacketOffset {
		t.Fatalf("reference offset = %s, first packet %s, key frame %s after the container start",
			media.Timestamp(flowInfo.ReferenceOffset), media.Timestamp(firstPacketOffset), media.Timestamp(keyFrame))
	}
	for _, essence := range flowInfo.Collected {
		if essence.Flow["format"] == "urn:x-nmos:format:video" && essence.Offset != flowInfo.ReferenceOffset {
			t.Fatalf("video essence offset %s differs from the reference offset %s", media.Timestamp(essence.Offset), media.Timestamp(flowInfo.ReferenceOffset))
		}
	}
	var records []media.SegmentRecord
	if err := (media.FFmpeg{}).Segment(ctx, media.SegmentRequest{
		Input: input, FlowStart: start, Duration: 2 * time.Second, Format: media.SegmentFormatSource,
		SourceContainer: media.SegmentContainer{Muxer: "mpegts", Extension: ".ts"},
		StreamIndices:   []int{media.AllStreams}, Directory: t.TempDir(),
	}, func(record media.SegmentRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	objects, err := placeRendered(ctx, pipeline, flowInfo, media.AllStreams, start, records)
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

// TestMPEGTSSegmentsFollowTheTransportClock renders sources whose sample
// periods are not whole nanoseconds, and some not whole ticks of the 90 kHz
// clock, and holds every Segment to the clock its Object carries. A Segment
// begins on its Object's first sample, every sample of the Object falls
// inside it once ts_offset has restored the rollover count, Segments that
// abut in the source abut on the Flow, a gap in the source stays a gap, and
// the Flow ends where the last sample does. Each source is placed from a
// Flow start at zero and from one two seconds before the clock's 33-bit
// rollover.
func TestMPEGTSSegmentsFollowTheTransportClock(t *testing.T) {
	requireEncoders(t, "aac", "libx264")
	const (
		wrap = int64(1) << 33
		// rollover is how long the clock takes to wrap, to the nanosecond.
		rollover = int64(95443_717688889)
	)
	ticks := func(nanoseconds int64) int64 {
		return new(big.Int).Div(new(big.Int).Add(new(big.Int).Mul(big.NewInt(nanoseconds), big.NewInt(90000)), big.NewInt(500_000_000)), big.NewInt(1_000_000_000)).Int64()
	}
	// position is where a tick is on the Flow as AppNote 0002 restores it:
	// the rollovers before it, which ts_offset carries, then its 33 bits.
	position := func(tick int64) int64 {
		return tick/wrap*rollover + (tick%wrap*1_000_000_000+45000)/90000
	}
	video := func(rate string) string {
		return streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate="+rate+":duration=12",
			"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:scenecut=0:bframes=2")
	}
	audio := func(rate string) string {
		return streamFixture(t, ".mp4", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate="+rate+":duration=12", "-c:a", "aac")
	}
	// late shows every picture from the fourth key frame on that many ticks
	// later. MPEG-TS input is timed in the ticks themselves.
	late := func(filename string, ticks int) string {
		transport := streamFixture(t, ".ts", "-i", filename, "-c", "copy")
		shift := "+if(gte(N\\,150)\\," + strconv.Itoa(ticks) + "\\,0)"
		return streamFixture(t, ".ts", "-i", transport, "-c", "copy", "-bsf:v", "setts=pts=PTS"+shift+":dts=DTS"+shift)
	}
	ntsc, film := video("30000/1001"), video("24000/1001")
	for _, fixture := range []struct {
		name, filename, format string
		cut                    time.Duration
		// storage is independent unless it says otherwise.
		storage media.EssenceStorage
		// period is the sample period in ticks where it is a whole number of
		// them. Where it is not, the clock rounds each sample and period is
		// zero.
		period int64
		// priming is how many ticks before the Flow the first Object begins.
		priming int64
		// gap is the hole the source has at one boundary, in ticks.
		gap int64
		// samples counts the packets the Flow shows.
		samples int
		// duration is what the reference stream lasts, gap included, where
		// its last sample lasts a known time.
		duration int64
		// arguments and metadata make the render a custom one.
		arguments []string
		metadata  tams.Flow
	}{
		// 360 pictures of 3003 ticks.
		{name: "29.97 fps video", filename: ntsc, format: "urn:x-nmos:format:video", cut: time.Second, period: 3003, samples: 360, duration: 12_012_000_000},
		// An AAC frame of 1024 samples is 1920 ticks, and one frame primes the decoder.
		{name: "48 kHz audio", filename: audio("48000"), format: "urn:x-nmos:format:audio", cut: 2 * time.Second, period: 1920, priming: 1920, samples: 563, duration: 12_000_000_000},
		// Six pictures are missing.
		{name: "29.97 fps video with a gap", filename: late(ntsc, 18018), format: "urn:x-nmos:format:video", cut: time.Second, period: 3003, gap: 18018, samples: 360, duration: 12_212_200_000},
		// The shortest gap the clock can carry is not taken for its rounding.
		{name: "29.97 fps video with a gap of one tick", filename: late(ntsc, 1), format: "urn:x-nmos:format:video", cut: time.Second, period: 3003, gap: 1, samples: 360, duration: 12_012_011_111},
		// 288 pictures of 3753.75 ticks.
		{name: "23.976 fps video", filename: film, format: "urn:x-nmos:format:video", cut: time.Second, samples: 288, duration: 12_012_000_000},
		// 720 pictures of 1501.5 ticks.
		{name: "59.94 fps video", filename: video("60000/1001"), format: "urn:x-nmos:format:video", cut: time.Second, samples: 720, duration: 12_012_000_000},
		// An AAC frame is 2089.8 ticks, which FFmpeg rounds up and FFprobe down.
		{name: "44.1 kHz audio", filename: audio("44100"), format: "urn:x-nmos:format:audio", cut: 2 * time.Second, priming: 2090, samples: 517, duration: 12_000_000_000},
		// Six pictures are missing, which is 22522.5 ticks.
		{name: "23.976 fps video with a gap", filename: late(film, 22523), format: "urn:x-nmos:format:video", cut: time.Second, gap: 22523, samples: 288, duration: 12_262_255_556},
		// One tick is a gap here too, though the clock moves a picture by as much.
		{name: "23.976 fps video with a gap of one tick", filename: late(film, 1), format: "urn:x-nmos:format:video", cut: time.Second, gap: 1, samples: 288, duration: 12_012_011_111},
		// One picture an Object: the cadence is carried from each to the next.
		{name: "23.976 fps video of one picture a Segment", format: "urn:x-nmos:format:video", cut: 40 * time.Millisecond, samples: 12, duration: 500_500_000,
			filename: streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=24000/1001:duration=0.5", "-c:v", "libx264", "-preset", "veryfast", "-g", "1")},
		// 200 pictures of 1800 ticks, then 100 of 3600. The rate FFprobe states
		// for the whole source is one that no two pictures keep, and the rate
		// it states for an Object of the slower part is the faster one, so
		// that Object is paced by the step its own pictures take.
		{name: "50 fps video that slows to 25 fps", format: "urn:x-nmos:format:video", cut: time.Second, samples: 300,
			filename: streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=50:duration=8", "-vf", "select=if(lt(t\\,4)\\,1\\,not(mod(n\\,2)))", "-fps_mode", "vfr",
				"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:scenecut=0:bframes=0")},
		// 99 pictures of 3600 ticks, then 202 of 1800, the first of them one
		// before a key frame. The Object that ends there is not taken to run
		// a slow picture's length past its last.
		{name: "25 fps video that quickens to 50 fps before a cut", format: "urn:x-nmos:format:video", cut: time.Second, samples: 301,
			filename: streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=50:duration=8", "-vf", "select=if(lt(n\\,198)\\,not(mod(n\\,2))\\,1)", "-fps_mode", "vfr",
				"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:scenecut=0:bframes=0")},
		// A render that changes the rate is not paced by its source's: 20
		// pictures of 1800 ticks, one an Object, from a source of 25 fps.
		{name: "25 fps video rendered at 50 fps, one picture a Segment", format: "urn:x-nmos:format:video", storage: media.EssenceStorageMuxed,
			cut: 20 * time.Millisecond, period: 1800, samples: 20, duration: 400_000_000,
			filename:  streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=0.4", "-c:v", "libx264", "-preset", "veryfast"),
			arguments: []string{"-vf", "fps=50", "-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=1:min-keyint=1:scenecut=0:bframes=0"},
			metadata: tams.Flow{"codec": "video/h264", "essence_parameters": map[string]any{
				"frame_width": 128, "frame_height": 96, "frame_rate": map[string]any{"numerator": 50, "denominator": 1}}}},
		// An Object whose own pictures keep the rate it states is paced by
		// that rate: 144 pictures of 3753.75 ticks from a source of 25 fps,
		// 25 to an Object, so that each Object begins between two ticks.
		{name: "25 fps video rendered at 23.976 fps", format: "urn:x-nmos:format:video", storage: media.EssenceStorageMuxed,
			cut: time.Second, samples: 144, duration: 6_006_000_000,
			filename:  streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=6", "-c:v", "libx264", "-preset", "veryfast"),
			arguments: []string{"-vf", "fps=24000/1001", "-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=25:min-keyint=25:scenecut=0:bframes=2"},
			metadata: tams.Flow{"codec": "video/h264", "essence_parameters": map[string]any{
				"frame_width": 128, "frame_height": 96, "frame_rate": map[string]any{"numerator": 24000, "denominator": 1001}}}},
		// A multiplex is cut and placed on its video.
		{name: "23.976 fps video muxed with 44.1 kHz audio", format: "urn:x-nmos:format:multi", storage: media.EssenceStorageMuxed, cut: time.Second, samples: 288, duration: 12_012_000_000,
			filename: streamFixture(t, ".mp4", "-i", film, "-i", audio("44100"), "-map", "0:v", "-map", "1:a", "-c", "copy")},
	} {
		for _, start := range []int64{0, 95441_717688889} {
			t.Run(fixture.name+"/from "+media.Timestamp(start), func(t *testing.T) {
				client := newFakeClient()
				storage, stream := media.EssenceStorageIndependent, "v:0"
				if fixture.storage != "" {
					storage = fixture.storage
				}
				if fixture.format == "urn:x-nmos:format:audio" {
					stream = "a:0"
				}
				pipeline := mustNew(t, Config{Start: start, TempDirectory: t.TempDir(), SegmentDuration: fixture.cut,
					SegmentFormat: media.SegmentFormatMPEGTS, EssenceStorage: storage, FFmpegArgs: fixture.arguments, FlowMetadata: fixture.metadata},
					client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
				batch, err := pipeline.Run(t.Context(), []source.Item{localSource(fixture.filename)})
				if err != nil {
					t.Fatal(err)
				}
				if batch.Succeeded != 1 {
					t.Fatalf("run failed: %+v: %s", batch.Results[0].Failure, batch.Results[0].Error)
				}
				segments := flowSegments(t, client, fixture.format)
				if len(segments) < 5 {
					t.Fatalf("%d Segments, want several boundaries", len(segments))
				}
				directory := t.TempDir()
				previous, gaps, samples := start, 0, 0
				for index, segment := range segments {
					from, to := segmentBounds(t, segment.Timerange)
					switch {
					case from == previous:
					case fixture.gap > 0 && ticks(from)-ticks(previous) == fixture.gap:
						gaps++
					default:
						t.Fatalf("Segment %d begins at %s, %d ns from the end of the one before", index, media.Timestamp(from), from-previous)
					}
					previous = to
					first := ticks(from)
					if from != position(first) {
						t.Fatalf("Segment %d begins at %s, which is not a tick of the clock", index, media.Timestamp(from))
					}
					offset := ""
					if first >= wrap {
						offset = media.Timestamp(rollover)
					}
					if segment.TSOffset != offset {
						t.Fatalf("Segment %d has ts_offset %q, want %q", index, segment.TSOffset, offset)
					}
					if _, shown := segmentBounds(t, segment.ObjectTimerange); shown+first/wrap*rollover < to {
						t.Fatalf("Segment %d is %s, and its Object's own range %s ends before it", index, segment.Timerange, segment.ObjectTimerange)
					}
					path := filepath.Join(directory, strconv.Itoa(index)+".ts")
					client.lock.Lock()
					object := client.objects[segment.ObjectID]
					client.lock.Unlock()
					if err := os.WriteFile(path, object, 0o600); err != nil {
						t.Fatal(err)
					}
					output, err := exec.Command("ffprobe", "-v", "error", "-select_streams", stream, "-show_entries", "packet=pts", "-of", "csv=p=0", path).Output()
					if err != nil {
						t.Fatal(err)
					}
					// Each packet is read by how far after the Segment's first
					// tick it is shown, whichever side of a rollover FFprobe
					// reports it on. Every one is inside the Segment, and one
					// is on its first tick.
					earliest, latest := wrap, int64(-1)
					for _, line := range strings.Fields(string(output)) {
						pts, err := strconv.ParseInt(strings.TrimRight(line, ","), 10, 64)
						if err != nil {
							t.Fatal(err)
						}
						after := ((pts-first)%wrap + wrap) % wrap
						if index == 0 && fixture.priming > 0 && after == wrap-fixture.priming {
							continue
						}
						if at := position(first + after); after >= wrap/2 || at >= to {
							t.Fatalf("Segment %d is %s and its Object shows a sample at tick %d, %d ticks from its start", index, segment.Timerange, pts, after)
						}
						earliest, latest = min(earliest, after), max(latest, after)
						samples++
					}
					if earliest != 0 {
						t.Fatalf("Segment %d begins on tick %d, and its Object's first sample is %d ticks later", index, first%wrap, earliest)
					}
					// A multiplex's last Segment runs to the end of its last stream.
					if stream == "v:0" && (storage != media.EssenceStorageMuxed || index < len(segments)-1) {
						if last := media.Timestamp(to - position(first+latest)); segment.LastDuration != last {
							t.Fatalf("Segment %d has last_duration %s, and its last picture is shown %s before it ends", index, segment.LastDuration, last)
						}
					}
					if fixture.period > 0 && index < len(segments)-1 && (ticks(to)-first)%fixture.period != 0 {
						t.Fatalf("Segment %d spans %d ticks, want whole samples of %d", index, ticks(to)-first, fixture.period)
					}
				}
				if (fixture.gap > 0) != (gaps == 1) {
					t.Fatalf("the Segments leave %d gaps, want the one of %d ticks the source has, if it has one", gaps, fixture.gap)
				}
				if samples != fixture.samples {
					t.Fatalf("the Segments show %d samples, want %d", samples, fixture.samples)
				}
				// A period that is not whole ticks ends between two, and the Flow on the nearer.
				end, slack := start+fixture.duration, int64(0)
				if fixture.period == 0 {
					slack = 11112 / 2
				}
				if fixture.duration > 0 && (previous < end-slack || previous > end+slack) || previous != position(ticks(previous)) {
					t.Fatalf("the Flow ends at %s, want the tick at %s", media.Timestamp(previous), media.Timestamp(end))
				}
			})
		}
	}
}
