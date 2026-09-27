package ingest

import (
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
)

func timed(start, end time.Duration) media.SegmentRecord {
	return media.SegmentRecord{Start: int64(start), End: int64(end), Timed: true}
}

func TestTimelineCursorFollowsTheSourceTimeline(t *testing.T) {
	t.Parallel()
	const flowStart = int64(10 * time.Second)
	// The reference stream begins 80 ms into the container (a reordering
	// delay); FFmpeg's first manifest entry still starts at the container
	// origin while its end, and every later entry, carry the 80 ms.
	cursor := newTimelineCursor(flowStart, flowStart+int64(80*time.Millisecond), discardLogger())
	// Audio leads the first cut by 13 ms, so the Object's union span is longer
	// than the video span; only the video span may advance the timeline.
	first, err := cursor.place(timed(0, 3080*time.Millisecond), objectMeasurement{
		duration: int64(3*time.Second + 13*time.Millisecond), referenceSpan: int64(3 * time.Second),
	})
	if err != nil || first != flowStart+int64(80*time.Millisecond) {
		t.Fatalf("first = %d, %v", first, err)
	}
	// The next entry starts where the previous one ended, within the
	// manifest's microsecond rounding: the Segments abut exactly.
	second, err := cursor.place(timed(3080*time.Millisecond+500*time.Nanosecond, 6080*time.Millisecond), objectMeasurement{
		duration: int64(3 * time.Second), referenceSpan: int64(3 * time.Second),
	})
	if err != nil || second != first+int64(3*time.Second) {
		t.Fatalf("second = %d, %v; want %d", second, err, first+int64(3*time.Second))
	}
	// A later start is a gap in the source and stays a gap on the Flow.
	third, err := cursor.place(timed(7080*time.Millisecond, 10080*time.Millisecond), objectMeasurement{
		duration: int64(3 * time.Second), referenceSpan: int64(3 * time.Second),
	})
	if err != nil || third != second+int64(4*time.Second) {
		t.Fatalf("third = %d, %v; want %d", third, err, second+int64(4*time.Second))
	}
	// An earlier start means frames presented before the cut: refuse rather
	// than describe overlapping Segments.
	_, err = cursor.place(timed(9*time.Second, 12*time.Second), objectMeasurement{referenceSpan: int64(3 * time.Second)})
	if err == nil || !strings.Contains(err.Error(), "not a stream access point") {
		t.Fatalf("overlapping cut accepted: %v", err)
	}
}

func TestTimelineCursorAccumulatesUntimedObjects(t *testing.T) {
	t.Parallel()
	const start = int64(2 * time.Second)
	cursor := newTimelineCursor(start, start, discardLogger())
	positions := make([]int64, 0, 3)
	for range 3 {
		position, err := cursor.place(media.SegmentRecord{}, objectMeasurement{referenceSpan: int64(time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		positions = append(positions, position)
	}
	if positions[0] != start || positions[1] != start+int64(time.Second) || positions[2] != start+int64(2*time.Second) {
		t.Fatalf("untimed positions = %v", positions)
	}
}

func TestPlaceObjectAnchorsTheReferenceStream(t *testing.T) {
	t.Parallel()
	// Audio leads video by 20 ms inside the Object: the union starts 20 ms
	// before the video, but ts_offset must map the video's first presentation
	// timestamp onto the Segment start, and the Segment covers the video span.
	measured := objectMeasurement{
		size: 1, checksum: "sum",
		objectStart: int64(1400 * time.Millisecond), duration: int64(3*time.Second + 20*time.Millisecond),
		referenceStart: int64(1420 * time.Millisecond), referenceSpan: int64(3 * time.Second),
	}
	object, err := placeObject("flow", "path", measured, int64(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if object.timerange != "[10:0_13:0)" || object.objectTimerange != "[1:400000000_4:420000000)" || object.tsOffset != "8:580000000" {
		t.Fatalf("placement = %s / %s / %s", object.timerange, object.objectTimerange, object.tsOffset)
	}
	if object.duration != int64(3*time.Second) || object.start != int64(10*time.Second) {
		t.Fatalf("segment span = %d at %d", object.duration, object.start)
	}
}

func TestTimelineCursorJudgesRegularisedSegmentsAtHalfAPeriod(t *testing.T) {
	t.Parallel()
	const (
		flowStart = int64(time.Hour)
		period    = int64(41666667)
		ten       = int64(10 * time.Second)
		short     = int64(9999 * time.Millisecond)
	)
	// A Matroska source: 240 frames at 24 fps measure 9.999 s and are
	// regularised to 10 s, and the manifest's ends are rounded the same way.
	regular := objectMeasurement{duration: short, referenceSpan: ten, measuredSpan: short, period: period, regular: true}
	cursor := newTimelineCursor(flowStart, flowStart, discardLogger())
	first, err := cursor.place(timed(0, 9999*time.Millisecond), regular)
	if err != nil || first != flowStart {
		t.Fatalf("first = %d, %v", first, err)
	}
	second, err := cursor.place(timed(10*time.Second, 19999*time.Millisecond), regular)
	if err != nil || second != first+ten {
		t.Fatalf("regularised segments should abut: second = %d, %v; want %d", second, err, first+ten)
	}
	// A frame missing between the cuts is a whole period, well past half a
	// period, so it is kept as a gap on the Flow.
	third, err := cursor.place(timed(20*time.Second+time.Duration(period), 29999*time.Millisecond+time.Duration(period)), regular)
	if err != nil || third != second+ten+period {
		t.Fatalf("dropped frame between cuts: third = %d, %v; want %d", third, err, second+ten+period)
	}

	// Without regularisation the tolerance stays at the manifest's rounding:
	// a millisecond short Object is followed by a millisecond gap, which is
	// what its own timestamps say.
	measured := objectMeasurement{duration: short, referenceSpan: short, measuredSpan: short, period: period}
	cursor = newTimelineCursor(flowStart, flowStart, discardLogger())
	if _, err = cursor.place(timed(0, 9999*time.Millisecond), measured); err != nil {
		t.Fatal(err)
	}
	next, err := cursor.place(timed(10*time.Second, 19999*time.Millisecond), measured)
	if err != nil || next != flowStart+ten {
		t.Fatalf("unregularised: next = %d, %v; want %d", next, err, flowStart+ten)
	}
}

func TestMeasuredObjectsCarrySegmentHints(t *testing.T) {
	t.Parallel()
	// A measured Object whose video regularises to a span past the rounded
	// timestamps of every stream: the Object's own range must grow to hold
	// the Segment, and the hints describe the reference stream.
	probe := media.Probe{Format: media.Format{StartTime: "0", Duration: "9.999"}, Streams: []media.Stream{
		{Index: 0, CodecType: "video", CodecName: "h264", AverageFrameRate: "24/1", StartTime: "0", Duration: "9999/1000",
			LastSampleTime: "9958/1000", SampleCount: 240, KeyFrames: 10},
		{Index: 1, CodecType: "audio", CodecName: "aac", SampleRate: "48000", StartTime: "0", Duration: "999/100",
			LastSampleTime: "9968/1000", SampleCount: 469, KeyFrames: 469},
	}}
	probe.Format.StartTime, probe.Format.Duration = "", ""
	measured, err := measureObject(probe, 1, "sum")
	if err != nil {
		t.Fatal(err)
	}
	const period = int64(41666667)
	if !measured.regular || !measured.measured || measured.referenceSpan != int64(10*time.Second) ||
		measured.measuredSpan != int64(9999*time.Millisecond) || measured.period != period || measured.keyFrames != 10 {
		t.Fatalf("measurement = %#v", measured)
	}
	if measured.duration != int64(10*time.Second) || measured.objectStart != 0 {
		t.Fatalf("object range must contain the regularised reference stream: %#v", measured)
	}
	object, err := placeObject("flow", "path", measured, int64(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if object.timerange != "[60:0_70:0)" || object.objectTimerange != "[0:0_10:0)" || object.tsOffset != "60:0" {
		t.Fatalf("placement = %#v", object)
	}
	if object.keyFrames != 10 || object.lastDuration != media.Timestamp(period) {
		t.Fatalf("hints = %d, %q", object.keyFrames, object.lastDuration)
	}

	// An unmeasured Object (a whole file) has no hints to offer.
	whole := objectMeasurement{size: 1, checksum: "sum", duration: int64(time.Second), referenceSpan: int64(time.Second)}
	if object, err = placeObject("flow", "path", whole, 0); err != nil {
		t.Fatal(err)
	}
	if object.keyFrames != 0 || object.lastDuration != "" {
		t.Fatalf("unmeasured object carries hints: %#v", object)
	}
}
