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
