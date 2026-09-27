package ingest

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
)

// manifestTolerance bounds the disagreement accepted between the exact end of
// one Segment's reference stream and the position FFmpeg's manifest gives the
// next cut. FFmpeg writes the segment list with microsecond precision;
// measured packet timing is exact to the nanosecond.
const manifestTolerance = int64(time.Microsecond)

// timelineCursor places rendered Segments on the Flow timeline.
//
// The first timed Segment goes where the reference stream begins on the Flow:
// the Flow start plus the stream's offset from the container start, taken from
// the source probe. FFmpeg's manifest cannot anchor it, because the manifest's
// first entry starts at the container origin while later entries start at the
// reference stream's presentation timestamps, which sit later by any
// reordering delay. From the second Segment on, the manifest is consistent with
// itself: the difference between one entry's end and the next entry's start is
// the source gap between them. Adjacent Segments therefore abut exactly, a gap
// in the source stays a gap on the Flow, as AppNote 0012 asks, and a cut that
// starts before the previous Segment's reference stream ends is refused rather
// than described with overlapping Segments.
//
// Untimed records (a whole file, a whole essence) have no manifest and follow
// one another from untimedStart, as they always did.
type timelineCursor struct {
	untimedStart    int64
	firstTimedStart int64
	position        int64
	lastManifestEnd int64
	placed          bool
	logger          *slog.Logger
}

func newTimelineCursor(untimedStart, firstTimedStart int64, logger *slog.Logger) *timelineCursor {
	return &timelineCursor{untimedStart: untimedStart, firstTimedStart: firstTimedStart, position: untimedStart, logger: logger}
}

// place returns the Flow position for record and advances past the Segment's
// reference-stream span.
func (c *timelineCursor) place(record media.SegmentRecord, measured objectMeasurement) (int64, error) {
	position := c.position
	if record.Timed {
		switch {
		case !c.placed:
			position = c.firstTimedStart
		default:
			gap := record.Start - c.lastManifestEnd
			switch {
			case absInt64(gap) <= manifestTolerance:
				position = c.position
			case gap > 0:
				position = c.position + gap
			default:
				return 0, fmt.Errorf("segment cut at %s begins %s before the previous segment's reference stream ends; the cut is not a stream access point",
					media.Timestamp(record.Start), media.Timestamp(-gap))
			}
			// Frames presented before the cut make the Object's reference span
			// longer than the manifest's interval. Open-GOP sources do this; the
			// Segment is still placed at the cut, so the discrepancy is recorded.
			if c.logger != nil && measured.referenceSpan-(record.End-record.Start) > manifestTolerance {
				c.logger.Debug("rendered segment presents frames before its cut",
					"manifest_span", media.Timestamp(record.End-record.Start), "reference_span", media.Timestamp(measured.referenceSpan))
			}
		}
		c.lastManifestEnd = record.End
	}
	next, err := media.TimestampShift(position, measured.referenceSpan)
	if err != nil {
		return 0, err
	}
	c.position = next
	c.placed = true
	return position, nil
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
