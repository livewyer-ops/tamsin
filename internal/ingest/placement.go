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
// measured packet timing is exact to the nanosecond. A regularised reference
// stream is compared at half its sample period instead, because its container
// rounds timestamps, and no real gap in a fixed-rate stream is shorter than a
// sample.
const manifestTolerance = int64(time.Microsecond)

func boundaryTolerance(measured objectMeasurement) int64 {
	if measured.regular && measured.period/2 > manifestTolerance {
		return measured.period / 2
	}
	return manifestTolerance
}

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
// When a Segment's reference span was regularised, the manifest end it is
// compared against moves by the same amount, so a container that rounds
// timestamps is judged on the nominal timeline it was regularised to.
//
// Untimed records (a whole file, a whole essence) have no manifest and follow
// one another from untimedStart, as they always did.
type timelineCursor struct {
	untimedStart    int64
	firstTimedStart int64
	position        int64
	lastManifestEnd int64
	lastTolerance   int64
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
			// The manifest entry starts at the frame the cut was made on. An
			// open GOP presents frames before that frame from the same Object,
			// so the Object's presentation begins lead earlier than its entry;
			// the gap to the previous Object is measured from there.
			gap := record.Start - measured.lead - c.lastManifestEnd
			switch {
			case absInt64(gap) <= c.lastTolerance:
				position = c.position
			case gap > 0:
				position = c.position + gap
			default:
				return 0, fmt.Errorf("segment cut at %s presents %s before the previous segment's reference stream ends; the cut is not a stream access point",
					media.Timestamp(record.Start), media.Timestamp(-gap))
			}
			if c.logger != nil && measured.lead > 0 {
				c.logger.Debug("rendered segment presents frames before its cut", "cut", media.Timestamp(record.Start), "lead", media.Timestamp(measured.lead))
			}
		}
		// A regularised span moves the end the manifest is compared against by
		// the same amount; an unregularised one is compared as recorded.
		c.lastManifestEnd = record.End
		if measured.regular {
			c.lastManifestEnd += measured.referenceSpan - measured.measuredSpan
		}
		c.lastTolerance = boundaryTolerance(measured)
	}
	next, err := media.TimestampShift(position, measured.referenceSpan)
	if err != nil {
		return 0, err
	}
	c.position = next
	c.placed = true
	return position, nil
}

// object places interior cuts on the reference stream. At the programme
// boundaries the other tracks can begin earlier or finish later.
func (c *timelineCursor) object(flowID, path string, record media.SegmentRecord, m objectMeasurement) (preparedObject, error) {
	first := !c.placed
	previousEnd := c.position
	position, err := c.place(record, m)
	if err != nil {
		return preparedObject{}, err
	}
	if record.MPEGTS {
		position, err = media.TransportPosition(position, m.referenceStart)
		if err != nil {
			return preparedObject{}, err
		}
		// The TS clock resolves to one tick. Exclude decoder preroll and
		// any sub-tick rounding overlap at the previous boundary.
		if position < previousEnd {
			trim := previousEnd - position
			if trim >= m.referenceSpan {
				return preparedObject{}, fmt.Errorf("transport Object ends before its Flow interval")
			}
			m.referenceStart += trim
			m.referenceSpan -= trim
			position = previousEnd
		}
	}
	if record.Timed && record.StreamIndex == media.AllStreams {
		if first {
			lead := min(m.referenceStart-m.objectStart, position-c.untimedStart)
			if lead > 0 {
				position -= lead
				m.referenceStart -= lead
				m.referenceSpan += lead
			}
		}
		if record.Final {
			if tail := m.objectStart + m.duration - m.referenceStart - m.referenceSpan; tail > 0 {
				m.referenceSpan += tail
				m.lastSample = m.lastObjectSample
			}
		}
	}
	// TS cannot carry the MP4 edit that shortened the final AAC packet.
	// The manifest retains the input packet duration for audio-only cuts.
	if record.MPEGTS && record.Timed && record.ProgrammeDuration > 0 && m.audioPeriod > 0 {
		endRelative := record.ProgrammeDuration
		if m.audioOnly {
			endRelative = min(endRelative, record.End)
		}
		end, err := media.TimestampShift(record.FlowStart, endRelative)
		if err != nil {
			return preparedObject{}, err
		}
		tail := position + m.referenceSpan - end
		if tail > manifestTolerance && tail < m.referenceSpan {
			m.referenceSpan -= tail
			m.lastSample = m.referenceStart + m.referenceSpan - m.audioPeriod
		}
	}
	if record.MPEGTS {
		epoch, err := media.TransportEpoch(position)
		if err != nil {
			return preparedObject{}, err
		}
		reference := position - epoch
		shift := reference - m.referenceStart
		m.referenceStart = reference
		m.objectStart += shift
		m.lastSample += shift
	}
	c.position, err = media.TimestampShift(position, m.referenceSpan)
	if err != nil {
		return preparedObject{}, err
	}
	return placeObject(flowID, path, m, position)
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
