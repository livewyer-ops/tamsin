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

// tickRounding bounds how far whole nanoseconds put a time that falls on a
// tick of the 90 kHz MPEG-TS clock from that tick's own position: the start,
// the span, the tick and the rollover epoch each round by up to half a
// nanosecond.
const tickRounding = 2

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
// one another from untimedStart.
type timelineCursor struct {
	untimedStart    int64
	firstTimedStart int64
	position        int64
	lastManifestEnd int64
	lastTolerance   int64
	placed          bool
	logger          *slog.Logger
	// run is the cadence the reference stream of an MPEG-TS Flow has kept up
	// to the last placed Object. follows is set when the Object after that
	// one was found to begin with the next sample of it.
	run     cadence
	follows bool
}

func newTimelineCursor(untimedStart, firstTimedStart int64, logger *slog.Logger) *timelineCursor {
	return &timelineCursor{untimedStart: untimedStart, firstTimedStart: firstTimedStart, position: untimedStart, logger: logger}
}

// refuseUnsafeCut refuses a cut after the first that loses pictures. An
// Object that begins a video stream between its key frames cannot show that
// stream until the next one. An Object that stores leading pictures it does
// not show leaves a hole where they would have begun, because the Segment
// before it ends there.
func (c *timelineCursor) refuseUnsafeCut(record media.SegmentRecord, hiddenLead int, midGOP bool) error {
	if !record.Timed || !c.placed {
		return nil
	}
	if midGOP {
		return withFailure(FailureCodeMediaUnsupported, FailureMessageMediaCutMidGOP, true,
			fmt.Errorf("segment cut at %s begins a video stream between its key frames", media.Timestamp(record.Start)))
	}
	if hiddenLead != 0 {
		return openGOPCut(fmt.Errorf("segment cut at %s stores %d leading pictures of an open GOP that its MP4 Object does not show",
			media.Timestamp(record.Start), hiddenLead))
	}
	return nil
}

// refuseParameterSets refuses an MPEG-TS Object whose H.264 video a decoder
// given that Object alone cannot decode, or would decode wrongly. Every
// picture must find the parameter sets it refers to earlier in the Object.
// Where the renderer repeats the sets of the sample description before every
// key frame, the stream itself must not replace one: the stale set would
// follow it at the next key frame. Every such Object begins with the repeated
// sets, so the one in which the stream replaces a set carries that set with
// two contents.
func refuseParameterSets(record media.SegmentRecord, streams []media.Stream) error {
	for _, stream := range streams {
		switch sets := stream.ParameterSets; {
		case sets == nil:
		case sets.Missing:
			return withFailure(FailureCodeMediaUnsupported, FailureMessageMediaNoParameterSets, true,
				fmt.Errorf("the Object cut at %s holds pictures of stream %d without their H.264 parameter sets", media.Timestamp(record.Start), stream.Index))
		case record.RepeatsParameterSets && sets.Changed:
			return withFailure(FailureCodeMediaUnsupported, FailureMessageMediaParameterSetsChange, true,
				fmt.Errorf("the Object cut at %s changes the H.264 parameter sets of stream %d", media.Timestamp(record.Start), stream.Index))
		}
	}
	return nil
}

// place returns the Flow position for record and advances past the Segment's
// reference-stream span.
func (c *timelineCursor) place(record media.SegmentRecord, measured objectMeasurement) (int64, error) {
	if err := refuseParameterSets(record, measured.streams); err != nil {
		return 0, err
	}
	position := c.position
	if record.Timed {
		switch {
		case !c.placed:
			position = c.firstTimedStart
		default:
			if err := c.refuseUnsafeCut(record, measured.hiddenLead, measured.midGOP); err != nil {
				return 0, err
			}
			// The manifest entry starts at the frame the cut was made on. An
			// open GOP presents frames before that frame from the same Object,
			// so the Object's presentation begins lead earlier than its entry;
			// the gap to the previous Object is measured from there.
			gap := record.Start - measured.lead - c.lastManifestEnd
			switch {
			// The Objects' own timestamps place this one's first sample
			// where the Segment before it ends. The manifest rounds the
			// same instants by up to a tick each and is not asked again.
			case c.follows, absInt64(gap) <= c.lastTolerance:
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
		// Where the Object's first sample is, on the Flow and in the Object.
		first, sampled := position, m.referenceStart
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
		// at is where a tick of the Object's clock is on the Flow.
		origin := media.TransportTicks(first) - m.firstPTS
		at := func(tick int64) (int64, error) {
			after := tick - origin - m.firstPTS
			after = after/90000*int64(time.Second) + after%90000*int64(time.Second)/90000
			return media.TransportPosition(first+after, sampled+after)
		}
		expected := position + m.referenceSpan
		end, err := media.TransportPosition(expected, m.referenceStart+m.referenceSpan)
		if err != nil {
			return preparedObject{}, err
		}
		// A span counted in nanoseconds drifts from the clock by its rounding.
		// Where the reference stream ends on a tick, the Segment ends at that
		// tick's position, which is where an Object beginning on it is placed.
		settled := absInt64(end-expected) <= tickRounding
		c.follows = false
		if m.ticks && len(m.lastPTS) > 0 {
			if m.cyclePackets != 0 && (m.cyclePackets != c.run.packets || m.cycleTicks != c.run.ticks) {
				c.run = cadence{packets: m.cyclePackets, ticks: m.cycleTicks}
			}
			for _, pts := range m.lastPTS {
				c.run.add(origin + pts)
			}
			// A picture is shown from its own tick, and last_duration runs
			// from the last one's. A regularised stream counts its last sample
			// in periods from its first, which is not where the clock rounded
			// it to.
			if !m.audio {
				last, err := at(origin + m.lastPTS[len(m.lastPTS)-1])
				if err != nil {
					return preparedObject{}, err
				}
				m.lastSample = m.referenceStart + last - position
			}
			// The clock rounds every sample of a stream whose period is not a
			// whole number of ticks, and this Object's span cannot say which
			// way it rounded the next. The stream's cadence can: the Segment
			// ends on the tick the next sample is due on. Where the next
			// Object begins on that tick, or within the rounding of a cadence
			// too young to name it, the two abut. A step that is only observed
			// says where the next Object follows and no more: the last sample
			// of a stream that states another rate may be the shorter.
			if due, known := c.run.next(); known && !m.observed {
				if end, err = at(due); err != nil {
					return preparedObject{}, err
				}
				settled = true
			}
			if m.next != nil && m.next.ticks {
				next, err := media.TransportPosition(expected, m.next.referenceStart)
				if err != nil {
					return preparedObject{}, err
				}
				if c.follows = c.run.keeps(media.TransportTicks(next)); c.follows {
					end, settled = next, true
				}
			}
		}
		if moved := end - expected; settled {
			m.referenceSpan += moved
			if m.audio {
				// The last audio sample ends where the Segment does.
				m.lastSample += moved
			}
			// The last sample is shown until then, so the Object's own range
			// reaches as far.
			if past := m.referenceStart + m.referenceSpan - m.objectStart - m.duration; past > 0 {
				m.duration += past
			}
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

// cadence is the run of packets that have kept an MPEG-TS reference stream's
// period up to now: packets of them last ticks ticks exactly, and shown is
// when the latest were, in ticks of the 90 kHz clock counted along the Flow.
// The clock rounds every packet of a period that is not a whole number of
// ticks, so two packets are apart by their count of periods rounded down or
// up, and by exactly that many ticks where it is a whole number.
type cadence struct {
	packets, ticks int64
	shown          []int64
}

// keeps reports whether a packet shown at tick is the next of the run: the
// right distance from every packet the run remembers.
func (c *cadence) keeps(tick int64) bool {
	if c.packets == 0 || len(c.shown) == 0 {
		return false
	}
	for back := 1; back <= len(c.shown); back++ {
		least := int64(back) * c.ticks / c.packets
		most := least
		if int64(back)*c.ticks%c.packets != 0 {
			most++
		}
		if apart := tick - c.shown[len(c.shown)-back]; apart < least || apart > most {
			return false
		}
	}
	return true
}

// add takes the stream's next packet. One that does not keep the period
// begins a new run. A run remembers the packets either end of one cycle,
// which is as far back as a packet can be held to an exact tick, and no more
// than longestRun of them.
func (c *cadence) add(tick int64) {
	if !c.keeps(tick) {
		c.shown = c.shown[:0]
	}
	c.shown = append(c.shown, tick)
	if most := min(c.packets+1, longestRun); int64(len(c.shown)) > most {
		c.shown = c.shown[int64(len(c.shown))-most:]
	}
}

// longestRun bounds the packets a run remembers. A stated period whose cycle
// is longer is still held to, and never seen whole.
const longestRun = 1 << 12

// next is the tick the run's next packet is due on. It is known once the run
// has kept a whole cycle: a stated period names nothing until packets are
// seen to keep it.
func (c *cadence) next() (int64, bool) {
	if c.packets == 0 || int64(len(c.shown)) <= c.packets {
		return 0, false
	}
	return c.shown[int64(len(c.shown))-c.packets] + c.ticks, true
}

// pace settles the period an Object's pictures are held to, given the one
// its source states. The period the Object states stands where its own
// packets keep it, as where a render changed the rate. Otherwise the source's
// is taken: a whole source states its frame rate better than one short Object
// of it does, and FFprobe gives a lone H.264 picture its field rate. Where
// the Object's packets keep neither and are all one step apart, that step is
// taken, and marked as observed.
func (m *objectMeasurement) pace(packets, ticks int64) {
	if m.audio || m.paced(m.cyclePackets, m.cycleTicks) {
		return
	}
	if len(m.lastPTS) >= steadyPackets && !m.paced(packets, ticks) {
		if step := m.lastPTS[1] - m.lastPTS[0]; step > 0 && m.paced(1, step) {
			m.cyclePackets, m.cycleTicks, m.observed = 1, step, true
			return
		}
	}
	if packets != 0 {
		m.cyclePackets, m.cycleTicks = packets, ticks
	}
}

// steadyPackets is how many packets one step apart are taken to show that
// step as their period. It is a guard for the rates in common use and no
// proof of a period of whole ticks. The clock rounds 47.952 fps to seven
// equal steps in a row and 23.976 fps to three, but a period a little off
// whole ticks keeps equal steps for far longer: 332 at 29.97 fps written as
// 2997/100. An observed step only says where the next Object follows, so one
// that is a tick out leaves that Object placed as it would be without it.
const steadyPackets = 16

// paced reports whether the Object's own packets keep a period, which takes
// two of them to show.
func (m objectMeasurement) paced(packets, ticks int64) bool {
	run := cadence{packets: packets, ticks: ticks}
	for index, pts := range m.lastPTS {
		if index > 0 && !run.keeps(pts) {
			return false
		}
		run.add(pts)
	}
	return len(m.lastPTS) > 1
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
