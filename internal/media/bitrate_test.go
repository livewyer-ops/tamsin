package media

import (
	"testing"
	"time"
)

const (
	megabyte = 1_000_000
	second   = int64(time.Second)
)

// TestSegmentBitRatesFollowTheAppNote pins the arithmetic AppNote 0013 defines,
// including the window the peak is measured over. These are the numbers a
// receiver sizes its buffer from, so being approximately right is not enough.
func TestSegmentBitRatesFollowTheAppNote(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		segments []SegmentMeasurement
		target   time.Duration
		wantAvg  int64
		wantMax  int64
	}{
		{
			// One megabyte per second is eight megabits per second.
			name: "even segments peak at their own rate",
			segments: []SegmentMeasurement{
				{Bytes: megabyte, Duration: second}, {Bytes: megabyte, Duration: second},
				{Bytes: megabyte, Duration: second}, {Bytes: megabyte, Duration: second},
			},
			target: time.Second, wantAvg: 8000, wantMax: 8000,
		},
		{
			// The peak is a single Segment, not the average across the Flow:
			// a buffer sized from the average would underflow on the third.
			name: "a burst raises the peak but not the average",
			segments: []SegmentMeasurement{
				{Bytes: megabyte, Duration: second}, {Bytes: megabyte, Duration: second},
				{Bytes: 3 * megabyte, Duration: second}, {Bytes: megabyte, Duration: second},
			},
			target: time.Second, wantAvg: 12000, wantMax: 24000,
		},
		{
			// A Segment longer than 1.5x the target falls outside the HLS
			// window. The AppNote extends the definition to count it alone, so
			// large-GOP segmentation still reports a peak.
			name: "an overlong segment still counts",
			segments: []SegmentMeasurement{
				{Bytes: megabyte, Duration: second},
				{Bytes: 5 * megabyte, Duration: 3 * second},
			},
			target: time.Second, wantAvg: 12000, wantMax: 13333,
		},
		{
			// Without segmentation there is no window, so each Segment stands
			// alone -- and there is only one.
			name:     "no target measures each segment alone",
			segments: []SegmentMeasurement{{Bytes: 2 * megabyte, Duration: second}},
			target:   0, wantAvg: 16000, wantMax: 16000,
		},
		{
			// Media shorter than half a target Segment fills no window at all.
			// Reporting a zero peak would be worse than reporting the average.
			name:     "media shorter than a window falls back to the average",
			segments: []SegmentMeasurement{{Bytes: megabyte, Duration: second}},
			target:   10 * time.Second, wantAvg: 8000, wantMax: 8000,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			average, peak, ok := SegmentBitRates(testCase.segments, testCase.target)
			if !ok {
				t.Fatal("bit rates could not be derived")
			}
			if average != testCase.wantAvg {
				t.Errorf("avg_bit_rate = %d, want %d", average, testCase.wantAvg)
			}
			if peak != testCase.wantMax {
				t.Errorf("max_bit_rate = %d, want %d", peak, testCase.wantMax)
			}
			if peak < average {
				t.Errorf("max_bit_rate %d is below avg_bit_rate %d, which cannot be true", peak, average)
			}
		})
	}
}

// TestSegmentBitRatesWithoutDurationAreNotInvented keeps the properties absent
// rather than wrong when there is nothing to divide by.
func TestSegmentBitRatesWithoutDurationAreNotInvented(t *testing.T) {
	t.Parallel()
	for _, segments := range [][]SegmentMeasurement{
		nil,
		{{Bytes: megabyte, Duration: 0}},
	} {
		if _, _, ok := SegmentBitRates(segments, time.Second); ok {
			t.Fatalf("derived a bit rate from %v, which has no duration", segments)
		}
	}
}

// TestSegmentBitRatesSurviveLargeInputs covers the overflow the intermediate
// product invites: bytes are multiplied by eight billion before dividing, which
// leaves int64 well before the file sizes a media store is built for.
func TestSegmentBitRatesSurviveLargeInputs(t *testing.T) {
	t.Parallel()
	// A terabyte over an hour.
	segments := []SegmentMeasurement{{Bytes: 1_000_000_000_000, Duration: 3600 * second}}
	average, peak, ok := SegmentBitRates(segments, 0)
	if !ok {
		t.Fatal("bit rates could not be derived")
	}
	const want = 1_000_000_000_000 * 8 / 3600 / 1000
	if average != want || peak != want {
		t.Fatalf("avg=%d max=%d, want %d for both", average, peak, want)
	}
}

func TestSegmentBitRateAccumulatorRetainsOnlyThePeakWindow(t *testing.T) {
	t.Parallel()
	accumulator := NewSegmentBitRateAccumulator(time.Second)
	for range 100_000 {
		accumulator.Add(SegmentMeasurement{Bytes: 1_000, Duration: int64(time.Second)})
		if retained := len(accumulator.window); retained > 2 {
			t.Fatalf("accumulator retained %d measurements outside its 1.5-second peak window", retained)
		}
	}
	average, peak, ok := accumulator.Result()
	if !ok || average != 8 || peak != 8 {
		t.Fatalf("100,000-segment rates = %d/%d valid=%v, want 8/8 true", average, peak, ok)
	}
}

func TestSegmentBitRateAccumulatorMatchesWholeSliceCalculation(t *testing.T) {
	t.Parallel()
	segments := make([]SegmentMeasurement, 200)
	for index := range segments {
		segments[index] = SegmentMeasurement{
			Bytes:    int64((index%11)+1) * 123_456,
			Duration: int64((index%7)+1) * int64(200*time.Millisecond),
		}
	}
	wantAverage, wantPeak, wantOK := referenceSegmentBitRates(segments, time.Second)
	accumulator := NewSegmentBitRateAccumulator(time.Second)
	for _, segment := range segments {
		accumulator.Add(segment)
	}
	average, peak, ok := accumulator.Result()
	if average != wantAverage || peak != wantPeak || ok != wantOK {
		t.Fatalf("streaming rates = %d/%d/%v, whole-slice reference = %d/%d/%v",
			average, peak, ok, wantAverage, wantPeak, wantOK)
	}
}

func referenceSegmentBitRates(segments []SegmentMeasurement, target time.Duration) (int64, int64, bool) {
	var totalBytes, totalDuration int64
	for _, segment := range segments {
		if segment.Duration > 0 {
			totalBytes += segment.Bytes
			totalDuration += segment.Duration
		}
	}
	if totalDuration <= 0 {
		return 0, 0, false
	}
	average := kilobitsPerSecond(totalBytes, totalDuration)
	lower, upper := int64(target)/2, int64(target)*3/2
	peak := int64(0)
	consider := func(bytes, duration int64) {
		if rate := kilobitsPerSecond(bytes, duration); rate > peak {
			peak = rate
		}
	}
	for start := range segments {
		var bytes, duration int64
		for _, segment := range segments[start:] {
			bytes += segment.Bytes
			duration += segment.Duration
			if duration > upper {
				if bytes == segment.Bytes && duration == segment.Duration {
					consider(bytes, duration)
				}
				break
			}
			if duration >= lower {
				consider(bytes, duration)
			}
		}
	}
	if peak == 0 {
		peak = average
	}
	return average, peak, true
}
