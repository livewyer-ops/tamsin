package media

import (
	"math"
	"math/big"
	"strings"
	"testing"
)

func TestTimestampRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		text string
		ns   int64
	}{
		{text: "0:0", ns: 0},
		{text: "1:250000000", ns: 1_250_000_000},
		{text: "-0:400000000", ns: -400_000_000},
		{text: "-0:80000000", ns: -80_000_000},
		{text: "-1:600000000", ns: -1_600_000_000},
		{text: "-0:1", ns: -1},
		{text: "9223372036:854775807", ns: 1<<63 - 1},
		{text: "-9223372036:854775808", ns: -1 << 63},
		{text: "-2:0", ns: -2_000_000_000},
	}
	for _, testCase := range cases {
		t.Run(testCase.text, func(t *testing.T) {
			t.Parallel()
			actual, err := ParseTimestamp(testCase.text)
			if err != nil {
				t.Fatalf("ParseTimestamp() error = %v", err)
			}
			if actual != testCase.ns {
				t.Fatalf("ParseTimestamp() = %d, want %d", actual, testCase.ns)
			}
			if formatted := Timestamp(actual); formatted != testCase.text {
				t.Fatalf("Timestamp() = %q, want %q", formatted, testCase.text)
			}
		})
	}
}

func TestParseSecondsRoundsToNanosecond(t *testing.T) {
	t.Parallel()
	actual, err := ParseSeconds("1001/30000")
	if err != nil {
		t.Fatal(err)
	}
	if actual != 33_366_667 {
		t.Fatalf("ParseSeconds() = %d, want 33366667", actual)
	}
}

// TestRoundNanosecondsHoldsTheTimelineRange pins both ends of the range a
// timeline can hold, half a nanosecond past each, a fractional cadence and a
// large product of one.
func TestRoundNanosecondsHoldsTheTimelineRange(t *testing.T) {
	t.Parallel()
	// seconds is nanoseconds plus a number of half nanoseconds, in seconds.
	seconds := func(nanoseconds, halves int64) *big.Rat {
		numerator := new(big.Int).Lsh(big.NewInt(nanoseconds), 1)
		numerator.Add(numerator, big.NewInt(halves))
		return new(big.Rat).SetFrac(numerator, big.NewInt(2*nanosecondsPerSecond))
	}
	cadence := big.NewRat(1001, 30000)
	for _, testCase := range []struct {
		name    string
		seconds *big.Rat
		want    int64
		refused bool
	}{
		{name: "largest", seconds: seconds(math.MaxInt64, 0), want: math.MaxInt64},
		{name: "smallest", seconds: seconds(math.MinInt64, 0), want: math.MinInt64},
		{name: "just under half past the largest", seconds: new(big.Rat).Add(seconds(math.MaxInt64, 0), big.NewRat(49, 100*nanosecondsPerSecond)), want: math.MaxInt64},
		{name: "half past the largest", seconds: seconds(math.MaxInt64, 1), refused: true},
		{name: "half past the smallest", seconds: seconds(math.MinInt64, -1), refused: true},
		{name: "fractional cadence", seconds: cadence, want: 33_366_667},
		{name: "large product", seconds: new(big.Rat).Mul(cadence, big.NewRat(1<<32, 1)), want: 143_308_742_109_866_667},
		{name: "product past the range", seconds: new(big.Rat).Mul(cadence, big.NewRat(1<<40, 1)), refused: true},
	} {
		got, ok := roundNanoseconds(testCase.seconds)
		if ok == testCase.refused || (ok && got != testCase.want) {
			t.Errorf("%s: roundNanoseconds() = %d, %t; want %d, refused %t", testCase.name, got, ok, testCase.want, testCase.refused)
		}
	}
}

// A period or a span the timeline cannot hold is refused. It is never
// carried on as the value its low 64 bits happen to spell.
func TestReferenceTimingRefusesWhatTheTimelineCannotHold(t *testing.T) {
	t.Parallel()
	for name, stream := range map[string]Stream{
		"frame period": {CodecType: "video", StartTime: "0", Duration: "1", AverageFrameRate: "1/9223372037", SampleCount: 1},
		"nominal span": {CodecType: "video", StartTime: "0", Duration: "1", AverageFrameRate: "1/4000000000", SampleCount: 3},
	} {
		_, err := ProbeReference(Probe{Streams: []Stream{stream}})
		if err == nil || !strings.Contains(err.Error(), name+" exceeds supported timeline range") {
			t.Errorf("%s: ProbeReference() error = %v", name, err)
		}
	}
}

func TestTimeRange(t *testing.T) {
	t.Parallel()
	actual, err := TimeRange(-400_000_000, 1_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if actual != "[-0:400000000_0:600000000)" {
		t.Fatalf("TimeRange() = %q", actual)
	}
	instant, err := TimeRange(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if instant != "[0:0]" {
		t.Fatalf("instant TimeRange() = %q", instant)
	}
}
func TestTimestampOffsetRejectsOverflow(t *testing.T) {
	t.Parallel()
	offset, err := TimestampOffset(5, 3)
	if err != nil || offset != 2 {
		t.Fatalf("TimestampOffset(5, 3) = %d, %v", offset, err)
	}
	maximum := int64(^uint64(0) >> 1)
	if _, err := TimestampOffset(maximum, -1); err == nil {
		t.Fatal("overflowing timestamp offset unexpectedly succeeded")
	}
}

func TestTimestampShiftRejectsOverflow(t *testing.T) {
	t.Parallel()
	shifted, err := TimestampShift(5, -3)
	if err != nil || shifted != 2 {
		t.Fatalf("TimestampShift(5, -3) = %d, %v", shifted, err)
	}
	maximum := int64(^uint64(0) >> 1)
	if _, err := TimestampShift(maximum, 1); err == nil {
		t.Fatal("overflowing timestamp shift unexpectedly succeeded")
	}
}

func TestParseTimestampRejectsNonCanonicalNanoseconds(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"1", "1:-1", "1:1000000000", "x:0"} {
		if _, err := ParseTimestamp(value); err == nil {
			t.Fatalf("ParseTimestamp(%q) unexpectedly succeeded", value)
		}
	}
}
