package media

import "testing"

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
