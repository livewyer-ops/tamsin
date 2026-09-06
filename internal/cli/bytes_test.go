package cli

import (
	"math"
	"strconv"
	"testing"
)

func TestParseByteSize(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		value string
		want  int64
	}{
		{value: "auto", want: 0},
		{value: "4096", want: 4096},
		{value: "1.5KiB", want: 1536},
		{value: "2GiB", want: 2 << 30},
		{value: "3GB", want: 3_000_000_000},
	} {
		t.Run(testCase.value, func(t *testing.T) {
			t.Parallel()
			got, err := parseByteSize(testCase.value)
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("parseByteSize(%q) = %d, want %d", testCase.value, got, testCase.want)
			}
		})
	}
}

func TestParseByteSizeRejectsUnsafeValues(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "0", "-1GiB", "12parsecs", "999999999999999999999PiB", strconv.FormatInt(math.MaxInt64, 10)} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			if _, err := parseByteSize(value); err == nil {
				t.Fatalf("parseByteSize(%q) unexpectedly succeeded", value)
			}
		})
	}
}
