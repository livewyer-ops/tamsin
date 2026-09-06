package tamstime

import "testing"

func TestParseParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       string
		seconds     int64
		nanoseconds int64
		wantError   bool
	}{
		{name: "zero", value: "0:0"},
		{name: "signed seconds", value: "-2:500000000", seconds: -2, nanoseconds: 500_000_000},
		{name: "upper nanosecond", value: "3:999999999", seconds: 3, nanoseconds: 999_999_999},
		{name: "missing separator", value: "3", wantError: true},
		{name: "invalid seconds", value: "no:1", wantError: true},
		{name: "invalid nanoseconds", value: "1:no", wantError: true},
		{name: "negative nanoseconds", value: "1:-1", wantError: true},
		{name: "nanoseconds overflow second", value: "1:1000000000", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			seconds, nanoseconds, err := ParseParts(test.value)
			if test.wantError {
				if err == nil {
					t.Fatalf("ParseParts(%q) unexpectedly succeeded", test.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseParts(%q) error = %v", test.value, err)
			}
			if seconds != test.seconds || nanoseconds != test.nanoseconds {
				t.Fatalf("ParseParts(%q) = (%d, %d), want (%d, %d)", test.value,
					seconds, nanoseconds, test.seconds, test.nanoseconds)
			}
		})
	}
}
