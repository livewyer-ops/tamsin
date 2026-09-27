package tamstime

import "testing"

func TestCanonicalTimestamp(t *testing.T) {
	t.Parallel()
	for value, want := range map[string]bool{
		"0:0": true, "12:500000000": true, "-12:5": true, "1:999999999": true, "-0:0": true,
		"+5:0": false, "05:0": false, "1:05": false, "1:0000000000": false, "1": false, "1:": false, ":1": false, "1.5": false, " 1:0": false,
	} {
		if got := CanonicalTimestamp(value); got != want {
			t.Errorf("CanonicalTimestamp(%q) = %t, want %t", value, got, want)
		}
	}
}
