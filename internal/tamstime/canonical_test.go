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

func TestSegmentTimeRange(t *testing.T) {
	t.Parallel()
	for value, want := range map[string]bool{
		"[0:0_1:0)": true, "[0:0_1:0]": true, "[5:0]": true, "[5:0_5:0]": true, "5:0": true,
		"0:0_1:0": true, "[0:0_1:0": true, "0:0_1:0]": true, "[-1:500000000_0:0)": true, "[-0:0_0:1)": true,
		"": false, "_": false, "()": false, "[": false, "_1:0)": false, "[_1:0)": false, "[1:0_": false, "[1:0_)": false,
		"(1:0_2:0)": false, "(1:0_2:0]": false, "(5:0]": false, "[1:0_1:0)": false, "[2:0_1:0)": false,
		"[+2:0_3:0)": false, "[02:0_3:0)": false, "[1:+0_2:0)": false, "[1:05_2:0)": false, "[1:0_2:1000000000)": false,
		"[1:0_2:0)x": false, " [1:0_2:0)": false, "0:0-1:0": false, "[99999999999999999999:0_99999999999999999999:1)": false,
	} {
		if got := SegmentTimeRange(value); got != want {
			t.Errorf("SegmentTimeRange(%q) = %t, want %t", value, got, want)
		}
	}
}
