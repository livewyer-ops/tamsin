package tamstime

import "testing"

func TestEqualTimeRanges(t *testing.T) {
	t.Parallel()
	equal := [][2]string{
		{"[0:0]", "[0:0_0:0]"},
		{"[-0:0]", "[0:0]"},
		{"[01:0_2:0)", "[1:0_2:0)"},
		{"0:0", "[0:0]"},
		{"0:0_1:0", "[0:0_1:0)"},
		{"[_1:0)", "_1:0)"},
		{"[0:0_]", "[0:0_"},
		{"[-1:441667000_0:0)", "[-1:441667000_0:0)"},
		{"_", "_"},
		{"()", "()"},
	}
	for _, pair := range equal {
		if !EqualTimeRanges(pair[0], pair[1]) {
			t.Errorf("%q and %q should be equal", pair[0], pair[1])
		}
	}
	different := [][2]string{
		{"[0:0]", "[0:0_0:0)"},
		{"[0:0_1:0)", "[0:0_1:0]"},
		{"[-0:1]", "[0:1]"},
		{"0:0_1:0", "[0:0_1:0]"},
		{"(0:0", "[0:0]"},
		{"[1:0_0:0)", "[1:0_0:0)"},
		{"[--1:0]", "[--1:0]"},
		{"invalid", "invalid"},
	}
	for _, pair := range different {
		if EqualTimeRanges(pair[0], pair[1]) {
			t.Errorf("%q and %q should not be equal", pair[0], pair[1])
		}
	}
}

func TestCanonicalTimeRange(t *testing.T) {
	t.Parallel()
	for value, want := range map[string]string{
		"[5:0]":                         "[5:0_5:0]",
		"5:0":                           "[5:0_5:0]",
		"[-0:500000000_-0:0)":           "[-0:500000000_0:0)",
		"(1:0_":                         "(1:0_",
		"_2:5]":                         "_2:5]",
		"[1:0_2:0)":                     "[1:0_2:0)",
		"1:0_2:0":                       "[1:0_2:0)",
		"[10000000000:0_10000000001:0)": "[10000000000:0_10000000001:0)",
	} {
		got, err := CanonicalTimeRange(value)
		if err != nil || got != want {
			t.Errorf("CanonicalTimeRange(%q) = %q, %v; want %q", value, got, err, want)
		}
	}
	for _, value := range []string{"", "[", "[1:0", "[2:0_1:0)", "[1:0_1:0)", "(1:0_1:0]", "[1:x]", "[1:0_2:0]x"} {
		if _, err := CanonicalTimeRange(value); err == nil {
			t.Errorf("CanonicalTimeRange(%q) unexpectedly succeeded", value)
		}
	}
}

func TestTimeRangesOverlap(t *testing.T) {
	t.Parallel()
	for _, pair := range [][2]string{
		{"[0:0_1:0)", "[0:500000000_2:0)"},
		{"[1:0]", "[1:0_2:0)"},
		{"[0:0_1:0]", "[1:0_2:0)"},
		{"_", "[5:0]"},
		{"0:0_1:0", "[0:0]"},
		{"[0:0_", "[10000000000:0]"},
	} {
		if overlap, err := TimeRangesOverlap(pair[0], pair[1]); err != nil || !overlap {
			t.Errorf("%q and %q should overlap (%v)", pair[0], pair[1], err)
		}
	}
	for _, pair := range [][2]string{
		{"[0:0_1:0)", "[1:0_2:0)"},
		{"[0:0_1:0)", "()"},
		{"(1:0_2:0)", "[1:0]"},
		{"[0:0_1:0)", "[2:0_"},
	} {
		if overlap, err := TimeRangesOverlap(pair[0], pair[1]); err != nil || overlap {
			t.Errorf("%q and %q should not overlap (%v)", pair[0], pair[1], err)
		}
	}
	if _, err := TimeRangesOverlap("nonsense", "[0:0]"); err == nil {
		t.Error("an invalid range compared as if it were clear")
	}
}

func TestEqualTimestamps(t *testing.T) {
	t.Parallel()
	if !EqualTimestamps("", "0:0") || !EqualTimestamps("-0:0", "0:0") || !EqualTimestamps("01:5", "1:5") {
		t.Error("equivalent timestamps compared unequal")
	}
	if EqualTimestamps("0:1", "") || EqualTimestamps("-1:0", "1:0") || EqualTimestamps("x", "x") {
		t.Error("different or invalid timestamps compared equal")
	}
}
