package ingest

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestJSONValuesCompareByJSONSemantics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		generated any
		profile   any
		equal     bool
		path      string
	}{
		{
			name: "nested integer representations",
			generated: map[string]any{
				"segment_duration": map[string]any{"numerator": int64(10), "denominator": int32(1)},
				"essence_parameters": map[string]any{
					"frame_rate": map[string]any{"numerator": uint(25), "denominator": int(1)},
				},
			},
			profile: map[string]any{
				"essence_parameters": map[string]any{
					"frame_rate": map[string]any{"denominator": json.Number("1.0"), "numerator": json.Number("2.5e1")},
				},
				"segment_duration": map[string]any{"denominator": json.Number("1"), "numerator": json.Number("10")},
			},
			equal: true,
		},
		{name: "integral float", generated: float64(10), profile: json.Number("10.0"), equal: true},
		{name: "decimal float", generated: float64(0.1), profile: json.Number("1e-1"), equal: true},
		{name: "unequal numbers", generated: map[string]any{"value": int64(10)}, profile: map[string]any{"value": json.Number("11")}, path: "/value"},
		{name: "large integer", generated: int64(9_007_199_254_740_993), profile: json.Number("9007199254740993"), equal: true},
		{name: "large integer is not rounded float", generated: int64(9_007_199_254_740_993), profile: float64(9_007_199_254_740_993), path: ""},
		{name: "number and string", generated: json.Number("10"), profile: "10", path: ""},
		{name: "object order", generated: map[string]any{"a": true, "b": "value"}, profile: map[string]any{"b": "value", "a": true}, equal: true},
		{name: "missing object field", generated: map[string]any{"a": 1}, profile: map[string]any{"a": 1, "b": nil}, path: "/b"},
		{name: "additional object field", generated: map[string]any{"a": 1, "b": nil}, profile: map[string]any{"a": 1}, path: "/b"},
		{name: "equal arrays", generated: []int64{1, 2, 3}, profile: []any{json.Number("1"), json.Number("2.0"), json.Number("3e0")}, equal: true},
		{name: "array order", generated: []int{1, 2}, profile: []any{json.Number("2"), json.Number("1")}, path: "/0"},
		{name: "array length", generated: []int{1}, profile: []any{json.Number("1"), json.Number("2")}, path: "/1"},
		{name: "null and omitted", generated: map[string]any{}, profile: map[string]any{"value": nil}, path: "/value"},
		{name: "null and empty object", generated: map[string]any{"value": nil}, profile: map[string]any{"value": map[string]any{}}, path: "/value"},
		{name: "null and empty array", generated: map[string]any{"value": nil}, profile: map[string]any{"value": []any{}}, path: "/value"},
		{name: "escaped pointer", generated: map[string]any{"a/b~c": 1}, profile: map[string]any{"a/b~c": 2}, path: "/a~1b~0c"},
		{name: "malformed JSON number", generated: json.Number("01"), profile: json.Number("01"), path: ""},
		{name: "non-finite float", generated: math.Inf(1), profile: math.Inf(1), path: ""},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			mismatch := firstJSONValueMismatch("", testCase.generated, true, testCase.profile, true)
			if testCase.equal {
				if mismatch != nil {
					t.Fatalf("firstJSONValueMismatch(%#v, %#v) = %#v, want nil", testCase.generated, testCase.profile, mismatch)
				}
				return
			}
			if mismatch == nil {
				t.Fatalf("firstJSONValueMismatch(%#v, %#v) = nil, want path %q", testCase.generated, testCase.profile, testCase.path)
			}
			if mismatch.path != testCase.path {
				t.Fatalf("firstJSONValueMismatch(%#v, %#v).path = %q, want %q", testCase.generated, testCase.profile, mismatch.path, testCase.path)
			}
		})
	}
}

func TestEveryGoIntegerRepresentationMatchesJSONNumber(t *testing.T) {
	t.Parallel()
	for _, value := range []any{
		int(10), int8(10), int16(10), int32(10), int64(10),
		uint(10), uint8(10), uint16(10), uint32(10), uint64(10), uintptr(10),
		float32(10), float64(10),
	} {
		if !equalJSONValues(value, json.Number("1e1")) {
			t.Errorf("equalJSONValues(%T(%v), json.Number(%q)) = false, want true", value, value, "1e1")
		}
	}
	if !equalJSONValues(uint64(math.MaxUint64), json.Number("18446744073709551615")) {
		t.Fatalf("equalJSONValues(uint64(%d), json.Number(%q)) = false, want true", uint64(math.MaxUint64), "18446744073709551615")
	}
}

func TestJSONMismatchFormattingDistinguishesPresenceAndBoundsValues(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		value   any
		present bool
		want    string
	}{
		{name: "missing", value: nil, present: false, want: "<missing>"},
		{name: "null", value: nil, present: true, want: "null"},
		{name: "string", value: "10", present: true, want: `"10"`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := formatJSONMismatchValue(testCase.value, testCase.present); got != testCase.want {
				t.Errorf("formatJSONMismatchValue(%#v, %t) = %q, want %q", testCase.value, testCase.present, got, testCase.want)
			}
		})
	}

	got := formatJSONMismatchValue(strings.Repeat("x", 1_000), true)
	if runeCount := len([]rune(got)); runeCount != 160 || !strings.HasSuffix(got, "...") {
		t.Errorf("formatJSONMismatchValue(long string, true) = %q (runes=%d), want 160 runes ending in %q", got, runeCount, "...")
	}
}
