package tamstime

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// timestamp is a parsed TAMS Timestamp with the sign applied to the whole
// seconds-and-nanoseconds value, as the specification defines it.
type timestamp struct {
	negative    bool
	seconds     int64
	nanoseconds int64
}

func parseTimestamp(value string) (timestamp, error) {
	negative := strings.HasPrefix(value, "-")
	seconds, nanoseconds, err := ParseParts(strings.TrimPrefix(value, "-"))
	if err != nil {
		return timestamp{}, err
	}
	if seconds < 0 {
		return timestamp{}, errors.New("timestamp sign must precede the whole value")
	}
	if seconds == 0 && nanoseconds == 0 {
		negative = false
	}
	return timestamp{negative: negative, seconds: seconds, nanoseconds: nanoseconds}, nil
}

func (t timestamp) String() string {
	text := strconv.FormatInt(t.seconds, 10) + ":" + strconv.FormatInt(t.nanoseconds, 10)
	if t.negative {
		return "-" + text
	}
	return text
}

func (t timestamp) compare(other timestamp) int {
	if t.negative != other.negative {
		if t.negative {
			return -1
		}
		return 1
	}
	sign := 1
	if t.negative {
		sign = -1
	}
	switch {
	case t.seconds != other.seconds:
		if t.seconds < other.seconds {
			return -sign
		}
		return sign
	case t.nanoseconds != other.nanoseconds:
		if t.nanoseconds < other.nanoseconds {
			return -sign
		}
		return sign
	}
	return 0
}

// EqualTimeRanges reports whether two TimeRanges denote the same span. The
// specification defines several spellings for one span: an instantaneous
// range may be written [t] or [t_t], a leading zero or minus zero may be
// present, markers may be omitted, and a marker beside an omitted Timestamp
// is ignored. Two invalid strings are never equal, even when identical.
func EqualTimeRanges(left, right string) bool {
	a, err := CanonicalTimeRange(left)
	if err != nil {
		return false
	}
	b, err := CanonicalTimeRange(right)
	return err == nil && a == b
}

// CanonicalTimeRange rewrites a TimeRange in one spelling: canonical
// Timestamps, explicit markers, and the two-Timestamp form for an instant.
// Omitted markers default to an inclusive start and an exclusive end, which is
// how the reference library reads them.
func CanonicalTimeRange(value string) (string, error) {
	if value == "_" || value == "()" {
		return value, nil
	}
	invalid := fmt.Errorf("invalid TAMS timerange %q", value)
	start, end, pair := strings.Cut(value, "_")
	if !pair {
		inner := value
		if len(value) >= 2 && value[0] == '[' && value[len(value)-1] == ']' {
			inner = value[1 : len(value)-1]
		} else if strings.ContainsAny(value, "[]()") {
			return "", invalid
		}
		instant, err := parseTimestamp(inner)
		if err != nil {
			return "", invalid
		}
		return "[" + instant.String() + "_" + instant.String() + "]", nil
	}
	if start == "[" || start == "(" {
		start = ""
	}
	if end == "]" || end == ")" {
		end = ""
	}
	var first, last timestamp
	if start != "" {
		if start[0] != '[' && start[0] != '(' {
			start = "[" + start
		}
		var err error
		if first, err = parseTimestamp(start[1:]); err != nil {
			return "", invalid
		}
		start = start[:1] + first.String()
	}
	if end != "" {
		if marker := end[len(end)-1]; marker != ']' && marker != ')' {
			end += ")"
		}
		var err error
		if last, err = parseTimestamp(end[:len(end)-1]); err != nil {
			return "", invalid
		}
		end = last.String() + end[len(end)-1:]
	}
	if start != "" && end != "" {
		switch first.compare(last) {
		case 1:
			return "", invalid
		case 0:
			if start[0] != '[' || end[len(end)-1] != ']' {
				return "", invalid
			}
		}
	}
	return start + "_" + end, nil
}

// EqualTimestamps compares two Timestamps by value. An empty string is the
// schema default of 0:0, as for an omitted ts_offset.
func EqualTimestamps(left, right string) bool {
	if left == "" {
		left = "0:0"
	}
	if right == "" {
		right = "0:0"
	}
	a, err := parseTimestamp(left)
	if err != nil {
		return false
	}
	b, err := parseTimestamp(right)
	return err == nil && a.compare(b) == 0
}

// TimeRangesOverlap reports whether two TimeRanges share at least one instant,
// which is the selection the TAMS timerange query parameter makes. Invalid
// input is an error rather than "no overlap", so a caller deciding whether a
// registration is safe cannot mistake a malformed answer for a clear one.
func TimeRangesOverlap(left, right string) (bool, error) {
	a, err := CanonicalTimeRange(left)
	if err != nil {
		return false, err
	}
	b, err := CanonicalTimeRange(right)
	if err != nil {
		return false, err
	}
	if a == "()" || b == "()" {
		return false, nil
	}
	if a == "_" || b == "_" {
		return true, nil
	}
	aStart, aEnd, _ := strings.Cut(a, "_")
	bStart, bEnd, _ := strings.Cut(b, "_")
	return startReachesEnd(aStart, bEnd) && startReachesEnd(bStart, aEnd), nil
}

// startReachesEnd reports whether a canonical start bound lies at or before a
// canonical end bound, counting a shared instant only when both are inclusive.
func startReachesEnd(start, end string) bool {
	if start == "" || end == "" {
		return true
	}
	s, _ := parseTimestamp(start[1:])
	e, _ := parseTimestamp(end[:len(end)-1])
	cmp := s.compare(e)
	return cmp < 0 || (cmp == 0 && start[0] == '[' && end[len(end)-1] == ']')
}
