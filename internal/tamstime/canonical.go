package tamstime

import (
	"regexp"
	"strings"
)

// timestampPattern is the TAMS timestamp.json pattern: a sign, seconds
// without leading zeros, a colon, and nanoseconds of at most nine digits
// without leading zeros.
var timestampPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*):(0|[1-9][0-9]{0,8})$`)

// CanonicalTimestamp reports whether value is spelled exactly as the TAMS
// schema allows. A lenient parser accepts "+5:0", "05:0" or "1:05" and
// silently reads the last as five nanoseconds; a value that will be written
// to a store must be the schema's own form.
func CanonicalTimestamp(value string) bool {
	return timestampPattern.MatchString(value)
}

// timeRangePattern is the TAMS timerange.json pattern.
var timeRangePattern = regexp.MustCompile(`^(\[|\()?(-?(0|[1-9][0-9]*):(0|[1-9][0-9]{0,8}))?(_(-?(0|[1-9][0-9]*):(0|[1-9][0-9]{0,8}))?)?(\]|\))?$`)

// SegmentTimeRange reports whether value can be the timerange of a stored
// Flow Segment: spelled as the schema allows, bounded at both ends, starting
// on an inclusive first sample and holding at least one instant. An instant
// and an inclusive end are both legal, and omitted markers take their
// defaults.
func SegmentTimeRange(value string) bool {
	if !timeRangePattern.MatchString(value) {
		return false
	}
	canonical, err := CanonicalTimeRange(value)
	if err != nil {
		return false
	}
	start, end, _ := strings.Cut(canonical, "_")
	return strings.HasPrefix(start, "[") && end != ""
}
