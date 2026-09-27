package tamstime

import "regexp"

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
