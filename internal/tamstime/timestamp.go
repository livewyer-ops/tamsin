// Package tamstime contains the shared lexical rules for TAMS timestamps.
package tamstime

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const NanosecondsPerSecond int64 = 1_000_000_000

// ParseParts parses the seconds and nanoseconds fields of a TAMS timestamp.
// Callers remain responsible for applying context-specific rules such as
// canonical formatting, whitespace handling, and whether negative seconds are
// meaningful.
func ParseParts(value string) (seconds, nanoseconds int64, err error) {
	secondsText, nanosecondsText, ok := strings.Cut(value, ":")
	if !ok {
		return 0, 0, errors.New("timestamp must contain seconds and nanoseconds")
	}
	seconds, err = strconv.ParseInt(secondsText, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse timestamp seconds: %w", err)
	}
	nanoseconds, err = strconv.ParseInt(nanosecondsText, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse timestamp nanoseconds: %w", err)
	}
	if nanoseconds < 0 || nanoseconds >= NanosecondsPerSecond {
		return 0, 0, errors.New("timestamp nanoseconds must be between 0 and 999999999")
	}
	return seconds, nanoseconds, nil
}
