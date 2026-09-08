package media

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/tamstime"
)

const nanosecondsPerSecond = tamstime.NanosecondsPerSecond

func ParseSeconds(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "N/A" {
		return 0, nil
	}
	rational, ok := new(big.Rat).SetString(value)
	if !ok {
		return 0, fmt.Errorf("invalid seconds value %q", value)
	}
	scaled := new(big.Rat).Mul(rational, big.NewRat(nanosecondsPerSecond, 1))
	numerator := scaled.Num()
	denominator := scaled.Denom()
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	// Round to the nearest nanosecond; ties round away from zero.
	if new(big.Int).Lsh(new(big.Int).Abs(remainder), 1).Cmp(denominator) >= 0 {
		if numerator.Sign() < 0 {
			quotient.Sub(quotient, big.NewInt(1))
		} else {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	if !quotient.IsInt64() {
		return 0, fmt.Errorf("seconds value %q exceeds supported timeline range", value)
	}
	return quotient.Int64(), nil
}
func ParseTimestamp(value string) (int64, error) {
	value = strings.TrimSpace(value)
	seconds, nanoseconds, err := tamstime.ParseParts(value)
	if err != nil {
		return 0, fmt.Errorf("invalid TAMS timestamp %q: %w", value, err)
	}
	total := new(big.Int).Mul(big.NewInt(seconds), big.NewInt(nanosecondsPerSecond))
	if strings.HasPrefix(value, "-") {
		total.Sub(total, big.NewInt(nanoseconds))
	} else {
		total.Add(total, big.NewInt(nanoseconds))
	}
	if !total.IsInt64() {
		return 0, fmt.Errorf("TAMS timestamp %q exceeds supported timeline range", value)
	}
	return total.Int64(), nil
}

func Timestamp(nanoseconds int64) string {
	seconds := nanoseconds / nanosecondsPerSecond
	remainder := nanoseconds % nanosecondsPerSecond
	sign := ""
	if nanoseconds < 0 {
		sign = "-"
		seconds = -seconds
		remainder = -remainder
	}
	return sign + strconv.FormatInt(seconds, 10) + ":" + strconv.FormatInt(remainder, 10)
}
func TimestampOffset(flowTimestamp, objectTimestamp int64) (int64, error) {
	offset := new(big.Int).Sub(big.NewInt(flowTimestamp), big.NewInt(objectTimestamp))
	if !offset.IsInt64() {
		return 0, errors.New("timestamp offset exceeds supported timeline range")
	}
	return offset.Int64(), nil
}

// TimestampShift applies a signed offset without allowing int64 wraparound to
// turn an invalid media timeline into a plausible but incorrect timestamp.
func TimestampShift(timestamp, offset int64) (int64, error) {
	shifted := new(big.Int).Add(big.NewInt(timestamp), big.NewInt(offset))
	if !shifted.IsInt64() {
		return 0, errors.New("shifted timestamp exceeds supported timeline range")
	}
	return shifted.Int64(), nil
}

func TimeRange(start, duration int64) (string, error) {
	if duration < 0 {
		return "", errors.New("duration cannot be negative")
	}
	if duration == 0 {
		return "[" + Timestamp(start) + "]", nil
	}
	end, err := TimestampShift(start, duration)
	if err != nil {
		return "", errors.New("timerange exceeds supported timeline range")
	}
	return "[" + Timestamp(start) + "_" + Timestamp(end) + ")", nil
}

// ParseAspectRatio reads an FFprobe aspect ratio, which is colon-separated
// rather than a fraction like the frame rates beside it. A ratio of 0:1 is
// FFprobe's way of saying the file did not state one.
func ParseAspectRatio(value string) (numerator, denominator int64, ok bool) {
	first, second, found := strings.Cut(strings.TrimSpace(value), ":")
	if !found {
		return 0, 0, false
	}
	numerator, numeratorErr := strconv.ParseInt(first, 10, 64)
	denominator, denominatorErr := strconv.ParseInt(second, 10, 64)
	if numeratorErr != nil || denominatorErr != nil || numerator <= 0 || denominator <= 0 {
		return 0, 0, false
	}
	return numerator, denominator, true
}

func ParseRate(value string) (numerator, denominator int64, ok bool) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 {
		return 0, 0, false
	}
	numerator, numeratorErr := strconv.ParseInt(parts[0], 10, 64)
	denominator, denominatorErr := strconv.ParseInt(parts[1], 10, 64)
	if numeratorErr != nil || denominatorErr != nil || numerator <= 0 || denominator <= 0 {
		return 0, 0, false
	}
	return numerator, denominator, true
}
