package cli

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// parseByteSize accepts the compact sizes operators use for ephemeral-storage
// limits. IEC suffixes are binary; SI suffixes are decimal. A bare integer is
// bytes. "auto" asks ingest to derive 80% of the staging filesystem's current
// free space.
func parseByteSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "auto") {
		return 0, nil
	}
	if value == "" {
		return 0, errors.New("staging byte budget cannot be empty; use auto or a size such as 80GiB")
	}

	boundary := 0
	seenDot := false
	for boundary < len(value) {
		character := value[boundary]
		switch {
		case character >= '0' && character <= '9':
			boundary++
		case character == '.' && !seenDot:
			seenDot = true
			boundary++
		default:
			goto parsedNumber
		}
	}

parsedNumber:
	if boundary == 0 || value[:boundary] == "." {
		return 0, fmt.Errorf("invalid staging byte budget %q; use auto or a size such as 80GiB", value)
	}
	number, err := strconv.ParseFloat(value[:boundary], 64)
	if err != nil || math.IsNaN(number) || math.IsInf(number, 0) || number <= 0 {
		return 0, fmt.Errorf("invalid staging byte budget %q; it must be positive", value)
	}
	unit := strings.ToLower(strings.TrimSpace(value[boundary:]))
	multipliers := map[string]float64{
		"": 1, "b": 1,
		"kb": 1_000, "mb": 1_000_000, "gb": 1_000_000_000, "tb": 1_000_000_000_000, "pb": 1_000_000_000_000_000,
		"kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30, "tib": 1 << 40, "pib": 1 << 50,
	}
	multiplier, ok := multipliers[unit]
	if !ok {
		return 0, fmt.Errorf("invalid staging byte budget unit %q; use B, KiB, MiB, GiB, TiB, or their SI equivalents", value[boundary:])
	}
	bytes := number * multiplier
	// float64(math.MaxInt64) rounds up to 2^63. Reject equality as well or
	// converting that rounded value to int64 wraps to a negative budget.
	if bytes >= float64(math.MaxInt64) || bytes < 1 {
		return 0, fmt.Errorf("staging byte budget %q is outside the supported byte range", value)
	}
	return int64(math.Round(bytes)), nil
}
