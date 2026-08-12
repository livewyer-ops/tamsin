package tams

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// SpecMajor and SpecMinor are the TAMS API version Tamsin is written against.
// The pinned specification and vendored schemas in contracts/ are this version.
const (
	SpecMajor          = 8
	SpecMinor          = 2
	CompatibilityMinor = 1
)

// APIVersion is the version of the TAMS specification a service implements.
//
// It is deliberately distinct from service_version, which the specification
// describes as "intentionally permissive and intended to be informative only"
// and tells clients not to use for determining compatibility.
type APIVersion struct {
	Major int
	Minor int
}

func (v APIVersion) String() string {
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor)
}

// SupportsSpec reports whether a service at this version can be talked to by a
// client written against the pinned specification.
//
// A differing major version means incompatible changes, so it is refused. A
// newer minor version is accepted: minor revisions add to the API rather than
// change what is already there, and refusing them would make every client an
// obstacle to a service upgrade.
func (v APIVersion) SupportsSpec() bool {
	return v.Major == SpecMajor && v.Minor >= CompatibilityMinor
}

// Predates reports whether the service is an older minor revision than the one
// Tamsin targets, which is worth saying out loud because a request may then be
// rejected for using something the service has not implemented yet.
func (v APIVersion) Predates() bool { return v.Major == SpecMajor && v.Minor < SpecMinor }

// AtLeast reports whether a service implements the requested TAMS revision.
// Feature gates use this instead of guessing support from failed requests.
func (v APIVersion) AtLeast(major, minor int) bool {
	return v.Major > major || v.Major == major && v.Minor >= minor
}

// SupportsFlowProfiles reports whether the immutable Flow Profile API and the
// compact profile-backed Flow write representation are available.
func (v APIVersion) SupportsFlowProfiles() bool { return v.AtLeast(8, 2) }

// ParseAPIVersion reads api_version from a service document.
//
// The property is required by the specification and constrained to MAJOR.MINOR,
// so anything else is reported rather than guessed at.
func ParseAPIVersion(document map[string]any) (APIVersion, error) {
	raw, present := document["api_version"]
	if !present {
		return APIVersion{}, fmt.Errorf("service did not report api_version")
	}
	text, ok := raw.(string)
	if !ok {
		return APIVersion{}, fmt.Errorf("service reported a non-string api_version")
	}
	if !apiVersionPattern.MatchString(text) {
		return APIVersion{}, fmt.Errorf("service reported api_version %q, which is not MAJOR.MINOR", text)
	}
	major, minor, _ := strings.Cut(text, ".")
	parsedMajor, majorErr := strconv.Atoi(major)
	parsedMinor, minorErr := strconv.Atoi(minor)
	if majorErr != nil || minorErr != nil {
		return APIVersion{}, fmt.Errorf("service reported api_version %q, which is not MAJOR.MINOR", text)
	}
	return APIVersion{Major: parsedMajor, Minor: parsedMinor}, nil
}

// ServiceLimits are the lifetimes a service advertises for the things it hands
// a client, and they are scheduling instructions rather than trivia.
//
// A Media Object is collected if it is not registered against a Flow Segment in
// time, and a presigned URL stops working when it expires. A client that
// allocates everything up front, uploads for an hour and registers at the end
// is relying on neither of those happening, which the specification does not
// promise: the guaranteed minimums are five minutes and thirty seconds.
type ServiceLimits struct {
	// ObjectRegistration is how long a Media Object survives unregistered.
	ObjectRegistration time.Duration
	// PresignedURL is how long a generated URL is valid for, in both
	// directions. When the service does not say, this is the pinned
	// specification minimum rather than an unlimited zero value.
	PresignedURL time.Duration
}

const (
	MinimumObjectRegistration = 5 * time.Minute
	MinimumPresignedURL       = 30 * time.Second
)

var (
	apiVersionPattern        = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	timestampDurationPattern = regexp.MustCompile(`^(0|[1-9][0-9]*):(0|[1-9][0-9]{0,8})$`)
)

// ParseServiceLimits reads the advertised lifetimes from a service document.
//
// The ingest path allocates Objects and consumes generated PUT/GET URLs, so it
// cannot safely schedule either operation without bounded guarantees. Treating
// a missing or malformed value as zero used to mean "unlimited" to the batching
// code, inverting an invalid service document into the least safe schedule.
// min_object_timeout is required by the schema and therefore fails when absent;
// min_presigned_url_timeout is conditional, so absence uses its explicit pinned
// minimum while a present value is strictly validated.
func ParseServiceLimits(document map[string]any) (ServiceLimits, error) {
	object, err := requiredLifetime(document, "min_object_timeout", MinimumObjectRegistration)
	if err != nil {
		return ServiceLimits{}, err
	}
	presigned := MinimumPresignedURL
	if _, present := document["min_presigned_url_timeout"]; present {
		presigned, err = requiredLifetime(document, "min_presigned_url_timeout", MinimumPresignedURL)
		if err != nil {
			return ServiceLimits{}, err
		}
	}
	if presigned > object {
		return ServiceLimits{}, fmt.Errorf(
			"TAMS service /min_presigned_url_timeout (%s) exceeds /min_object_timeout (%s); TAMS 8.1 requires it to be no greater",
			formatDurationTimestamp(presigned), formatDurationTimestamp(object))
	}
	return ServiceLimits{ObjectRegistration: object, PresignedURL: presigned}, nil
}

func requiredLifetime(document map[string]any, field string, minimum time.Duration) (time.Duration, error) {
	value, present := document[field]
	if !present {
		return 0, fmt.Errorf("TAMS service /%s is required before Object ingest", field)
	}
	duration, err := parseTimestampDuration(value)
	if err != nil {
		return 0, fmt.Errorf("TAMS service /%s: %w", field, err)
	}
	if duration < minimum {
		return 0, fmt.Errorf("TAMS service /%s is %s; TAMS 8.1 requires at least %s",
			field, formatDurationTimestamp(duration), formatDurationTimestamp(minimum))
	}
	return duration, nil
}

// parseTimestampDuration reads a TAMS `seconds:nanoseconds` timestamp used as a
// duration.
func parseTimestampDuration(value any) (time.Duration, error) {
	text, ok := value.(string)
	if !ok {
		return 0, fmt.Errorf("must be a string timestamp in seconds:nanoseconds form")
	}
	if !timestampDurationPattern.MatchString(text) {
		return 0, fmt.Errorf("value %q is not a non-negative TAMS seconds:nanoseconds timestamp", text)
	}
	secondsText, nanosecondsText, _ := strings.Cut(text, ":")
	seconds, err := strconv.ParseInt(secondsText, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("seconds in %q exceed the supported range", text)
	}
	nanoseconds, err := strconv.ParseInt(nanosecondsText, 10, 64)
	if err != nil || nanoseconds >= int64(time.Second) {
		return 0, fmt.Errorf("nanoseconds in %q exceed the supported range", text)
	}
	// A value large enough to overflow is not a limit worth scheduling around.
	if seconds > int64(math.MaxInt64/int64(time.Second))-1 {
		return 0, fmt.Errorf("value %q exceeds the supported duration range", text)
	}
	return time.Duration(seconds)*time.Second + time.Duration(nanoseconds), nil
}

func formatDurationTimestamp(duration time.Duration) string {
	return fmt.Sprintf("%d:%d", duration/time.Second, duration%time.Second)
}
