package ingest

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
)

// ProfileVersion is bumped when a named profile changes media bytes or Flow
// semantics. Keeping the version explicit makes a profile a reproducible
// contract rather than a convenient alias whose meaning can drift in place.
const ProfileVersion = "1"

const (
	ProfilePreserve    = "preserve"
	ProfileEditorial   = "editorial"
	ProfileStreamingTS = "streaming-ts"
	ProfileCustom      = "custom"
)

const streamingSegmentDuration = 2 * time.Second

// Profile is the resolved media treatment. Name is custom when an override
// changes a named profile; Version then versions the custom-profile identity
// schema rather than promising that two arbitrary custom configurations match.
type Profile struct {
	Name            string
	Version         string
	SegmentDuration time.Duration
	SegmentFormat   media.SegmentFormat
	EssenceStorage  media.EssenceStorage
}

// ProfileOverrides distinguishes an explicit operator choice from a value
// inherited from a named profile. A nil field leaves that part of the profile
// alone. FFmpegArgs marks a custom treatment because those arguments can alter
// the generated media even when the three structured choices are unchanged.
type ProfileOverrides struct {
	SegmentDuration *time.Duration
	SegmentFormat   *media.SegmentFormat
	EssenceStorage  *media.EssenceStorage
	FFmpegArgs      bool
}

// ResolveProfile resolves a named, versioned policy and applies deliberate
// overrides. A matching override retains the named profile; a differing one is
// reported as custom while preserving the resolved settings.
func ResolveProfile(selection string, overrides ProfileOverrides) (Profile, error) {
	profile, err := namedProfile(selection)
	if err != nil {
		return Profile{}, err
	}
	custom := overrides.FFmpegArgs
	if overrides.SegmentDuration != nil {
		custom = custom || *overrides.SegmentDuration != profile.SegmentDuration
		profile.SegmentDuration = *overrides.SegmentDuration
	}
	if overrides.SegmentFormat != nil {
		custom = custom || *overrides.SegmentFormat != profile.SegmentFormat
		profile.SegmentFormat = *overrides.SegmentFormat
	}
	if overrides.EssenceStorage != nil {
		custom = custom || *overrides.EssenceStorage != profile.EssenceStorage
		profile.EssenceStorage = *overrides.EssenceStorage
	}
	if custom {
		profile.Name = ProfileCustom
	}
	return profile, nil
}

// TreatmentRequiresFFmpeg reports whether a resolved input-independent policy
// can write a representation rather than upload the source bytes untouched.
// Independent storage is conservative without an input: a multiplex requires
// FFmpeg extraction even when segment duration is zero.
func TreatmentRequiresFFmpeg(profile Profile) bool {
	return profile.SegmentDuration > 0 || profile.EssenceStorage == media.EssenceStorageIndependent
}

// ValidateTreatment rejects resolved combinations that are invalid or would
// silently ignore an operator's media-writing intent.
func ValidateTreatment(profile Profile, ffmpegArgs []string) error {
	if profile.SegmentDuration < 0 {
		return errors.New("segment duration cannot be negative")
	}
	if err := profile.SegmentFormat.Validate(); err != nil {
		return err
	}
	if err := profile.EssenceStorage.Validate(); err != nil {
		return err
	}
	if profile.SegmentDuration > 0 || profile.EssenceStorage == media.EssenceStorageIndependent {
		return nil
	}
	var unusable []string
	if len(ffmpegArgs) > 0 {
		unusable = append(unusable, "--ffmpeg-arg")
	}
	if profile.SegmentFormat.ContainerMIME() != "" {
		unusable = append(unusable, "--segment-format "+string(profile.SegmentFormat))
	}
	if len(unusable) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s cannot take effect without segmentation, because the input is stored as it stands; "+
			"set --segment-duration, or remove the option",
		strings.Join(unusable, " and "))
}

func namedProfile(selection string) (Profile, error) {
	name, version, versioned := strings.Cut(strings.ToLower(strings.TrimSpace(selection)), "@")
	if name == "" {
		return Profile{}, errors.New("ingest profile is required; use preserve, editorial, or streaming-ts")
	}
	if versioned && version == "" {
		return Profile{}, errors.New("ingest profile version cannot be empty")
	}
	version = strings.TrimPrefix(version, "v")
	if versioned && version == "" {
		return Profile{}, errors.New("ingest profile version cannot be empty")
	}
	if version != "" && version != ProfileVersion {
		return Profile{}, fmt.Errorf("unsupported ingest profile version %q: only version %s is available", version, ProfileVersion)
	}
	profile := Profile{
		Name: name, Version: ProfileVersion,
		SegmentFormat: media.SegmentFormatSource,
	}
	switch name {
	case ProfilePreserve:
		profile.EssenceStorage = media.EssenceStorageMuxed
	case ProfileEditorial:
		profile.SegmentDuration = 10 * time.Second
		profile.EssenceStorage = media.EssenceStorageIndependent
	case ProfileStreamingTS:
		profile.SegmentDuration = streamingSegmentDuration
		profile.SegmentFormat = media.SegmentFormatMPEGTS
		profile.EssenceStorage = media.EssenceStorageIndependent
	case ProfileCustom:
		return Profile{}, errors.New("custom is a resolved profile name, not a selectable profile; select a named profile and override it")
	default:
		return Profile{}, fmt.Errorf("unsupported ingest profile %q: use preserve, editorial, or streaming-ts", selection)
	}
	return profile, nil
}

func validateResolvedProfile(config Config) error {
	if config.Profile == ProfileCustom {
		return nil
	}
	expected, err := namedProfile(config.Profile + "@" + config.ProfileVersion)
	if err != nil {
		return err
	}
	if config.SegmentDuration != expected.SegmentDuration ||
		config.SegmentFormat != expected.SegmentFormat ||
		config.EssenceStorage != expected.EssenceStorage || len(config.FFmpegArgs) > 0 {
		return fmt.Errorf("resolved %s@%s settings do not match the named profile; resolve overrides as custom@%s", config.Profile, config.ProfileVersion, ProfileVersion)
	}
	return nil
}

// validateStreamingTSCodecs keeps the named MPEG-TS promise conservative. The
// FFmpeg muxer accepts more combinations than broadcast and streaming readers
// reliably consume, so compatibility is an explicit allow-list rather than an
// assumption that a successful mux is interoperable.
func validateStreamingTSCodecs(probe media.Probe) error {
	compatible := map[string]map[string]bool{
		"video": {"h264": true, "hevc": true, "mpeg2video": true},
		"audio": {"aac": true, "mp2": true, "mp3": true, "ac3": true, "eac3": true},
	}
	essences := 0
	for _, stream := range probe.Streams {
		if stream.Disposition.AttachedPicture != 0 {
			continue
		}
		essences++
		codecType := strings.ToLower(stream.CodecType)
		codec := strings.ToLower(stream.CodecName)
		if !compatible[codecType][codec] {
			return fmt.Errorf("stream %d codec %s/%s is outside the streaming-ts compatibility profile; supported video codecs are h264, hevc and mpeg2video, and supported audio codecs are aac, mp2, mp3, ac3 and eac3", stream.Index, codecType, codec)
		}
	}
	if essences == 0 {
		return errors.New("input has no audio or video essence supported by the streaming-ts profile")
	}
	return nil
}

// validateMuxerArguments rejects an FFmpeg argument that contradicts the
// structured container policy. Matching redundant values are accepted so an
// existing explicit profile is not rejected merely for spelling out the same
// muxer twice.
func validateMuxerArguments(arguments []string, segmented bool, containerMuxer string) error {
	outerMuxer := containerMuxer
	if segmented {
		outerMuxer = "segment"
	}
	for index := 0; index < len(arguments); index++ {
		name, value, hasValue := strings.Cut(arguments[index], "=")
		if name != "-f" && name != "-format" && name != "-segment_format" {
			continue
		}
		if !hasValue {
			if index+1 >= len(arguments) {
				return fmt.Errorf("FFmpeg argument %s requires a value", name)
			}
			index++
			value = arguments[index]
		}
		expected := outerMuxer
		if name == "-segment_format" {
			if !segmented {
				return errors.New("FFmpeg argument -segment_format is only valid when segmentation is enabled")
			}
			expected = containerMuxer
		}
		if expected == "" || !strings.EqualFold(value, expected) {
			return fmt.Errorf("FFmpeg argument %s=%s conflicts with the resolved container policy (expected %s); use --segment-format or a named profile", name, value, expected)
		}
	}
	return nil
}
