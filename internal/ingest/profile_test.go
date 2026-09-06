package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

type profilePolicyProber struct{ probe media.Probe }

func (p profilePolicyProber) Probe(context.Context, string) (media.Probe, error) {
	return p.probe, nil
}

func (profilePolicyProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

func TestNamedProfilesAreVersionedMediaContracts(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		selection string
		name      string
		duration  time.Duration
		format    media.SegmentFormat
		storage   media.EssenceStorage
	}{
		{selection: "preserve@1", name: ProfilePreserve, format: media.SegmentFormatSource, storage: media.EssenceStorageMuxed},
		{selection: "demux", name: ProfileDemux, format: media.SegmentFormatSource, storage: media.EssenceStorageIndependent},
		{selection: "muxed-segments@v1", name: ProfileMuxedSegments, duration: 10 * time.Second, format: media.SegmentFormatSource, storage: media.EssenceStorageMuxed},
		{selection: "essence-segments", name: ProfileEssenceSegments, duration: 10 * time.Second, format: media.SegmentFormatSource, storage: media.EssenceStorageIndependent},
		{selection: "mpegts-segments@v1", name: ProfileMPEGTSSegments, duration: 2 * time.Second, format: media.SegmentFormatMPEGTS, storage: media.EssenceStorageIndependent},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			profile, err := ResolveProfile(testCase.selection, ProfileOverrides{})
			if err != nil {
				t.Fatal(err)
			}
			if profile.Name != testCase.name || profile.Version != "1" ||
				profile.SegmentDuration != testCase.duration || profile.SegmentFormat != testCase.format ||
				profile.EssenceStorage != testCase.storage {
				t.Fatalf("resolved profile = %#v", profile)
			}
		})
	}
}

func TestAllBuiltInProfilesCompleteExactDryRuns(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, definition := range BuiltInProfiles() {
		t.Run(definition.Name, func(t *testing.T) {
			t.Parallel()
			pipeline, err := New(Config{
				Profile: definition.Name, ProfileVersion: definition.Version,
				SegmentDuration: definition.SegmentDuration, SegmentFormat: definition.SegmentFormat,
				EssenceStorage: definition.EssenceStorage, DryRunMode: DryRunExact,
				Concurrency: 1, Transfers: 1, ProbeConcurrency: 1,
			}, nil, fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatalf("Pipeline.Run() = _, %v, want nil", err)
			}
			if batch.Succeeded != 1 || batch.Failed != 0 || len(batch.Results) != 1 {
				t.Fatalf("exact dry-run batch = %#v, want one successful result", batch)
			}
			if got := batch.Results[0].Status; got != ResultStatusPlanned {
				t.Fatalf("exact dry-run result status = %q, want %q", got, ResultStatusPlanned)
			}
		})
	}
}

func TestBuiltInProfileCatalogueIsStableAndDoesNotAliasRemovedNames(t *testing.T) {
	t.Parallel()
	definitions := BuiltInProfiles()
	want := []string{ProfilePreserve, ProfileDemux, ProfileMuxedSegments, ProfileEssenceSegments, ProfileMPEGTSSegments}
	if len(definitions) != len(want) {
		t.Fatalf("catalogue length = %d, want %d", len(definitions), len(want))
	}
	for index, definition := range definitions {
		if definition.Name != want[index] || definition.Version != "1" {
			t.Fatalf("catalogue[%d] = %#v, want %s@1", index, definition, want[index])
		}
	}
	for removed, replacement := range map[string]string{
		"editorial": "essence-segments", "streaming-ts": "mpegts-segments",
	} {
		_, err := ResolveProfile(removed, ProfileOverrides{})
		if err == nil || !strings.Contains(err.Error(), replacement) {
			t.Fatalf("removed profile %q error = %v, want replacement %q", removed, err, replacement)
		}
	}
}

func TestNamedProfileVersionsAreValidatedIndependently(t *testing.T) {
	t.Parallel()
	for _, selection := range []string{"preserve@2", "demux@2", "muxed-segments@2", "essence-segments@2", "mpegts-segments@2"} {
		_, err := ResolveProfile(selection, ProfileOverrides{})
		if err == nil || !strings.Contains(err.Error(), "only version 1") {
			t.Errorf("ResolveProfile(%q) error = %v, want profile-specific version guidance", selection, err)
		}
	}
}

// TestTutorialPreservationTreatment pins the exact-byte escape hatch described
// by the first-ingest tutorial. A muxed two-stream input avoids FFmpeg only
// when it is also whole-file; changing either dimension writes a new container
// representation. The upload path separately proves that the no-write case
// hashes and stores the input bytes verbatim.
func TestTutorialPreservationTreatment(t *testing.T) {
	t.Parallel()
	preserve, err := ResolveProfile(ProfilePreserve, ProfileOverrides{})
	if err != nil {
		t.Fatal(err)
	}
	if preserve.SegmentDuration != 0 || preserve.SegmentFormat != media.SegmentFormatSource ||
		preserve.EssenceStorage != media.EssenceStorageMuxed {
		t.Fatalf("preserve@%s = %#v, want whole-file muxed source bytes", "1", preserve)
	}
	probe := media.Probe{Streams: []media.Stream{
		{Index: 0, CodecType: "video"},
		{Index: 1, CodecType: "audio"},
	}}
	config := Config{
		SegmentDuration: preserve.SegmentDuration,
		SegmentFormat:   preserve.SegmentFormat,
		EssenceStorage:  preserve.EssenceStorage,
	}
	if ffmpegWritesOutput(config, probe) {
		t.Fatal("preserve profile rewrites the source instead of storing it whole")
	}

	for _, testCase := range []struct {
		name   string
		change func(*Config)
	}{
		{name: "segmented mux", change: func(c *Config) { c.SegmentDuration = time.Second }},
		{name: "whole independent essences", change: func(c *Config) { c.EssenceStorage = media.EssenceStorageIndependent }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			changed := config
			testCase.change(&changed)
			if !ffmpegWritesOutput(changed, probe) {
				t.Fatalf("%s unexpectedly preserves the original container bytes", testCase.name)
			}
		})
	}
}

func TestProfileOverridesAreExplicitAndObservable(t *testing.T) {
	t.Parallel()
	duration := 4 * time.Second
	profile, err := ResolveProfile(ProfileMPEGTSSegments, ProfileOverrides{SegmentDuration: &duration})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != ProfileCustom || profile.Version != "1" || profile.SegmentDuration != duration {
		t.Fatalf("resolved profile = %#v, want custom@%s with a four-second target", profile, "1")
	}

	// Repeating a named setting does not turn it into a different contract.
	format := media.SegmentFormatMPEGTS
	profile, err = ResolveProfile(ProfileMPEGTSSegments, ProfileOverrides{SegmentFormat: &format})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != ProfileMPEGTSSegments {
		t.Fatalf("matching override resolved as %q, want %q", profile.Name, ProfileMPEGTSSegments)
	}

	profile, err = ResolveProfile(ProfileEssenceSegments, ProfileOverrides{FFmpegArgs: true})
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != ProfileCustom {
		t.Fatalf("explicit FFmpeg treatment resolved as %q, want custom", profile.Name)
	}
}

func TestNamedProfileCannotMisreportDifferentSettings(t *testing.T) {
	t.Parallel()
	_, err := New(Config{
		Profile: ProfileEssenceSegments, ProfileVersion: "1",
		SegmentDuration: 2 * time.Second, SegmentFormat: media.SegmentFormatSource,
		EssenceStorage: media.EssenceStorageIndependent, DryRunMode: DryRunExact,
	}, nil, fakeProber{}, fakeSegmenter{}, discardLogger(), nil)
	if err == nil || !strings.Contains(err.Error(), "custom@1") {
		t.Fatalf("mismatched named-profile error = %v", err)
	}
	if _, err := ResolveProfile("essence-segments@", ProfileOverrides{}); err == nil {
		t.Fatal("empty profile version was accepted")
	}
}

func TestMPEGTSSegmentsRejectUnsupportedCodecs(t *testing.T) {
	t.Parallel()
	compatible := media.Probe{Streams: []media.Stream{
		{Index: 0, CodecType: "video", CodecName: "h264"},
		{Index: 1, CodecType: "audio", CodecName: "aac"},
	}}
	if err := validateMPEGTSSegmentCodecs(compatible); err != nil {
		t.Fatalf("compatible transport stream rejected: %v", err)
	}
	unsupported := compatible
	unsupported.Streams = append([]media.Stream(nil), compatible.Streams...)
	unsupported.Streams[1].CodecName = "flac"
	if err := validateMPEGTSSegmentCodecs(unsupported); err == nil || !strings.Contains(err.Error(), "audio/flac") {
		t.Fatalf("unsupported codec error = %v", err)
	}
}

func TestMuxerChangingArgumentsMustMatchContainerPolicy(t *testing.T) {
	t.Parallel()
	for _, arguments := range [][]string{{"-f", "segment"}, {"-segment_format=mpegts"}} {
		if err := validateMuxerArguments(arguments, true, "mpegts"); err != nil {
			t.Fatalf("matching arguments %v rejected: %v", arguments, err)
		}
	}
	for _, arguments := range [][]string{{"-f", "matroska"}, {"-segment_format", "matroska"}, {"-segment_format"}} {
		if err := validateMuxerArguments(arguments, true, "mpegts"); err == nil {
			t.Fatalf("conflicting arguments %v accepted", arguments)
		}
	}
}

func TestMPEGTSSegmentPolicyFailsBeforeInvokingFFmpeg(t *testing.T) {
	directory := t.TempDir()
	filename := directory + "/audio.flac"
	if err := os.WriteFile(filename, []byte("flac source"), 0o600); err != nil {
		t.Fatal(err)
	}
	prober := profilePolicyProber{probe: media.Probe{
		Format:  media.Format{Name: "flac", Duration: "1"},
		Streams: []media.Stream{{Index: 0, CodecType: "audio", CodecName: "flac", SampleRate: "48000", Channels: 2}},
	}}
	pipeline, err := New(Config{
		Profile: ProfileMPEGTSSegments, ProfileVersion: "1",
		Concurrency: 1, DryRunMode: DryRunExact, SegmentDuration: 2 * time.Second,
		SegmentFormat: media.SegmentFormatMPEGTS, EssenceStorage: media.EssenceStorageIndependent,
	}, nil, prober, versionedCountingSegmenter{err: errors.New("FFmpeg must not run")}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || len(batch.Results) != 1 || !strings.Contains(batch.Results[0].Error, "audio/flac") {
		t.Fatalf("result = %#v, want an unsupported MPEG-TS codec failure", batch)
	}
}

func TestRendererEpochAndByteProfilesDeriveStableFlowIDs(t *testing.T) {
	t.Parallel()
	config := Config{
		Profile: ProfileEssenceSegments, ProfileVersion: "1",
		SegmentDuration: 10 * time.Second, SegmentFormat: media.SegmentFormatSource,
		EssenceStorage: media.EssenceStorageIndependent,
	}
	one := namedID("flow", "input", flowProfile("bytes-one", config))
	differentBytes := namedID("flow", "input", flowProfile("bytes-two", config))
	newProfile := config
	newProfile.ProfileVersion = "2"
	differentProfileVersion := namedID("flow", "input", flowProfile("bytes-one", newProfile))
	differentRendererEpoch := namedID("flow", "input", flowProfileForRendererEpoch("bytes-one", config, "3"))
	for label, id := range map[string]string{
		"different bytes": differentBytes, "different semantic profile": differentProfileVersion,
		"different renderer epoch": differentRendererEpoch,
	} {
		if id == one {
			t.Fatalf("%s derived the same Flow ID %s", label, id)
		}
	}
	if rendererIdentityEpoch != "2" {
		t.Fatalf("renderer identity epoch changed without an explicit migration: %q", rendererIdentityEpoch)
	}
}

func TestWholeFileProfileIsPartOfGeneratedIdentity(t *testing.T) {
	t.Parallel()
	preserve := Config{
		Profile: ProfilePreserve, ProfileVersion: "1",
		SegmentFormat: media.SegmentFormatSource, EssenceStorage: media.EssenceStorageMuxed,
	}
	custom := preserve
	custom.Profile = ProfileCustom
	if flowProfile("same-bytes", preserve) == flowProfile("same-bytes", custom) {
		t.Fatal("preserve@1 and custom@1 derive one identity when FFmpeg writes nothing")
	}
	newVersion := preserve
	newVersion.ProfileVersion = "2"
	if flowProfile("same-bytes", preserve) == flowProfile("same-bytes", newVersion) {
		t.Fatal("two semantic profile versions derive one whole-file identity")
	}
}

func TestMachineResultReportsTheToolchainThatWroteMedia(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "fixture.ts")
	if err := os.WriteFile(filename, []byte("muxed-media"), 0o600); err != nil {
		t.Fatal(err)
	}
	const report = "ffmpeg version 7.0\nconfiguration: --enable-example\nlibavformat 61.0"
	pipeline, err := New(Config{
		Profile: ProfileEssenceSegments, ProfileVersion: "1",
		Concurrency: 1, Transfers: 2, DryRunMode: DryRunExact,
		SegmentDuration: 10 * time.Second, SegmentFormat: media.SegmentFormatSource,
		EssenceStorage: media.EssenceStorageIndependent,
	}, nil, fakeProber{}, versionedCountingSegmenter{
		countingSegmenter: countingSegmenter{objects: 2}, version: report,
	}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Results) != 1 {
		t.Fatalf("results = %d, want 1", len(batch.Results))
	}
	result := batch.Results[0]
	if result.FFmpegVersion != "ffmpeg version 7.0" {
		t.Fatalf("FFmpegVersion = %q", result.FFmpegVersion)
	}
	wantFingerprint := mediaToolchainFingerprint(ProfileEssenceSegments, "1", report)
	if result.MediaToolchain != wantFingerprint {
		t.Fatalf("MediaToolchain = %q, want %q", result.MediaToolchain, wantFingerprint)
	}
}
