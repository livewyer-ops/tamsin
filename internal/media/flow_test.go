package media

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildVideoFlow(t *testing.T) {
	t.Parallel()
	probe := Probe{Format: Format{Name: "mov,mp4", Duration: "1.5", BitRate: "12000000"}}
	probe.Streams = []Stream{{
		CodecName: "h264", CodecType: "video", Width: 1920, Height: 1080,
		AverageFrameRate: "30000/1001", PixelFormat: "yuv420p10le", FieldOrder: "progressive", ColorSpace: "bt709",
	}}
	flow, info, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["format"] != "urn:x-nmos:format:video" || flow["codec"] != "video/h264" || flow["container"] != "video/mp4" {
		t.Fatalf("unexpected Flow metadata: %#v", flow)
	}
	parameters := flow["essence_parameters"].(map[string]any)
	if parameters["frame_width"] != 1920 || parameters["bit_depth"] != 10 {
		t.Fatalf("unexpected essence parameters: %#v", parameters)
	}
	if info.Duration != 1_500_000_000 || info.ContentType != "video/mp4" {
		t.Fatalf("unexpected Flow info: %#v", info)
	}
}

func TestBuildFlowRecordsResolvedMediaToolchain(t *testing.T) {
	t.Parallel()
	identity := testIdentity()
	identity.IngestProfile = "essence-segments"
	identity.IngestProfileVersion = "1"
	identity.FFmpegVersion = "ffmpeg version 7.0"
	identity.MediaToolchain = "sha256:toolchain"
	probe := Probe{
		Format:  Format{Name: "mov,mp4", Duration: "1"},
		Streams: []Stream{{CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"}},
	}
	flow, _, err := BuildFlow(probe, identity, "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	tags := flow["tags"].(map[string]any)
	for name, want := range map[string]string{
		"_tamsin_ingest_profile": "essence-segments", "_tamsin_ingest_profile_version": "1",
		"_tamsin_ffmpeg_version": "ffmpeg version 7.0", "_tamsin_media_toolchain": "sha256:toolchain",
	} {
		if tags[name] != want {
			t.Fatalf("tag %s = %v, want %q", name, tags[name], want)
		}
	}
}

func TestBuildMuxedFlow(t *testing.T) {
	t.Parallel()
	probe := Probe{Format: Format{Name: "matroska", Duration: "2"}}
	probe.Streams = []Stream{
		{CodecName: "h264", CodecType: "video", Width: 1280, Height: 720, AverageFrameRate: "25/1"},
		{CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
	}
	flow, _, err := BuildFlow(probe, testIdentity(), "video/x-matroska", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["format"] != "urn:x-nmos:format:multi" || flow["container"] != "video/matroska" {
		t.Fatalf("unexpected muxed Flow: %#v", flow)
	}
	if _, exists := flow["codec"]; exists {
		t.Fatal("muxed Flow must not claim one codec")
	}
}

func TestBuildAudioFlow(t *testing.T) {
	t.Parallel()
	probe := Probe{Format: Format{Name: "wav", Duration: "0.25"}}
	probe.Streams = []Stream{{CodecName: "pcm_s24le", CodecType: "audio", SampleRate: "48000", Channels: 2, BitsPerRawSample: "24"}}
	flow, _, err := BuildFlow(probe, testIdentity(), "audio/wav", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	parameters := flow["essence_parameters"].(map[string]any)
	if flow["codec"] != "audio/x-raw-int" || parameters["bit_depth"] != 24 {
		t.Fatalf("unexpected audio Flow: %#v", flow)
	}
}

func TestPCMLayoutFollowsFFmpegCodecEvidence(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		codec string
		want  string
	}{
		{codec: "pcm_s24le", want: "interleaved"},
		{codec: "pcm_s24le_planar", want: "planar"},
		{codec: "pcm_lxf", want: "planar"},
	} {
		t.Run(testCase.codec, func(t *testing.T) {
			t.Parallel()
			probe := Probe{
				Format: Format{Name: "wav", Duration: "1"},
				Streams: []Stream{{
					CodecName: testCase.codec, CodecType: "audio", SampleRate: "48000", Channels: 2,
					BitsPerRawSample: "24",
				}},
			}
			flow, _, err := BuildFlow(probe, testIdentity(), "audio/wav", EssenceStorageMuxed)
			if err != nil {
				t.Fatal(err)
			}
			parameters := flow["essence_parameters"].(map[string]any)
			unc := parameters["unc_parameters"].(map[string]any)
			if got := unc["unc_type"]; got != testCase.want {
				t.Fatalf("unc_type = %v, want %q", got, testCase.want)
			}
		})
	}
}

func TestMPEGLayerTwoUsesTheStreamingProfileCodecType(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mpegts", Duration: "1"},
		Streams: []Stream{{
			CodecName: "mp2", CodecType: "audio", SampleRate: "48000", Channels: 2,
		}},
	}
	flow, info, err := BuildFlow(probe, testIdentity(), "video/mp2t", EssenceStorageIndependent)
	if err != nil {
		t.Fatal(err)
	}
	if got := flow["codec"]; got != "audio/mpeg" {
		t.Fatalf("MP2 codec = %v, want audio/mpeg", got)
	}
	if len(info.UnsupportedCodecs) != 0 {
		t.Fatalf("supported MP2 stream reported as unsupported: %#v", info.UnsupportedCodecs)
	}
}

func TestBuildStillImageFlow(t *testing.T) {
	t.Parallel()
	probe := Probe{Format: Format{Name: "png_pipe"}}
	probe.Streams = []Stream{{CodecName: "png", CodecType: "video", Width: 64, Height: 32}}
	flow, info, err := BuildFlow(probe, testIdentity(), "image/png", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["format"] != "urn:x-tam:format:image" || flow["container"] != "image/png" || info.Duration != 0 {
		t.Fatalf("unexpected image Flow: %#v, %#v", flow, info)
	}
}

func TestProbeTimingIncludesDelayedStreamEnd(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "matroska"},
		Streams: []Stream{
			{
				Index: 0, CodecType: "video", CodecName: "mjpeg", Width: 8, Height: 8,
				StartTime: "-100", Duration: "200",
				Disposition: struct {
					AttachedPicture int `json:"attached_pic"`
				}{AttachedPicture: 1},
			},
			{Index: 1, CodecType: "video", CodecName: "h264", Width: 64, Height: 64,
				AverageFrameRate: "25/1", StartTime: "10", Duration: "5"},
			{Index: 2, CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2,
				StartTime: "12", Duration: "7"},
		},
	}
	_, info, err := BuildFlow(probe, testIdentity(), "video/matroska", EssenceStorageIndependent)
	if err != nil {
		t.Fatal(err)
	}
	if info.Start != int64(10*time.Second) {
		t.Fatalf("Flow start = %d, want 10s; attached picture timing must be ignored", info.Start)
	}
	if info.Duration != int64(9*time.Second) {
		t.Fatalf("Flow duration = %d, want 9s through the delayed audio end", info.Duration)
	}
	if got := info.Collected[1].Offset; got != int64(2*time.Second) {
		t.Fatalf("audio offset = %d, want 2s", got)
	}
}

func TestProbeTimingRejectsInvalidDurationAndOffset(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name  string
		probe Probe
	}{
		{
			name:  "negative container duration",
			probe: Probe{Format: Format{Duration: "-1"}},
		},
		{
			name:  "negative stream duration",
			probe: Probe{Streams: []Stream{{Index: 3, StartTime: "0", Duration: "-1"}}},
		},
		{
			name: "stream offset overflow",
			probe: Probe{
				Format:  Format{StartTime: "-9223372036.854775808"},
				Streams: []Stream{{Index: 4, StartTime: "9223372036.854775807", Duration: "0"}},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := ProbeTiming(testCase.probe); err == nil {
				t.Fatal("invalid probe timing unexpectedly succeeded")
			}
		})
	}
}

func TestBuildDataFlowIncludesStorageContainer(t *testing.T) {
	t.Parallel()
	flow, info, err := BuildFlow(Probe{}, testIdentity(), "application/octet-stream", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["format"] != "urn:x-nmos:format:data" || flow["container"] != "application/octet-stream" ||
		info.ContentType != "application/octet-stream" {
		t.Fatalf("unexpected data Flow: %#v, %#v", flow, info)
	}
}

func TestUnsupportedCodecIsNotInvented(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "matroska", Duration: "1"},
		Streams: []Stream{{
			Index: 4, CodecName: "future_picture_codec", CodecType: "video",
			Width: 1920, Height: 1080, AverageFrameRate: "25/1",
		}},
	}
	flow, info, err := BuildFlow(probe, testIdentity(), "video/matroska", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if codec, present := flow["codec"]; present {
		t.Fatalf("unknown codec was invented as %q", codec)
	}
	if len(info.UnsupportedCodecs) != 1 || info.UnsupportedCodecs[0].Name != "future_picture_codec" ||
		info.UnsupportedCodecs[0].StreamIndex != 4 {
		t.Fatalf("unsupported codec evidence = %#v", info.UnsupportedCodecs)
	}
	for _, prefix := range []string{"video/x-", "audio/x-", "application/x-"} {
		if got := codecMIME("future_picture_codec"); strings.HasPrefix(got, prefix) {
			t.Fatalf("codecMIME invented %q", got)
		}
	}
}

func TestInterlaceModeOmitsAmbiguousAndPsFClaims(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		format     Format
		fieldOrder string
		want       string
	}{
		{fieldOrder: "progressive", want: "progressive"},
		{format: Format{Name: "mxf"}, fieldOrder: "progressive", want: ""},
		{fieldOrder: "tt", want: "interlaced_tff"},
		{fieldOrder: "bb", want: "interlaced_bff"},
		{fieldOrder: "tb", want: ""},
		{fieldOrder: "bt", want: ""},
		{fieldOrder: "unknown", want: ""},
	} {
		if got := interlaceMode(testCase.format, testCase.fieldOrder); got != testCase.want {
			t.Errorf("interlaceMode(%q) = %q, want %q", testCase.fieldOrder, got, testCase.want)
		}
		if got := interlaceMode(testCase.format, testCase.fieldOrder); got == "interlaced_psf" {
			t.Errorf("interlaceMode(%q) invented PsF", testCase.fieldOrder)
		}
	}
}

// TestContainerProfilePolicy pins the small set of decisions Tamsin owns. The
// mapper describes supported outputs; it is not a promise to classify every
// file FFmpeg can open. Unknowns therefore stay visibly generic rather than
// inheriting an extension or being guessed into the ISO family.
func TestContainerProfilePolicy(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		format     Format
		streamType string
		detected   string
		want       string
		supported  bool
	}{
		{name: "mpeg-ts audio remains file-level video", format: Format{Name: "mpegts"}, streamType: "audio", want: "video/mp2t", supported: true},
		{name: "mxf", format: Format{Name: "mxf"}, streamType: "video", want: "application/mxf", supported: true},
		{name: "webm content in shared demuxer", format: Format{Name: "matroska,webm"}, detected: "video/webm", streamType: "video", want: "video/webm", supported: true},
		{name: "audio webm", format: Format{Name: "matroska,webm"}, detected: "video/webm", streamType: "audio", want: "audio/webm", supported: true},
		{name: "matroska shared demuxer", format: Format{Name: "matroska,webm"}, streamType: "video", want: "video/matroska", supported: true},
		{name: "registered video matroska", format: Format{Name: "matroska"}, streamType: "video", want: "video/matroska", supported: true},
		{name: "registered audio matroska", format: Format{Name: "matroska"}, streamType: "audio", want: "audio/matroska", supported: true},
		{name: "quicktime has no invented audio type", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "qt  "}}, streamType: "audio", want: "video/quicktime", supported: true},
		{name: "3gpp video", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "3gp6"}}, streamType: "video", want: "video/3gpp", supported: true},
		{name: "3gpp2 audio", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "3g2a"}}, streamType: "audio", want: "audio/3gpp2", supported: true},
		{name: "mp4 video", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "isom"}}, streamType: "video", want: "video/mp4", supported: true},
		{name: "mp4 audio", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "M4A "}}, streamType: "audio", want: "audio/mp4", supported: true},
		{name: "new mp4 brand uses declared compatibility", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "new1", "compatible_brands": "new1isomiso2mp41"}}, streamType: "video", want: "video/mp4", supported: true},
		{name: "mp4 without presentation", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "mp42"}}, streamType: "data", want: "application/mp4", supported: true},
		{name: "unknown iso brand is not mp4", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "avif"}}, streamType: "video", detected: "image/avif", want: "application/octet-stream"},
		{name: "unknown ffprobe format ignores plausible sniff", format: Format{Name: "mystery"}, streamType: "video", detected: "video/mp4", want: "application/octet-stream"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got := describeContainer(testCase.format, testCase.streamType, testCase.detected)
			if got.mediaType != testCase.want || got.supported != testCase.supported {
				t.Fatalf("describeContainer() = %#v, want type %q supported=%t", got, testCase.want, testCase.supported)
			}
		})
	}
}

// The table tests above cannot prove that FFprobe's format tags reach the
// policy. Exercise the real process boundary as well: these three files share
// one demuxer name and are distinguishable only by the brand FFprobe returns.
func TestProbeCarriesISOContainerBrand(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}

	for _, testCase := range []struct {
		extension string
		brand     string
		mediaType string
	}{
		{extension: "mp4", brand: "isom", mediaType: "video/mp4"},
		{extension: "mov", brand: "qt", mediaType: "video/quicktime"},
		{extension: "3gp", brand: "3gp", mediaType: "video/3gpp"},
	} {
		t.Run(testCase.extension, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "fixture."+testCase.extension)
			command := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
				"-f", "lavfi", "-i", "testsrc=size=128x96:rate=15", "-t", "0.2",
				"-c:v", "mpeg4", "-pix_fmt", "yuv420p", filename)
			if output, err := command.CombinedOutput(); err != nil {
				t.Skipf("cannot build %s fixture: %v: %s", testCase.extension, err, output)
			}

			probe, err := (FFprobe{}).Probe(context.Background(), filename)
			if err != nil {
				t.Fatal(err)
			}
			brand := strings.ToLower(strings.TrimSpace(probe.Format.Tags["major_brand"]))
			if !strings.HasPrefix(brand, testCase.brand) {
				t.Fatalf("major_brand = %q, want prefix %q", brand, testCase.brand)
			}
			container := describeContainer(probe.Format, "video", "")
			if !container.supported || container.mediaType != testCase.mediaType {
				t.Fatalf("container = %#v, want supported %s", container, testCase.mediaType)
			}
		})
	}
}

func TestMuxedMP4UsesActualPresentation(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "1", Tags: map[string]string{"major_brand": "isom"}},
		Streams: []Stream{
			{CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
		},
	}
	flow, _, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["container"] != "video/mp4" {
		t.Fatalf("muxed A/V MP4 container = %v, want video/mp4", flow["container"])
	}
}

func TestSourceSegmentationProfileComesFromProbe(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		format   Format
		detected string
		want     SegmentContainer
	}{
		{name: "mpeg-ts", format: Format{Name: "mpegts"}, want: SegmentContainer{Muxer: "mpegts", Extension: ".ts"}},
		{name: "webm content", format: Format{Name: "matroska,webm"}, detected: "video/webm", want: SegmentContainer{Muxer: "webm", Extension: ".webm"}},
		{name: "matroska shared demuxer", format: Format{Name: "matroska,webm"}, want: SegmentContainer{Muxer: "matroska", Extension: ".mkv"}},
		{name: "quicktime brand", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "qt  "}}, want: SegmentContainer{Muxer: "mov", Extension: ".mov"}},
		{name: "mp4 compatible brand", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "new1", "compatible_brands": "new1isom"}}, want: SegmentContainer{Muxer: "mp4", Extension: ".mp4"}},
		{name: "unknown", format: Format{Name: "proprietary"}},
		{name: "mj2 is storable but not safely remuxed", format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Tags: map[string]string{"major_brand": "mjp2"}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := SourceSegmentContainer(testCase.format, testCase.detected); got != testCase.want {
				t.Fatalf("SourceSegmentContainer() = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestMatroskaAndWebMUseContentEvidence(t *testing.T) {
	for _, executable := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(executable); err != nil {
			t.Skipf("requires %s", executable)
		}
	}
	for _, fixture := range []struct{ extension, codec, muxer, mime string }{
		{"mkv", "libx264", "matroska", "video/matroska"},
		{"webm", "libvpx", "webm", "video/webm"},
	} {
		t.Run(fixture.extension, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "fixture."+fixture.extension)
			buildFixture(t, "-f", "lavfi", "-i", "color=size=64x64:rate=25", "-t", "0.2", "-c:v", fixture.codec, filename)
			probe, err := (FFprobe{}).Probe(context.Background(), filename)
			if err != nil {
				t.Fatal(err)
			}
			detected, err := DetectContentType(filename)
			if err != nil {
				t.Fatal(err)
			}
			if got := describeContainer(probe.Format, "video", detected); got.mediaType != fixture.mime {
				t.Fatalf("container = %#v; probe=%q detected=%q", got, probe.Format.Name, detected)
			}
			if got := SourceSegmentContainer(probe.Format, detected); got.Muxer != fixture.muxer {
				t.Fatalf("segment container = %#v, want %s", got, fixture.muxer)
			}
		})
	}
}

func TestDetectContentTypeDoesNotTrustExtension(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "not-media.mp4")
	if err := os.WriteFile(filename, []byte("plain text, despite the suffix\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := DetectContentType(filename)
	if err != nil {
		t.Fatal(err)
	}
	if got != "text/plain" {
		t.Fatalf("DetectContentType() = %q, want content-derived text/plain", got)
	}
}

func TestEBMLContentDetectionRequiresACompleteDocTypeElement(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, header, want string
	}{
		{"webm", "\x1a\x45\xdf\xa3\x87\x42\x82\x84webm", "video/webm"},
		{"matroska", "\x1a\x45\xdf\xa3\x8b\x42\x82\x88matroska", "video/matroska"},
		{"two-byte size", "\x1a\x45\xdf\xa3\x88\x42\x82\x40\x04webm", "video/webm"},
		{"void payload is not a document type", "\x1a\x45\xdf\xa3\x89\xec\x87\x42\x82\x84webm", "application/octet-stream"},
		{"outside header", "\x1a\x45\xdf\xa3\x80\x42\x82\x84webm", "application/octet-stream"},
		{"truncated", "\x1a\x45\xdf\xa3\x87\x42\x82\x84web", "application/octet-stream"},
		{"invalid size", "\x1a\x45\xdf\xa3\x00", "application/octet-stream"},
		{"unknown size", "\x1a\x45\xdf\xa3\xff\x42\x82\x84webm", "application/octet-stream"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "media.bin")
			if err := os.WriteFile(path, []byte(test.header), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := DetectContentType(path)
			if err != nil || got != test.want {
				t.Fatalf("DetectContentType() = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func testIdentity() Identity {
	return Identity{
		FlowID: "00000000-0000-4000-8000-000000000001", SourceID: "00000000-0000-4000-8000-000000000002",
		Label: "fixture", URI: "file:///fixture", SHA256: "abc", Size: 42,
	}
}

// TestCollectedEssencesCarryContainerPositionAndOffset covers the two facts a
// demultiplexed ingest needs about each stream, both of which were previously
// inferred from its position in the collection.
//
// The position is not the container index. Anything that is not essence -- an
// attached picture, most obviously -- is filtered out before the collection is
// built, so counting the collection maps the wrong track: FFmpeg is told
// `-map 0:N` and N has to mean what the container means by it.
//
// The offset matters because essences do not necessarily start together. Once
// each is a Flow of its own, the only thing keeping them in sync is where each
// was placed on the shared timeline.
func TestCollectedEssencesCarryContainerPositionAndOffset(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mpegts", StartTime: "0.5", Duration: "10.0"},
		Streams: []Stream{
			// Cover art sits first in the container and is not essence.
			{Index: 0, CodecType: "video", CodecName: "mjpeg", Width: 8, Height: 8,
				AverageFrameRate: "0/0", StartTime: "0.5",
				Disposition: struct {
					AttachedPicture int `json:"attached_pic"`
				}{AttachedPicture: 1}},
			{Index: 1, CodecType: "video", CodecName: "h264", Width: 64, Height: 64,
				AverageFrameRate: "25/1", StartTime: "0.5"},
			// Audio arrives half a second after the container begins.
			{Index: 2, CodecType: "audio", CodecName: "aac", SampleRate: "48000",
				Channels: 2, StartTime: "1.0"},
		},
	}
	identity := Identity{FlowID: "flow", SourceID: "source", Label: "programme", URI: "file:///programme.ts"}

	_, info, err := BuildFlow(probe, identity, "video/mp2t", EssenceStorageIndependent)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Collected) != 2 {
		t.Fatalf("expected the two essences, got %d", len(info.Collected))
	}

	video, audio := info.Collected[0], info.Collected[1]
	if video.StreamIndex != 1 {
		t.Fatalf("video StreamIndex = %d, want 1; using the collection position would demultiplex the attached picture",
			video.StreamIndex)
	}
	if audio.StreamIndex != 2 {
		t.Fatalf("audio StreamIndex = %d, want 2", audio.StreamIndex)
	}
	if video.Offset != 0 {
		t.Fatalf("video Offset = %d, want 0: it starts with the container", video.Offset)
	}
	if want := int64(500 * time.Millisecond); audio.Offset != want {
		t.Fatalf("audio Offset = %d, want %d; losing it puts the audio half a second early",
			audio.Offset, want)
	}
}

// TestContainerMappingCountsContainerTracks covers the muxed arrangement's side
// of the same distinction. AppNote 0006 defines track_index over every track in
// the container, so a reader locating the essence has the whole container in
// front of it -- including the tracks Tamsin filtered out.
func TestContainerMappingCountsContainerTracks(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mpegts", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{
			{Index: 0, CodecType: "video", CodecName: "mjpeg", Width: 8, Height: 8,
				AverageFrameRate: "0/0",
				Disposition: struct {
					AttachedPicture int `json:"attached_pic"`
				}{AttachedPicture: 1}},
			{Index: 1, CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{Index: 2, CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
		},
	}
	identity := Identity{FlowID: "flow", SourceID: "source", Label: "programme", URI: "file:///programme.ts"}

	_, info, err := BuildFlow(probe, identity, "video/mp2t", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	for _, collected := range info.Collected {
		mapping := collected.ContainerMapping
		if mapping == nil {
			t.Fatalf("%s carries no container_mapping", collected.Role)
		}
		if _, present := collected.Flow["container_mapping"]; present {
			t.Fatalf("%s puts container_mapping on the child rather than its parent Collection Item", collected.Role)
		}
		if mapping["track_index"] != collected.StreamIndex {
			t.Fatalf("%s track_index = %v, want the container index %d",
				collected.Role, mapping["track_index"], collected.StreamIndex)
		}
	}
}

// TestVideoColorspaceComesFromPrimaries covers which field names the colour
// system.
//
// FFprobe's color_space is the matrix coefficients. They usually agree with the
// primaries, and they are a different property: a file can carry BT.709 matrix
// coefficients with BT.2020 primaries, and reading the matrix would describe it
// as the wrong system. BT.2100 is the further trap -- it is BT.2020 primaries
// with an HDR transfer, so reporting BT2020 beside HLG or PQ names a system
// that does not use those transfer functions.
func TestVideoColorspaceComesFromPrimaries(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		primaries string
		matrix    string
		transfer  string
		want      string
	}{
		{name: "rec 709", primaries: "bt709", matrix: "bt709", transfer: "SDR", want: "BT709"},
		{name: "standard definition", primaries: "bt470bg", matrix: "smpte170m", transfer: "SDR", want: "BT601"},
		{name: "wide gamut sdr", primaries: "bt2020", matrix: "bt2020nc", transfer: "SDR", want: "BT2020"},
		{name: "hlg is bt2100", primaries: "bt2020", matrix: "bt2020nc", transfer: "HLG", want: "BT2100"},
		{name: "pq is bt2100", primaries: "bt2020", matrix: "bt2020nc", transfer: "PQ", want: "BT2100"},
		{
			// The case that motivates preferring primaries at all.
			name:      "primaries win over disagreeing coefficients",
			primaries: "bt2020", matrix: "bt709", transfer: "SDR", want: "BT2020",
		},
		{
			// Older material often states no primaries, and the matrix is then
			// the only thing available.
			name:      "matrix is used when no primaries are stated",
			primaries: "", matrix: "bt709", transfer: "SDR", want: "BT709",
		},
		{name: "nothing stated says nothing", primaries: "", matrix: "", transfer: "", want: ""},
		{name: "unknown values say nothing", primaries: "reserved", matrix: "unknown", transfer: "", want: ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got := videoColorspace(testCase.primaries, testCase.matrix, testCase.transfer)
			if got != testCase.want {
				t.Fatalf("videoColorspace(primaries=%q, matrix=%q, transfer=%q) = %q, want %q",
					testCase.primaries, testCase.matrix, testCase.transfer, got, testCase.want)
			}
		})
	}
}

// TestAnamorphicVideoKeepsItsShape covers a picture whose pixels are not square.
//
// Anamorphic standard definition is the ordinary case: 720x576 stored with
// 16:15 pixels and displayed at 4:3. Frame dimensions alone describe it as
// 5:4, so a reader given only those stretches the picture. Neither ratio was
// being read from the probe at all.
func TestAnamorphicVideoKeepsItsShape(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "1.0", StartTime: "0.0"},
		Streams: []Stream{{
			Index: 0, CodecType: "video", CodecName: "h264",
			Width: 720, Height: 576, AverageFrameRate: "25/1",
			SampleAspectRatio: "16:15", DisplayAspectRatio: "4:3",
		}},
	}
	identity := Identity{FlowID: "flow", SourceID: "source", Label: "sd", URI: "file:///sd.mp4"}
	flow, _, err := BuildFlow(probe, identity, "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	parameters := flow["essence_parameters"].(map[string]any)

	pixel, ok := parameters["pixel_aspect_ratio"].(map[string]any)
	if !ok {
		t.Fatalf("no pixel_aspect_ratio: %#v", parameters)
	}
	if pixel["numerator"] != int64(16) || pixel["denominator"] != int64(15) {
		t.Fatalf("pixel_aspect_ratio = %#v, want 16:15", pixel)
	}
	display, ok := parameters["aspect_ratio"].(map[string]any)
	if !ok {
		t.Fatalf("no aspect_ratio: %#v", parameters)
	}
	if display["numerator"] != int64(4) || display["denominator"] != int64(3) {
		t.Fatalf("aspect_ratio = %#v, want 4:3", display)
	}
}

// TestAspectRatioIsNotInvented keeps square-pixel and unstated cases quiet.
// FFprobe writes 0:1 when a file states no ratio, and a Flow claiming one it
// was never told is worse than a Flow that omits it.
func TestAspectRatioIsNotInvented(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "0:1", "N/A", "1:0", "16/9", "nonsense"} {
		if _, _, ok := ParseAspectRatio(value); ok {
			t.Fatalf("ParseAspectRatio(%q) accepted a ratio it should not have", value)
		}
	}
	if numerator, denominator, ok := ParseAspectRatio("16:15"); !ok || numerator != 16 || denominator != 15 {
		t.Fatalf("ParseAspectRatio(\"16:15\") = %d:%d ok=%v", numerator, denominator, ok)
	}
}

// TestProbeReadsColourAndAspectFromRealMedia covers the plumbing the tables
// above cannot. Neither the aspect ratios nor the colour primaries were parsed
// from FFprobe before, so a correct mapping would have been fed nothing.
func TestProbeReadsColourAndAspectFromRealMedia(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("needs ffmpeg and ffprobe")
	}
	directory := t.TempDir()

	t.Run("anamorphic standard definition", func(t *testing.T) {
		filename := filepath.Join(directory, "anamorphic.mp4")
		build := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
			"-f", "lavfi", "-i", "testsrc=size=720x576:rate=25", "-t", "1",
			"-vf", "setsar=16/15", "-c:v", "libx264", "-pix_fmt", "yuv420p", filename)
		if output, err := build.CombinedOutput(); err != nil {
			t.Skipf("could not build the fixture: %v\n%s", err, output)
		}
		probe, err := FFprobe{}.Probe(context.Background(), filename)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok := ParseAspectRatio(probe.Streams[0].SampleAspectRatio); !ok {
			t.Fatalf("sample_aspect_ratio %q did not reach the mapper", probe.Streams[0].SampleAspectRatio)
		}
		if _, _, ok := ParseAspectRatio(probe.Streams[0].DisplayAspectRatio); !ok {
			t.Fatalf("display_aspect_ratio %q did not reach the mapper", probe.Streams[0].DisplayAspectRatio)
		}
	})

	t.Run("hlg is described as bt2100", func(t *testing.T) {
		filename := filepath.Join(directory, "hlg.mp4")
		build := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
			"-f", "lavfi", "-i", "testsrc=size=320x180:rate=25", "-t", "1",
			"-c:v", "libx264", "-pix_fmt", "yuv420p10le",
			"-color_primaries", "bt2020", "-color_trc", "arib-std-b67", "-colorspace", "bt2020nc",
			filename)
		if output, err := build.CombinedOutput(); err != nil {
			t.Skipf("could not build the fixture: %v\n%s", err, output)
		}
		probe, err := FFprobe{}.Probe(context.Background(), filename)
		if err != nil {
			t.Fatal(err)
		}
		stream := probe.Streams[0]
		if stream.ColorPrimaries == "" {
			t.Fatal("color_primaries did not reach the mapper")
		}
		transfer := transferCharacteristic(stream.ColorTransfer)
		if transfer != "HLG" {
			t.Fatalf("transfer_characteristic = %q, want HLG", transfer)
		}
		// Reading the matrix alone would answer BT2020 here, which names a
		// system that does not use an HLG transfer.
		if got := videoColorspace(stream.ColorPrimaries, stream.ColorSpace, transfer); got != "BT2100" {
			t.Fatalf("colorspace = %q, want BT2100 (primaries=%q matrix=%q)",
				got, stream.ColorPrimaries, stream.ColorSpace)
		}
	})
}
