package media

import (
	"strings"
	"testing"
)

func TestCodecMIMEVocabulary(t *testing.T) {
	t.Parallel()
	for codec, want := range map[string]string{
		"mpeg1video": "video/mpeg", "dvvideo": "video/DV", "dnxhd": "video/x-dnxhd",
		"dts": "audio/vnd.dts", "truehd": "audio/vnd.dolby.mlp", "s302m": "audio/x-smpte302m",
		"pcm_alaw": "audio/PCMA", "pcm_mulaw": "audio/PCMU",
		"pcm_s24le": "audio/x-raw-int", "pcm_f32le": "audio/x-raw-float", "pcm_f64be": "audio/x-raw-float",
		"pcm_f16le": "", "pcm_f24le": "",
		"mov_text": "text/x-quicktime-text", "dvb_subtitle": "application/x-dvb-subtitle",
		"dvb_teletext": "application/x-dvb-teletext", "scte_35": "application/x-scte35",
		"eia_608": "application/x-cea-608",
		"tmcd":    "", "bin_data": "", "timed_id3": "", "ttf": "",
	} {
		if got := codecMIME(codec); got != want {
			t.Errorf("codecMIME(%q) = %q, want %q", codec, got, want)
		}
	}
}

func TestJPEG2000IsVideoWhenItMoves(t *testing.T) {
	t.Parallel()
	moving := Probe{Format: Format{Name: "mpegts", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{{CodecName: "jpeg2000", CodecType: "video", Width: 1920, Height: 1080, AverageFrameRate: "25/1"}}}
	flow, _, err := BuildFlow(moving, testIdentity(), "video/mp2t", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["codec"] != "video/jp2" || flow["format"] != "urn:x-nmos:format:video" {
		t.Fatalf("moving JPEG 2000 = %v / %v", flow["codec"], flow["format"])
	}
	still := Probe{Format: Format{Name: "j2k_pipe"},
		Streams: []Stream{{CodecName: "jpeg2000", CodecType: "video", Width: 64, Height: 32}}}
	if _, _, err = BuildFlow(still, testIdentity(), "image/jp2", EssenceStorageMuxed); err == nil {
		t.Fatal("standalone image accepted")
	}
}

func TestG711IsACodecNotUncompressedPCM(t *testing.T) {
	t.Parallel()
	probe := Probe{Format: Format{Name: "mpegts", StartTime: "0.0", Duration: "1.0"},
		Streams: []Stream{{CodecName: "pcm_alaw", CodecType: "audio", SampleRate: "8000", Channels: 1}}}
	flow, _, err := BuildFlow(probe, testIdentity(), "video/mp2t", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	parameters, _ := flow["essence_parameters"].(map[string]any)
	if flow["codec"] != "audio/PCMA" {
		t.Fatalf("codec = %v", flow["codec"])
	}
	if _, present := parameters["unc_parameters"]; present {
		t.Fatalf("companded audio declared as uncompressed: %#v", parameters)
	}
}

func TestUndescribableDataTracksRetainSourcePositions(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{
			{Index: 0, CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "24/1"},
			{Index: 1, CodecType: "data", CodecName: "tmcd"},
			{Index: 2, CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
		},
	}
	flow, info, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.UnsupportedCodecs) != 0 {
		t.Fatalf("a timecode track is not an unsupported essence: %#v", info.UnsupportedCodecs)
	}
	if len(info.UndescribedStreams) != 1 || info.UndescribedStreams[0] != (UnsupportedCodec{Name: "tmcd", StreamType: "data", StreamIndex: 1}) {
		t.Fatalf("undescribed streams = %#v", info.UndescribedStreams)
	}
	if len(info.Collected) != 2 || info.Collected[0].Role != "video" || info.Collected[1].Role != "audio" {
		t.Fatalf("collected = %#v", info.Collected)
	}
	if flow["format"] != "urn:x-nmos:format:multi" {
		t.Fatalf("root format = %v", flow["format"])
	}
	// The undescribed timecode track still counts towards the audio position.
	audio := info.Collected[1]
	if audio.ContainerMapping["track_index"] != 2 || audio.ContainerMapping["format_track_index"] != 0 {
		t.Fatalf("source mapping = %#v", audio.ContainerMapping)
	}

	// An undescribed data track also counts towards the data format index.
	probe.Streams = append(probe.Streams, Stream{Index: 3, CodecType: "data", CodecName: "scte_35"})
	if _, info, err = BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed); err != nil {
		t.Fatal(err)
	}
	data := info.Collected[2]
	if data.Role != "data" || data.Flow["codec"] != "application/x-scte35" ||
		data.ContainerMapping["track_index"] != 3 || data.ContainerMapping["format_track_index"] != 1 {
		t.Fatalf("data essence = %#v", data)
	}
}

func TestOnlyUndescribableTracksIsAnError(t *testing.T) {
	t.Parallel()
	// FFprobe names a timecode track only by its tag.
	probe := Probe{Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{{Index: 0, CodecType: "data", CodecTagString: "tmcd"}}}
	if _, _, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed); err == nil || !strings.Contains(err.Error(), "stream 0 is tmcd") {
		t.Fatalf("an input with nothing describable: %v", err)
	}
	probe.Streams = append(probe.Streams, Stream{Index: 1, CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2})
	_, info, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.UndescribedStreams) != 1 || info.UndescribedStreams[0].Name != "tmcd" {
		t.Fatalf("undescribed stream should be named by its tag: %#v", info.UndescribedStreams)
	}
	if !UndescribableDataStream(probe.Streams[0]) || UndescribableDataStream(Stream{CodecType: "video", CodecName: "unknown"}) {
		t.Fatal("undescribable rule misclassifies streams")
	}
}

func TestPixelFormatDepthReadsOnlyExplicitDepths(t *testing.T) {
	t.Parallel()
	for format, want := range map[string]int{
		"yuv420p10le": 10, "yuv422p12be": 12, "yuv444p16le": 16, "gbrp14le": 14, "yuva420p9be": 9,
		"gray16be": 16, "gray10le": 10, "ya16le": 16, "p010le": 10, "p016le": 16, "p210be": 10,
		"rgb48le": 16, "bgra64be": 16, "x2rgb10le": 10,
		// Digits in a chroma-subsampling group are not a depth, and formats
		// that state none give none.
		"nv12": 0, "nv21": 0, "yuv410p": 0, "yuv411p": 0, "yuv420p": 0, "yuvj422p": 0, "rgb24": 0, "gray": 0, "": 0,
	} {
		if got := pixelFormatDepth(format); got != want {
			t.Errorf("pixelFormatDepth(%q) = %d, want %d", format, got, want)
		}
	}
}

func TestContainerMappingCarriesContainerIdentifiersForWholeFiles(t *testing.T) {
	t.Parallel()
	transport := Probe{
		Format: Format{Name: "mpegts", StartTime: "1.4", Duration: "10.0"},
		Streams: []Stream{
			{Index: 0, ID: "0x100", CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{Index: 1, ID: "0x101", CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
		},
	}
	_, info, err := BuildFlow(transport, testIdentity(), "video/mp2t", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	audio := info.Collected[1]
	if pid, _ := audio.ContainerMapping["mp2ts_container"].(map[string]any); pid["pid"] != int64(0x101) || audio.ContainerMapping["track_index"] != 1 {
		t.Fatalf("transport stream mapping = %#v", audio.ContainerMapping)
	}
	// Rendering retains positions and replaces source identifiers without
	// changing the preserved mapping.
	ApplyRenderedTrackMapping(&info)
	mapping := info.Collected[1].ContainerMapping
	if len(mapping) != 2 || mapping["track_index"] != 1 || mapping["format_track_index"] != 0 {
		t.Fatalf("rendered mapping = %#v", mapping)
	}
	if _, present := audio.ContainerMapping["mp2ts_container"]; !present {
		t.Fatal("rendering changed the source mapping")
	}

	mp4 := transport
	mp4.Format.Name = "mov,mp4,m4a,3gp,3g2,mj2"
	mp4.Streams = []Stream{
		{Index: 0, ID: "0x1", CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "24/1"},
		{Index: 1, ID: "0x3", CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
	}
	if _, info, err = BuildFlow(mp4, testIdentity(), "video/mp4", EssenceStorageMuxed); err != nil {
		t.Fatal(err)
	}
	if track, _ := info.Collected[1].ContainerMapping["isobmff_container"].(map[string]any); track["track_id"] != int64(3) {
		t.Fatalf("MP4 mapping = %#v", info.Collected[1].ContainerMapping)
	}
	ApplyRenderedTrackMapping(&info)
	mapping = info.Collected[1].ContainerMapping
	if len(mapping) != 2 || mapping["track_index"] != 1 || mapping["format_track_index"] != 0 {
		t.Fatalf("rendered MP4 mapping = %#v", mapping)
	}
	if id, ok := parseStreamID("0xzz"); ok || id != 0 {
		t.Fatalf("unreadable id parsed as %d", id)
	}
}

// Caption metadata remains describable when preserving the original bytes.
func TestClosedCaptionTrackIsDescribed(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{
			{Index: 0, CodecType: "video", CodecName: "prores", Width: 1920, Height: 1080, AverageFrameRate: "25/1"},
			{Index: 1, CodecType: "audio", CodecName: "pcm_s24le", SampleRate: "48000", Channels: 2},
			{Index: 2, CodecType: "subtitle", CodecName: "eia_608"},
		},
	}
	_, info, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.UnsupportedCodecs) != 0 {
		t.Fatalf("caption track unsupported: %#v", info.UnsupportedCodecs)
	}
	captions := info.Collected[2]
	if captions.Role != "data" || captions.Flow["codec"] != "application/x-cea-608" {
		t.Fatalf("caption essence = %#v", captions)
	}
}
