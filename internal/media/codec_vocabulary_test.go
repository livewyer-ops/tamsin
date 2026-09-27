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
		"tmcd": "", "bin_data": "", "timed_id3": "", "ttf": "",
	} {
		if got := codecMIME(codec); got != want {
			t.Errorf("codecMIME(%q) = %q, want %q", codec, got, want)
		}
	}
}

func TestJPEG2000IsVideoWhenItMoves(t *testing.T) {
	t.Parallel()
	moving := Probe{Format: Format{Name: "mxf", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{{CodecName: "jpeg2000", CodecType: "video", Width: 1920, Height: 1080, AverageFrameRate: "25/1"}}}
	flow, _, err := BuildFlow(moving, testIdentity(), "application/mxf", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if flow["codec"] != "video/jp2" || flow["format"] != "urn:x-nmos:format:video" {
		t.Fatalf("moving JPEG 2000 = %v / %v", flow["codec"], flow["format"])
	}
	still := Probe{Format: Format{Name: "j2k_pipe"},
		Streams: []Stream{{CodecName: "jpeg2000", CodecType: "video", Width: 64, Height: 32}}}
	if flow, _, err = BuildFlow(still, testIdentity(), "image/jp2", EssenceStorageMuxed); err != nil {
		t.Fatal(err)
	}
	if flow["codec"] != "image/jp2" || flow["format"] != "urn:x-tam:format:image" {
		t.Fatalf("still JPEG 2000 = %v / %v", flow["codec"], flow["format"])
	}
}

func TestG711IsACodecNotUncompressedPCM(t *testing.T) {
	t.Parallel()
	probe := Probe{Format: Format{Name: "wav", StartTime: "0.0", Duration: "1.0"},
		Streams: []Stream{{CodecName: "pcm_alaw", CodecType: "audio", SampleRate: "8000", Channels: 1}}}
	flow, _, err := BuildFlow(probe, testIdentity(), "audio/wav", EssenceStorageMuxed)
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

func TestUndescribableDataTracksAreDroppedNotInvented(t *testing.T) {
	t.Parallel()
	probe := Probe{
		Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{
			{Index: 0, CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "24/1"},
			{Index: 1, CodecType: "data", CodecName: "tmcd"},
			{Index: 2, CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
		},
	}
	flow, info, err := BuildFlow(probe, testIdentity(), "video/quicktime", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.UnsupportedCodecs) != 0 {
		t.Fatalf("a timecode track is not an unsupported essence: %#v", info.UnsupportedCodecs)
	}
	if len(info.DroppedStreams) != 1 || info.DroppedStreams[0] != (UnsupportedCodec{Name: "tmcd", StreamType: "data", StreamIndex: 1}) {
		t.Fatalf("dropped streams = %#v", info.DroppedStreams)
	}
	if got := DroppedStreamIndices(info); len(got) != 1 || got[0] != 1 {
		t.Fatalf("dropped indices = %v", got)
	}
	if len(info.Collected) != 2 || info.Collected[0].Role != "video" || info.Collected[1].Role != "audio" {
		t.Fatalf("collected = %#v", info.Collected)
	}
	if flow["format"] != "urn:x-nmos:format:multi" {
		t.Fatalf("root format = %v", flow["format"])
	}
	// In the source container the audio is track 2; a render leaves the
	// timecode track out, so there it is track 1.
	audio := info.Collected[1]
	if audio.ContainerMapping["track_index"] != 2 || audio.ContainerMapping["format_track_index"] != 0 {
		t.Fatalf("source mapping = %#v", audio.ContainerMapping)
	}
	if audio.RenderedContainerMapping["track_index"] != 1 || audio.RenderedContainerMapping["format_track_index"] != 0 {
		t.Fatalf("rendered mapping = %#v", audio.RenderedContainerMapping)
	}
	if video := info.Collected[0].RenderedContainerMapping; video["track_index"] != 0 || video["format_track_index"] != 0 {
		t.Fatalf("video precedes the dropped track and keeps its position: %#v", video)
	}
	ApplyRenderedTrackMapping(&info)
	if info.Collected[1].ContainerMapping["track_index"] != 1 || info.Collected[1].RenderedContainerMapping != nil {
		t.Fatalf("applied mapping = %#v", info.Collected[1])
	}

	// Two data tracks: a dropped one ahead of a described one shifts the
	// described one's format_track_index in a render but not in the source.
	probe.Streams = append(probe.Streams, Stream{Index: 3, CodecType: "data", CodecName: "scte_35"})
	if _, info, err = BuildFlow(probe, testIdentity(), "video/quicktime", EssenceStorageMuxed); err != nil {
		t.Fatal(err)
	}
	data := info.Collected[2]
	if data.Role != "data" || data.Flow["codec"] != "application/x-scte35" ||
		data.ContainerMapping["track_index"] != 3 || data.ContainerMapping["format_track_index"] != 1 ||
		data.RenderedContainerMapping["track_index"] != 2 || data.RenderedContainerMapping["format_track_index"] != 0 {
		t.Fatalf("data essence = %#v", data)
	}
}

func TestOnlyUndescribableTracksIsAnError(t *testing.T) {
	t.Parallel()
	// FFprobe names a QuickTime timecode track only by its tag.
	probe := Probe{Format: Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", StartTime: "0.0", Duration: "10.0"},
		Streams: []Stream{{Index: 0, CodecType: "data", CodecTagString: "tmcd"}}}
	if _, _, err := BuildFlow(probe, testIdentity(), "video/quicktime", EssenceStorageMuxed); err == nil || !strings.Contains(err.Error(), "stream 0 is tmcd") {
		t.Fatalf("an input with nothing describable: %v", err)
	}
	probe.Streams = append(probe.Streams, Stream{Index: 1, CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2})
	_, info, err := BuildFlow(probe, testIdentity(), "video/quicktime", EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.DroppedStreams) != 1 || info.DroppedStreams[0].Name != "tmcd" {
		t.Fatalf("dropped stream should be named by its tag: %#v", info.DroppedStreams)
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
	// FFmpeg reassigns PIDs when it renders, so a rendered Segment maps by
	// position alone.
	if _, present := audio.RenderedContainerMapping["mp2ts_container"]; present || audio.RenderedContainerMapping["track_index"] != 1 {
		t.Fatalf("rendered mapping = %#v", audio.RenderedContainerMapping)
	}

	quicktime := transport
	quicktime.Format.Name = "mov,mp4,m4a,3gp,3g2,mj2"
	quicktime.Streams = []Stream{
		{Index: 0, ID: "0x1", CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "24/1"},
		{Index: 1, ID: "0x3", CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
	}
	if _, info, err = BuildFlow(quicktime, testIdentity(), "video/quicktime", EssenceStorageMuxed); err != nil {
		t.Fatal(err)
	}
	if track, _ := info.Collected[1].ContainerMapping["isobmff_container"].(map[string]any); track["track_id"] != int64(3) {
		t.Fatalf("QuickTime mapping = %#v", info.Collected[1].ContainerMapping)
	}
	// A container with no identifier vocabulary in AppNote 0006 carries none,
	// and an unreadable id is left out rather than guessed.
	matroska := quicktime
	matroska.Format.Name = "matroska,webm"
	if _, info, err = BuildFlow(matroska, testIdentity(), "video/matroska", EssenceStorageMuxed); err != nil {
		t.Fatal(err)
	}
	if len(info.Collected[1].ContainerMapping) != 2 {
		t.Fatalf("Matroska mapping = %#v", info.Collected[1].ContainerMapping)
	}
	if id, ok := parseStreamID("0xzz"); ok || id != 0 {
		t.Fatalf("unreadable id parsed as %d", id)
	}
}
