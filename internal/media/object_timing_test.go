package media

import (
	"context"
	"errors"
	"math/big"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestObjectPacketBounds(t *testing.T) {
	for _, tc := range []struct {
		name, packets           string
		wantStart, wantDuration int64
		invalid                 bool
	}{
		{"reordered video", `[{"stream_index":0,"pts":80,"duration":40},{"stream_index":0,"pts":0,"duration":40},{"stream_index":0,"pts":40,"duration":40}]`, 0, 120000000, false},
		{"negative PTS", `[{"stream_index":0,"pts":-80,"duration":40}]`, -80000000, 40000000, false},
		{"audio padding", `[{"stream_index":0,"pts":0,"duration":40,"side_data_list":[{"skip_samples":480,"discard_padding":480}]}]`, 10000000, 20000000, false},
		{"missing duration", `[{"stream_index":0,"pts":0}]`, 0, 0, true},
		{"missing PTS", `[{"stream_index":0,"duration":40}]`, 0, 0, true},
		{"empty", `[]`, 0, 0, true},
		{"invalid padding", `[{"stream_index":0,"pts":0,"duration":40,"side_data_list":[{"skip_samples":4800}]}]`, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := Probe{Format: Format{StartTime: "999", Duration: "999"},
				Streams: []Stream{{Index: 0, CodecType: "audio", TimeBase: "1/1000", SampleRate: "48000"}}}
			err := measureObjectPackets(strings.NewReader(`{"packets":`+tc.packets+`}`), &probe)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid timing accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			start, duration, err := ProbeTiming(probe)
			if err != nil || start != tc.wantStart || duration != tc.wantDuration {
				t.Fatalf("bounds = %d, %d, %v", start, duration, err)
			}
		})
	}
}

func TestSparseTracksDoNotSupplyObjectBounds(t *testing.T) {
	for _, kind := range []string{"subtitle", "data", "audio", "video"} {
		t.Run(kind, func(t *testing.T) {
			probe := Probe{Format: Format{Name: "mov,mp4", Tags: map[string]string{"major_brand": "isom"}}, Streams: []Stream{
				{Index: 0, CodecType: "video", CodecName: "h264", TimeBase: "1/1000", Width: 64, Height: 64, AverageFrameRate: "25/1"},
				{Index: 1, CodecType: kind, CodecName: "mov_text", StartTime: "-99", Duration: "999", TimeBase: "1/1000"},
			}}
			err := measureObjectPackets(strings.NewReader(`{"packets":[{"stream_index":0,"pts":2000,"duration":40}]}`), &probe)
			if kind == "audio" || kind == "video" {
				if err == nil {
					t.Fatal("missing A/V timing accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			start, duration, err := ProbeTiming(probe)
			if err != nil || start != 2000000000 || duration != 40000000 || !probe.Streams[1].Empty {
				t.Fatalf("bounds: %d %d %v; sparse=%+v", start, duration, err, probe.Streams[1])
			}
			_, info, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
			if err != nil || len(info.Collected) != 2 {
				t.Fatalf("sparse descriptor lost: %+v, %v", info, err)
			}
		})
	}
}

func TestMovTextClearingSampleHasNoPresentationDuration(t *testing.T) {
	for _, size := range []string{"2", "8"} {
		probe := Probe{Streams: []Stream{{Index: 0, CodecType: "subtitle", CodecName: "mov_text", TimeBase: "1/1000"}}}
		err := measureObjectPackets(strings.NewReader(`{"packets":[{"stream_index":0,"pts":2000,"size":"`+size+`"}]}`), &probe)
		if size == "2" {
			if err != nil || !probe.Streams[0].Empty {
				t.Fatalf("empty clearing sample: %v, %+v", err, probe.Streams[0])
			}
		} else if err == nil {
			t.Fatal("subtitle payload without duration accepted")
		}
	}
}

func TestTransportStreamTracksShareOnePresentationClock(t *testing.T) {
	probe := Probe{Format: Format{Name: "mpegts"}, Streams: []Stream{
		{Index: 0, CodecType: "video", TimeBase: "1/90000"},
		{Index: 1, CodecType: "audio", TimeBase: "1/90000", SampleRate: "48000"},
	}}
	// The first audio PTS is one AAC frame before zero. FFprobe may return
	// both wrapped and unwrapped counts even within a single PES group.
	const packets = `{"packets":[{"stream_index":0,"pts":0,"duration":3750},{"stream_index":1,"pts":8589932672,"duration":1920},{"stream_index":1,"pts":8589934592,"duration":1920},{"stream_index":1,"pts":1920,"duration":1920}]}`
	if err := measureObjectPackets(strings.NewReader(packets), &probe); err != nil {
		t.Fatal(err)
	}
	start, duration, err := ProbeTiming(probe)
	if err != nil || start != -21333333 || duration != 64000000 {
		t.Fatalf("mixed-epoch bounds: start=%d duration=%d error=%v", start, duration, err)
	}
	// Each stream's packets are named on that one clock, and the reference
	// stream's are what a Segment is placed by.
	if audio := probe.Streams[1]; audio.FirstPTS != -1920 || !slices.Equal(audio.LastPTS, []int64{-1920, 0, 1920}) {
		t.Fatalf("audio packets run from tick %d and end with %v, want -1920, 0 and 1920", audio.FirstPTS, audio.LastPTS)
	}
	reference, err := ProbeReference(probe)
	if err != nil || !reference.Ticks || reference.Audio || reference.FirstPTS != 0 || !slices.Equal(reference.LastPTS, []int64{0}) {
		t.Fatalf("reference = %+v, %v, want the one video packet at tick 0", reference, err)
	}
	// Three AAC frames of 1024 samples at 48 kHz are 1920 ticks apart.
	probe.Streams = probe.Streams[1:]
	if reference, err = ProbeReference(probe); err != nil || !reference.Ticks || !reference.Audio || reference.FirstPTS != -1920 ||
		reference.CyclePackets != 1 || reference.CycleTicks != 1920 {
		t.Fatalf("audio reference = %+v, %v, want packets from tick -1920 that are 1920 ticks apart", reference, err)
	}
}

// TestPacketCycle covers the packet period read from a measured MPEG-TS
// stream: a video stream's frame period, and for audio the whole number of
// samples nearest the mean step between its packets.
func TestPacketCycle(t *testing.T) {
	// first and last are the ticks nearest the first and the last of count
	// packets that are ticks over packets apart.
	last := func(packets, ticks int64, count int64) []int64 {
		return []int64{(2*(count-1)*ticks + packets) / (2 * packets)}
	}
	for _, tc := range []struct {
		name           string
		stream         Stream
		packets, ticks int64
	}{
		{"29.97 fps", Stream{CodecType: "video", AverageFrameRate: "30000/1001"}, 1, 3003},
		{"23.976 fps", Stream{CodecType: "video", AverageFrameRate: "24000/1001"}, 4, 15015},
		{"59.94 fps", Stream{CodecType: "video", AverageFrameRate: "60000/1001"}, 2, 3003},
		{"video of no stated rate", Stream{CodecType: "video", AverageFrameRate: "0/0", RealFrameRate: "0/0"}, 0, 0},
		{"48 kHz AAC", Stream{CodecType: "audio", SampleRate: "48000", SampleCount: 94, LastPTS: last(1, 1920, 94)}, 1, 1920},
		{"44.1 kHz AAC", Stream{CodecType: "audio", SampleRate: "44100", SampleCount: 86, LastPTS: last(49, 102400, 86)}, 49, 102400},
		{"44.1 kHz AAC in three packets", Stream{CodecType: "audio", SampleRate: "44100", SampleCount: 3, LastPTS: last(49, 102400, 3)}, 49, 102400},
		{"44.1 kHz MPEG audio", Stream{CodecType: "audio", SampleRate: "44100", SampleCount: 40, LastPTS: last(49, 115200, 40)}, 49, 115200},
		{"one audio packet", Stream{CodecType: "audio", SampleRate: "44100", SampleCount: 1, LastPTS: []int64{0}}, 0, 0},
		{"subtitles", Stream{CodecType: "subtitle", SampleCount: 4, LastPTS: []int64{9000}}, 0, 0},
	} {
		if packets, ticks := packetCycle(tc.stream); packets != tc.packets || ticks != tc.ticks {
			t.Errorf("%s: %d packets last %d ticks, want %d and %d", tc.name, packets, ticks, tc.packets, tc.ticks)
		}
	}
}

// TestPictureCycle covers which source stream gives a render its frame
// period: the one it is cut on, and only where that is video.
func TestPictureCycle(t *testing.T) {
	source := []Stream{
		{Index: 0, CodecType: "audio", SampleRate: "48000"},
		{Index: 1, CodecType: "video", AverageFrameRate: "24000/1001"},
		{Index: 2, CodecType: "video", AverageFrameRate: "25/1"},
	}
	for _, tc := range []struct {
		name           string
		streams        []Stream
		index          int
		packets, ticks int64
	}{
		{"a multiplex, cut on its first video", source, AllStreams, 4, 15015},
		{"a video essence", source, 2, 1, 3600},
		{"an audio essence", source, 0, 0, 0},
		{"a multiplex without video", source[:1], AllStreams, 0, 0},
		{"no stream", nil, AllStreams, 0, 0},
	} {
		if packets, ticks := PictureCycle(tc.streams, tc.index); packets != tc.packets || ticks != tc.ticks {
			t.Errorf("%s: %d pictures last %d ticks, want %d and %d", tc.name, packets, ticks, tc.packets, tc.ticks)
		}
	}
}

// TestLatestPacketsAreKeptInTheOrderTheyAreShown covers the timestamps a
// measurement keeps of a stream's last packets, which video stores out of
// the order it shows them in.
func TestLatestPacketsAreKeptInTheOrderTheyAreShown(t *testing.T) {
	var latest []int64
	// Each group of three is stored as it is decoded: the last shown first.
	for group := range int64(40) {
		for _, pts := range []int64{2, 0, 1} {
			latest = keepLatest(latest, 3*group+pts)
		}
	}
	if len(latest) != latestPackets || !slices.IsSorted(latest) || latest[0] != 120-latestPackets || latest[latestPackets-1] != 119 {
		t.Fatalf("latest = %v, want the last %d of 120 in order", latest, latestPackets)
	}
}

// TestVideoCutEvidence covers what an Object's packets say about the cut that
// began it. Only a video packet stored after the first shown frame and
// presenting before it is a hidden leading picture of an open GOP, and only a
// video stream other than the reference stream is held to beginning on a key
// frame.
func TestVideoCutEvidence(t *testing.T) {
	t.Parallel()
	const shown = `{"stream_index":0,"pts":0,"duration":40,"flags":"K__"},{"stream_index":0,"pts":120,"duration":40,"flags":"___"},{"stream_index":0,"pts":40,"duration":40,"flags":"___"},{"stream_index":0,"pts":80,"duration":40,"flags":"___"}`
	for name, tc := range map[string]struct {
		packets string
		hidden  int
		leading []int64
		midGOP  bool
	}{
		"closed GOP":                                                     {packets: shown},
		"leading pictures after the cut":                                 {packets: `{"stream_index":0,"pts":0,"duration":40,"flags":"K__"},{"stream_index":0,"pts":-40,"duration":40,"flags":"_D_"},{"stream_index":0,"pts":-80,"duration":40,"flags":"_D_"},{"stream_index":0,"pts":40,"duration":40,"flags":"___"}`, hidden: 2},
		"discarded before the first shown":                               {packets: `{"stream_index":0,"pts":-80,"duration":40,"flags":"KD_"},{"stream_index":0,"pts":-40,"duration":40,"flags":"_D_"},` + shown},
		"discarded after the last shown":                                 {packets: shown + `,{"stream_index":0,"pts":160,"duration":40,"flags":"_D_"}`},
		"discarded audio priming":                                        {packets: `{"stream_index":1,"pts":0,"duration":21,"flags":"K__"},{"stream_index":1,"pts":-21,"duration":21,"flags":"KD_"},` + shown},
		"the reference stream is cut on its key frames by the segmenter": {packets: `{"stream_index":0,"pts":0,"duration":40,"flags":"___"},{"stream_index":0,"pts":40,"duration":40,"flags":"K__"}`},
		"a second video stream on a key frame":                           {packets: shown + `,{"stream_index":2,"pts":0,"duration":40,"flags":"K__"},{"stream_index":2,"pts":40,"duration":40,"flags":"___"}`},
		"a second video stream between key frames":                       {packets: shown + `,{"stream_index":2,"pts":0,"duration":40,"flags":"___"},{"stream_index":2,"pts":40,"duration":40,"flags":"K__"}`, midGOP: true},
		"a second video stream whose first stored picture is hidden":     {packets: shown + `,{"stream_index":2,"pts":-80,"duration":40,"flags":"_D_"},{"stream_index":2,"pts":80,"duration":40,"flags":"___"},{"stream_index":2,"pts":0,"duration":40,"flags":"___"}`, midGOP: true},
		"leading pictures of a second video stream":                      {packets: shown + `,{"stream_index":2,"pts":0,"duration":40,"flags":"K__"},{"stream_index":2,"pts":-40,"duration":40,"flags":"_D_"},{"stream_index":2,"pts":40,"duration":40,"flags":"___"}`, hidden: 1},
		"leading pictures an Object shows":                               {packets: `{"stream_index":0,"pts":80,"duration":40,"flags":"K__"},{"stream_index":0,"pts":0,"duration":40,"flags":"___"},{"stream_index":0,"pts":40,"duration":40,"flags":"___"},{"stream_index":0,"pts":120,"duration":40,"flags":"___"}`, leading: []int64{0, 40}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			probe := Probe{Streams: []Stream{
				{Index: 0, CodecType: "video", TimeBase: "1/1000"},
				{Index: 1, CodecType: "audio", TimeBase: "1/1000", SampleRate: "48000"},
			}}
			packets := tc.packets
			if !strings.Contains(packets, `"stream_index":1`) {
				packets += `,{"stream_index":1,"pts":0,"duration":21,"flags":"K__"}`
			}
			if strings.Contains(packets, `"stream_index":2`) {
				probe.Streams = append(probe.Streams, Stream{Index: 2, CodecType: "video", TimeBase: "1/1000"})
			}
			if err := measureObjectPackets(strings.NewReader(`{"packets":[`+packets+`]}`), &probe); err != nil {
				t.Fatal(err)
			}
			if hidden, midGOP := VideoCut(probe); hidden != tc.hidden || midGOP != tc.midGOP || probe.Streams[1].HiddenLead != 0 || probe.Streams[1].MidGOP {
				t.Fatalf("VideoCut() = %d hidden, mid-GOP %t (audio: %d, %t); want %d, %t",
					hidden, midGOP, probe.Streams[1].HiddenLead, probe.Streams[1].MidGOP, tc.hidden, tc.midGOP)
			}
			if leading := probe.Streams[0].LeadingPTS; !slices.Equal(leading, tc.leading) || probe.Streams[1].LeadingPTS != nil {
				t.Fatalf("leading pictures shown at %v (audio: %v), want %v", leading, probe.Streams[1].LeadingPTS, tc.leading)
			}
		})
	}
}

func TestLeadingPictureScan(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		packets string
		open    bool
	}{
		"closed GOPs":                  {"pts=0|flags=K__\npts=120|flags=___\npts=40|flags=___\npts=80|flags=___\npts=160|flags=K__\npts=280|flags=___\npts=200|flags=___", false},
		"open GOP after the first":     {"pts=0|flags=K__\npts=80|flags=___\npts=40|flags=___\npts=200|flags=K__\npts=120|flags=___\npts=160|flags=___", true},
		"open GOP on the first only":   {"pts=80|flags=K__\npts=0|flags=___\npts=40|flags=___\npts=200|flags=___\npts=120|flags=___\npts=240|flags=K__\npts=280|flags=___", false},
		"packets before the first key": {"pts=0|flags=___\npts=40|flags=___\npts=80|flags=K__\npts=120|flags=___\npts=160|flags=K__\npts=200|flags=___", false},
		"untimed packet after a key":   {"pts=0|flags=K__\npts=40|flags=___\npts=80|flags=K__\npts=N/A|flags=___\npts=120|flags=___", false},
		"negative leading picture":     {"pts=-80|flags=K__\npts=-40|flags=___\npts=40|flags=K__\npts=0|flags=___", true},
		"no packets":                   {"", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := scanLeadingPictures(strings.NewReader(tc.packets))
			if (err == errLeadingPicture) != tc.open || (err != nil && err != errLeadingPicture) {
				t.Fatalf("scan = %v, want open=%t", err, tc.open)
			}
		})
	}
}

// TestProbeLeadingPicturesOnRealMedia reads MPEG-2 video, which FFmpeg
// encodes with open GOPs unless told otherwise, and the same video closed.
func TestProbeLeadingPicturesOnRealMedia(t *testing.T) {
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	directory := t.TempDir()
	for name, tc := range map[string]struct {
		arguments []string
		open      bool
	}{
		"open":   {nil, true},
		"closed": {[]string{"-flags", "+cgop", "-sc_threshold", "1000000000"}, false},
	} {
		input := filepath.Join(directory, name+".mp4")
		buildFixture(t, append(append([]string{"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=4",
			"-map", "0:a", "-map", "1:v", "-c:a", "aac", "-c:v", "mpeg2video", "-g", "12", "-bf", "2"}, tc.arguments...), input)...)
		// Audio is stream 0, so the scan must select the video by its index.
		open, err := (FFprobe{}).ProbeLeadingPictures(context.Background(), input, 1)
		if err != nil || open != tc.open {
			t.Errorf("%s GOP video: leading pictures = %t, %v; want %t", name, open, err, tc.open)
		}
		if open, err := (FFprobe{}).ProbeLeadingPictures(context.Background(), input, 0); err != nil || open {
			t.Errorf("%s GOP video: audio stream reported leading pictures (%t, %v)", name, open, err)
		}
	}
	if _, err := (FFprobe{}).ProbeLeadingPictures(context.Background(), filepath.Join(directory, "missing.mp4"), 0); err == nil {
		t.Error("a missing input was read as having no open GOP")
	}
}

// TestFirstKeyFrameSearch reads captures that begin mid-GOP. The search
// follows the stream to its first key frame however long the GOP, and gives
// up, without a result, at the end of the input or at KeyFrameSearchLimit.
func TestFirstKeyFrameSearch(t *testing.T) {
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	directory := t.TempDir()
	capture := func(name string, encode, cut []string) string {
		full, input := filepath.Join(directory, name+"-full.mp4"), filepath.Join(directory, name+".mp4")
		buildFixture(t, append(append([]string{"-f", "lavfi", "-i"}, encode...), full)...)
		buildFixture(t, append(append([]string{"-i", full, "-ss", "0.5"}, cut...), "-c", "copy", "-copyinkf", input)...)
		return input
	}
	// x264 places a key frame every 250 frames by default: 10.4 s at 24 fps.
	long := []string{"testsrc2=size=128x96:rate=24:duration=30", "-c:v", "libx264", "-preset", "veryfast"}
	// One frame a second keeps a GOP of more than five minutes small.
	sparse := []string{"testsrc2=size=64x64:rate=1:duration=400", "-c:v", "libx264", "-preset", "veryfast",
		"-g", "331", "-keyint_min", "331", "-sc_threshold", "0", "-bf", "0"}
	for name, tc := range map[string]struct {
		input string
		// earliest is the least presentation time the file's first key frame
		// must have for the case to be real; negative means it has none.
		earliest time.Duration
		found    bool
	}{
		"key frame after ten seconds":     {capture("long", long, nil), 9 * time.Second, true},
		"input ends before a key frame":   {capture("short", long, []string{"-t", "5"}), -1, false},
		"key frame past the search limit": {capture("sparse", sparse, nil), KeyFrameSearchLimit, false},
	} {
		t.Run(name, func(t *testing.T) {
			// The expected time is the key frame's integer timestamp in the
			// stream's time base; FFprobe's printed time is rounded.
			listing, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "0", "-show_entries", "packet=pts,flags:stream=time_base",
				"-of", "csv=p=1", tc.input).Output()
			if err != nil {
				t.Fatal(err)
			}
			var (
				stream   Stream
				keyFrame *big.Rat
			)
			for _, line := range strings.Split(string(listing), "\n") {
				fields := strings.Split(line, ",")
				switch {
				case fields[0] == "stream":
					stream.TimeBase = fields[1]
				case fields[0] == "packet" && keyFrame == nil && strings.Contains(fields[2], "K"):
					ticks, err := strconv.ParseInt(fields[1], 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					keyFrame = big.NewRat(ticks, 1)
				}
			}
			tick, ok := new(big.Rat).SetString(stream.TimeBase)
			if !ok {
				t.Fatalf("the fixture's time base is %q", stream.TimeBase)
			}
			var first *big.Rat
			if keyFrame != nil {
				first = keyFrame.Mul(keyFrame, tick)
			}
			if (first == nil) != (tc.earliest < 0) || (first != nil && first.Cmp(big.NewRat(int64(tc.earliest), int64(time.Second))) < 0) {
				t.Fatalf("the fixture's first key frame is at %v s, want none or one after %s", first, tc.earliest)
			}
			at, found, err := (FFprobe{}).ProbeFirstKeyFrame(context.Background(), tc.input, stream)
			if err != nil || found != tc.found || (found && at.Cmp(first) != 0) {
				t.Fatalf("ProbeFirstKeyFrame() = %v, %t, %v; want found=%t at %v", at, found, err, tc.found, first)
			}
		})
	}
	stream := Stream{TimeBase: "1/12288"}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, found, err := (FFprobe{}).ProbeFirstKeyFrame(cancelled, filepath.Join(directory, "long.mp4"), stream); !errors.Is(err, context.Canceled) || found {
		t.Fatalf("a cancelled search returned found=%t, %v", found, err)
	}
	if _, _, err := (FFprobe{}).ProbeFirstKeyFrame(context.Background(), filepath.Join(directory, "missing.mp4"), stream); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("a missing input returned %v", err)
	}
	if _, _, err := (FFprobe{}).ProbeFirstKeyFrame(context.Background(), filepath.Join(directory, "long.mp4"), Stream{}); err == nil {
		t.Fatal("a stream without a time base was searched")
	}
}

// TestOffsetsAreReadInStreamTicks covers a container whose streams begin
// between microseconds. FFprobe prints 0.040250 and 0.003333 for these
// starts, and offsets worked from the printed values would be a third of a
// microsecond out.
func TestOffsetsAreReadInStreamTicks(t *testing.T) {
	t.Parallel()
	video, audio := int64(966), int64(160)
	probe := Probe{Format: Format{StartTime: "0.003333"}, Streams: []Stream{
		{Index: 0, CodecType: "video", TimeBase: "1/24000", StartTime: "0.040250", StartPTS: &video},
		{Index: 1, CodecType: "audio", TimeBase: "1/48000", StartTime: "0.003333", StartPTS: &audio},
	}}
	start, err := probeStart(probe, probe.Streams)
	if err != nil || start != 3333000 {
		t.Fatalf("container start = %d, %v", start, err)
	}
	// 966/24000 s less 160/48000 s is 36916666.67 ns.
	if offset, err := referenceOffset(probe.Streams, start); err != nil || offset != 36916667 {
		t.Fatalf("video offset = %d, %v; want 36916667", offset, err)
	}
	if offset, err := streamOffset(probe.Streams, probe.Streams[1], start); err != nil || offset != 0 {
		t.Fatalf("audio offset = %d, %v; want none", offset, err)
	}
	// 295302/24000 s less 160/48000 s is 12300916666.67 ns.
	if offset, err := PresentationOffset(probe, start, big.NewRat(295302, 24000)); err != nil || offset != 12300916667 {
		t.Fatalf("key frame offset = %d, %v; want 12300916667", offset, err)
	}
	// A start beyond the nanosecond timeline is an error, not a wrapped value.
	beyond := int64(9223372036855)
	distant := Stream{Index: 0, CodecType: "video", TimeBase: "1/1000", StartTime: "9223372036.855", StartPTS: &beyond}
	if offset, err := streamOffset([]Stream{distant}, distant, 0); err == nil {
		t.Fatalf("a stream start beyond the timeline range gave offset %d", offset)
	}
	if offset, err := PresentationOffset(Probe{Streams: []Stream{distant}}, 0, big.NewRat(beyond, 1000)); err == nil {
		t.Fatalf("a key frame beyond the timeline range gave offset %d", offset)
	}
	// Two streams that both print 0.000001: the earlier begins the container
	// and the later keeps its 300 ns after it.
	earlier, later := int64(6), int64(9)
	close := []Stream{
		{Index: 0, CodecType: "video", TimeBase: "1/10000000", StartTime: "0.000001", StartPTS: &later},
		{Index: 1, CodecType: "video", TimeBase: "1/10000000", StartTime: "0.000001", StartPTS: &earlier},
	}
	for index, want := range []int64{300, 0} {
		if offset, err := streamOffset(close, close[index], 1000); err != nil || offset != want {
			t.Fatalf("stream %d offset = %d, %v; want %d", index, offset, err, want)
		}
	}
	// Without ticks the printed times are all there is.
	printed := []Stream{{Index: 0, CodecType: "video", StartTime: "0.040250"}, {Index: 1, CodecType: "audio", StartTime: "0.003333"}}
	if offset, err := referenceOffset(printed, start); err != nil || offset != 36917000 {
		t.Fatalf("printed video offset = %d, %v; want 36917000", offset, err)
	}
	// A measured Object's start replaces the source ticks.
	object := Probe{Streams: []Stream{{Index: 0, CodecType: "video", TimeBase: "1/24000", StartPTS: &video}}}
	if err := measureObjectPackets(strings.NewReader(`{"packets":[{"stream_index":0,"pts":80,"duration":1001,"flags":"K__"}]}`), &object); err != nil {
		t.Fatal(err)
	}
	if object.Streams[0].StartPTS != nil || object.Streams[0].StartTime != "1/300" {
		t.Fatalf("measured stream starts at %q with source ticks %v, want 1/300 and none", object.Streams[0].StartTime, object.Streams[0].StartPTS)
	}
}
