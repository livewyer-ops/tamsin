package media

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestPresentationTimestampClassification(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		timestamps string
		rate       string
		want       CadenceEvidence
	}{
		{name: "fixed integer ticks", timestamps: "0\n40\n80\n120\n", rate: "25/1", want: CadenceFixed},
		{name: "fixed quantised rational", timestamps: "0\n33\n67\n100\n", rate: "30000/1001", want: CadenceFixed},
		{name: "variable", timestamps: "0\n40\n80\n130\n", rate: "0/0", want: CadenceVariable},
		{name: "duplicate presentation timestamp", timestamps: "0\n40\n40\n", rate: "25/1", want: CadenceUnknown},
		{name: "overflowing presentation step", timestamps: "9223372036854775807\n-9223372036854775808\n", rate: "25/1", want: CadenceUnknown},
		{name: "missing timestamp", timestamps: "0\nN/A\n80\n", rate: "25/1", want: CadenceUnknown},
		{name: "one frame with declared rate", timestamps: "0\n", rate: "25/1", want: CadenceFixed},
		{name: "one frame with no rate", timestamps: "0\n", rate: "0/0", want: CadenceUnknown},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			stream := Stream{Index: 7, CodecType: "video", AverageFrameRate: testCase.rate}
			state := &cadenceState{stream: &stream}
			var compact strings.Builder
			for _, timestamp := range strings.Fields(testCase.timestamps) {
				compact.WriteString("stream_index=7|best_effort_timestamp=")
				compact.WriteString(timestamp)
				compact.WriteString("|side_data_type=ignored\n")
			}
			if err := scanPresentationTimestamps(strings.NewReader(compact.String()), map[int]*cadenceState{7: state}, "best_effort_timestamp"); err != nil {
				t.Fatal(err)
			}
			state.finish()
			if stream.Cadence != testCase.want {
				t.Fatalf("Cadence = %v, want %v", stream.Cadence, testCase.want)
			}
		})
	}
}

func TestPacketTimestampClassificationUsesTheSameStrictRules(t *testing.T) {
	t.Parallel()
	stream := Stream{Index: 7, CodecType: "video", AverageFrameRate: "25/1"}
	state := &cadenceState{stream: &stream}
	if err := scanPresentationTimestamps(strings.NewReader(
		"stream_index=7|pts=0\nstream_index=7|pts=40\nstream_index=7|pts=35\n"),
		map[int]*cadenceState{7: state}, "pts"); err != nil {
		t.Fatal(err)
	}
	state.finish()
	if stream.Cadence != CadenceUnknown {
		t.Fatalf("non-monotonic packet timestamps produced cadence %v, want unknown and decoded-frame fallback", stream.Cadence)
	}
}

// A transport stream's 33-bit timestamps wrap about every 26.5 hours. The
// pictures either side of a wrap are one period apart, and only a transport
// stream's timestamps are read that way.
func TestCadenceScanFollowsATransportStreamAcrossAWrap(t *testing.T) {
	t.Parallel()
	const pictures = "stream_index=0|pts=8589927392\nstream_index=0|pts=8589930992\nstream_index=0|pts=0\nstream_index=0|pts=3600\n"
	for _, testCase := range []struct {
		format string
		want   CadenceEvidence
		last   int64
	}{
		{format: "mpegts", want: CadenceFixed, last: mpegTSWrap + 3600},
		{format: "mov,mp4,m4a,3gp,3g2,mj2", want: CadenceUnknown, last: 3600},
	} {
		probe := Probe{Streams: []Stream{{CodecType: "video", TimeBase: "1/90000", AverageFrameRate: "25/1"}}}
		probe.Format.Name = testCase.format
		states := cadenceStates(&probe)
		if err := scanPresentationTimestamps(strings.NewReader(pictures), states, "pts"); err != nil {
			t.Fatal(err)
		}
		states[0].finish()
		span := probe.Streams[0].Presentation
		if probe.Streams[0].Cadence != testCase.want || span.Last != testCase.last {
			t.Errorf("%s: cadence = %v, last = %d; want %v and %d", testCase.format, probe.Streams[0].Cadence, span.Last, testCase.want, testCase.last)
		}
		if testCase.want == CadenceFixed && (span.MinimumStep != 3600 || span.MaximumStep != 3600) {
			t.Errorf("%s: steps = %d to %d, want 3600", testCase.format, span.MinimumStep, span.MaximumStep)
		}
	}
}

func TestCadenceTimelineRetainsBoundaryEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		step, nextEnd int64
		// leading is when the second Object's packets show its leading
		// pictures, against the zero of the first frame read from it.
		leading []int64
		want    CadenceEvidence
	}{
		{"fixed", 40, 2_000_000_000, nil, CadenceFixed},
		{"gap at boundary", 40, 2_040_000_000, nil, CadenceVariable},
		{"leading pictures a decoder drops", 40, 2_080_000_000, []int64{-40, -80}, CadenceFixed},
		{"leading pictures a decoder shows", 40, 2_000_000_000, []int64{40, 0}, CadenceFixed},
		{"leading pictures at uneven steps", 40, 2_080_000_000, []int64{-30, -80}, CadenceVariable},
		{"gap before the leading pictures", 40, 2_280_000_000, []int64{-40}, CadenceVariable},
		{"gap between the leading pictures and the cut frame", 40, 2_240_000_000, []int64{-240}, CadenceVariable},
		{"later different constant rate", 50, 2_250_000_000, nil, CadenceVariable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := Stream{CodecType: "video", TimeBase: "1/1000", AverageFrameRate: "25/1",
				Presentation: PresentationSpan{First: 1480, Last: 2440, LastDuration: 40, Frames: 25, MinimumStep: 40, MaximumStep: 40}}
			var timeline CadenceTimeline
			if cadence, err := timeline.Observe(stream, stream, 1_000_000_000); err != nil || cadence != CadenceFixed {
				t.Fatalf("initial cadence=%v, %v", cadence, err)
			}
			stream.Presentation = PresentationSpan{First: 0, Last: tc.step * 24, LastDuration: tc.step, Frames: 25, MinimumStep: tc.step, MaximumStep: tc.step}
			stream.LeadingPTS = tc.leading
			cadence, err := timeline.Observe(stream, stream, tc.nextEnd)
			if err != nil || cadence != tc.want {
				t.Fatalf("second cadence=%v, want %v, err=%v", cadence, tc.want, err)
			}
		})
	}
}

// The packets of a transport stream Object may be read past a timestamp wrap
// that the frames decoded from it are read before.
func TestCadenceTimelineReadsLeadingPicturesAcrossAWrap(t *testing.T) {
	t.Parallel()
	stream := Stream{CodecType: "video", TimeBase: "1/90000", AverageFrameRate: "25/1",
		Presentation: PresentationSpan{First: 0, Last: 86400, LastDuration: 3600, Frames: 25, MinimumStep: 3600, MaximumStep: 3600}}
	var timeline CadenceTimeline
	if cadence, err := timeline.Observe(stream, stream, 1_000_000_000); err != nil || cadence != CadenceFixed {
		t.Fatalf("initial cadence=%v, %v", cadence, err)
	}
	stream.Presentation.First, stream.Presentation.Last = 100, 86500
	stream.LeadingPTS = []int64{mpegTSWrap + 100 - 3600, mpegTSWrap + 100 - 7200}
	if cadence, err := timeline.Observe(stream, stream, 2_080_000_000); err != nil || cadence != CadenceFixed {
		t.Fatalf("cadence=%v, %v; want the leading pictures found before the cut frame", cadence, err)
	}
}

func TestCadenceTimelineAllowsQuantisedRationalRateAndRejectsOverlap(t *testing.T) {
	t.Parallel()
	stream := Stream{CodecType: "video", TimeBase: "1/1000", AverageFrameRate: "30000/1001",
		Presentation: PresentationSpan{First: 0, Last: 967, LastDuration: 34, Frames: 30, MinimumStep: 33, MaximumStep: 34}}
	var timeline CadenceTimeline
	for _, end := range []int64{1_001_000_000, 2_002_000_000} {
		if cadence, err := timeline.Observe(stream, stream, end); err != nil || cadence != CadenceFixed {
			t.Fatalf("quantised cadence=%v, %v", cadence, err)
		}
	}
	if _, err := timeline.Observe(stream, stream, 2_002_000_000); err == nil {
		t.Fatal("repeated interval was accepted")
	}
}

// TestProbePresentationDistinguishesVariableCadence is the real-media guard
// against falling back to avg_frame_rate/r_frame_rate heuristics. It also pins
// the packet fast path and decoded-frame fallback on real media.
func TestProbePresentationDistinguishesVariableCadence(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("needs ffmpeg")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("needs ffprobe")
	}
	directory := t.TempDir()

	fixed := filepath.Join(directory, "fixed.mp4")
	buildFixture(t,
		"-f", "lavfi", "-i", "testsrc=size=128x96:rate=30000/1001:duration=2",
		"-c:v", "libx264", "-bf", "0", "-video_track_timescale", "1000", fixed,
	)
	reordered := filepath.Join(directory, "reordered.mp4")
	buildFixture(t,
		"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=2",
		"-c:v", "libx264", "-bf", "3", "-x264-params", "b-adapt=0", reordered,
	)
	variable := filepath.Join(directory, "variable.mp4")
	buildFixture(t,
		"-f", "lavfi", "-i", "testsrc=size=128x96:rate=24:duration=1",
		"-f", "lavfi", "-i", "testsrc=size=128x96:rate=30:duration=1",
		"-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]",
		"-map", "[v]", "-fps_mode", "vfr", "-c:v", "libx264", "-bf", "0", variable,
	)

	for _, testCase := range []struct {
		name string
		path string
		want CadenceEvidence
	}{
		{name: "fixed quantised", path: fixed, want: CadenceFixed},
		{name: "fixed reordered", path: reordered, want: CadenceFixed},
		{name: "variable", path: variable, want: CadenceVariable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			prober := FFprobe{}
			packetProbe, err := prober.Probe(context.Background(), testCase.path)
			if err != nil {
				t.Fatal(err)
			}
			complete, err := prober.probePresentationMode(context.Background(), testCase.path, &packetProbe, presentationPackets)
			if err != nil {
				t.Fatal(err)
			}
			if testCase.path == reordered {
				if packetProbe.Streams[0].HasBFrames == 0 || complete {
					t.Fatal("fixture must contain B-frames and non-monotonic packet PTS")
				}
			} else if !complete || packetProbe.Streams[0].Cadence != testCase.want {
				t.Fatal("packet evidence did not classify non-reordered media correctly")
			}
			frameProbe, err := prober.Probe(context.Background(), testCase.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prober.probePresentationMode(context.Background(), testCase.path, &frameProbe, presentationFrames); err != nil {
				t.Fatal(err)
			}
			if err := prober.ProbePresentation(context.Background(), testCase.path, &packetProbe); err != nil {
				t.Fatal(err)
			}
			if got := packetProbe.Streams[0].Cadence; got != testCase.want || got != frameProbe.Streams[0].Cadence {
				t.Fatalf("Cadence = %v, want %v (avg=%q real=%q)",
					got, testCase.want, packetProbe.Streams[0].AverageFrameRate, packetProbe.Streams[0].RealFrameRate)
			}

			flow, _, err := BuildFlow(packetProbe, testIdentity(), "video/mp4", EssenceStorageMuxed)
			if err != nil {
				t.Fatal(err)
			}
			parameters := flow["essence_parameters"].(map[string]any)
			_, hasRate := parameters["frame_rate"]
			vfr, _ := parameters["vfr"].(bool)
			if testCase.want == CadenceVariable && (!vfr || hasRate) {
				t.Fatalf("variable Flow metadata = %#v", parameters)
			}
			if testCase.want == CadenceFixed && (vfr || !hasRate) {
				t.Fatalf("fixed Flow metadata = %#v", parameters)
			}
		})
	}
}

func TestProbeReadsDefensibleInterlaceEvidence(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("needs ffmpeg")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("needs ffprobe")
	}
	directory := t.TempDir()
	fixtures := []struct {
		name   string
		filter string
		top    string
		want   string
	}{
		{name: "top-field-first", filter: "tinterlace=interleave_top", top: "1", want: "interlaced_tff"},
		{name: "bottom-field-first", filter: "tinterlace=interleave_bottom", top: "0", want: "interlaced_bff"},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			path := filepath.Join(directory, fixture.name+".ts")
			buildFixture(t,
				"-f", "lavfi", "-i", "testsrc2=size=720x576:rate=50:duration=1",
				"-vf", fixture.filter, "-c:v", "mpeg2video", "-flags", "+ilme+ildct", "-top", fixture.top, path,
			)
			probe, err := (FFprobe{}).Probe(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if got := interlaceMode(probe.Streams[0].FieldOrder); got != fixture.want {
				t.Fatalf("field_order=%q maps to %q, want %q", probe.Streams[0].FieldOrder, got, fixture.want)
			}
		})
	}
}

func buildFixture(t *testing.T, arguments ...string) {
	t.Helper()
	arguments = append([]string{"-hide_banner", "-loglevel", "error", "-y"}, arguments...)
	command := exec.Command("ffmpeg", arguments...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("could not build real-media fixture: %v\n%s", err, output)
	}
}

func TestReorderedVideoSkipsPacketScan(t *testing.T) {
	// Keep executable creation outside parallel tests: another fork can inherit
	// its writable descriptor until exec and cause a transient ETXTBSY.
	executable := filepath.Join(t.TempDir(), "ffprobe")
	// A packet scan would fail: only a decoded-frame invocation is accepted.
	if err := os.WriteFile(executable, []byte("#!/bin/sh\ncase \" $* \" in *' -show_frames '*) printf 'stream_index=0|best_effort_timestamp=0\\nstream_index=0|best_effort_timestamp=40\\n';; *) exit 1;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	probe := Probe{Streams: []Stream{{CodecType: "video", HasBFrames: 2, AverageFrameRate: "25/1"}}}
	if err := (FFprobe{Executable: executable}).ProbePresentation(context.Background(), "fixture.mp4", &probe); err != nil {
		t.Fatal(err)
	}
	if probe.Streams[0].Cadence != CadenceFixed {
		t.Fatal("decoded-frame evidence was not used")
	}
}

// The decoded-frame scan decodes the whole input, so it asks for a thread for
// each CPU up to four. The packet scan decodes nothing and asks for none.
func TestFrameScanBoundsItsDecoderThreads(t *testing.T) {
	for cpus, want := range map[int]int{1: 1, 2: 2, 4: 4, 8: 4} {
		if got := frameScanThreads(cpus); got != want {
			t.Errorf("frameScanThreads(%d) = %d, want %d", cpus, got, want)
		}
	}
	// Keep executable creation outside parallel tests, as above.
	executable := filepath.Join(t.TempDir(), "ffprobe")
	script := `#!/bin/sh
case " $* " in *' -show_frames '*) mode=frames; field=best_effort_timestamp;; *) mode=packets; field=pts;; esac
printf '%s\n' "$@" > "$0.$mode"
printf 'stream_index=0|%s=0\nstream_index=0|%s=40\n' "$field" "$field"
`
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	recorded := func(mode string) []string {
		data, err := os.ReadFile(executable + "." + mode)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(strings.TrimSpace(string(data)), "\n")
	}
	reordered := Probe{Streams: []Stream{{CodecType: "video", HasBFrames: 2, AverageFrameRate: "25/1"}}}
	if err := (FFprobe{Executable: executable}).ProbePresentation(context.Background(), "fixture.mp4", &reordered); err != nil {
		t.Fatal(err)
	}
	frames := recorded("frames")
	at := slices.Index(frames, "-threads")
	if at < 0 || at > slices.Index(frames, "fixture.mp4") {
		t.Fatalf("frame scan arguments = %q, want -threads before the input", frames)
	}
	if want := strconv.Itoa(frameScanThreads(runtime.GOMAXPROCS(0))); frames[at+1] != want {
		t.Fatalf("frame scan threads = %q, want %s", frames[at+1], want)
	}

	ordered := Probe{Streams: []Stream{{CodecType: "video", AverageFrameRate: "25/1"}}}
	if err := (FFprobe{Executable: executable}).ProbePresentation(context.Background(), "fixture.mp4", &ordered); err != nil {
		t.Fatal(err)
	}
	if packets := recorded("packets"); slices.Contains(packets, "-threads") {
		t.Fatalf("packet scan arguments = %q, want no -threads", packets)
	}
}
