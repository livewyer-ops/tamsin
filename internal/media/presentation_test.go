package media

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
			if err := scanPresentation(strings.NewReader(compact.String()), map[int]*cadenceState{7: state}); err != nil {
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

	fixed := filepath.Join(directory, "fixed.mkv")
	buildFixture(t,
		"-f", "lavfi", "-i", "testsrc=size=128x96:rate=30000/1001:duration=2",
		"-c:v", "ffv1", fixed,
	)
	reordered := filepath.Join(directory, "reordered.mp4")
	buildFixture(t,
		"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=2",
		"-c:v", "libx264", "-bf", "3", "-x264-params", "b-adapt=0", reordered,
	)
	variable := filepath.Join(directory, "variable.mkv")
	buildFixture(t,
		"-f", "lavfi", "-i", "testsrc=size=128x96:rate=24:duration=1",
		"-f", "lavfi", "-i", "testsrc=size=128x96:rate=30:duration=1",
		"-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]",
		"-map", "[v]", "-fps_mode", "vfr", "-c:v", "ffv1", variable,
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

			flow, _, err := BuildFlow(packetProbe, testIdentity(), "video/matroska", EssenceStorageMuxed)
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
			path := filepath.Join(directory, fixture.name+".mpg")
			buildFixture(t,
				"-f", "lavfi", "-i", "testsrc2=size=720x576:rate=50:duration=1",
				"-vf", fixture.filter, "-c:v", "mpeg2video", "-flags", "+ilme+ildct", "-top", fixture.top, path,
			)
			probe, err := (FFprobe{}).Probe(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if got := interlaceMode(probe.Format, probe.Streams[0].FieldOrder); got != fixture.want {
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
