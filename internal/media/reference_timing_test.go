package media

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// matroskaPackets writes the packet listing of a fixed-rate video stream as a
// millisecond container records it: every timestamp rounded to the tick and
// every duration the truncated nominal one, so the stream measures a
// millisecond short. frames may skip indices to drop a frame.
func matroskaPackets(frames []int, fps int, keyEvery int) string {
	entries := make([]string, 0, len(frames))
	for _, frame := range frames {
		pts := (frame*1000 + fps/2) / fps
		flags := "_"
		if frame%keyEvery == 0 {
			flags = "K_"
		}
		entries = append(entries, fmt.Sprintf(`{"stream_index":0,"pts":%d,"duration":%d,"flags":"%s"}`, pts, 1000/fps, flags))
	}
	return `{"packets":[` + strings.Join(entries, ",") + `]}`
}

func TestReferenceTimingRegularisesFixedRateVideo(t *testing.T) {
	t.Parallel()
	frames := make([]int, 0, 240)
	for frame := range 240 {
		frames = append(frames, frame)
	}
	probe := Probe{Format: Format{StartTime: "999", Duration: "999"},
		Streams: []Stream{{Index: 0, CodecType: "video", CodecName: "h264", TimeBase: "1/1000", AverageFrameRate: "24/1"}}}
	if err := measureObjectPackets(strings.NewReader(matroskaPackets(frames, 24, 24)), &probe); err != nil {
		t.Fatal(err)
	}
	timing, err := ProbeReference(probe)
	if err != nil {
		t.Fatal(err)
	}
	const period = int64(41666667)
	if !timing.Regular || !timing.Measured || timing.Period != period || timing.Samples != 240 || timing.KeyFrames != 10 {
		t.Fatalf("timing = %#v", timing)
	}
	// 240 frames at 24 fps present for ten seconds even though the rounded
	// timestamps measure a millisecond short.
	if timing.Span != int64(10*time.Second) || timing.MeasuredSpan >= timing.Span || timing.Span-timing.MeasuredSpan > int64(2*time.Millisecond) {
		t.Fatalf("span = %d, measured %d", timing.Span, timing.MeasuredSpan)
	}
	if timing.LastSample != timing.Start+timing.Span-period {
		t.Fatalf("last sample = %d, want %d", timing.LastSample, timing.Start+timing.Span-period)
	}

	// A frame missing inside the Object is a whole period short of the
	// nominal: that is a real hole, and the measured span stands.
	missing := append(append([]int(nil), frames[:100]...), frames[101:]...)
	probe.Streams[0] = Stream{Index: 0, CodecType: "video", CodecName: "h264", TimeBase: "1/1000", AverageFrameRate: "24/1"}
	if err := measureObjectPackets(strings.NewReader(matroskaPackets(missing, 24, 24)), &probe); err != nil {
		t.Fatal(err)
	}
	if timing, err = ProbeReference(probe); err != nil {
		t.Fatal(err)
	}
	if timing.Regular || timing.Samples != 239 || timing.Span != timing.MeasuredSpan || timing.Span < int64(9998*time.Millisecond) {
		t.Fatalf("frame-dropped timing = %#v", timing)
	}
}

func TestReferenceTimingLeavesAudioAndUnmeasuredProbesAlone(t *testing.T) {
	t.Parallel()
	audio := Probe{Format: Format{StartTime: "0", Duration: "1"},
		Streams: []Stream{{Index: 0, CodecType: "audio", CodecName: "aac", TimeBase: "1/48000", SampleRate: "48000"}}}
	if err := measureObjectPackets(strings.NewReader(`{"packets":[{"stream_index":0,"pts":0,"duration":1024,"flags":"K_"},{"stream_index":0,"pts":1024,"duration":1024,"flags":"K_"}]}`), &audio); err != nil {
		t.Fatal(err)
	}
	timing, err := ProbeReference(audio)
	if err != nil {
		t.Fatal(err)
	}
	// Two AAC frames: 1024/48000 s each, rounded to the nanosecond.
	if timing.Regular || timing.Period != 0 || !timing.Measured || timing.KeyFrames != 2 || timing.Samples != 2 ||
		timing.LastSample != 21333333 || timing.Span != 42666667 {
		t.Fatalf("audio timing = %#v", timing)
	}

	whole := Probe{Format: Format{StartTime: "0", Duration: "10"},
		Streams: []Stream{{Index: 0, CodecType: "video", CodecName: "h264", AverageFrameRate: "25/1"}}}
	if timing, err = ProbeReference(whole); err != nil {
		t.Fatal(err)
	}
	if timing.Measured || timing.Regular || timing.Span != int64(10*time.Second) || timing.KeyFrames != 0 {
		t.Fatalf("unmeasured timing = %#v", timing)
	}
}
