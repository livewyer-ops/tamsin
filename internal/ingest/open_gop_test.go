package ingest

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tamstime"
)

// cutProber reports the streams named in open as having open GOPs, the
// rendered Objects named in hidden as storing leading pictures they do not
// show, and those named in midGOP as beginning their second video stream
// between its key frames. It records the streams and Objects it was asked
// about.
type cutProber struct {
	fakeProber
	open    map[int]bool
	hidden  map[string]int
	midGOP  map[string]bool
	err     error
	scanned []int
	read    []string
}

func (p *cutProber) ProbeLeadingPictures(_ context.Context, _ string, streamIndex int) (bool, error) {
	p.scanned = append(p.scanned, streamIndex)
	return p.open[streamIndex], p.err
}

func (p *cutProber) ProbeObject(_ context.Context, filename string) (media.Probe, error) {
	name := filepath.Base(filename)
	p.read = append(p.read, name)
	return media.Probe{Streams: []media.Stream{
		{Index: 0, CodecType: "video", HiddenLead: p.hidden[name]},
		{Index: 1, CodecType: "video", MidGOP: p.midGOP[name]},
	}}, nil
}

type failedRender struct{ countingSegmenter }

var errRender = errors.New("render failed")

func (failedRender) Segment(context.Context, media.SegmentRequest, media.SegmentSink) error {
	return errRender
}

// TestUnsafeCutPreflight pins which renders are cut before anything is
// written, which streams are read for open GOPs first, and that only a cut
// after the first that loses pictures is refused.
func TestUnsafeCutPreflight(t *testing.T) {
	t.Parallel()
	picture := media.Stream{Index: 4, CodecType: "video", HasBFrames: 2}
	picture.Disposition.AttachedPicture = 1
	one := media.Probe{Streams: []media.Stream{
		{Index: 0, CodecType: "audio"},
		{Index: 1, CodecType: "video", HasBFrames: 2},
		picture,
	}}
	several := media.Probe{Streams: []media.Stream{
		{Index: 0, CodecType: "audio"},
		{Index: 1, CodecType: "video", HasBFrames: 2},
		{Index: 2, CodecType: "video", HasBFrames: 1},
		{Index: 3, CodecType: "video"},
		picture,
	}}
	collected := media.FlowInfo{Collected: make([]media.CollectedFlow, 4)}
	segmented := Config{SegmentDuration: 2 * time.Second}
	independent := Config{SegmentDuration: 2 * time.Second, EssenceStorage: media.EssenceStorageIndependent}
	transcoded := Config{SegmentDuration: 2 * time.Second, FFmpegArgs: []string{"-c:v", "libx264"}}
	fast := Config{SegmentDuration: 2 * time.Second, DryRunMode: DryRunFast}
	scanFailure := errors.New("scan failed")
	second, third := "segment-00000001.mp4", "segment-00000002.mp4"
	for name, tc := range map[string]struct {
		config    Config
		muxer     string
		probe     media.Probe
		staged    stagedFile
		segmenter media.Segmenter
		open      map[int]bool
		hidden    map[string]int
		midGOP    map[string]bool
		err       error
		scanned   []int
		read      []string
		refusal   string
		failure   error
		staging   bool
	}{
		"muxed reads the reference stream": {config: segmented, muxer: "mp4", probe: one, scanned: []int{1}},
		"muxed refuses a cut on an open GOP": {config: segmented, muxer: "mp4", probe: one, open: map[int]bool{1: true},
			hidden: map[string]int{second: 2}, scanned: []int{1}, read: []string{second}, refusal: FailureMessageMediaOpenGOP},
		"muxed keeps open GOPs it does not cut on": {config: segmented, muxer: "mp4", probe: one, open: map[int]bool{1: true},
			scanned: []int{1}, read: []string{second, third}},
		"the first Object may begin on an open GOP": {config: segmented, muxer: "mp4", probe: one, open: map[int]bool{1: true},
			hidden: map[string]int{"segment-00000000.mp4": 2}, scanned: []int{1}, read: []string{second, third}},
		"several muxed videos are cut without being read": {config: segmented, muxer: "mp4", probe: several, read: []string{second, third}},
		"several muxed videos are refused a cut between key frames": {config: segmented, muxer: "mp4", probe: several,
			midGOP: map[string]bool{third: true}, read: []string{second, third}, refusal: FailureMessageMediaCutMidGOP},
		"several muxed videos are refused a cut on an open GOP": {config: segmented, muxer: "mp4", probe: several,
			hidden: map[string]int{second: 1}, read: []string{second}, refusal: FailureMessageMediaOpenGOP},
		"several muxed videos are cut as MPEG-TS too": {config: segmented, muxer: "mpegts", probe: several,
			midGOP: map[string]bool{second: true}, read: []string{second}, refusal: FailureMessageMediaCutMidGOP},
		"independent reads every reordered video": {config: independent, muxer: "mp4", probe: several, scanned: []int{1, 2}},
		"independent refuses a cut of any open video": {config: independent, muxer: "mp4", probe: several, open: map[int]bool{2: true},
			hidden: map[string]int{"essence-2-00000002.mp4": 1}, scanned: []int{1, 2},
			read: []string{"essence-2-00000001.mp4", "essence-2-00000002.mp4"}, refusal: FailureMessageMediaOpenGOP},
		"independent cuts only the open video": {config: independent, muxer: "mp4", probe: several, open: map[int]bool{2: true},
			scanned: []int{1, 2}, read: []string{"essence-2-00000001.mp4", "essence-2-00000002.mp4"}},
		"independent MPEG-TS is not read": {config: independent, muxer: "mpegts", probe: several, open: map[int]bool{1: true}},
		"a failed scan is not a refusal":  {config: segmented, muxer: "mp4", probe: one, err: scanFailure, scanned: []int{1}, failure: scanFailure},
		"a failed render is not a refusal": {config: segmented, muxer: "mp4", probe: one, segmenter: failedRender{}, open: map[int]bool{1: true},
			scanned: []int{1}, failure: errRender},
		"MPEG-TS output is not read":           {config: segmented, muxer: "mpegts", probe: one, open: map[int]bool{1: true}},
		"an unsegmented render is not read":    {config: Config{EssenceStorage: media.EssenceStorageIndependent}, muxer: "mp4", probe: several, open: map[int]bool{1: true}},
		"custom FFmpeg arguments are not read": {config: transcoded, muxer: "mp4", probe: several, open: map[int]bool{1: true}},
		"a fast dry run is not read":           {config: fast, muxer: "mp4", probe: several, open: map[int]bool{1: true}},
		"a streamed input is not read":         {config: segmented, muxer: "mp4", probe: one, staged: stagedFile{bridge: &source.Bridge{}}, open: map[int]bool{1: true}},
		"streamed independent videos are not read": {config: independent, muxer: "mp4", probe: several, staged: stagedFile{bridge: &source.Bridge{}},
			open: map[int]bool{1: true}},
		"several streamed muxed videos must be staged": {config: segmented, muxer: "mp4", probe: several, staged: stagedFile{bridge: &source.Bridge{}},
			staging: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			prober := &cutProber{open: tc.open, hidden: tc.hidden, midGOP: tc.midGOP, err: tc.err}
			segmenter := tc.segmenter
			if segmenter == nil {
				segmenter = countingSegmenter{objects: 3}
			}
			tc.config.TempDirectory = t.TempDir()
			pipeline, err := New(tc.config, newFakeClient(), prober, segmenter, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			err = pipeline.refuseUnsafeCuts(t.Context(), tc.staged, tc.muxer, tc.probe, collected)
			if !slices.Equal(prober.scanned, tc.scanned) {
				t.Fatalf("streams read = %v, want %v", prober.scanned, tc.scanned)
			}
			if !slices.Equal(prober.read, tc.read) {
				t.Fatalf("Objects read = %v, want %v", prober.read, tc.read)
			}
			var classified *classifiedFailure
			refusal := ""
			if errors.As(err, &classified) && classified.Code == FailureCodeMediaUnsupported {
				refusal = classified.Message
			}
			failed := tc.failure != nil && errors.Is(err, tc.failure)
			var unavailable *source.StreamUnavailableError
			staging := errors.As(err, &unavailable) && classified != nil && classified.Code == FailureCodeStreamUnavailable
			if refusal != tc.refusal || failed != (tc.failure != nil) || staging != tc.staging ||
				(err != nil) != (tc.refusal != "" || tc.failure != nil || tc.staging) {
				t.Fatalf("refuseUnsafeCuts() = %v, want refusal %q failure %v staging %t", err, tc.refusal, tc.failure, tc.staging)
			}
			if left, err := os.ReadDir(tc.config.TempDirectory); err != nil || len(left) != 0 {
				t.Fatalf("the check left %v in staging (%v)", left, err)
			}
		})
	}
}

// segmentBounds reads the bounds of a registered [start_end) timerange in
// nanoseconds.
func segmentBounds(t *testing.T, timerange string) (int64, int64) {
	t.Helper()
	start, end, found := strings.Cut(strings.TrimSuffix(strings.TrimPrefix(timerange, "["), ")"), "_")
	if !found {
		t.Fatalf("timerange %q has no end", timerange)
	}
	var bounds [2]int64
	for index, value := range []string{start, end} {
		seconds, nanoseconds, err := tamstime.ParseParts(value)
		if err != nil {
			t.Fatalf("timerange %q: %v", timerange, err)
		}
		bounds[index] = seconds*tamstime.NanosecondsPerSecond + nanoseconds
	}
	return bounds[0], bounds[1]
}

// registeredVideoSpans returns, for every Flow that owns Segments carrying
// video, the Segment bounds in timeline order.
func registeredVideoSpans(t *testing.T, client *fakeClient) [][][2]int64 {
	t.Helper()
	client.lock.Lock()
	defer client.lock.Unlock()
	var flows [][][2]int64
	for flowID, segments := range client.segments {
		if client.flows[flowID]["format"] == "urn:x-nmos:format:audio" || len(segments) == 0 {
			continue
		}
		var spans [][2]int64
		for _, segment := range segments {
			start, end := segmentBounds(t, segment.Timerange)
			spans = append(spans, [2]int64{start, end})
		}
		slices.SortFunc(spans, func(a, b [2]int64) int { return int(a[0] - b[0]) })
		flows = append(flows, spans)
	}
	return flows
}

// ingestVideoSpans ingests item into a fake store and returns what the store
// then holds for video, which must be free of holes whatever the outcome.
func ingestVideoSpans(t *testing.T, config Config, item source.Item) (BatchResult, *fakeClient, [][][2]int64) {
	t.Helper()
	client := newFakeClient()
	config.TempDirectory = t.TempDir()
	pipeline, err := New(config, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(t.Context(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	flows := registeredVideoSpans(t, client)
	for _, spans := range flows {
		for index := 1; index < len(spans); index++ {
			if spans[index][0] != spans[index-1][1] {
				t.Fatalf("registered Segments leave a hole: %s then %s",
					media.Timestamp(spans[index-1][1]), media.Timestamp(spans[index][0]))
			}
		}
	}
	return batch, client, flows
}

func storeWrites(client *fakeClient) int {
	client.lock.Lock()
	defer client.lock.Unlock()
	return client.putFlowCalls + client.allocations + client.uploads
}

func openGOPRefusal(result Result) bool {
	return result.Failure != nil && result.Failure.Code == FailureCodeMediaUnsupported && result.Failure.Message == FailureMessageMediaOpenGOP
}

func requireEncoders(t *testing.T, names ...string) {
	t.Helper()
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip("requires " + tool)
		}
	}
	encoders, err := exec.Command("ffmpeg", "-hide_banner", "-encoders").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if !bytes.Contains(encoders, []byte(" "+name+" ")) {
			t.Skip("requires the " + name + " encoder")
		}
	}
}

// TestOpenGOPVideoIsRefusedForMP4Segments covers video whose key frames are
// followed in the file by pictures that present before them. An MP4 Object
// cut on such a frame does not show those pictures, so the Segment before it
// would end short and leave a hole. A staged input is refused before anything
// is written; a streamed input is refused at the first such cut and keeps the
// Segments before it. The same input as MPEG-TS, and closed-GOP video as MP4,
// are placed without holes.
func TestOpenGOPVideoIsRefusedForMP4Segments(t *testing.T) {
	for _, fixture := range []struct {
		name, encoder string
		arguments     []string
		open          bool
	}{
		{"mpeg2 open GOP", "mpeg2video", []string{"-g", "12", "-bf", "2"}, true},
		{"h264 open GOP", "libx264", []string{"-preset", "veryfast", "-x264-params", "open-gop=1:keyint=50:min-keyint=50:bframes=2:scenecut=0"}, true},
		{"hevc default GOP structure", "libx265", []string{"-preset", "veryfast", "-x265-params", "keyint=50:min-keyint=50:scenecut=0:log-level=error", "-tag:v", "hvc1"}, true},
		{"mpeg2 closed GOP", "mpeg2video", []string{"-g", "12", "-bf", "2", "-flags", "+cgop", "-sc_threshold", "1000000000"}, false},
		{"h264 closed GOP", "libx264", []string{"-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:bframes=2:scenecut=0"}, false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			requireEncoders(t, "aac", fixture.encoder)
			filename := streamFixture(t, ".mp4", append([]string{
				"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8",
				"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=8",
				"-c:a", "aac", "-c:v", fixture.encoder}, fixture.arguments...)...)
			for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
				for _, test := range []struct {
					name   string
					mode   InputMode
					format media.SegmentFormat
				}{
					{"staged MP4", InputStage, media.SegmentFormatSource},
					{"streamed MP4", InputStream, media.SegmentFormatSource},
					{"staged MPEG-TS", InputStage, media.SegmentFormatMPEGTS},
					{"streamed MPEG-TS", InputStream, media.SegmentFormatMPEGTS},
				} {
					t.Run(string(storage)+"/"+test.name, func(t *testing.T) {
						item := localSource(filename)
						if test.mode == InputStream {
							item, _ = serveStreamFixture(t, filename, 0)
						}
						batch, client, flows := ingestVideoSpans(t, Config{InputMode: test.mode, SegmentDuration: 2 * time.Second,
							SegmentFormat: test.format, EssenceStorage: storage}, item)
						result := batch.Results[0]
						if !fixture.open || test.format == media.SegmentFormatMPEGTS {
							if batch.Succeeded != 1 {
								t.Fatalf("run failed: %+v: %s", result.Failure, result.Error)
							}
							if len(flows) != 1 || len(flows[0]) < 3 {
								t.Fatalf("registered video Segments = %v, want several on one Flow", flows)
							}
							if span := flows[0][len(flows[0])-1][1] - flows[0][0][0]; absInt64(span-int64(8*time.Second)) > int64(100*time.Millisecond) {
								t.Fatalf("video Segments span %s, want 8 s", media.Timestamp(span))
							}
							return
						}
						if batch.Failed != 1 || !openGOPRefusal(result) {
							t.Fatalf("run = %+v (%s), want the open-GOP refusal", result.Failure, result.Error)
						}
						if !strings.Contains(result.Failure.Message, "leading pictures") ||
							!strings.Contains(result.Failure.Message, "mpegts-segments") || !strings.Contains(result.Failure.Message, "preserve") {
							t.Fatalf("refusal does not name its cause and remedies: %q", result.Failure.Message)
						}
						if writes := storeWrites(client); test.mode == InputStage && writes != 0 {
							t.Fatalf("a staged input was refused after %d writes to the store", writes)
						}
						// A muxed stream is cut at 2 s, so the Segment before
						// the first refused cut is already committed. Independent
						// essences arrive in either order, and the refusal can
						// come before any of them is.
						if test.mode == InputStream && (len(flows) > 1 || (len(flows) == 1 && flows[0][0][0] != 0) ||
							(len(flows) == 0 && storage == media.EssenceStorageMuxed)) {
							t.Fatalf("a streamed input kept video Segments %v, want a prefix from the start", flows)
						}
					})
				}
			}
		})
	}
}

// TestOpenGOPVideoIsKeptWhereNoCutLandsOnOne covers video that mixes open and
// closed GOPs: an open GOP begins every two seconds and a closed one every
// four. Cut every four seconds, or into a single Object, every picture is
// shown and the input is kept whole. Cut every two seconds it is refused.
func TestOpenGOPVideoIsKeptWhereNoCutLandsOnOne(t *testing.T) {
	requireEncoders(t, "aac", "libx264")
	filename := streamFixture(t, ".mp4",
		"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=12",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=12",
		"-c:a", "aac", "-c:v", "libx264", "-preset", "veryfast",
		"-x264-params", "open-gop=1:keyint=50:min-keyint=50:bframes=2:scenecut=0",
		"-force_key_frames", "expr:gte(t,n_forced*4)", "-forced-idr", "1")
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		for _, test := range []struct {
			name     string
			duration time.Duration
			segments int
		}{
			{"cut on the closed GOPs", 4 * time.Second, 3},
			{"one Object", time.Minute, 1},
			{"cut on an open GOP", 2 * time.Second, 0},
		} {
			t.Run(string(storage)+"/"+test.name, func(t *testing.T) {
				batch, client, flows := ingestVideoSpans(t, Config{InputMode: InputStage, SegmentDuration: test.duration,
					EssenceStorage: storage}, localSource(filename))
				result := batch.Results[0]
				if test.segments == 0 {
					if batch.Failed != 1 || !openGOPRefusal(result) || storeWrites(client) != 0 {
						t.Fatalf("run = %+v (%s) after %d writes, want the open-GOP refusal before any write",
							result.Failure, result.Error, storeWrites(client))
					}
					return
				}
				if batch.Succeeded != 1 {
					t.Fatalf("run failed: %+v: %s", result.Failure, result.Error)
				}
				if len(flows) != 1 || len(flows[0]) != test.segments {
					t.Fatalf("registered video Segments = %v, want %d on one Flow", flows, test.segments)
				}
				if start, end := flows[0][0][0], flows[0][len(flows[0])-1][1]; start != 0 || end != int64(12*time.Second) {
					t.Fatalf("video Segments run from %s to %s, want 0 to 12 s", media.Timestamp(start), media.Timestamp(end))
				}
			})
		}
	}
}

// TestCustomRenderOfOpenGOPVideo covers a staged input rendered with custom
// FFmpeg arguments. What those arguments produce is known only once it is
// rendered, so a copy of open-GOP video is refused at the first cut that
// lands on an open GOP and keeps the Segments before it, and a re-encode to
// closed GOPs is ingested whole.
func TestCustomRenderOfOpenGOPVideo(t *testing.T) {
	requireEncoders(t, "libx264")
	filename := streamFixture(t, ".mp4",
		"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "open-gop=1:keyint=50:min-keyint=50:bframes=2:scenecut=0")
	metadata := map[string]any{"codec": "video/h264", "essence_parameters": map[string]any{
		"frame_width": 128, "frame_height": 96, "frame_rate": map[string]any{"numerator": 25, "denominator": 1}}}
	for name, test := range map[string]struct {
		arguments []string
		refused   bool
	}{
		"copied": {arguments: []string{"-c", "copy"}, refused: true},
		"re-encoded to closed GOPs": {arguments: []string{"-c:v", "libx264", "-preset", "veryfast",
			"-x264-params", "keyint=50:min-keyint=50:bframes=2:scenecut=0"}},
	} {
		t.Run(name, func(t *testing.T) {
			batch, client, flows := ingestVideoSpans(t, Config{InputMode: InputStage, SegmentDuration: 2 * time.Second,
				FFmpegArgs: test.arguments, FlowMetadata: metadata}, localSource(filename))
			result := batch.Results[0]
			if len(flows) != 1 || flows[0][0][0] != 0 {
				t.Fatalf("registered video Segments = %v, want one Flow from the start", flows)
			}
			end := flows[0][len(flows[0])-1][1]
			if test.refused {
				if batch.Failed != 1 || !openGOPRefusal(result) {
					t.Fatalf("run = %+v (%s), want the open-GOP refusal", result.Failure, result.Error)
				}
				if storeWrites(client) == 0 || end >= int64(8*time.Second) {
					t.Fatalf("kept Segments to %s after %d writes, want the Segments before the refused cut",
						media.Timestamp(end), storeWrites(client))
				}
				return
			}
			if batch.Succeeded != 1 || len(flows[0]) != 4 || end != int64(8*time.Second) {
				t.Fatalf("run = %+v (%s) with Segments %v, want four Segments to 8 s", result.Failure, result.Error, flows)
			}
		})
	}
}

// decodedFrames decodes one video stream of a file on its own and returns a
// checksum of each picture, in presentation order.
func decodedFrames(t *testing.T, path string, video int) []string {
	t.Helper()
	output, err := exec.Command("ffmpeg", "-v", "error", "-i", path, "-map", "0:v:"+strconv.Itoa(video), "-f", "framecrc", "-").Output()
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	var frames []string
	for _, line := range strings.Split(string(output), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		frames = append(frames, strings.TrimSpace(line[strings.LastIndex(line, ",")+1:]))
	}
	return frames
}

// TestMuxedCutsNeedAKeyFrameOfEveryVideo covers a multiplex of two video
// streams. Its Segments are cut on the first stream's key frames. Where the
// second stream's key frames arrive with them, every Object decodes both
// streams whole. Where the second stream has longer GOPs, an Object would
// begin it between its key frames and could not show it, so the input is
// refused before anything is written. Every cut has to be checked first, so
// a remote input is staged, and is refused when it may only be streamed.
func TestMuxedCutsNeedAKeyFrameOfEveryVideo(t *testing.T) {
	requireEncoders(t, "aac", "libx264")
	const structure = ":bframes=2:scenecut=0"
	for _, fixture := range []struct {
		name, second string
		refused      bool
	}{
		{"key frames together", "keyint=50:min-keyint=50" + structure, false},
		{"second stream's key frames elsewhere", "keyint=75:min-keyint=75" + structure, true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			filename := streamFixture(t, ".mp4",
				"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8",
				"-f", "lavfi", "-i", "testsrc=size=128x96:rate=25:duration=8",
				"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=8",
				"-map", "0:v", "-map", "1:v", "-map", "2:a", "-c:a", "aac", "-c:v", "libx264", "-preset", "veryfast",
				"-x264-params:v:0", "keyint=50:min-keyint=50"+structure, "-x264-params:v:1", fixture.second)
			for _, test := range []struct {
				name      string
				mode      InputMode
				format    media.SegmentFormat
				extension string
			}{
				{"local MP4", InputStage, media.SegmentFormatSource, ".mp4"},
				{"remote MP4", InputAuto, media.SegmentFormatSource, ".mp4"},
				{"local MPEG-TS", InputStage, media.SegmentFormatMPEGTS, ".ts"},
				{"remote MPEG-TS", InputAuto, media.SegmentFormatMPEGTS, ".ts"},
				{"streamed without staging", InputStream, media.SegmentFormatSource, ".mp4"},
			} {
				t.Run(test.name, func(t *testing.T) {
					item := localSource(filename)
					if test.mode != InputStage {
						item, _ = serveStreamFixture(t, filename, 0)
					}
					batch, client, _ := ingestVideoSpans(t, Config{InputMode: test.mode, SegmentDuration: 2 * time.Second,
						SegmentFormat: test.format}, item)
					result := batch.Results[0]
					if test.mode == InputStream {
						if batch.Failed != 1 || result.Failure == nil || result.Failure.Code != FailureCodeStreamUnavailable || storeWrites(client) != 0 {
							t.Fatalf("run = %+v (%s) after %d writes, want the stream refused before any write",
								result.Failure, result.Error, storeWrites(client))
						}
						return
					}
					if fixture.refused {
						if batch.Failed != 1 || result.Failure == nil || result.Failure.Code != FailureCodeMediaUnsupported ||
							result.Failure.Message != FailureMessageMediaCutMidGOP || storeWrites(client) != 0 {
							t.Fatalf("run = %+v (%s) after %d writes, want the cut refused before any write",
								result.Failure, result.Error, storeWrites(client))
						}
						return
					}
					if batch.Succeeded != 1 {
						t.Fatalf("run failed: %+v: %s", result.Failure, result.Error)
					}
					segments := flowSegments(t, client, "urn:x-nmos:format:multi")
					decoded := make([][]string, 2)
					for _, segment := range segments {
						path := filepath.Join(t.TempDir(), "object"+test.extension)
						client.lock.Lock()
						object := client.objects[segment.ObjectID]
						client.lock.Unlock()
						if err := os.WriteFile(path, object, 0o600); err != nil {
							t.Fatal(err)
						}
						for video := range decoded {
							frames := decodedFrames(t, path, video)
							if len(frames) == 0 {
								t.Fatalf("Object at %s decodes no pictures of video stream %d", segment.Timerange, video)
							}
							decoded[video] = append(decoded[video], frames...)
						}
					}
					for video := range decoded {
						if source := decodedFrames(t, filename, video); len(segments) != 4 || len(source) != 200 || !slices.Equal(decoded[video], source) {
							t.Fatalf("%d Objects decode %d pictures of video stream %d, want the source's %d in order from four Objects",
								len(segments), len(decoded[video]), video, len(source))
						}
					}
				})
			}
		})
	}
}

// A streamed input is measured one Object at a time, and an Object stores
// leading pictures a decoder given it alone may or may not show. The frame
// rate declared for the Flow has to hold across every one of them: a late key
// frame and leading pictures at uneven steps are a variable rate, and leading
// pictures at even steps are not, whether the decoder shows them or not.
func TestStreamedLeadingPicturesKeepCadenceEvidence(t *testing.T) {
	// The fixtures' 1/12800 time base puts 200 ms in 2560 ticks and 10 ms in 128.
	const structure = "keyint=50:min-keyint=50:scenecut=0"
	x264 := []string{"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "open-gop=1:bframes=2:" + structure}
	for _, fixture := range []struct {
		name, encoder string
		arguments     []string
		// retime shifts packets of the constant encode, given the place in
		// decode order of its last key frame, which is shown at 6 s and which
		// two leading pictures follow.
		retime func(key int) string
	}{
		{name: "leading pictures a decoder drops", encoder: "libx264", arguments: x264},
		{name: "leading pictures a decoder shows", encoder: "libx265", arguments: []string{"-c:v", "libx265", "-preset", "veryfast",
			"-x265-params", "open-gop=0:radl=2:bframes=3:log-level=error:" + structure, "-tag:v", "hvc1"}},
		{name: "a late key frame", encoder: "libx264", arguments: x264, retime: func(key int) string {
			delay := `+if(gte(N\,` + strconv.Itoa(key) + `)\,2560\,0)`
			return "setts=pts=PTS" + delay + ":dts=DTS" + delay
		}},
		{name: "leading pictures at uneven steps", encoder: "libx264", arguments: x264, retime: func(key int) string {
			return `setts=pts=PTS+if(eq(N\,` + strconv.Itoa(key+2) + `)\,128\,0)`
		}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			requireEncoders(t, fixture.encoder)
			input := streamFixture(t, ".mp4", append([]string{"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8"}, fixture.arguments...)...)
			packets, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts,flags", "-of", "csv=p=0", input).Output()
			if err != nil {
				t.Fatal(err)
			}
			var shown []int64
			var keys []int
			for index, packet := range strings.Fields(string(packets)) {
				pts, flags, _ := strings.Cut(packet, ",")
				value, err := strconv.ParseInt(pts, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				shown = append(shown, value)
				if strings.Contains(flags, "K") {
					keys = append(keys, index)
				}
			}
			key := keys[len(keys)-1]
			if key == 0 || key+2 >= len(shown) || shown[key+1] >= shown[key] || shown[key+2] >= shown[key] {
				t.Fatalf("fixture has key frames at %v, want two leading pictures after the last", keys)
			}
			if fixture.retime != nil {
				input = streamFixture(t, ".mp4", "-i", input, "-c", "copy", "-bsf:v", fixture.retime(key))
			}
			for _, mode := range []InputMode{InputStage, InputStream} {
				t.Run(string(mode), func(t *testing.T) {
					item := localSource(input)
					if mode == InputStream {
						item, _ = serveStreamFixture(t, input, 0)
					}
					client := newFakeClient()
					pipeline, err := New(Config{InputMode: mode, SegmentDuration: 2 * time.Second, SegmentFormat: media.SegmentFormatMPEGTS,
						EssenceStorage: media.EssenceStorageIndependent, TempDirectory: t.TempDir()}, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
					if err != nil {
						t.Fatal(err)
					}
					batch, err := pipeline.Run(t.Context(), []source.Item{item})
					if err != nil {
						t.Fatal(err)
					}
					result := batch.Results[0]
					flows := registeredVideoSpans(t, client)
					if len(flows) != 1 {
						t.Fatalf("run = %+v (%s) with video Segments %v, want one video Flow", result.Failure, result.Error, flows)
					}
					end := flows[0][len(flows[0])-1][1]
					if mode == InputStream && fixture.retime != nil {
						if batch.Failed != 1 || result.Failure == nil || result.Failure.Code != FailureCodeMediaPrepareFailed ||
							!strings.Contains(result.Error, "/essence_parameters/frame_rate") {
							t.Fatalf("streamed run = %+v (%s), want the retimed pictures to contradict the declared frame rate", result.Failure, result.Error)
						}
						if end > int64(6*time.Second) {
							t.Fatalf("streamed run kept video Segments %v, want only those before the retimed pictures", flows)
						}
						return
					}
					client.lock.Lock()
					defer client.lock.Unlock()
					for _, flow := range client.flows {
						parameters, _ := flow["essence_parameters"].(map[string]any)
						retimed := fixture.retime != nil
						if variable := parameters["vfr"] == true; batch.Succeeded != 1 || variable != retimed || (!retimed && end != int64(8*time.Second)) {
							t.Fatalf("run = %+v (%s) with essence parameters %v and video Segments %v, want a variable frame rate only for retimed pictures and otherwise all 8 s",
								result.Failure, result.Error, parameters, flows)
						}
					}
				})
			}
		})
	}
}
