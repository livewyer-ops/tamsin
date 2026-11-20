package ingest

import (
	"bytes"
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
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// storedPictures counts the pictures an Object stores for one video stream,
// and how many of them are leading pictures: stored after the key frame the
// Object begins on and shown before it.
func storedPictures(t *testing.T, path string, video int) (stored, leading int) {
	t.Helper()
	output, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:"+strconv.Itoa(video),
		"-show_entries", "packet=pts", "-of", "csv=p=0", path).Output()
	if err != nil {
		t.Fatalf("read the packets of %s: %v", path, err)
	}
	var first int64
	for _, line := range strings.Fields(string(output)) {
		pts, err := strconv.ParseInt(strings.TrimRight(line, ","), 10, 64)
		if err != nil {
			t.Fatalf("packet timestamp %q in %s: %v", line, path, err)
		}
		if stored == 0 {
			first = pts
		}
		if pts < first {
			leading++
		}
		stored++
	}
	return stored, leading
}

// decodeObjectsAlone decodes each registered Object on its own and requires
// every picture it stores for a video stream except the leading pictures of
// its first GOP, as the source shows them. It returns how many of the
// source's pictures the Objects account for and how many of those are
// leading pictures, which need the Object before.
func decodeObjectsAlone(t *testing.T, client *fakeClient, segments []tams.Segment, source []string, video int) (shown, lost int) {
	t.Helper()
	directory := t.TempDir()
	for index, segment := range segments {
		path := filepath.Join(directory, strconv.Itoa(index)+".ts")
		client.lock.Lock()
		object := client.objects[segment.ObjectID]
		client.lock.Unlock()
		if err := os.WriteFile(path, object, 0o600); err != nil {
			t.Fatal(err)
		}
		stored, leading := storedPictures(t, path, video)
		frames := decodedFrames(t, path, video)
		from := shown + leading
		if len(frames) == 0 || len(frames) != stored-leading || from+len(frames) > len(source) ||
			!slices.Equal(frames, source[from:from+len(frames)]) {
			t.Fatalf("Object at %s decodes %d pictures of video stream %d on its own, want the %d it stores less %d leading pictures, as the source shows them from picture %d",
				segment.Timerange, len(frames), video, stored, leading, from)
		}
		shown, lost = from+len(frames), lost+leading
	}
	return shown, lost
}

// TestOpenGOPH264DecodesFromEachMPEGTSObject cuts H.264 with open GOPs out of
// an MP4 into MPEG-TS Objects and decodes each Object on its own. An MP4
// keeps the parameter sets in its sample description, and an open GOP begins
// on a key frame that is not an IDR picture, where FFmpeg's conversion to
// Annex B writes none. Each Object must give every picture it stores except
// the leading pictures of its first GOP, which need the Object before it.
func TestOpenGOPH264DecodesFromEachMPEGTSObject(t *testing.T) {
	requireEncoders(t, "aac", "libx264")
	type run struct {
		name    string
		mode    InputMode
		storage media.EssenceStorage
	}
	for _, fixture := range []struct {
		name   string
		videos []string
		runs   []run
	}{
		{"one video stream", []string{"testsrc2"}, []run{
			{"independent", InputStage, media.EssenceStorageIndependent},
			{"independent streamed", InputStream, media.EssenceStorageIndependent},
			{"muxed", InputStage, media.EssenceStorageMuxed},
			{"muxed streamed", InputStream, media.EssenceStorageMuxed},
		}},
		// The same pictures twice, so both streams have their key frames at every cut.
		{"two video streams", []string{"testsrc2", "testsrc2"}, []run{
			{"muxed", InputStage, media.EssenceStorageMuxed},
		}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			// The sound comes first, so a video stream's place in the container is not its place among the video streams.
			arguments := []string{"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=8"}
			maps := []string{"-map", "0:a"}
			for index, pattern := range fixture.videos {
				arguments = append(arguments, "-f", "lavfi", "-i", pattern+"=size=128x96:rate=25:duration=8")
				maps = append(maps, "-map", strconv.Itoa(index+1)+":v")
			}
			filename := streamFixture(t, ".mp4", append(append(arguments, maps...), "-c:a", "aac", "-c:v", "libx264", "-preset", "veryfast",
				"-x264-params", "open-gop=1:keyint=50:min-keyint=50:bframes=2:scenecut=0")...)
			for _, test := range fixture.runs {
				t.Run(test.name, func(t *testing.T) {
					item := localSource(filename)
					if test.mode == InputStream {
						item, _ = serveStreamFixture(t, filename, 0)
					}
					batch, client, _ := ingestVideoSpans(t, Config{InputMode: test.mode, SegmentDuration: 2 * time.Second,
						SegmentFormat: media.SegmentFormatMPEGTS, EssenceStorage: test.storage}, item)
					if batch.Succeeded != 1 {
						t.Fatalf("run failed: %+v: %s", batch.Results[0].Failure, batch.Results[0].Error)
					}
					format := "urn:x-nmos:format:multi"
					if test.storage == media.EssenceStorageIndependent {
						format = "urn:x-nmos:format:video"
					}
					segments := flowSegments(t, client, format)
					if len(segments) != 4 {
						t.Fatalf("%d Segments, want a cut on each of the four key frames", len(segments))
					}
					for video := range fixture.videos {
						source := decodedFrames(t, filename, video)
						if shown, lost := decodeObjectsAlone(t, client, segments, source, video); shown != len(source) || lost == 0 {
							t.Fatalf("the Objects account for %d of the %d pictures of video stream %d with %d leading pictures at their cuts, want every picture and an open GOP at a cut",
								shown, len(source), video, lost)
						}
					}
				})
			}
		})
	}
}

// TestMPEGTSObjectsKeepH264PresentationTimes shows one picture of an MP4 10 ms
// late and requires the MPEG-TS Objects to show every picture when the source
// does. Writing the parameter sets must not move a picture onto the grid of
// the frame rate.
func TestMPEGTSObjectsKeepH264PresentationTimes(t *testing.T) {
	requireEncoders(t, "libx264")
	constant := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=4",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:bframes=2:scenecut=0")
	// 128 ticks of the fixture's 1/12800 time base are 10 ms.
	filename := streamFixture(t, ".mp4", "-i", constant, "-c", "copy", "-bsf:v", `setts=pts=PTS+if(eq(N\,60)\,128\,0)`)
	shown := func(path string) []int64 {
		output, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "packet=pts", "-of", "csv=p=0", path).Output()
		if err != nil {
			t.Fatalf("read the packets of %s: %v", path, err)
		}
		var times []int64
		for _, line := range strings.Fields(string(output)) {
			pts, err := strconv.ParseInt(strings.TrimRight(line, ","), 10, 64)
			if err != nil {
				t.Fatalf("packet timestamp %q in %s: %v", line, path, err)
			}
			times = append(times, pts)
		}
		slices.Sort(times)
		return times
	}
	want := shown(filename)
	late := false
	for index := range want {
		// MPEG-TS counts 90000 ticks a second.
		want[index] = want[index] * 90000 / 12800
		late = late || want[index]%3600 != 0
	}
	if !late {
		t.Fatal("fixture shows every picture on the grid of its frame rate")
	}
	batch, client, _ := ingestVideoSpans(t, Config{SegmentDuration: 2 * time.Second, SegmentFormat: media.SegmentFormatMPEGTS,
		EssenceStorage: media.EssenceStorageIndependent}, localSource(filename))
	if batch.Succeeded != 1 {
		t.Fatalf("run failed: %+v: %s", batch.Results[0].Failure, batch.Results[0].Error)
	}
	var got []int64
	directory := t.TempDir()
	for index, segment := range flowSegments(t, client, "urn:x-nmos:format:video") {
		path := filepath.Join(directory, strconv.Itoa(index)+".ts")
		client.lock.Lock()
		object := client.objects[segment.ObjectID]
		client.lock.Unlock()
		if err := os.WriteFile(path, object, 0o600); err != nil {
			t.Fatal(err)
		}
		got = append(got, shown(path)...)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("the Objects show their pictures at %v, want the source's %v", got, want)
	}
}

// TestRepeatedH264ParameterSetsMayDifferInPriority carries the picture
// parameter set again at every IDR picture with another reference priority in
// its first byte, which H.264 leaves free for a parameter set. The set itself
// is the one in the MP4's sample description, so nothing changes and every
// Object is kept.
func TestRepeatedH264ParameterSetsMayDifferInPriority(t *testing.T) {
	requireEncoders(t, "libx264")
	transport := streamFixture(t, ".ts", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", "keyint=50:min-keyint=50:scenecut=0")
	data, err := os.ReadFile(transport)
	if err != nil {
		t.Fatal(err)
	}
	// 0x68 is a picture parameter set of priority 3, and 0x48 one of priority 2.
	sets := 0
	for at := 0; ; sets++ {
		next := bytes.Index(data[at:], []byte{0, 0, 1, 0x68})
		if next < 0 {
			break
		}
		at += next + 4
		if sets > 0 {
			data[at-1] = 0x48
		}
	}
	if sets < 2 {
		t.Fatalf("fixture carries its picture parameter set %d times, want it at several IDR pictures", sets)
	}
	if err := os.WriteFile(transport, data, 0o600); err != nil {
		t.Fatal(err)
	}
	filename := streamFixture(t, ".mp4", "-i", transport, "-c", "copy")
	source := decodedFrames(t, filename, 0)
	batch, client, _ := ingestVideoSpans(t, Config{SegmentDuration: 2 * time.Second, SegmentFormat: media.SegmentFormatMPEGTS,
		EssenceStorage: media.EssenceStorageIndependent}, localSource(filename))
	if batch.Succeeded != 1 {
		t.Fatalf("run failed: %+v: %s", batch.Results[0].Failure, batch.Results[0].Error)
	}
	if shown, _ := decodeObjectsAlone(t, client, flowSegments(t, client, "urn:x-nmos:format:video"), source, 0); shown != len(source) || shown != 200 {
		t.Fatalf("the Objects account for %d of the source's %d pictures, want all 200", shown, len(source))
	}
}

// refusedPrefix runs an ingest that must be refused for its H.264 parameter
// sets and returns the video Segments it kept. Those are a prefix of the
// input, and each must decode on its own.
func refusedPrefix(t *testing.T, config Config, item source.Item, message string, source []string) [][2]int64 {
	t.Helper()
	batch, client, flows := ingestVideoSpans(t, config, item)
	result := batch.Results[0]
	if batch.Failed != 1 || result.Failure == nil || result.Failure.Code != FailureCodeMediaUnsupported || result.Failure.Message != message {
		t.Fatalf("run = %+v (%s), want it refused: %s", result.Failure, result.Error, message)
	}
	if len(flows) == 0 {
		return nil
	}
	if len(flows) != 1 || flows[0][0][0] != 0 {
		t.Fatalf("kept video Segments %v, want a prefix of one Flow", flows)
	}
	decodeObjectsAlone(t, client, flowSegments(t, client, "urn:x-nmos:format:video"), source, 0)
	return flows[0]
}

// TestChangedH264ParameterSetsAreRefusedForMPEGTS joins two encodes with
// different picture parameter sets. The second encode's arrive in the stream
// at its IDR pictures, and its other key frames carry none. Where the second
// encode replaces a set, repeating the sets of the sample description would
// give a decoder the first encode's set for the second encode's pictures, so
// the input is refused where the change arrives. Where it brings sets with
// new identifiers, the Object that begins on its IDR picture holds them, and
// an Object cut on another of its key frames holds pictures that refer to
// sets it has not carried, even if an IDR picture later in that Object
// carries them again.
func TestChangedH264ParameterSetsAreRefusedForMPEGTS(t *testing.T) {
	requireEncoders(t, "libx264")
	const structure = "open-gop=1:keyint=50:min-keyint=50:bframes=2:scenecut=0"
	half := func(parameters string, arguments ...string) string {
		return streamFixture(t, ".mp4", append([]string{"-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8",
			"-c:v", "libx264", "-preset", "veryfast", "-x264-params", parameters}, arguments...)...)
	}
	first := half(structure)
	for _, fixture := range []struct {
		name, second, message string
		// arguments are added to the second encode.
		arguments []string
		// cut is the Segment duration, and kept is where the Segments kept must end.
		cut, kept time.Duration
	}{
		{"a set replaced", structure + ":cabac=0", FailureMessageMediaParameterSetsChange, nil, 2 * time.Second, 8 * time.Second},
		{"sets with new identifiers", structure + ":sps-id=1", FailureMessageMediaNoParameterSets, nil, 2 * time.Second, 10 * time.Second},
		// The second encode has an IDR picture at 6 s, inside the Object cut at 12 s.
		{"sets with new identifiers that a later IDR picture repeats", structure + ":sps-id=1", FailureMessageMediaNoParameterSets,
			[]string{"-force_key_frames", "expr:gte(t,n_forced*6)", "-forced-idr", "1"}, 4 * time.Second, 12 * time.Second},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			list := filepath.Join(t.TempDir(), "list.txt")
			if err := os.WriteFile(list, []byte("file '"+first+"'\nfile '"+half(fixture.second, fixture.arguments...)+"'\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			filename := streamFixture(t, ".mp4", "-f", "concat", "-safe", "0", "-i", list, "-c", "copy")
			source := decodedFrames(t, filename, 0)
			if len(source) != 400 {
				t.Fatalf("fixture decodes %d pictures as a whole, want 400", len(source))
			}
			for _, mode := range []InputMode{InputStage, InputStream} {
				t.Run(string(mode), func(t *testing.T) {
					item := localSource(filename)
					if mode == InputStream {
						item, _ = serveStreamFixture(t, filename, 0)
					}
					kept := refusedPrefix(t, Config{InputMode: mode, SegmentDuration: fixture.cut, SegmentFormat: media.SegmentFormatMPEGTS,
						EssenceStorage: media.EssenceStorageIndependent}, item, fixture.message, source)
					// A leading picture of the Object refused may end the last one kept just short of the cut.
					if len(kept) == 0 || kept[len(kept)-1][1] > int64(fixture.kept) || kept[len(kept)-1][1] <= int64(fixture.kept-fixture.cut) {
						t.Fatalf("kept video Segments %v, want those up to the cut at %s", kept, fixture.kept)
					}
				})
			}
		})
	}
}

// TestMPEGTSObjectWithoutH264ParameterSetsIsRefused covers renders that leave
// the stream as it arrives. An MPEG-TS source that carries its parameter sets
// once or only at some key frames, and a custom copy out of an MP4, cut
// Objects on key frames that have none: such an Object cannot be decoded from
// its start without an earlier one.
func TestMPEGTSObjectWithoutH264ParameterSetsIsRefused(t *testing.T) {
	requireEncoders(t, "libx264")
	const openGOP = "open-gop=1:keyint=50:min-keyint=50:bframes=2:scenecut=0"
	original := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=8",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", openGOP)
	once := streamFixture(t, ".ts", "-i", original, "-c", "copy")
	// An IDR picture every six seconds carries the sets. The open GOPs every
	// two seconds between them do not.
	periodic := streamFixture(t, ".ts", "-i", streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=25:duration=12",
		"-c:v", "libx264", "-preset", "veryfast", "-x264-params", openGOP, "-force_key_frames", "expr:gte(t,n_forced*6)", "-forced-idr", "1"),
		"-c", "copy")
	metadata := map[string]any{"codec": "video/h264", "essence_parameters": map[string]any{
		"frame_width": 128, "frame_height": 96, "frame_rate": map[string]any{"numerator": 25, "denominator": 1}}}
	for _, test := range []struct {
		name     string
		filename string
		pictures int
		config   Config
	}{
		{"MPEG-TS source with its sets once", once, 200, Config{InputMode: InputStage, SegmentDuration: 2 * time.Second}},
		{"streamed MPEG-TS source with its sets once", once, 200, Config{InputMode: InputStream, SegmentDuration: 2 * time.Second}},
		// The Object cut at four seconds holds the sets from six seconds on.
		{"MPEG-TS source with its sets at some key frames", periodic, 300, Config{InputMode: InputStage, SegmentDuration: 4 * time.Second}},
		{"custom copy from MP4", original, 200, Config{InputMode: InputStage, SegmentDuration: 2 * time.Second,
			FFmpegArgs: []string{"-c", "copy"}, FlowMetadata: metadata}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := decodedFrames(t, test.filename, 0)
			if len(source) != test.pictures {
				t.Fatalf("fixture decodes %d pictures as a whole, want %d", len(source), test.pictures)
			}
			item := localSource(test.filename)
			if test.config.InputMode == InputStream {
				item, _ = serveStreamFixture(t, test.filename, 0)
			}
			config := test.config
			config.SegmentFormat, config.EssenceStorage = media.SegmentFormatMPEGTS, media.EssenceStorageIndependent
			if kept := refusedPrefix(t, config, item, FailureMessageMediaNoParameterSets, source); len(kept) > 1 {
				t.Fatalf("kept video Segments %v, want at most the first", kept)
			}
		})
	}
}
