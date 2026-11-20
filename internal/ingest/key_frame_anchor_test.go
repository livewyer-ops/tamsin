package ingest

import (
	"context"
	"errors"
	"math/big"
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

// tickPacket is one packet's presentation start and end in exact seconds.
type tickPacket struct {
	at, end *big.Rat
	flags   string
}

// sourcePackets lists one stream's packets in stored order with ffprobe.
// Times are the packets' integer timestamps in the stream's time base:
// FFprobe's printed times are rounded to the microsecond, which would hide
// an error smaller than that.
func sourcePackets(t *testing.T, filename, stream string) []tickPacket {
	t.Helper()
	output, err := exec.Command("ffprobe", "-v", "error", "-select_streams", stream, "-show_entries",
		"packet=pts,duration,flags:stream=time_base", "-of", "csv=p=1", filename).Output()
	if err != nil {
		t.Fatalf("list %s packets of %s: %v", stream, filename, err)
	}
	var (
		tick    *big.Rat
		packets []tickPacket
	)
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Split(line, ",")
		switch {
		case fields[0] == "stream" && len(fields) >= 2:
			var ok bool
			if tick, ok = new(big.Rat).SetString(fields[1]); !ok {
				t.Fatalf("time base %q", fields[1])
			}
		case fields[0] == "packet" && len(fields) >= 4:
			pts, ptsErr := strconv.ParseInt(fields[1], 10, 64)
			duration, durationErr := strconv.ParseInt(fields[2], 10, 64)
			if ptsErr != nil || durationErr != nil {
				t.Fatalf("unexpected packet line %q", line)
			}
			packets = append(packets, tickPacket{at: big.NewRat(pts, 1), end: big.NewRat(pts+duration, 1), flags: fields[3]})
		default:
			t.Fatalf("unexpected line %q", line)
		}
	}
	if tick == nil {
		t.Fatalf("%s of %s has no time base", stream, filename)
	}
	for _, packet := range packets {
		packet.at.Mul(packet.at, tick)
		packet.end.Mul(packet.end, tick)
	}
	return packets
}

// nanoseconds rounds a span of exact seconds, which must not be negative, to
// the nearest nanosecond.
func nanoseconds(seconds *big.Rat) int64 {
	scaled := new(big.Rat).Mul(seconds, big.NewRat(int64(time.Second), 1))
	scaled.Add(scaled, big.NewRat(1, 2))
	return new(big.Int).Div(scaled.Num(), scaled.Denom()).Int64()
}

// presented returns the first presentation time among packets that satisfy
// keep, and the latest presentation end among all packets from that one on.
func presented(t *testing.T, packets []tickPacket, keep func(flags string) bool) (first, end *big.Rat) {
	t.Helper()
	for _, packet := range packets {
		if first == nil && !keep(packet.flags) {
			continue
		}
		if first == nil || packet.at.Cmp(first) < 0 {
			first = packet.at
		}
		if end == nil || packet.end.Cmp(end) > 0 {
			end = packet.end
		}
	}
	if first == nil {
		t.Fatal("no packet to present")
	}
	return first, end
}

// flowSegments returns a Flow's registered Segments in timeline order.
func flowSegments(t *testing.T, client *fakeClient, format string) []tams.Segment {
	t.Helper()
	client.lock.Lock()
	defer client.lock.Unlock()
	var segments []tams.Segment
	for flowID, registered := range client.segments {
		if client.flows[flowID]["format"] != format {
			continue
		}
		if segments != nil {
			t.Fatalf("more than one %s Flow owns Segments", format)
		}
		for _, segment := range registered {
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		t.Fatalf("no %s Flow owns Segments", format)
	}
	slices.SortFunc(segments, func(a, b tams.Segment) int {
		left, _ := segmentBounds(t, a.Timerange)
		right, _ := segmentBounds(t, b.Timerange)
		return int(left - right)
	})
	return segments
}

// TestLongGOPCaptureIsAnchoredOnItsFirstKeyFrame ingests a capture that
// starts mid-GOP in a GOP of more than ten seconds, which is ordinary x264
// output at its default key frame interval. Stream copy drops the frames
// before the first key frame, so the rendered video begins there: an
// independent video Flow starts at that frame, and a muxed Flow starts with
// the audio and keeps the picture where it was against the sound.
//
// The capture's streams begin between microseconds, at 23.976 frames a second
// against 48 kHz sound. Expected positions come from the packets' integer
// timestamps, and MP4 output is held to them to the nanosecond.
func TestLongGOPCaptureIsAnchoredOnItsFirstKeyFrame(t *testing.T) {
	full := streamFixture(t, ".mp4", "-f", "lavfi", "-i", "testsrc2=size=128x96:rate=24000/1001:duration=30",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=30",
		"-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac")
	input := filepath.Join(t.TempDir(), "capture.mp4")
	// The default movie timescale would round each stream's start to a
	// millisecond.
	if output, err := exec.Command("ffmpeg", "-v", "error", "-i", full, "-ss", "0.2", "-c", "copy", "-copyinkf",
		"-movie_timescale", "48000", input).CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v: %s", err, output)
	}
	video, audio := sourcePackets(t, input, "v:0"), sourcePackets(t, input, "a:0")
	origin, _ := presented(t, video, func(string) bool { return true })
	firstSound, lastSound := presented(t, audio, func(string) bool { return true })
	if firstSound.Cmp(origin) < 0 {
		origin = firstSound
	}
	firstKeyFrame, lastPicture := presented(t, video, func(flags string) bool { return strings.Contains(flags, "K") })
	fromOrigin := func(at *big.Rat) int64 { return nanoseconds(new(big.Rat).Sub(at, origin)) }
	keyFrame, videoEnd, audioStart, audioEnd := fromOrigin(firstKeyFrame), fromOrigin(lastPicture), fromOrigin(firstSound), fromOrigin(lastSound)
	if keyFrame <= int64(10*time.Second) {
		t.Fatalf("the fixture's first key frame is %s after its start; the case needs more than 10 s", media.Timestamp(keyFrame))
	}
	if keyFrame%int64(time.Microsecond) == 0 || audioStart%int64(time.Microsecond) == 0 {
		t.Fatalf("the fixture's key frame at %s and sound at %s must both fall between microseconds",
			media.Timestamp(keyFrame), media.Timestamp(audioStart))
	}
	// An MP4 Flow is placed from the source's ticks, so its start is exact.
	// MPEG-TS is placed by its own 90 kHz clock, to within one tick. The end
	// of the picture is held to a millisecond and the end of the sound to one
	// AAC frame: the container trims the last one, and each output keeps a
	// different part of it.
	const transportTick, tolerance, audioFrame = int64(time.Second) / 90000, int64(time.Millisecond), int64(time.Second) * 1024 / 48000
	near := func(got, want int64) bool { return absInt64(got-want) <= tolerance }

	for _, profile := range []string{ProfileEssenceSegments, ProfileMuxedSegments, ProfileMPEGTSSegments} {
		t.Run(profile, func(t *testing.T) {
			definition, err := ResolveProfile(profile, ProfileOverrides{})
			if err != nil {
				t.Fatal(err)
			}
			startTolerance := int64(0)
			if definition.SegmentFormat == media.SegmentFormatMPEGTS {
				startTolerance = transportTick
			}
			client := newFakeClient()
			pipeline, err := New(Config{SegmentDuration: definition.SegmentDuration, SegmentFormat: definition.SegmentFormat,
				EssenceStorage: definition.EssenceStorage, TempDirectory: t.TempDir()}, client, media.FFprobe{}, media.FFmpeg{}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(t.Context(), []source.Item{localSource(input)})
			if err != nil || batch.Succeeded != 1 {
				t.Fatalf("run: %v, %+v: %s", err, batch.Results[0].Failure, batch.Results[0].Error)
			}
			if definition.EssenceStorage == media.EssenceStorageIndependent {
				for _, essence := range []struct {
					format                   string
					start, end, endTolerance int64
				}{
					{"urn:x-nmos:format:video", keyFrame, videoEnd, tolerance},
					{"urn:x-nmos:format:audio", audioStart, audioEnd, audioFrame},
				} {
					segments := flowSegments(t, client, essence.format)
					start, _ := segmentBounds(t, segments[0].Timerange)
					_, end := segmentBounds(t, segments[len(segments)-1].Timerange)
					if absInt64(start-essence.start) > startTolerance || absInt64(end-essence.end) > essence.endTolerance {
						t.Errorf("%s Flow covers %s to %s, want %s to %s", essence.format, media.Timestamp(start), media.Timestamp(end),
							media.Timestamp(essence.start), media.Timestamp(essence.end))
					}
				}
				return
			}
			segments := flowSegments(t, client, "urn:x-nmos:format:multi")
			start, _ := segmentBounds(t, segments[0].Timerange)
			_, end := segmentBounds(t, segments[len(segments)-1].Timerange)
			if start != audioStart || absInt64(end-max(videoEnd, audioEnd)) > audioFrame {
				t.Errorf("muxed Flow covers %s to %s, want %s to %s", media.Timestamp(start), media.Timestamp(end),
					media.Timestamp(audioStart), media.Timestamp(max(videoEnd, audioEnd)))
			}
			// ts_offset maps the Object's own timestamps onto the Flow, so the
			// first Object shows where its picture and sound really sit.
			object := filepath.Join(t.TempDir(), "first"+filepath.Ext(input))
			client.lock.Lock()
			data := client.objects[segments[0].ObjectID]
			client.lock.Unlock()
			if err := os.WriteFile(object, data, 0o600); err != nil {
				t.Fatal(err)
			}
			offset := int64(0)
			if segments[0].TSOffset != "" {
				if offset, err = media.ParseTimestamp(segments[0].TSOffset); err != nil {
					t.Fatal(err)
				}
			}
			shown := func(flags string) bool { return !strings.Contains(flags, "D") }
			picture, _ := presented(t, sourcePackets(t, object, "v:0"), shown)
			sound, _ := presented(t, sourcePackets(t, object, "a:0"), shown)
			if at, heard := nanoseconds(picture)+offset, nanoseconds(sound)+offset; !near(at, keyFrame) || !near(heard, audioStart) {
				t.Errorf("first muxed Object puts its picture at %s and its sound at %s, want %s and %s", media.Timestamp(at),
					media.Timestamp(heard), media.Timestamp(keyFrame), media.Timestamp(audioStart))
			}
		})
	}
}

// keyFrameProber answers the first key frame lookup from a script and counts
// the lookups made for each stream.
type keyFrameProber struct {
	fakeProber
	at      map[int]*big.Rat
	err     error
	cancel  context.CancelFunc
	lookups map[int]int
}

func (p *keyFrameProber) ProbeFirstKeyFrame(ctx context.Context, _ string, stream media.Stream) (*big.Rat, bool, error) {
	if p.lookups == nil {
		p.lookups = make(map[int]int)
	}
	p.lookups[stream.Index]++
	if p.cancel != nil {
		p.cancel()
		return nil, false, ctx.Err()
	}
	if p.err != nil {
		return nil, false, p.err
	}
	at, found := p.at[stream.Index]
	return at, found, nil
}

// TestKeyFrameAnchorOutcomes covers the four answers a first key frame
// lookup can give. A muxed render's reference stream is also one of its
// collected essences, and it is looked up once.
func TestKeyFrameAnchorOutcomes(t *testing.T) {
	t.Parallel()
	// The container starts two seconds in.
	const containerStart, firstPacket = int64(2 * time.Second), int64(40 * time.Millisecond)
	probe := media.Probe{Streams: []media.Stream{{Index: 0, CodecType: "video"}, {Index: 1, CodecType: "audio"}}}
	scanFailure := errors.New("scan failed")
	for name, tc := range map[string]struct {
		prober  keyFrameProber
		muxer   string
		offset  int64
		refused bool
		failure error
	}{
		"found later moves the anchor":       {prober: keyFrameProber{at: map[int]*big.Rat{0: big.NewRat(13, 1)}}, muxer: "mp4", offset: int64(11 * time.Second)},
		"found earlier leaves the anchor":    {prober: keyFrameProber{at: map[int]*big.Rat{0: big.NewRat(2, 1)}}, muxer: "mp4", offset: firstPacket},
		"absent refuses MP4 output":          {muxer: "mp4", offset: firstPacket, refused: true},
		"absent leaves MPEG-TS to its clock": {muxer: "mpegts", offset: firstPacket},
		"failed is an analysis failure":      {prober: keyFrameProber{err: scanFailure}, muxer: "mp4", offset: firstPacket, failure: scanFailure},
		"cancelled is neither":               {prober: keyFrameProber{err: context.Canceled}, muxer: "mp4", offset: firstPacket, failure: context.Canceled},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			prober := tc.prober
			pipeline, err := New(Config{SegmentDuration: time.Second}, newFakeClient(), &prober, countingSegmenter{objects: 1}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			info := media.FlowInfo{Start: containerStart, ReferenceOffset: firstPacket, Collected: []media.CollectedFlow{
				{StreamIndex: 0, Offset: firstPacket, Flow: tams.Flow{"format": "urn:x-nmos:format:video"}},
				{StreamIndex: 1, Offset: 0, Flow: tams.Flow{"format": "urn:x-nmos:format:audio"}},
			}}
			err = pipeline.anchorOnKeyFrames(t.Context(), "input.mp4", tc.muxer, probe, &info)
			var classified *classifiedFailure
			refused := errors.As(err, &classified) && classified.Code == FailureCodeMediaUnsupported && classified.Message == FailureMessageMediaNoKeyFrame
			failed := tc.failure != nil && errors.Is(err, tc.failure)
			if refused != tc.refused || failed != (tc.failure != nil) || (err != nil) != (tc.refused || tc.failure != nil) {
				t.Fatalf("anchorOnKeyFrames() = %v, want refused=%t failure=%v", err, tc.refused, tc.failure)
			}
			if tc.failure != nil && classified != nil {
				t.Fatalf("a lookup that did not finish was classified as %s", classified.Code)
			}
			if info.ReferenceOffset != tc.offset || info.Collected[0].Offset != tc.offset || info.Collected[1].Offset != 0 {
				t.Fatalf("offsets = reference %s, video %s, audio %s; want %s for the video and none for the audio", media.Timestamp(info.ReferenceOffset),
					media.Timestamp(info.Collected[0].Offset), media.Timestamp(info.Collected[1].Offset), media.Timestamp(tc.offset))
			}
			if prober.lookups[0] != 1 || prober.lookups[1] != 0 {
				t.Fatalf("lookups per stream = %v, want one for the video stream", prober.lookups)
			}
		})
	}
}

// TestRunWithoutAKeyFrameWritesNothing carries the absent and cancelled
// outcomes through a whole run: neither may reach the store.
func TestRunWithoutAKeyFrameWritesNothing(t *testing.T) {
	t.Parallel()
	for name, cancelled := range map[string]bool{"absent": false, "cancelled": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			prober := &keyFrameProber{}
			code, message := FailureCodeMediaUnsupported, FailureMessageMediaNoKeyFrame
			if cancelled {
				prober.cancel = cancel
				code, message = FailureCodeInterrupted, FailureMessageInputInterrupt
			}
			client := newFakeClient()
			pipeline, err := New(Config{SegmentDuration: time.Second}, client, prober, countingSegmenter{objects: 2}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, _ := pipeline.Run(ctx, []source.Item{localSource(writeFixture(t, "fixture.mp4", "media"))})
			if len(batch.Results) != 1 || batch.Results[0].Failure == nil || batch.Results[0].Failure.Code != code || batch.Results[0].Failure.Message != message {
				t.Fatalf("results = %+v, want %s", batch.Results, code)
			}
			client.lock.Lock()
			defer client.lock.Unlock()
			if writes := client.putFlowCalls + client.allocations + client.uploads; writes != 0 || prober.lookups[0] != 1 {
				t.Fatalf("%d store writes and %d lookups, want none and one", writes, prober.lookups[0])
			}
		})
	}
}
