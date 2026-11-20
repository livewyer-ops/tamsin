package media

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"
)

func (p FFprobe) ProbeObject(ctx context.Context, filename string) (Probe, error) {
	probe, err := p.Probe(ctx, filename)
	if err != nil {
		return Probe{}, err
	}
	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	arguments := append([]string{"-v", "error"}, inputOptions(filename)...)
	arguments = append(arguments, "-show_packets", "-show_entries",
		"packet=stream_index,pts,duration,size,flags:packet_side_data=skip_samples,discard_padding",
		"-of", "json", filename)
	step, stderr, err := streamTool(ctx, executable, arguments, func(stdout io.Reader) error {
		return measureObjectPackets(stdout, &probe)
	})
	if step == toolStepWait && err != nil {
		err = fmt.Errorf("probe object timing: %w: %s", err, strings.TrimSpace(string(stderr)))
	}
	return probe, err
}

type objectPacket struct {
	StreamIndex int         `json:"stream_index"`
	PTS         json.Number `json:"pts"`
	Duration    json.Number `json:"duration"`
	Size        string      `json:"size"`
	Flags       string      `json:"flags"`
	SideData    []struct {
		SkipSamples    int64 `json:"skip_samples"`
		DiscardPadding int64 `json:"discard_padding"`
	} `json:"side_data_list"`
}

func measureObjectPackets(reader io.Reader, probe *Probe) error {
	type span struct {
		first, end, last, decoded *big.Rat
		samples, keyFrames        int
		hiddenLead                int
		leading                   []int64
	}
	bounds := make(map[int]span)
	startsOnKey := make(map[int]bool)
	transport := probe.Format.Name == "mpegts"
	var previousPTS int64
	havePTS := false
	streams := make(map[int]Stream)
	for _, stream := range contentStreams(probe.Streams) {
		streams[stream.Index] = stream
	}
	decoder := json.NewDecoder(reader)
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return errors.New("invalid object timing response")
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return err
		}
		if key != "packets" {
			var ignored json.RawMessage
			if err := decoder.Decode(&ignored); err != nil {
				return err
			}
			continue
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
			return errors.New("invalid object packet list")
		}
		for decoder.More() {
			var packet objectPacket
			if err := decoder.Decode(&packet); err != nil {
				return err
			}
			stream, present := streams[packet.StreamIndex]
			if !present {
				continue
			}
			if _, stored := startsOnKey[stream.Index]; !stored {
				startsOnKey[stream.Index] = strings.Contains(packet.Flags, "K")
			}
			pts, ptsErr := packet.PTS.Int64()
			tick, tickOK := new(big.Rat).SetString(stream.TimeBase)
			if strings.Contains(packet.Flags, "D") {
				// An edit list keeps this packet from being shown. One stored
				// after the frame the Object was cut on that presents before
				// it is a leading picture of an open GOP.
				if bound := bounds[stream.Index]; stream.CodecType == "video" && bound.decoded != nil && ptsErr == nil && tickOK &&
					new(big.Rat).Mul(new(big.Rat).SetInt64(pts), tick).Cmp(bound.decoded) < 0 {
					bound.hiddenLead++
					bounds[stream.Index] = bound
				}
				continue
			}
			duration, durationErr := packet.Duration.Int64()
			// FFmpeg emits a two-byte empty mov_text sample to clear a cue.
			// Its terminal, zero-duration sample has no duration field in FFprobe.
			if stream.CodecType == "subtitle" && stream.CodecName == "mov_text" && packet.Size == "2" && packet.Duration == "" && ptsErr == nil && tickOK && tick.Sign() > 0 {
				continue
			}
			if ptsErr != nil || durationErr != nil || duration < 0 || !tickOK || tick.Sign() <= 0 {
				return fmt.Errorf("stream %d has incomplete object presentation timing", stream.Index)
			}
			if transport && stream.TimeBase == "1/90000" {
				// FFprobe may unwrap one PES group but leave the next group
				// (or a different PID) on the other side of the 33-bit wrap.
				if havePTS {
					pts = unwrapTransportPTS(pts, previousPTS)
				}
				previousPTS, havePTS = pts, true
			}
			first := new(big.Rat).Mul(new(big.Rat).SetInt64(pts), tick)
			end := new(big.Rat).Add(first, new(big.Rat).Mul(new(big.Rat).SetInt64(duration), tick))
			for _, data := range packet.SideData {
				if data.SkipSamples == 0 && data.DiscardPadding == 0 {
					continue
				}
				rate, err := strconv.ParseInt(stream.SampleRate, 10, 64)
				if err != nil || rate <= 0 || data.SkipSamples < 0 || data.DiscardPadding < 0 {
					return errors.New("invalid audio padding in object timing")
				}
				first.Add(first, new(big.Rat).SetFrac64(data.SkipSamples, rate))
				end.Sub(end, new(big.Rat).SetFrac64(data.DiscardPadding, rate))
			}
			if end.Cmp(first) < 0 {
				return errors.New("object audio padding exceeds packet duration")
			}
			if end.Cmp(first) == 0 && stream.CodecType == "audio" {
				continue
			}
			bound := bounds[stream.Index]
			if bound.first == nil || first.Cmp(bound.first) < 0 {
				bound.first = first
			}
			if bound.end == nil || end.Cmp(bound.end) > 0 {
				bound.end = end
			}
			if bound.last == nil || first.Cmp(bound.last) > 0 {
				bound.last = first
			}
			if bound.decoded == nil {
				bound.decoded = first
			} else if stream.CodecType == "video" && first.Cmp(bound.decoded) < 0 && len(bound.leading) <= maxLeadingPictures {
				bound.leading = append(bound.leading, pts)
			}
			bound.samples++
			if strings.Contains(packet.Flags, "K") {
				bound.keyFrames++
			}
			bounds[stream.Index] = bound
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	for index := range probe.Streams {
		stream := &probe.Streams[index]
		if stream.Disposition.AttachedPicture != 0 {
			continue
		}
		bound, present := bounds[stream.Index]
		if !present {
			if stream.CodecType == "subtitle" || stream.CodecType == "data" {
				stream.Empty = true
				stream.StartTime, stream.Duration, stream.StartPTS = "", "", nil
				continue
			}
			return fmt.Errorf("stream %d has no object presentation timestamps", stream.Index)
		}
		stream.StartTime, stream.StartPTS = bound.first.RatString(), nil
		stream.Duration = new(big.Rat).Sub(bound.end, bound.first).RatString()
		if stream.CodecType == "audio" {
			rate, err := strconv.ParseInt(stream.SampleRate, 10, 64)
			if err != nil || rate <= 0 {
				return fmt.Errorf("stream %d has no valid audio sample rate", stream.Index)
			}
			bound.last = new(big.Rat).Sub(bound.end, new(big.Rat).SetFrac64(1, rate))
		}
		stream.LastSampleTime = bound.last.RatString()
		stream.FirstDecodedTime = bound.decoded.RatString()
		stream.SampleCount = bound.samples
		stream.KeyFrames = bound.keyFrames
		stream.HiddenLead = bound.hiddenLead
		stream.LeadingPTS = bound.leading
		stream.MidGOP = stream.CodecType == "video" && !startsOnKey[stream.Index]
	}
	probe.Format.StartTime = ""
	probe.Format.Duration = ""
	return nil
}

// ProbeLeadingPictures reports whether a video stream has an open GOP after
// its first key frame: a picture stored after a key frame that presents
// before it. Packets are listed in stored order, so the whole stream is read
// without decoding, and the scan stops at the first such picture. The first
// key frame is left out because nothing precedes the Segment that starts on
// it.
func (p FFprobe) ProbeLeadingPictures(ctx context.Context, filename string, streamIndex int) (bool, error) {
	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	arguments := append([]string{"-v", "error"}, inputOptions(filename)...)
	arguments = append(arguments, "-select_streams", strconv.Itoa(streamIndex),
		"-show_packets", "-show_entries", "packet=pts,flags", "-of", "compact=p=0:nk=0", filename)
	step, stderr, err := streamTool(ctx, executable, arguments, scanLeadingPictures)
	if errors.Is(err, errLeadingPicture) {
		return true, nil
	}
	if step == toolStepWait && err != nil {
		return false, fmt.Errorf("probe leading pictures: %w: %s", err, strings.TrimSpace(string(stderr)))
	}
	return false, err
}

var errLeadingPicture = errors.New("leading picture found")

func scanLeadingPictures(reader io.Reader) error {
	keyFrames := 0
	var keyFramePTS int64
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		var (
			pts   int64
			timed bool
			key   bool
		)
		for _, field := range strings.Split(scanner.Text(), "|") {
			name, value, _ := strings.Cut(field, "=")
			switch name {
			case "pts":
				var err error
				pts, err = strconv.ParseInt(value, 10, 64)
				timed = err == nil
			case "flags":
				key = strings.Contains(value, "K")
			}
		}
		switch {
		case !timed:
		case key:
			keyFrames++
			keyFramePTS = pts
		case keyFrames > 1 && pts < keyFramePTS:
			return errLeadingPicture
		}
	}
	return scanner.Err()
}

// ProbeFirstKeyFrame finds where stream copy of a video stream actually
// begins. FFmpeg's copy drops the frames before the first key frame, so a
// capture that starts mid-GOP loses up to a GOP of leading frames; the
// stream's start_time still names the first of them. Packets are read in
// stored order and the read stops at the first key frame. Its time is its
// timestamp in the stream's time base, because FFprobe's pts_time is rounded
// to the microsecond. It reports false when the input ends, or
// KeyFrameSearchLimit of the stream passes, without one.
func (p FFprobe) ProbeFirstKeyFrame(ctx context.Context, filename string, stream Stream) (*big.Rat, bool, error) {
	tick, ok := new(big.Rat).SetString(stream.TimeBase)
	if !ok || tick.Sign() <= 0 {
		return nil, false, fmt.Errorf("stream %d has no time base", stream.Index)
	}
	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	arguments := append([]string{"-v", "error"}, inputOptions(filename)...)
	arguments = append(arguments, "-select_streams", strconv.Itoa(stream.Index), "-read_intervals", "%+"+strconv.Itoa(int(KeyFrameSearchLimit.Seconds())),
		"-show_packets", "-show_entries", "packet=pts,flags", "-of", "compact=p=0:nk=0", filename)
	var at int64
	step, stderr, err := streamTool(ctx, executable, arguments, func(stdout io.Reader) error {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var presented, flags string
			for _, field := range strings.Split(scanner.Text(), "|") {
				switch name, value, _ := strings.Cut(field, "="); name {
				case "pts":
					presented = value
				case "flags":
					flags = value
				}
			}
			if !strings.Contains(flags, "K") || presented == "" || presented == "N/A" {
				continue
			}
			pts, err := strconv.ParseInt(presented, 10, 64)
			if err != nil {
				return fmt.Errorf("parse key frame timestamp: %w", err)
			}
			at = pts
			return errKeyFrame
		}
		return scanner.Err()
	})
	if errors.Is(err, errKeyFrame) {
		return tick.Mul(tick, new(big.Rat).SetInt64(at)), true, nil
	}
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}
	if step == toolStepWait && err != nil {
		return nil, false, fmt.Errorf("probe first key frame: %w: %s", err, strings.TrimSpace(string(stderr)))
	}
	return nil, false, err
}

var errKeyFrame = errors.New("key frame found")

// KeyFrameSearchLimit bounds how much of a stream is read for its first key
// frame. A longer wait than this for a picture that can be decoded is not
// one a stream copy should start from.
const KeyFrameSearchLimit = 5 * time.Minute
