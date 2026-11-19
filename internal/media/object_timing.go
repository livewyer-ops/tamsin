package media

import (
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
	}
	bounds := make(map[int]span)
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
			if !present || strings.Contains(packet.Flags, "D") {
				continue
			}
			pts, ptsErr := packet.PTS.Int64()
			duration, durationErr := packet.Duration.Int64()
			tick, tickOK := new(big.Rat).SetString(stream.TimeBase)
			// FFmpeg emits a two-byte empty mov_text sample to clear a cue.
			// Its terminal, zero-duration sample has no duration field in FFprobe.
			if stream.CodecType == "subtitle" && stream.CodecName == "mov_text" && packet.Size == "2" && packet.Duration == "" && ptsErr == nil && tickOK && tick.Sign() > 0 {
				continue
			}
			if ptsErr != nil || durationErr != nil || duration < 0 || !tickOK || tick.Sign() <= 0 {
				return fmt.Errorf("stream %d has incomplete object presentation timing", stream.Index)
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
				stream.StartTime, stream.Duration = "", ""
				continue
			}
			return fmt.Errorf("stream %d has no object presentation timestamps", stream.Index)
		}
		stream.StartTime = bound.first.RatString()
		stream.Duration = new(big.Rat).Sub(bound.end, bound.first).RatString()
		stream.LastSampleTime = bound.last.RatString()
		stream.FirstDecodedTime = bound.decoded.RatString()
		stream.SampleCount = bound.samples
		stream.KeyFrames = bound.keyFrames
	}
	probe.Format.StartTime = ""
	probe.Format.Duration = ""
	return nil
}

// ProbeFirstKeyFrame finds where stream copy of a video stream actually
// begins. FFmpeg's copy drops the frames before the first key frame, so a
// capture that starts mid-GOP loses up to a GOP of leading frames; the
// stream's start_time still names the first of them. The probe reads the
// first seconds of the file only. It reports false when no key frame was
// found within that window.
func (p FFprobe) ProbeFirstKeyFrame(ctx context.Context, filename string, streamIndex int) (int64, bool, error) {
	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	arguments := append([]string{"-v", "error"}, inputOptions(filename)...)
	arguments = append(arguments, "-select_streams", strconv.Itoa(streamIndex), "-read_intervals", "%+"+strconv.Itoa(int(keyFrameProbeWindow.Seconds())),
		"-show_packets", "-show_entries", "packet=pts,pts_time,flags", "-of", "json", filename)
	var (
		found bool
		at    int64
	)
	step, stderr, err := streamTool(ctx, executable, arguments, func(stdout io.Reader) error {
		var listing struct {
			Packets []struct {
				PTSTime string `json:"pts_time"`
				Flags   string `json:"flags"`
			} `json:"packets"`
		}
		if err := json.NewDecoder(stdout).Decode(&listing); err != nil {
			return err
		}
		for _, packet := range listing.Packets {
			if !strings.Contains(packet.Flags, "K") || packet.PTSTime == "" || packet.PTSTime == "N/A" {
				continue
			}
			seconds, err := ParseSeconds(packet.PTSTime)
			if err != nil {
				return fmt.Errorf("parse key frame time: %w", err)
			}
			at, found = seconds, true
			return nil
		}
		return nil
	})
	if step == toolStepWait && err != nil {
		return 0, false, fmt.Errorf("probe first key frame: %w: %s", err, strings.TrimSpace(string(stderr)))
	}
	if err != nil {
		return 0, false, err
	}
	return at, found, nil
}

// keyFrameProbeWindow bounds how far into a file the first key frame is
// looked for; a GOP longer than this is not stream-copied sensibly anyway.
const keyFrameProbeWindow = 10 * time.Second
