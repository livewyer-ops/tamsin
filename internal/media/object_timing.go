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
)

// ObjectProber measures the presentation timeline of emitted media, rather
// than treating container header durations or segment-list DTS as sample bounds.
type ObjectProber interface {
	ProbeObject(context.Context, string) (Probe, error)
}

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
		"packet=stream_index,pts,duration,flags:packet_side_data=skip_samples,discard_padding",
		"-of", "json", filename)
	command := toolCommand(ctx, executable, arguments...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return Probe{}, err
	}
	var stderr limitedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return Probe{}, err
	}
	scanErr := measureObjectPackets(stdout, &probe)
	if scanErr != nil {
		_ = command.Process.Kill()
	}
	if err := command.Wait(); scanErr == nil && err != nil {
		scanErr = fmt.Errorf("probe object timing: %w: %s", err, strings.TrimSpace(string(stderr.Bytes())))
	}
	return probe, scanErr
}

type objectPacket struct {
	StreamIndex int         `json:"stream_index"`
	PTS         json.Number `json:"pts"`
	Duration    json.Number `json:"duration"`
	Flags       string      `json:"flags"`
	SideData    []struct {
		SkipSamples    int64 `json:"skip_samples"`
		DiscardPadding int64 `json:"discard_padding"`
	} `json:"side_data_list"`
}

func measureObjectPackets(reader io.Reader, probe *Probe) error {
	type span struct{ first, end *big.Rat }
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
			return fmt.Errorf("stream %d has no object presentation timestamps", stream.Index)
		}
		stream.StartTime = bound.first.RatString()
		stream.Duration = new(big.Rat).Sub(bound.end, bound.first).RatString()
	}
	probe.Format.StartTime = ""
	probe.Format.Duration = ""
	return nil
}
