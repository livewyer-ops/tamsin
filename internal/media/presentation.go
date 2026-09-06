package media

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// CadenceEvidence records what a presentation-timestamp scan established.
//
// FFprobe's avg_frame_rate and r_frame_rate are summaries and guesses. They do
// not establish that every presentation interval in a finite Flow is equal,
// which is the distinction TAMS's vfr property asks for.
type CadenceEvidence uint8

const (
	// CadenceUnexamined is retained for callers that supply Probe values
	// directly. The production pipeline always asks a PresentationProber to
	// inspect video before building its Flow.
	CadenceUnexamined CadenceEvidence = iota
	// CadenceUnknown means the scan ran but the input did not expose enough
	// usable presentation timestamps to make either claim.
	CadenceUnknown
	CadenceFixed
	CadenceVariable
)

// ProbePresentation classifies each video stream's cadence from presentation
// timestamps. Packet timestamps avoid decoding video without frame reordering.
// Known B-frame reordering goes straight to decoded frames; otherwise missing,
// duplicated, or non-monotonic packet evidence
// falls back to decoded frame timestamps rather than weakening the claim.
// Output is parsed as it arrives, so neither path retains one timestamp per
// frame in memory.
func (p FFprobe) ProbePresentation(ctx context.Context, filename string, probe *Probe) error {
	if probe == nil {
		return errors.New("presentation probe result is nil")
	}
	if !hasCadenceStreams(probe) {
		return nil
	}
	if !hasReorderedVideo(probe) {
		if complete, err := p.probePresentationMode(ctx, filename, probe, presentationPackets); err != nil {
			return err
		} else if complete {
			return nil
		}
	}
	_, err := p.probePresentationMode(ctx, filename, probe, presentationFrames)
	return err
}

func hasReorderedVideo(probe *Probe) bool {
	for _, stream := range probe.Streams {
		if stream.CodecType == "video" && stream.Disposition.AttachedPicture == 0 && stream.HasBFrames > 0 {
			return true
		}
	}
	return false
}

type presentationMode uint8

const (
	presentationPackets presentationMode = iota
	presentationFrames
)

func hasCadenceStreams(probe *Probe) bool {
	for index := range probe.Streams {
		stream := probe.Streams[index]
		if stream.CodecType == "video" && stream.Disposition.AttachedPicture == 0 {
			return true
		}
	}
	return false
}

func cadenceStates(probe *Probe) map[int]*cadenceState {
	states := make(map[int]*cadenceState)
	for index := range probe.Streams {
		stream := &probe.Streams[index]
		if stream.CodecType != "video" || stream.Disposition.AttachedPicture != 0 {
			continue
		}
		stream.Cadence = CadenceUnknown
		states[stream.Index] = &cadenceState{stream: stream}
	}
	return states
}

func (p FFprobe) probePresentationMode(ctx context.Context, filename string, probe *Probe, mode presentationMode) (bool, error) {
	states := cadenceStates(probe)
	section, timestampField := "packet", "pts"
	showArgument := "-show_packets"
	if mode == presentationFrames {
		section, timestampField = "frame", "best_effort_timestamp"
		showArgument = "-show_frames"
	}

	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	command := toolCommand(ctx, executable,
		"-v", "error",
		"-select_streams", "v",
		showArgument,
		"-show_entries", section+"=stream_index,"+timestampField,
		"-of", "compact=p=0:nk=0",
		filename,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return false, fmt.Errorf("open presentation probe output: %w", err)
	}
	var stderr limitedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return false, fmt.Errorf("start presentation probe %q: %w", filename, err)
	}

	scanErr := scanPresentationTimestamps(stdout, states, timestampField)
	if scanErr != nil && command.Process != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if scanErr != nil {
		return false, fmt.Errorf("read presentation probe %q: %w", filename, scanErr)
	}
	if waitErr != nil {
		return false, fmt.Errorf("probe presentation %q: %w: %s", filename, waitErr, strings.TrimSpace(string(stderr.Bytes())))
	}

	complete := true
	for _, state := range states {
		state.finish()
		if state.stream.Cadence == CadenceUnknown {
			complete = false
		}
	}
	return complete, nil
}

type cadenceState struct {
	stream      *Stream
	previous    int64
	frames      int64
	minimumStep int64
	maximumStep int64
	invalid     bool
}

func (s *cadenceState) observe(timestamp string) {
	value, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		s.invalid = true
		return
	}
	if s.frames > 0 {
		step, offsetErr := TimestampOffset(value, s.previous)
		if offsetErr != nil || step <= 0 {
			s.invalid = true
		} else {
			if s.minimumStep == 0 || step < s.minimumStep {
				s.minimumStep = step
			}
			if step > s.maximumStep {
				s.maximumStep = step
			}
		}
	}
	s.previous = value
	s.frames++
}

func (s *cadenceState) finish() {
	s.stream.Cadence = CadenceUnknown
	if s.invalid || s.frames == 0 {
		return
	}
	if s.frames == 1 {
		if _, _, ok := streamFrameRate(*s.stream); ok {
			s.stream.Cadence = CadenceFixed
		}
		return
	}

	// A rational cadence represented on a coarser integer time base naturally
	// alternates adjacent tick counts: 30000/1001 fps in a 1/1000 time base is
	// 33, 34, 33, ... ticks. A spread of one tick is therefore quantisation,
	// not evidence of VFR. A wider spread cannot represent one fixed cadence.
	if s.maximumStep-s.minimumStep > 1 {
		s.stream.Cadence = CadenceVariable
		return
	}
	if _, _, ok := streamFrameRate(*s.stream); ok {
		s.stream.Cadence = CadenceFixed
	}
}

func scanPresentation(reader io.Reader, states map[int]*cadenceState) error {
	return scanPresentationTimestamps(reader, states, "best_effort_timestamp")
}

func scanPresentationTimestamps(reader io.Reader, states map[int]*cadenceState, timestampField string) error {
	scanner := bufio.NewScanner(reader)
	// Restricted compact output is tiny, but leave headroom for FFprobe side
	// data it may append despite show_entries (for example an H.264 SEI label).
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		index, timestamp, ok := compactTimestampFields(scanner.Text(), timestampField)
		if !ok {
			continue
		}
		state := states[index]
		if state == nil {
			continue
		}
		if timestamp == "N/A" {
			state.invalid = true
			continue
		}
		state.observe(timestamp)
	}
	return scanner.Err()
}

func compactTimestampFields(line, timestampField string) (int, string, bool) {
	index := -1
	timestamp := ""
	for line != "" {
		part, remainder, found := strings.Cut(line, "|")
		key, value, ok := strings.Cut(part, "=")
		if ok {
			switch key {
			case "stream_index":
				parsed, err := strconv.Atoi(value)
				if err == nil {
					index = parsed
				}
			case timestampField:
				timestamp = value
			}
		}
		if !found {
			break
		}
		line = remainder
	}
	return index, timestamp, index >= 0 && timestamp != ""
}

func streamFrameRate(stream Stream) (int64, int64, bool) {
	if numerator, denominator, ok := ParseRate(stream.AverageFrameRate); ok {
		return numerator, denominator, true
	}
	return ParseRate(stream.RealFrameRate)
}
