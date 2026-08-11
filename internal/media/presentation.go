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

// ProbePresentation walks decoded video frames in presentation order and
// classifies each video stream's cadence. Output is parsed as it arrives, so a
// long-running broadcast asset costs a decoder pass but not one in-memory
// timestamp per frame.
func (p FFprobe) ProbePresentation(ctx context.Context, filename string, probe *Probe) error {
	if probe == nil {
		return errors.New("presentation probe result is nil")
	}
	states := make(map[int]*cadenceState)
	for index := range probe.Streams {
		stream := &probe.Streams[index]
		if stream.CodecType != "video" || stream.Disposition.AttachedPicture != 0 {
			continue
		}
		stream.Cadence = CadenceUnknown
		states[stream.Index] = &cadenceState{stream: stream}
	}
	if len(states) == 0 {
		return nil
	}

	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	command := toolCommand(ctx, executable,
		"-v", "error",
		"-select_streams", "v",
		"-show_frames",
		"-show_entries", "frame=stream_index,best_effort_timestamp",
		"-of", "compact=p=0:nk=0",
		filename,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open presentation probe output: %w", err)
	}
	var stderr limitedBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start presentation probe %q: %w", filename, err)
	}

	scanErr := scanPresentation(stdout, states)
	if scanErr != nil && command.Process != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if scanErr != nil {
		return fmt.Errorf("read presentation probe %q: %w", filename, scanErr)
	}
	if waitErr != nil {
		return fmt.Errorf("probe presentation %q: %w: %s", filename, waitErr, strings.TrimSpace(string(stderr.Bytes())))
	}

	for _, state := range states {
		state.finish()
	}
	return nil
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
	scanner := bufio.NewScanner(reader)
	// Restricted compact output is tiny, but leave headroom for FFprobe side
	// data it may append despite show_entries (for example an H.264 SEI label).
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		index, timestamp, ok := compactPresentationFields(scanner.Text())
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

func compactPresentationFields(line string) (int, string, bool) {
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
			case "best_effort_timestamp":
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
