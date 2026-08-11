package media

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const maxToolOutput = 4 << 20

type Stream struct {
	Index         int    `json:"index"`
	CodecName     string `json:"codec_name"`
	CodecLongName string `json:"codec_long_name"`
	CodecType     string `json:"codec_type"`
	Profile       string `json:"profile"`
	Width         int    `json:"width"`
	Height        int    `json:"height"`
	PixelFormat   string `json:"pix_fmt"`
	FieldOrder    string `json:"field_order"`
	ColorSpace    string `json:"color_space"`
	// ColorPrimaries names the colour system. ColorSpace is the matrix
	// coefficients, which usually agree with it and are not the same question.
	ColorPrimaries string `json:"color_primaries"`
	// SampleAspectRatio and DisplayAspectRatio are colon-separated, unlike the
	// frame rates alongside them.
	SampleAspectRatio  string `json:"sample_aspect_ratio"`
	DisplayAspectRatio string `json:"display_aspect_ratio"`
	ColorTransfer      string `json:"color_transfer"`
	SampleRate         string `json:"sample_rate"`
	Channels           int    `json:"channels"`
	BitsPerRawSample   string `json:"bits_per_raw_sample"`
	BitsPerSample      int    `json:"bits_per_sample"`
	AverageFrameRate   string `json:"avg_frame_rate"`
	RealFrameRate      string `json:"r_frame_rate"`
	// Cadence is derived by a separate presentation-timestamp scan. It is not
	// an FFprobe JSON field and therefore cannot be confused with the rate
	// summaries above.
	Cadence     CadenceEvidence `json:"-"`
	StartTime   string          `json:"start_time"`
	Duration    string          `json:"duration"`
	BitRate     string          `json:"bit_rate"`
	Disposition struct {
		AttachedPicture int `json:"attached_pic"`
	} `json:"disposition"`
}

type Format struct {
	Name      string            `json:"format_name"`
	LongName  string            `json:"format_long_name"`
	StartTime string            `json:"start_time"`
	Duration  string            `json:"duration"`
	Size      string            `json:"size"`
	BitRate   string            `json:"bit_rate"`
	Tags      map[string]string `json:"tags"`
}

type Probe struct {
	Streams []Stream `json:"streams"`
	Format  Format   `json:"format"`
}

type Prober interface {
	Probe(context.Context, string) (Probe, error)
	Version(context.Context) (string, error)
}

type FFprobe struct {
	Executable string
}

func (p FFprobe) Probe(ctx context.Context, filename string) (Probe, error) {
	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	stdout, stderr, err := runTool(ctx, executable,
		"-v", "error",
		"-show_streams",
		"-show_format",
		"-of", "json",
		filename,
	)
	if err != nil {
		return Probe{}, fmt.Errorf("probe %q: %w: %s", filename, err, strings.TrimSpace(string(stderr)))
	}
	var result Probe
	if err := json.Unmarshal(stdout, &result); err != nil {
		return Probe{}, fmt.Errorf("decode ffprobe output: %w", err)
	}
	return result, nil
}

func (p FFprobe) Version(ctx context.Context) (string, error) {
	executable := p.Executable
	if executable == "" {
		executable = "ffprobe"
	}
	stdout, stderr, err := runTool(ctx, executable, "-version")
	if err != nil {
		return "", fmt.Errorf("run %s: %w: %s", executable, err, strings.TrimSpace(string(stderr)))
	}
	line, _, _ := strings.Cut(string(stdout), "\n")
	return strings.TrimSpace(line), nil
}

// PresentationProber augments a container/stream probe with evidence that
// requires walking the media timeline. Keeping it separate from Prober avoids
// decoding every generated Segment when the ingest pipeline only needs its
// start and duration; presentation metadata is established once, from the
// staged input.
type PresentationProber interface {
	ProbePresentation(context.Context, string, *Probe) error
}

func runTool(ctx context.Context, executable string, arguments ...string) ([]byte, []byte, error) {
	if executable == "" {
		return nil, nil, errors.New("tool executable is empty")
	}
	command := toolCommand(ctx, executable, arguments...)
	var stdout, stderr limitedBuffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// toolCommand gives every FFmpeg-family process the same cancellation
// contract. os/exec's default is an immediate kill; asking the tool to stop
// first lets it close readers and still bounds an uncooperative exit.
func toolCommand(ctx context.Context, executable string, arguments ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.WaitDelay = 5 * time.Second
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		return command.Process.Signal(os.Interrupt)
	}
	return command
}

type limitedBuffer struct {
	buffer bytes.Buffer
	total  int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	b.total += len(data)
	remaining := maxToolOutput - b.buffer.Len()
	if remaining > 0 {
		if len(data) > remaining {
			_, _ = b.buffer.Write(data[:remaining])
		} else {
			_, _ = b.buffer.Write(data)
		}
	}
	return len(data), nil
}

func (b *limitedBuffer) Bytes() []byte {
	if b.total <= maxToolOutput {
		return b.buffer.Bytes()
	}
	result := append([]byte(nil), b.buffer.Bytes()...)
	return append(result, []byte("\n[output truncated]\n")...)
}
