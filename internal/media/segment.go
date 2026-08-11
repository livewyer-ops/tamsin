package media

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SegmentFormat names the container Tamsin should cut Segments into. It is
// expressed as intent so callers do not have to assemble muxer flags by hand,
// and so the Flow's `container` can be kept truthful about what was written.
type SegmentFormat string

const (
	// SegmentFormatSource keeps the input's own container.
	SegmentFormatSource SegmentFormat = "source"
	// SegmentFormatMPEGTS cuts MPEG-TS Segments. TS carries its own decoder
	// configuration in every Segment, so Segments are independently decodable
	// without a separate initialisation Object, which TAMS 8.1 has no way to
	// reference.
	SegmentFormatMPEGTS SegmentFormat = "mpegts"
)

// ContainerMIME reports the container MIME type Segments of this format carry,
// or empty when the format follows the source and the probe already knows it.
func (f SegmentFormat) ContainerMIME() string {
	if f == SegmentFormatMPEGTS {
		return "video/mp2t"
	}
	return ""
}

// Validate rejects unknown formats before any media is staged.
func (f SegmentFormat) Validate() error {
	switch f {
	case "", SegmentFormatSource, SegmentFormatMPEGTS:
		return nil
	default:
		return fmt.Errorf("unsupported segment format %q: use source or mpegts", string(f))
	}
}

// EssenceStorage selects whether a muxed input is stored as it arrived or
// demultiplexed so each essence owns its own Media Objects. AppNote 0001
// presents both: independent storage "affords more flexibility" where essences
// are manipulated separately, muxed storage suits retaining an original stream.
type EssenceStorage string

const (
	// EssenceStorageIndependent demultiplexes so each essence owns its Media
	// Objects, which is the arrangement AppNote 0001 leads with.
	EssenceStorageIndependent EssenceStorage = "independent"
	// EssenceStorageMuxed stores the multiplex as it arrived, described by a
	// multi-essence Flow collecting one mono-essence Flow per stream.
	EssenceStorageMuxed EssenceStorage = "muxed"
)

// Validate rejects unknown modes before any media is staged.
func (e EssenceStorage) Validate() error {
	switch e {
	case "", EssenceStorageIndependent, EssenceStorageMuxed:
		return nil
	default:
		return fmt.Errorf("unsupported essence storage %q: use independent or muxed", string(e))
	}
}

// AllStreams selects every stream in the container rather than one essence.
const AllStreams = -1

// SegmentContainer names the muxer and suffix used when SegmentFormatSource
// preserves a probed source container. Both come from media evidence rather
// than the input filename, which may be absent or misleading.
type SegmentContainer struct {
	Muxer     string
	Extension string
}

// SegmentRequest describes one FFmpeg invocation. StreamIndices normally has
// one entry; a renderer may accept several independent essences so a
// high-track-count source is opened only once.
type SegmentRequest struct {
	Input           string
	Duration        time.Duration
	Format          SegmentFormat
	SourceContainer SegmentContainer
	StreamIndices   []int
	Directory       string
	AdditionalArgs  []string
	// StagingWindow applies process-level backpressure while a streaming sink
	// commits and removes completed outputs. It is optional because callers
	// which retain every output cannot safely acknowledge reclaimed space.
	StagingWindow *SegmentStagingWindow
}

// SegmentStagingWindow bounds completed output in one FFmpeg staging directory.
// FFmpeg is paused at a closed-output boundary once total staging reaches
// HighBytes. Active outputs may cross that threshold before they can be closed
// and reclaimed; the caller's capacity ledger must bound that unavoidable
// overshoot.
type SegmentStagingWindow struct {
	HighBytes int64
	LowBytes  int64
}

func (w SegmentStagingWindow) validate() error {
	if w.HighBytes <= 0 {
		return errors.New("segment staging high watermark must be positive")
	}
	if w.LowBytes < 0 || w.LowBytes >= w.HighBytes {
		return errors.New("segment staging low watermark must be non-negative and below the high watermark")
	}
	return nil
}

// SegmentRecord is emitted when FFmpeg closes one output. Start and End are
// exact nanosecond values parsed from the segment muxer's CSV manifest. Timed
// is false for an unsegmented whole-essence extraction.
type SegmentRecord struct {
	StreamIndex int
	Path        string
	Start       int64
	End         int64
	Timed       bool
	// FlushStaging asks a streaming sink to reclaim all completed outputs
	// before returning. It is set when the staging high watermark pauses
	// FFmpeg at a closed-output boundary.
	FlushStaging bool
}

type SegmentSink func(SegmentRecord) error

type Segmenter interface {
	Segment(context.Context, SegmentRequest, SegmentSink) error
	Version(context.Context) (string, error)
}

type FFmpeg struct {
	Executable string
}

type segmentOutput struct {
	streamIndex int
	path        string
	reader      *os.File
	writer      *os.File
}

// Segment uses stream copy by default. Tamsin never silently transcodes or
// increments media generation; callers may provide explicit additional args.
// Segment cuts input into Media Objects. streamIndex selects a single
// elementary stream for independent essence storage, or AllStreams to carry the
// whole container through. A zero duration is only meaningful for a single
// stream, where it extracts that essence whole rather than cutting it.
func (f FFmpeg) Segment(ctx context.Context, request SegmentRequest, sink SegmentSink) error {
	if len(request.StreamIndices) == 0 {
		return errors.New("at least one stream index is required")
	}
	seenStreams := make(map[int]struct{}, len(request.StreamIndices))
	for _, streamIndex := range request.StreamIndices {
		if len(request.StreamIndices) > 1 && streamIndex == AllStreams {
			return errors.New("all-streams output cannot be combined with independent stream outputs")
		}
		if _, duplicate := seenStreams[streamIndex]; duplicate {
			return fmt.Errorf("stream index %d is selected more than once", streamIndex)
		}
		seenStreams[streamIndex] = struct{}{}
	}
	if sink == nil {
		return errors.New("segment sink is required")
	}
	if request.Duration <= 0 && len(request.StreamIndices) == 1 && request.StreamIndices[0] == AllStreams {
		return errors.New("segment duration must be positive")
	}
	if len(request.StreamIndices) > 1 && len(request.AdditionalArgs) > 0 {
		return errors.New("multi-output segmentation does not support custom FFmpeg arguments")
	}
	if request.StagingWindow != nil {
		if err := request.StagingWindow.validate(); err != nil {
			return err
		}
	}
	executable := f.Executable
	if executable == "" {
		executable = "ffmpeg"
	}
	if err := os.MkdirAll(request.Directory, 0o700); err != nil {
		return fmt.Errorf("create segment directory: %w", err)
	}
	outputContainer := request.SourceContainer
	if request.Format == SegmentFormatMPEGTS {
		outputContainer = SegmentContainer{Muxer: "mpegts", Extension: ".ts"}
	}
	if outputContainer.Muxer == "" || outputContainer.Extension == "" {
		return errors.New("source container is outside Tamsin's segmentation profile; use --segment-format mpegts or store it whole")
	}
	extension := strings.ToLower(outputContainer.Extension)
	if !strings.HasPrefix(extension, ".") {
		extension = "." + extension
	}
	arguments := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-i", request.Input,
	}
	outputs := make([]segmentOutput, 0, len(request.StreamIndices))
	for outputIndex, streamIndex := range request.StreamIndices {
		mapSpec := "0"
		prefix := "segment"
		if streamIndex != AllStreams {
			mapSpec = "0:" + strconv.Itoa(streamIndex)
			prefix = "essence-" + strconv.Itoa(streamIndex)
		}
		arguments = append(arguments, "-map", mapSpec, "-c", "copy")
		arguments = append(arguments, request.AdditionalArgs...)
		if request.Duration <= 0 {
			path := filepath.Join(request.Directory, prefix+extension)
			arguments = append(arguments, "-f", outputContainer.Muxer, path)
			outputs = append(outputs, segmentOutput{streamIndex: streamIndex, path: path})
			continue
		}
		reader, writer, err := os.Pipe()
		if err != nil {
			closeSegmentPipes(outputs)
			return fmt.Errorf("create segment manifest pipe: %w", err)
		}
		pattern := filepath.Join(request.Directory, prefix+"-%08d"+extension)
		arguments = append(arguments,
			"-f", "segment",
			"-segment_format", outputContainer.Muxer,
			"-segment_time", strconv.FormatFloat(request.Duration.Seconds(), 'f', 9, 64),
			"-reset_timestamps", "1",
			"-segment_list", "pipe:"+strconv.Itoa(3+outputIndex),
			"-segment_list_type", "csv",
			"-segment_list_flags", "+live",
			pattern,
		)
		outputs = append(outputs, segmentOutput{streamIndex: streamIndex, reader: reader, writer: writer})
	}

	if request.Duration <= 0 {
		_, stderr, err := runTool(ctx, executable, arguments...)
		if err != nil {
			return fmt.Errorf("extract essences from %q: %w: %s", request.Input, err, strings.TrimSpace(string(stderr)))
		}
		for _, output := range outputs {
			if err := sink(SegmentRecord{StreamIndex: output.streamIndex, Path: output.path}); err != nil {
				return err
			}
		}
		return nil
	}

	command := toolCommand(ctx, executable, arguments...)
	var stderr limitedBuffer
	command.Stderr = &stderr
	for _, output := range outputs {
		command.ExtraFiles = append(command.ExtraFiles, output.writer)
	}
	if err := command.Start(); err != nil {
		closeSegmentPipes(outputs)
		return fmt.Errorf("start segmenting %q: %w", request.Input, err)
	}
	var backpressure *segmentBackpressure
	if request.StagingWindow != nil {
		backpressure = &segmentBackpressure{
			process: osStagingProcess{process: command.Process}, directory: request.Directory,
			window: *request.StagingWindow,
		}
	}
	for _, output := range outputs {
		_ = output.writer.Close()
	}

	counts := make([]int, len(outputs))
	var (
		parsers  sync.WaitGroup
		sinkMu   sync.Mutex
		errMu    sync.Mutex
		parseErr error
	)
	recordError := func(err error) {
		errMu.Lock()
		if parseErr == nil {
			parseErr = err
			_ = continueSegmentProcess(command.Process)
			_ = command.Cancel()
		}
		errMu.Unlock()
	}
	for index, output := range outputs {
		parsers.Add(1)
		go func() {
			defer parsers.Done()
			defer output.reader.Close()
			previousStart := int64(-1)
			decoder := csv.NewReader(output.reader)
			decoder.FieldsPerRecord = 3
			for {
				fields, err := decoder.Read()
				if errors.Is(err, io.EOF) {
					return
				}
				if err != nil {
					recordError(fmt.Errorf("decode segment manifest for stream %d: %w", output.streamIndex, err))
					return
				}
				start, err := ParseSeconds(fields[1])
				if err != nil {
					recordError(fmt.Errorf("decode segment start for stream %d: %w", output.streamIndex, err))
					return
				}
				end, err := ParseSeconds(fields[2])
				if err != nil || end <= start || (previousStart >= 0 && start < previousStart) {
					recordError(fmt.Errorf("segment manifest for stream %d has a non-monotonic interval %q to %q", output.streamIndex, fields[1], fields[2]))
					return
				}
				path, err := containedSegmentPath(request.Directory, fields[0])
				if err != nil {
					recordError(err)
					return
				}
				record := SegmentRecord{StreamIndex: output.streamIndex, Path: path, Start: start, End: end, Timed: true}
				if backpressure != nil {
					flush, controlErr := backpressure.beforeSink()
					if controlErr != nil {
						recordError(controlErr)
						return
					}
					record.FlushStaging = flush
				}
				sinkMu.Lock()
				err = sink(record)
				sinkMu.Unlock()
				if backpressure != nil {
					if controlErr := backpressure.afterSink(); err == nil {
						err = controlErr
					}
				}
				if err != nil {
					recordError(err)
					return
				}
				counts[index]++
				previousStart = start
			}
		}()
	}
	waitResult := make(chan error, 1)
	go func() { waitResult <- command.Wait() }()
	parsers.Wait()
	if backpressure != nil {
		if err := backpressure.finish(); err != nil {
			recordError(err)
		}
	}
	waitErr := <-waitResult
	errMu.Lock()
	manifestErr := parseErr
	errMu.Unlock()
	if manifestErr != nil {
		return manifestErr
	}
	if waitErr != nil {
		return fmt.Errorf("segment %q: %w: %s", request.Input, waitErr, strings.TrimSpace(string(stderr.Bytes())))
	}
	for index, count := range counts {
		if count == 0 {
			return fmt.Errorf("FFmpeg produced no segments for stream %d", outputs[index].streamIndex)
		}
	}
	return nil
}

func closeSegmentPipes(outputs []segmentOutput) {
	for _, output := range outputs {
		if output.reader != nil {
			_ = output.reader.Close()
		}
		if output.writer != nil {
			_ = output.writer.Close()
		}
	}
}

func containedSegmentPath(directory, path string) (string, error) {
	absoluteDirectory, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve segment directory: %w", err)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(absoluteDirectory, path)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve generated segment path: %w", err)
	}
	relative, err := filepath.Rel(absoluteDirectory, absolutePath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("FFmpeg segment manifest named a path outside its staging directory")
	}
	return absolutePath, nil
}

func (f FFmpeg) Version(ctx context.Context) (string, error) {
	executable := f.Executable
	if executable == "" {
		executable = "ffmpeg"
	}
	stdout, stderr, err := runTool(ctx, executable, "-version")
	if err != nil {
		return "", fmt.Errorf("run %s: %w: %s", executable, err, strings.TrimSpace(string(stderr)))
	}
	// The full report includes configure flags and linked library versions.
	// Two binaries with the same banner version can still produce different
	// bytes when those build inputs differ, so callers fingerprint all of it and
	// select the first line separately for concise provenance.
	return strings.TrimSpace(string(stdout)), nil
}
