package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

type InputMode string

const (
	InputAuto   InputMode = "auto"
	InputStream InputMode = "stream"
	InputStage  InputMode = "stage"
)

func (m InputMode) Validate() error {
	switch m {
	case InputAuto, InputStream, InputStage:
		return nil
	default:
		return fmt.Errorf("input mode must be auto, stream, or stage, got %q", m)
	}
}

func (p *Pipeline) ingestOne(ctx context.Context, item source.Item, storageID string) (Result, error) {
	result, err := p.ingestInput(ctx, item, storageID, p.config.InputMode)
	var unavailable *source.StreamUnavailableError
	if p.config.InputMode == InputAuto && errors.As(err, &unavailable) {
		p.logger.Warn("staging remote input", "input", safeURI(item.URI), "reason", unavailable.Reason)
		return p.ingestInput(ctx, item, storageID, InputStage)
	}
	return result, err
}

func (p *Pipeline) prepareInput(ctx context.Context, item source.Item, mode InputMode) (stagedFile, error) {
	remote := strings.HasPrefix(item.URI, "http://") || strings.HasPrefix(item.URI, "https://") || strings.HasPrefix(item.URI, "s3://")
	stream := remote && mode != InputStage
	var snapshot *source.Snapshot
	if stream {
		if p.config.SegmentDuration <= 0 || len(p.config.FFmpegArgs) > 0 || item.Snapshot == nil {
			return stagedFile{}, &source.StreamUnavailableError{Reason: "streaming requires a seekable remote input and stream-copy segmentation"}
		}
		var err error
		snapshot, err = item.Snapshot(ctx)
		if err != nil {
			return stagedFile{}, err
		}
		item.Size = snapshot.Size
		mode = InputStream
	} else {
		mode = InputStage
	}
	reservation := p.config
	reservation.InputMode = mode
	lease, required, available, err := p.staging.reserve(ctx, item, reservation)
	if err != nil {
		return stagedFile{}, withFailure(FailureCodeStagingCapacity, FailureMessageStagingCapacity, true, err)
	}
	p.logger.Debug("input preflight", "input", safeURI(item.URI), "input_mode", mode,
		"estimated_required_bytes", required, "available_bytes", available)
	if stream {
		bridge, err := source.NewBridge(ctx, snapshot, p.config.Retries, p.observability)
		if err != nil {
			lease.release()
			return stagedFile{}, err
		}
		resource := snapshot.Resource
		if p.config.SourceID != "" {
			resource = "source-id:" + p.config.SourceID
		}
		revision := identityFingerprint("input-revision/v1", resource, snapshot.Revision, strconv.FormatInt(snapshot.Size, 10))
		p.logger.Info("streaming remote input", "input", safeURI(item.URI))
		return stagedFile{path: bridge.URL, size: snapshot.Size, revision: revision, bridge: bridge,
			lease: lease, cleanup: func() { bridge.Close(); lease.release() }}, nil
	}
	staged, err := stage(ctx, item, p.config.TempDirectory, p.config.Retries, lease, p.observability)
	if err != nil {
		lease.release()
		return stagedFile{}, err
	}
	cleanup := staged.cleanup
	staged.cleanup = func() { cleanup(); lease.release() }
	p.observability.Staged(staged.size)
	return staged, nil
}

func (p *Pipeline) probePreparedInput(ctx context.Context, input stagedFile) (media.Probe, error) {
	if input.bridge == nil {
		return p.probeInput(ctx, input.path)
	}
	release, err := p.acquireProbe(ctx)
	if err != nil {
		return media.Probe{}, err
	}
	defer release()
	probe, err := p.prober.Probe(ctx, input.path)
	if err != nil {
		return media.Probe{}, err
	}
	for index := range probe.Streams {
		stream := &probe.Streams[index]
		if stream.CodecType != "video" || stream.Disposition.AttachedPicture != 0 {
			continue
		}
		stream.Cadence = media.CadenceUnknown
		if p.config.DryRunMode == DryRunFast {
			_, _, average := media.ParseRate(stream.AverageFrameRate)
			_, _, real := media.ParseRate(stream.RealFrameRate)
			if average || real {
				stream.Cadence = media.CadenceUnexamined
			}
		}
	}
	if p.config.DryRunMode == DryRunFast {
		p.logger.Warn("streaming fast dry run leaves cadence and output metadata unverified; use --dry-run=exact for validation")
	}
	return probe, nil
}

func (s stagedFile) contentType(ctx context.Context) (string, error) {
	if s.bridge == nil {
		return media.DetectContentType(s.path)
	}
	// Reading through the bridge gives the sniff the same retry and failure
	// accounting as every tool read.
	reader := s.bridge.Reader(ctx)
	defer reader.Close()
	return media.DetectReaderContentType(reader)
}

// classifyPrepareFailure keeps a streaming refusal distinguishable from a
// failed transfer: auto mode stages instead, and explicit stream mode reports
// a stable code whose remedy is a different input mode.
func classifyPrepareFailure(err error) error {
	var unavailable *source.StreamUnavailableError
	if errors.As(err, &unavailable) {
		return withFailure(FailureCodeStreamUnavailable, FailureMessageStreamUnavailable, true, err)
	}
	return withFailure(FailureCodeSourceTransferFailed, FailureMessageSourceTransferFailed, true, err)
}
