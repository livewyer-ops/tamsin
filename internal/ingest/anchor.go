package ingest

import (
	"context"
	"fmt"

	"github.com/livewyer-ops/tamsin/internal/media"
)

// keyFrameProber finds where stream copy of a video stream begins. The
// real prober implements it; test fakes need not.
type keyFrameProber interface {
	ProbeFirstKeyFrame(ctx context.Context, filename string, streamIndex int) (int64, bool, error)
}

// anchorOnKeyFrames moves the offsets a render is anchored on from a video
// stream's first packet to its first key frame. FFmpeg's stream copy drops
// the frames before the first key frame, so a capture that starts mid-GOP
// begins its rendered video up to a GOP later than the probe's start_time;
// anchoring on start_time would place every Segment early, and under
// independent storage would slide the video Flow against the audio Flow.
// Offsets only ever move later.
func (p *Pipeline) anchorOnKeyFrames(ctx context.Context, path string, probe media.Probe, info *media.FlowInfo) error {
	prober, ok := p.prober.(keyFrameProber)
	if !ok {
		return nil
	}
	release, err := p.acquireProbe(ctx)
	if err != nil {
		return err
	}
	defer release()
	keyFrameOffset := func(streamIndex int) (int64, bool, error) {
		at, found, err := prober.ProbeFirstKeyFrame(ctx, path, streamIndex)
		if err != nil {
			return 0, false, fmt.Errorf("find the first key frame of stream %d: %w", streamIndex, err)
		}
		if !found {
			p.logger.Warn("no key frame found at the start of the input; the first Segment is anchored on the stream's first packet",
				"stream_index", streamIndex)
			return 0, false, nil
		}
		return at - info.Start, true, nil
	}
	probed := make(map[int]int64)
	if reference, isVideo := media.ReferenceVideoStream(probe); isVideo {
		offset, found, err := keyFrameOffset(reference)
		if err != nil {
			return err
		}
		if found {
			probed[reference] = offset
			info.ReferenceOffset = max(info.ReferenceOffset, offset)
		}
	}
	for index := range info.Collected {
		essence := &info.Collected[index]
		if format, _ := essence.Flow["format"].(string); format != "urn:x-nmos:format:video" {
			continue
		}
		offset, found := probed[essence.StreamIndex]
		if !found {
			var err error
			if offset, found, err = keyFrameOffset(essence.StreamIndex); err != nil {
				return err
			}
		}
		if found {
			essence.Offset = max(essence.Offset, offset)
		}
	}
	return nil
}
