package ingest

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

// anchorOnKeyFrames moves the offsets a render is anchored on from a video
// stream's first packet to its first key frame. FFmpeg's stream copy drops
// the frames before the first key frame, so a capture that starts mid-GOP
// begins its rendered video up to a GOP later than the probe's start_time;
// anchoring on start_time would place every Segment early, and under
// independent storage would slide the video Flow against the audio Flow.
// Offsets only ever move later.
//
// Each stream is looked up once. MP4 output is placed from this anchor, so a
// stream with no key frame within the search limit is refused. An MPEG-TS
// Object carries the Flow clock and is placed by it, so that render goes on
// from the stream's first packet.
func (p *Pipeline) anchorOnKeyFrames(ctx context.Context, path, muxer string, probe media.Probe, info *media.FlowInfo) error {
	release, err := p.acquireProbe(ctx)
	if err != nil {
		return err
	}
	defer release()
	type anchor struct {
		offset int64
		found  bool
	}
	anchors := make(map[int]anchor)
	keyFrameOffset := func(streamIndex int) (int64, bool, error) {
		if known, looked := anchors[streamIndex]; looked {
			return known.offset, known.found, nil
		}
		position := slices.IndexFunc(probe.Streams, func(stream media.Stream) bool { return stream.Index == streamIndex })
		if position < 0 {
			return 0, false, fmt.Errorf("stream %d was not probed", streamIndex)
		}
		at, found, err := p.prober.ProbeFirstKeyFrame(ctx, path, probe.Streams[position])
		if err != nil {
			return 0, false, fmt.Errorf("find the first key frame of stream %d: %w", streamIndex, err)
		}
		if !found {
			if muxer != "mpegts" {
				return 0, false, withFailure(FailureCodeMediaUnsupported, FailureMessageMediaNoKeyFrame, true,
					fmt.Errorf("video stream %d has no key frame in its first %s", streamIndex, media.KeyFrameSearchLimit))
			}
			p.logger.Warn("no key frame found at the start of the input; MPEG-TS Segments are placed by their own clock",
				"stream_index", streamIndex, "searched", media.KeyFrameSearchLimit.String())
		}
		var offset int64
		if found {
			if offset, err = media.PresentationOffset(probe, info.Start, at); err != nil {
				return 0, false, fmt.Errorf("place the first key frame of stream %d: %w", streamIndex, err)
			}
		}
		anchors[streamIndex] = anchor{offset: offset, found: found}
		return offset, found, nil
	}
	if reference, isVideo := media.ReferenceVideoStream(probe); isVideo {
		offset, found, err := keyFrameOffset(reference)
		if err != nil {
			return err
		}
		if found {
			info.ReferenceOffset = max(info.ReferenceOffset, offset)
		}
	}
	for index := range info.Collected {
		essence := &info.Collected[index]
		if format, _ := essence.Flow["format"].(string); format != "urn:x-nmos:format:video" {
			continue
		}
		offset, found, err := keyFrameOffset(essence.StreamIndex)
		if err != nil {
			return err
		}
		if found {
			essence.Offset = max(essence.Offset, offset)
		}
	}
	return nil
}

// openGOPCut classifies a refusal to cut MP4 Segments on an open GOP. An MP4
// Object begins presenting at the key frame it was cut on, so the leading
// pictures stored after that frame are not shown, and the Segment before it
// ends where they would have begun.
func openGOPCut(err error) error {
	return withFailure(FailureCodeMediaUnsupported, FailureMessageMediaOpenGOP, true, err)
}

// refuseUnsafeCuts stops a staged input before anything is written when its
// Segments would be cut where pictures are lost. The input is cut as the
// render will cut it, and each Object is held to the check its placement
// makes, so a cut is refused only where the segmenter puts one.
//
// An input is cut this way only when it could fail the check. MP4 output of
// a stream with open GOPs could, and each video stream the render cuts is
// read through for them: a muxed render is cut on its reference stream alone,
// and under independent storage every video essence is cut on its own key
// frames. Leading pictures need a reordering decoder, so a stream without one
// is not read. A muxed render of several video streams could in either
// container, because only the reference stream is cut on its key frames.
//
// Other renders are left to the check on each Object as it is placed. A
// streamed input cannot be read twice, custom FFmpeg arguments may re-encode
// the video, and a fast dry run does not render. A streamed multiplex of
// several video streams is the exception: the Object before a refused cut
// already holds the other stream's key frame without the pictures that lead
// up to it, so no prefix of it can be kept and the input has to be staged.
func (p *Pipeline) refuseUnsafeCuts(ctx context.Context, staged stagedFile, muxer string, probe media.Probe, flowInfo media.FlowInfo) error {
	if p.config.SegmentDuration <= 0 || p.config.DryRunMode == DryRunFast || len(p.config.FFmpegArgs) > 0 {
		return nil
	}
	independent := p.config.EssenceStorage == media.EssenceStorageIndependent && len(flowInfo.Collected) > 0
	reference, _ := media.ReferenceVideoStream(probe)
	var videos []media.Stream
	for _, stream := range probe.Streams {
		if stream.CodecType == "video" && stream.Disposition.AttachedPicture == 0 {
			videos = append(videos, stream)
		}
	}
	if !independent && len(videos) > 1 {
		if staged.bridge != nil {
			return classifyPrepareFailure(&source.StreamUnavailableError{
				Reason: "a multiplex of several video streams is cut only after every cut has been checked on a staged input"})
		}
		return p.refuseUnsafeCutsOf(ctx, staged, flowInfo, media.AllStreams)
	}
	if muxer != "mp4" || staged.bridge != nil {
		return nil
	}
	for _, stream := range videos {
		if stream.HasBFrames == 0 || (!independent && stream.Index != reference) {
			continue
		}
		open, err := p.probeLeadingPictures(ctx, staged.path, stream.Index)
		if err != nil {
			return fmt.Errorf("look for open GOPs in stream %d: %w", stream.Index, err)
		}
		if !open {
			continue
		}
		rendered := media.AllStreams
		if independent {
			rendered = stream.Index
		}
		if err := p.refuseUnsafeCutsOf(ctx, staged, flowInfo, rendered); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) probeLeadingPictures(ctx context.Context, path string, streamIndex int) (bool, error) {
	release, err := p.acquireProbe(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return p.prober.ProbeLeadingPictures(ctx, path, streamIndex)
}

// refuseUnsafeCutsOf renders one output of a staged input as the ingest will
// and holds each Object to the check its placement makes. Each Object is
// removed once read; nothing is kept or written to the store.
func (p *Pipeline) refuseUnsafeCutsOf(ctx context.Context, staged stagedFile, flowInfo media.FlowInfo, streamIndex int) error {
	var cursor timelineCursor
	cleanup, err := p.renderSegmentsTo(ctx, staged, flowInfo, []int{streamIndex}, nil, p.rollingStagingWindow(staged),
		func(record media.SegmentRecord) error {
			defer os.Remove(record.Path)
			if cursor.placed {
				release, err := p.acquireProbe(ctx)
				if err != nil {
					return err
				}
				defer release()
				object, err := p.probeObject(ctx, record.Path)
				if err != nil {
					return err
				}
				hiddenLead, midGOP := media.VideoCut(object)
				if err := cursor.refuseUnsafeCut(record, hiddenLead, midGOP); err != nil {
					return err
				}
			}
			cursor.placed = true
			return nil
		})
	cleanup()
	return withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaPrepareFailed, true, err)
}
