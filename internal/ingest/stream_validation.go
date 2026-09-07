package ingest

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

type streamValidation struct {
	seen         map[string]bool
	ready        map[string]bool
	timelines    map[[2]int]*media.CadenceTimeline
	contentTypes map[int]string
}

func (e *rollingExecution) begin(graph flowGraph, streamed bool) error {
	e.graph = graph
	if streamed {
		e.stream = &streamValidation{seen: make(map[string]bool), ready: make(map[string]bool),
			timelines: make(map[[2]int]*media.CadenceTimeline), contentTypes: make(map[int]string)}
		return nil
	}
	var err error
	e.planned, err = e.pipeline.beginRollingFlowPlan(e.ctx, graph, e.result.Flows)
	return err
}

func (e *rollingExecution) finishStatus(err *error) {
	if e.planned != nil {
		e.pipeline.finishRollingFlowStatus(e.ctx, e.graph, err)
	}
}

func (e *rollingExecution) validateSegment(record media.SegmentRecord, state *rollingFlowState) (*media.Probe, bool, error) {
	if !record.Timed {
		return nil, false, errors.New("streamed segment has no input timeline")
	}
	probe, err := e.pipeline.probeStreamSegment(e.ctx, record.Path)
	if err != nil {
		return nil, false, err
	}
	var reference media.Stream
	for _, stream := range probe.Streams {
		if stream.CodecType == "video" && stream.Disposition.AttachedPicture == 0 {
			reference = stream
			break
		}
	}
	cadenceReady := true
	for index := range probe.Streams {
		stream := &probe.Streams[index]
		if stream.CodecType != "video" || stream.Disposition.AttachedPicture != 0 {
			continue
		}
		key := [2]int{record.StreamIndex, stream.Index}
		timeline := e.stream.timelines[key]
		if timeline == nil {
			timeline = &media.CadenceTimeline{}
			e.stream.timelines[key] = timeline
		}
		stream.Cadence, err = timeline.Observe(*stream, reference, record.End)
		if err != nil {
			return nil, false, err
		}
		if stream.Cadence == media.CadenceUnknown {
			cadenceReady = false
		}
	}
	if !cadenceReady && e.planned != nil {
		// Absent timestamp evidence is not a contradiction: the declared
		// cadence stands and every other fact is still compared below.
		e.pipeline.logger.Debug("segment lacks cadence evidence", "stream", record.StreamIndex)
	}
	// The output container is fixed for the whole render.
	contentType, cached := e.stream.contentTypes[record.StreamIndex]
	if !cached {
		contentType, err = media.DetectContentType(record.Path)
		if err != nil {
			return nil, false, err
		}
		e.stream.contentTypes[record.StreamIndex] = contentType
	}
	observed, info, err := media.BuildFlow(probe, media.Identity{}, contentType, e.graph.storage)
	if err != nil {
		return nil, false, err
	}
	members := []graphFlow{{id: state.flowID, flow: state.flow, ownsMedia: true}}
	candidates := []tams.Flow{observed}
	if record.StreamIndex == media.AllStreams {
		members = e.graph.flows
		candidates = make([]tams.Flow, 0, len(info.Collected)+1)
		for _, collected := range info.Collected {
			candidates = append(candidates, collected.Flow)
		}
		candidates = append(candidates, observed)
	} else if len(info.Collected) != 0 {
		return nil, false, errors.New("independent segment contains multiple essences")
	}
	if len(members) != len(candidates) {
		return nil, false, errors.New("segment stream count differs from the input Flow graph")
	}
	for index, member := range members {
		candidate := candidates[index]
		if !e.stream.seen[member.id] {
			if _, overridden := e.pipeline.config.FlowMetadata["essence_parameters"]; !overridden {
				if parameters, present := candidate["essence_parameters"]; present {
					member.flow["essence_parameters"] = parameters
				}
			}
		}
		expected, _ := member.flow["essence_parameters"].(map[string]any)
		parameters, _ := candidate["essence_parameters"].(map[string]any)
		if !e.stream.ready[member.id] && cadenceReady {
			if _, overridden := e.pipeline.config.FlowMetadata["essence_parameters"]; !overridden && stringField(candidate, "format") == "urn:x-nmos:format:video" {
				expected = maps.Clone(expected)
				delete(expected, "frame_rate")
				delete(expected, "vfr")
				for _, field := range []string{"frame_rate", "vfr"} {
					if value, present := parameters[field]; present {
						expected[field] = value
					}
				}
				member.flow["essence_parameters"] = expected
			}
		}
		if expected["vfr"] == true {
			// A variable-rate declaration permits fixed stretches. Never change
			// the declared metadata when later segments become variable.
			parameters = maps.Clone(parameters)
			delete(parameters, "frame_rate")
			parameters["vfr"] = true
			candidate["essence_parameters"] = parameters
		}
		declaredFlow := member.flow
		if !cadenceReady && stringField(candidate, "format") == "urn:x-nmos:format:video" {
			// Compare every other fact while accumulating enough timestamps.
			// Neither this provisional value nor its objects may be written yet.
			declaredFlow = maps.Clone(member.flow)
			expected = maps.Clone(expected)
			parameters = maps.Clone(parameters)
			for _, field := range []string{"frame_rate", "vfr"} {
				delete(expected, field)
				delete(parameters, field)
			}
			declaredFlow["essence_parameters"] = expected
			candidate["essence_parameters"] = parameters
		}
		for _, field := range []string{"format", "codec", "container", "essence_parameters"} {
			actual, actualPresent := candidate[field]
			declared, declaredPresent := declaredFlow[field]
			if _, overridden := e.pipeline.config.FlowMetadata[field]; overridden && !actualPresent {
				// The override supplies a value the media tools cannot derive;
				// a segment that shows a different value still contradicts it.
				continue
			}
			if mismatch := firstJSONValueMismatch("/"+field, actual, actualPresent, declared, declaredPresent); mismatch != nil {
				return nil, false, fmt.Errorf("segment contradicts Flow %s at %s", member.id, safeMetadataPointer(mismatch.path))
			}
		}
		e.stream.seen[member.id] = true
		// Readiness is monotonic: a later segment without cadence evidence
		// neither withdraws the declaration nor re-derives it.
		e.stream.ready[member.id] = e.stream.ready[member.id] || cadenceReady || stringField(candidate, "format") != "urn:x-nmos:format:video"
	}
	for _, member := range e.graph.flows {
		if stringField(member.flow, "format") == "urn:x-nmos:format:multi" && !member.ownsMedia {
			continue
		}
		if !e.stream.ready[member.id] {
			return &probe, false, nil
		}
	}
	return &probe, true, nil
}

func (e *rollingExecution) renderFailure(err error) error {
	var capacity *stagingCapacityError
	if e.stream != nil && !e.started && errors.As(err, &capacity) {
		return insufficientStreamEvidence()
	}
	return err
}

func (p *Pipeline) probeStreamSegment(ctx context.Context, path string) (media.Probe, error) {
	release, err := p.acquireProbe(ctx)
	if err != nil {
		return media.Probe{}, err
	}
	defer release()
	probe, err := p.prober.Probe(ctx, path)
	if err != nil {
		return media.Probe{}, err
	}
	if presentation, ok := p.prober.(media.PresentationProber); ok {
		if err := presentation.ProbePresentation(ctx, path, &probe); err != nil {
			return media.Probe{}, err
		}
	}
	return probe, nil
}

func insufficientStreamEvidence() error {
	return &source.StreamUnavailableError{Reason: "initial segments did not establish metadata for every essence within the output spool; staged preflight is required"}
}
