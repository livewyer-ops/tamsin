package ingest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/livewyer-ops/tamsin/internal/tamsschema"
)

const (
	rollingCommitBytes   = int64(64 << 20)
	rollingCommitObjects = 64
)

type rollingObjectPreparer struct {
	pipeline      *Pipeline
	flowID        string
	streamIndex   int
	flowPosition  int64
	anchorStart   int64
	manifestStart int64
	anchored      bool
	rates         *media.SegmentBitRateAccumulator
}

func newRollingObjectPreparer(p *Pipeline, flowID string, streamIndex int, start int64) *rollingObjectPreparer {
	return &rollingObjectPreparer{
		pipeline: p, flowID: flowID, streamIndex: streamIndex,
		flowPosition: start, rates: media.NewSegmentBitRateAccumulator(p.config.SegmentDuration),
	}
}

func (p *rollingObjectPreparer) prepare(ctx context.Context, record media.SegmentRecord) (preparedObject, error) {
	if record.StreamIndex != p.streamIndex {
		return preparedObject{}, fmt.Errorf("renderer emitted stream %d for rolling stream %d", record.StreamIndex, p.streamIndex)
	}
	release, err := p.pipeline.acquireProbe(ctx)
	if err != nil {
		return preparedObject{}, err
	}
	defer release()

	size, checksum, err := digestFile(ctx, record.Path)
	if err != nil {
		return preparedObject{}, err
	}
	var objectStart, duration int64
	if record.Timed {
		if record.End <= record.Start {
			return preparedObject{}, errors.New("renderer emitted a non-positive segment interval")
		}
		if !p.anchored {
			probe, probeErr := p.pipeline.prober.Probe(ctx, record.Path)
			if probeErr != nil {
				return preparedObject{}, probeErr
			}
			p.anchorStart, _, err = media.ProbeTiming(probe)
			if err != nil {
				return preparedObject{}, err
			}
			p.manifestStart = record.Start
			p.anchored = true
		}
		manifestOffset, offsetErr := media.TimestampOffset(record.Start, p.manifestStart)
		if offsetErr != nil {
			return preparedObject{}, fmt.Errorf("calculate segment manifest offset: %w", offsetErr)
		}
		objectStart, err = media.TimestampShift(p.anchorStart, manifestOffset)
		if err != nil {
			return preparedObject{}, fmt.Errorf("calculate segment object start: %w", err)
		}
		duration, err = media.TimestampOffset(record.End, record.Start)
		if err != nil {
			return preparedObject{}, fmt.Errorf("calculate segment duration: %w", err)
		}
	} else {
		probe, probeErr := p.pipeline.prober.Probe(ctx, record.Path)
		if probeErr != nil {
			return preparedObject{}, probeErr
		}
		if objectStart, duration, err = media.ProbeTiming(probe); err != nil {
			return preparedObject{}, err
		}
	}

	timerange, err := media.TimeRange(p.flowPosition, duration)
	if err != nil {
		return preparedObject{}, err
	}
	objectTimerange, err := media.TimeRange(objectStart, duration)
	if err != nil {
		return preparedObject{}, err
	}
	offset, err := media.TimestampOffset(p.flowPosition, objectStart)
	if err != nil {
		return preparedObject{}, err
	}
	object := preparedObject{
		id: namedID("object", p.flowID, checksum, timerange), path: record.Path,
		size: size, sha256: checksum, start: p.flowPosition, duration: duration,
		timerange: timerange, objectTimerange: objectTimerange,
	}
	if offset != 0 {
		object.tsOffset = media.Timestamp(offset)
	}
	p.flowPosition, err = media.TimestampShift(p.flowPosition, duration)
	if err != nil {
		return preparedObject{}, err
	}
	p.rates.Add(media.SegmentMeasurement{Bytes: size, Duration: duration})
	return object, nil
}

type rollingFlowState struct {
	flowID      string
	resultIndex int
	flow        tams.Flow
	preparer    *rollingObjectPreparer
	pending     []preparedObject
	pendingSize int64
	throughput  float64
}

type rollingExecution struct {
	pipeline     *Pipeline
	ctx          context.Context
	result       *Result
	storageID    string
	byStream     map[int]*rollingFlowState
	ordered      []*rollingFlowState
	active       *rollingFlowState
	commitBytes  int64
	totalObjects int
	totalBytes   int64
	pendingBytes int64
	pendingCount int
}

func newRollingExecution(p *Pipeline, ctx context.Context, result *Result, storageID string,
	window *media.SegmentStagingWindow, states ...*rollingFlowState) *rollingExecution {
	commitBytes := rollingCommitBytes
	if window != nil {
		commitBytes = min(commitBytes, max(window.LowBytes, 1))
	}
	execution := &rollingExecution{
		pipeline: p, ctx: ctx, result: result, storageID: storageID,
		byStream: make(map[int]*rollingFlowState, len(states)), ordered: states,
		commitBytes: commitBytes,
	}
	for _, state := range states {
		execution.byStream[state.preparer.streamIndex] = state
	}
	return execution
}

func (e *rollingExecution) accept(record media.SegmentRecord) error {
	state := e.byStream[record.StreamIndex]
	if e.active != nil {
		state = e.active
	}
	if state == nil {
		return fmt.Errorf("renderer emitted unplanned stream %d", record.StreamIndex)
	}
	object, err := state.preparer.prepare(e.ctx, record)
	if err != nil {
		return err
	}
	state.pending = append(state.pending, object)
	state.pendingSize += object.size
	e.totalObjects++
	e.totalBytes += object.size
	e.pendingBytes += object.size
	e.pendingCount++
	if record.FlushStaging || e.pendingBytes >= e.commitBytes || e.pendingCount >= rollingCommitObjects {
		return e.flushPending()
	}
	return nil
}

func (e *rollingExecution) flushPending() error {
	for _, state := range e.ordered {
		if err := e.flush(state); err != nil {
			return err
		}
	}
	return nil
}

func (e *rollingExecution) flush(state *rollingFlowState) error {
	if len(state.pending) == 0 {
		return nil
	}
	e.publishTotals(false)
	objects := state.pending
	pendingSize := state.pendingSize
	objectResults := make([]ObjectResult, len(objects))
	objectIDs := make(map[string]struct{}, len(objects))
	for index, object := range objects {
		objectResults[index] = e.pipeline.newObjectResult(object)
		objectIDs[object.id] = struct{}{}
	}

	var operationErr error
	if e.pipeline.config.DryRunMode != DryRunOff {
		operationErr = e.pipeline.observeObjectBatch(e.ctx, state.flowID, objectResults, objectIDs)
	} else {
		operationErr = e.pipeline.registerRollingChunk(
			e.ctx, state.flowID, objects, objectResults, e.storageID, &state.throughput)
		if operationErr != nil {
			for index := range objectResults {
				if objectResults[index].Status == ObjectStatusPlanned {
					objectResults[index].Disposition = ObjectDispositionUnattempted
				}
			}
			operationErr = errors.Join(operationErr,
				e.pipeline.observeObjectBatch(e.ctx, state.flowID, objectResults, objectIDs))
		}
	}

	flowResult := &e.result.Flows[state.resultIndex]
	for _, objectResult := range objectResults {
		AccumulateObjectSummary(&flowResult.ObjectSummary, objectResult)
		if e.pipeline.config.RetainObjectResults || actionRequiredObject(objectResult) {
			flowResult.Objects = append(flowResult.Objects, objectResult)
		}
	}
	state.pending = state.pending[:0]
	state.pendingSize = 0
	e.pendingBytes = max(e.pendingBytes-pendingSize, 0)
	e.pendingCount = max(e.pendingCount-len(objects), 0)
	if operationErr != nil {
		return operationErr
	}
	for _, object := range objects {
		if err := os.Remove(object.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove committed rolling segment %q: %w", object.path, err)
		}
	}
	return nil
}

func actionRequiredObject(object ObjectResult) bool {
	return object.Status == ObjectStatusStranded || object.Status == ObjectStatusRetractionIndeterminate ||
		object.Disposition == ObjectDispositionStranded ||
		object.Disposition == ObjectDispositionRegistrationIndeterminate
}

func (e *rollingExecution) finish() error {
	var failures []error
	for _, state := range e.ordered {
		if err := e.flush(state); err != nil {
			failures = append(failures, err)
		}
	}
	e.publishTotals(true)
	return errors.Join(failures...)
}

func (e *rollingExecution) publishTotals(final bool) {
	tracker := progress.FromContext(e.ctx)
	if tracker == nil || e.pipeline.config.DryRunMode != DryRunOff {
		return
	}
	set := func(phase progress.Phase) {
		if err := tracker.SetTotals(phase, e.totalObjects, e.totalBytes, final); err != nil {
			e.pipeline.logger.Error("invalid rolling progress totals", "phase", phase,
				"objects", e.totalObjects, "bytes", e.totalBytes, "final", final, "error", err)
		}
	}
	set(progress.PhaseStore)
	if e.pipeline.config.VerificationMode != VerificationNone {
		set(progress.PhaseVerify)
	}
}

func (p *Pipeline) rollingStagingWindow(staged stagedFile) *media.SegmentStagingWindow {
	high := min(staged.lease.reservedHeadroom(), rollingOutputWindowBytes)
	if high <= 0 {
		high = 1
	}
	return &media.SegmentStagingWindow{HighBytes: high, LowBytes: high / 2}
}

func (p *Pipeline) beginRollingFlowPlan(ctx context.Context, inputURI string, graph flowGraph,
	results []FlowResult) ([]plannedFlowWrite, error) {
	state := flowExecutionState{graph: graph, inputURI: inputURI, results: results}
	if err := planFlowWrites(ctx, p, &state); err != nil {
		return nil, err
	}
	// A new Flow plan may point directly at graph member maps. Keep the value
	// actually written here separate from bitrate fields discovered later, or
	// an in-memory client can make an unwritten mutation look persisted.
	for index := range state.planned {
		state.planned[index].effective = maps.Clone(state.planned[index].effective)
		state.planned[index].request = maps.Clone(state.planned[index].request)
	}
	if err := writeFlowGraph(ctx, p, &state); err != nil {
		return nil, err
	}
	return state.planned, nil
}

func (p *Pipeline) finishRollingFlowMetadata(ctx context.Context, graph flowGraph, planned []plannedFlowWrite) error {
	if p.config.DryRunMode != DryRunOff {
		return nil
	}
	byID := make(map[string]graphFlow, len(graph.flows))
	for _, member := range graph.flows {
		byID[member.id] = member
	}
	for index := range planned {
		member := byID[planned[index].member.id]
		planned[index].changed = false
		fields := []string{"avg_bit_rate", "max_bit_rate"}
		if planned[index].member.profileID != "" {
			fields = []string{"max_bit_rate"}
		}
		for _, field := range fields {
			if value, present := member.flow[field]; present {
				if planned[index].effective[field] != value {
					planned[index].effective[field] = value
					planned[index].changed = true
				}
			}
		}
		planned[index].request = flowPutProjection(planned[index].effective, planned[index].member.profileID)
		if err := tamsschema.ValidateFlowGet(p.apiVersion, planned[index].effective); err != nil {
			return withFailure(FailureCodeFlowPlanFailed, FailureMessageFlowPlanFailed, true,
				fmt.Errorf("final rolling Flow metadata for %s is invalid at %w", member.id, err))
		}
		if err := tamsschema.ValidateFlowPut(p.apiVersion, planned[index].request); err != nil {
			return withFailure(FailureCodeFlowPlanFailed, FailureMessageFlowPlanFailed, true,
				fmt.Errorf("final rolling Flow PUT metadata for %s is invalid at %w", member.id, err))
		}
	}
	if err := validateFlowGraph(graph, planned); err != nil {
		return withFailure(FailureCodeFlowPlanFailed, FailureMessageFlowPlanFailed, true,
			fmt.Errorf("final rolling Flow graph is invalid: %w", err))
	}
	for _, plan := range planned {
		if !plan.changed {
			continue
		}
		if err := p.writeFlow(ctx, plan); err != nil {
			return withFailure(FailureCodeFlowWriteFailed, FailureMessageFlowWriteFailed, true,
				fmt.Errorf("write final rolling Flow metadata: %w", err))
		}
	}
	return nil
}

func applyRollingBitRates(flow tams.Flow, rates *media.SegmentBitRateAccumulator) {
	average, peak, ok := rates.Result()
	if !ok {
		return
	}
	flow["avg_bit_rate"] = average
	flow["max_bit_rate"] = peak
}

func rollingResultStatus(result Result) ResultStatus {
	total, resumed := 0, 0
	for _, flow := range result.Flows {
		total += flow.ObjectSummary.Total
		resumed += flow.ObjectSummary.Resumed
	}
	if total > 0 && resumed == total {
		return ResultStatusResumed
	}
	return ResultStatusIngested
}

func (p *Pipeline) ingestMuxedRolling(ctx context.Context, itemLabel string, staged stagedFile,
	flow tams.Flow, flowInfo media.FlowInfo, flowID, sourceID, storageID string,
	collected []collectedFlow, ffmpegVersion, toolchainFingerprint string) (result Result, returnErr error) {
	result = Result{
		Input: itemLabel, Profile: p.config.Profile, ProfileVersion: p.config.ProfileVersion,
		FFmpegVersion: ffmpegVersion, MediaToolchain: toolchainFingerprint,
		RootFlowID: flowID, Bytes: staged.size, SHA256: staged.sha256,
		Status: ResultStatusPlanned, Verification: p.initialVerificationStatus(),
		Flows: make([]FlowResult, 0, len(collected)+1),
	}
	graph := flowGraph{flows: make([]graphFlow, 0, len(collected)+1), storage: p.config.EssenceStorage}
	for _, child := range collected {
		result.Flows = append(result.Flows, FlowResult{
			FlowID: child.id, SourceID: child.sourceID, Role: child.role,
			Disposition: FlowPlanned, Kind: FlowKindEssence,
		})
		graph.flows = append(graph.flows, graphFlow{
			id: child.id, role: child.role, flow: child.flow, containerMapping: child.containerMapping,
		})
	}
	rootKind, rootRole, parentRole := FlowKindEssence, flowPlanRole(FlowKindEssence, "single", stringField(flow, "format")), "single"
	if len(collected) > 0 {
		rootKind, rootRole, parentRole = FlowKindMuxed, "", "multi"
		graph.collectorID = flowID
	}
	rootIndex := len(result.Flows)
	result.Flows = append(result.Flows, FlowResult{
		FlowID: flowID, SourceID: sourceID, Role: rootRole, Disposition: FlowPlanned, Kind: rootKind,
	})
	graph.flows = append(graph.flows, graphFlow{id: flowID, role: parentRole, flow: flow, ownsMedia: true})
	if !staged.owned {
		if err := ensureStagedInputUnchanged(ctx, staged); err != nil {
			return result, withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true, err)
		}
	}
	planned, err := p.beginRollingFlowPlan(ctx, itemLabel, graph, result.Flows)
	if err != nil {
		return result, err
	}
	defer p.finishRollingFlowStatus(ctx, graph, &returnErr)

	state := &rollingFlowState{
		flowID: flowID, resultIndex: rootIndex, flow: flow,
		preparer: newRollingObjectPreparer(p, flowID, media.AllStreams, p.config.Start),
	}
	window := p.rollingStagingWindow(staged)
	execution := newRollingExecution(p, ctx, &result, storageID, window, state)
	cleanup, renderErr := p.renderSegmentsTo(ctx, staged, flowInfo, []int{media.AllStreams},
		p.config.FFmpegArgs, window, execution.accept)
	finishErr := execution.finish()
	cleanup()
	if execution.totalObjects == 0 && renderErr == nil && finishErr == nil {
		renderErr = errors.New("media renderer produced no objects")
	}
	if renderErr != nil || finishErr != nil {
		return result, rollingExecutionError(renderErr, finishErr)
	}
	if !staged.owned {
		if err := ensureStagedInputUnchanged(ctx, staged); err != nil {
			return result, withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true, err)
		}
	}
	applyRollingBitRates(flow, state.preparer.rates)
	if err := p.finishRollingFlowMetadata(ctx, graph, planned); err != nil {
		return result, err
	}
	if p.config.DryRunMode != DryRunOff {
		return result, nil
	}
	result.Status = rollingResultStatus(result)
	return result, nil
}

func rollingExecutionError(renderErr, finishErr error) error {
	if renderErr == nil && finishErr == nil {
		return nil
	}
	joined := errors.Join(renderErr, finishErr)
	var classified *classifiedFailure
	if errors.As(joined, &classified) {
		return joined
	}
	if finishErr != nil {
		return withFailure(FailureCodeTAMSRegistrationFailed, FailureMessageTAMSRegistrationFailed, true, joined)
	}
	return withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaPrepareFailed, true, joined)
}

func (p *Pipeline) ingestIndependentRolling(ctx context.Context, itemLabel string, staged stagedFile,
	collector tams.Flow, flowInfo media.FlowInfo, collectorID, collectorSourceID, storageID string) (result Result, returnErr error) {
	result = Result{
		Input: itemLabel, Profile: p.config.Profile, ProfileVersion: p.config.ProfileVersion,
		RootFlowID: collectorID, Bytes: staged.size, SHA256: staged.sha256,
		Status: ResultStatusPlanned, Verification: p.initialVerificationStatus(),
		Flows: []FlowResult{{
			FlowID: collectorID, SourceID: collectorSourceID, Disposition: FlowPlanned, Kind: FlowKindCollection,
		}},
	}
	graph := flowGraph{
		flows:       make([]graphFlow, 0, len(flowInfo.Collected)+1),
		collectorID: collectorID, storage: media.EssenceStorageIndependent,
	}
	collectionItems := make([]map[string]any, 0, len(flowInfo.Collected))
	states := make([]*rollingFlowState, 0, len(flowInfo.Collected))
	for index, essence := range flowInfo.Collected {
		position := fmt.Sprint(index)
		flowID := generatedChildFlowID(collectorID, "essence", position)
		sourceID := sourceIdentity(staged.sha256, "essence", position)
		flow := essence.Flow
		mergeFlow(flow, p.config.FlowMetadata)
		flow["id"] = flowID
		flow["source_id"] = sourceID
		flow["segment_duration"] = durationRational(p.config.SegmentDuration)
		if container := p.config.SegmentFormat.ContainerMIME(); container != "" {
			flow["container"] = container
		}
		resultIndex := len(result.Flows)
		result.Flows = append(result.Flows, FlowResult{
			FlowID: flowID, SourceID: sourceID, Role: essence.Role,
			Disposition: FlowPlanned, Kind: FlowKindEssence,
		})
		graph.flows = append(graph.flows, graphFlow{
			id: flowID, role: essence.Role, flow: flow, ownsMedia: true,
		})
		collectionItems = append(collectionItems, map[string]any{"id": flowID, "role": essence.Role})
		states = append(states, &rollingFlowState{
			flowID: flowID, resultIndex: resultIndex, flow: flow,
			preparer: newRollingObjectPreparer(
				p, flowID, essence.StreamIndex, p.config.Start+essence.Offset),
		})
	}
	mergeFlow(collector, p.config.FlowMetadata)
	collector["id"] = collectorID
	collector["source_id"] = collectorSourceID
	collector["flow_collection"] = collectionItems
	delete(collector, "container")
	graph.flows = append(graph.flows, graphFlow{id: collectorID, role: "multi", flow: collector})
	if !staged.owned {
		if err := ensureStagedInputUnchanged(ctx, staged); err != nil {
			return result, withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true, err)
		}
	}
	planned, err := p.beginRollingFlowPlan(ctx, itemLabel, graph, result.Flows)
	if err != nil {
		return result, err
	}
	defer p.finishRollingFlowStatus(ctx, graph, &returnErr)

	window := p.rollingStagingWindow(staged)
	execution := newRollingExecution(p, ctx, &result, storageID, window, states...)
	var renderErr error
	if canRenderRollingMultiOutput(states, p.config.FFmpegArgs) {
		streamIndices := make([]int, len(states))
		for index, state := range states {
			streamIndices[index] = state.preparer.streamIndex
		}
		cleanup, err := p.renderSegmentsTo(ctx, staged, flowInfo, streamIndices, nil,
			window, execution.accept)
		finishErr := execution.finish()
		cleanup()
		renderErr = rollingExecutionError(err, finishErr)
	} else {
		for _, state := range states {
			execution.active = state
			cleanup, err := p.renderSegmentsTo(ctx, staged, flowInfo, []int{state.preparer.streamIndex},
				p.config.FFmpegArgs, window, execution.accept)
			flushErr := execution.flush(state)
			execution.active = nil
			cleanup()
			if err != nil || flushErr != nil {
				renderErr = rollingExecutionError(err, flushErr)
				break
			}
		}
		if finishErr := execution.finish(); finishErr != nil {
			renderErr = errors.Join(renderErr, rollingExecutionError(nil, finishErr))
		}
	}
	if renderErr == nil {
		for _, state := range states {
			if result.Flows[state.resultIndex].ObjectSummary.Total == 0 {
				renderErr = withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaStreamPrepareFailed, true,
					fmt.Errorf("renderer produced no objects for essence %s", result.Flows[state.resultIndex].Role))
				break
			}
		}
	}
	if renderErr != nil {
		return result, renderErr
	}
	if !staged.owned {
		if err := ensureStagedInputUnchanged(ctx, staged); err != nil {
			return result, withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true, err)
		}
	}
	for _, state := range states {
		applyRollingBitRates(state.flow, state.preparer.rates)
	}
	if err := p.finishRollingFlowMetadata(ctx, graph, planned); err != nil {
		return result, err
	}
	if p.config.DryRunMode != DryRunOff {
		return result, nil
	}
	result.Status = rollingResultStatus(result)
	return result, nil
}

func canRenderRollingMultiOutput(states []*rollingFlowState, additionalArgs []string) bool {
	if len(states) < multiOutputEssenceThreshold || len(additionalArgs) > 0 {
		return false
	}
	seen := make(map[int]struct{}, len(states))
	for _, state := range states {
		streamIndex := state.preparer.streamIndex
		if _, duplicate := seen[streamIndex]; duplicate {
			return false
		}
		seen[streamIndex] = struct{}{}
	}
	return true
}
