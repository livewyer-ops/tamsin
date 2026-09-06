package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/livewyer-ops/tamsin/internal/version"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var idNamespace = uuid.MustParse("d1f5657c-cee0-5ac4-a0a5-c78ba122fa39")

// The measured four-track crossover saves enough process startup and input
// reopening to justify the multi-output command, while the common video/audio
// pair stays on the simpler isolated-output path.
const multiOutputEssenceThreshold = 4

// rendererIdentityEpoch changes only when TAMSin deliberately changes the
// semantics of media it writes. Package rebuilds and FFmpeg patch releases are
// provenance, not a new ingest policy, and must not manufacture a new Flow.
const rendererIdentityEpoch = "2"

func New(config Config, client TAMSClient, prober media.Prober, segmenter media.Segmenter, logger *slog.Logger, reporter progress.Reporter) (*Pipeline, error) {
	if config.DryRunMode == "" {
		config.DryRunMode = DryRunOff
	}
	if err := config.DryRunMode.Validate(); err != nil {
		return nil, err
	}
	if config.VerificationMode == "" {
		config.VerificationMode = VerificationNone
	}
	if err := config.VerificationMode.Validate(); err != nil {
		return nil, err
	}
	if config.Profile == "" {
		config.Profile = ProfileCustom
	}
	if config.ProfileVersion == "" {
		if config.Profile == ProfileCustom {
			config.ProfileVersion = CustomProfileVersion
		} else if profile, err := namedProfile(config.Profile); err == nil {
			config.ProfileVersion = profile.Version
		}
	}
	if err := validateResolvedProfile(config); err != nil {
		return nil, err
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if config.Concurrency > 256 {
		return nil, errors.New("concurrency cannot exceed 256")
	}
	if config.Transfers <= 0 {
		config.Transfers = config.Concurrency
	}
	if config.Transfers > 256 {
		return nil, errors.New("transfers cannot exceed 256")
	}
	if config.ProbeConcurrency <= 0 {
		config.ProbeConcurrency = 2
	}
	if config.ProbeConcurrency > 256 {
		return nil, errors.New("probe concurrency cannot exceed 256")
	}
	if config.Retries < 0 || config.Retries > 20 {
		return nil, errors.New("retries must be between 0 and 20")
	}
	if config.StagingByteBudget < 0 {
		return nil, errors.New("staging byte budget cannot be negative")
	}
	if config.SegmentDuration < 0 {
		return nil, errors.New("segment duration cannot be negative")
	}
	if err := config.SegmentFormat.Validate(); err != nil {
		return nil, err
	}
	if err := config.EssenceStorage.Validate(); err != nil {
		return nil, err
	}
	if err := validateFlowMetadataOverrides(config.FlowMetadata); err != nil {
		return nil, err
	}
	profileAssignments, normalizedProfiles, err := parseFlowProfileAssignments(config.TAMSFlowProfiles)
	if err != nil {
		return nil, err
	}
	config.TAMSFlowProfiles = normalizedProfiles
	for _, identifier := range [...]struct {
		label string
		value string
	}{
		{label: "flow ID", value: config.FlowID},
		{label: "source ID", value: config.SourceID},
		{label: "storage ID", value: config.StorageID},
	} {
		if identifier.value == "" {
			continue
		}
		if _, err := uuid.Parse(identifier.value); err != nil {
			return nil, fmt.Errorf("%s must be a UUID: %w", identifier.label, err)
		}
	}
	if (config.DryRunMode == DryRunOff || len(profileAssignments) > 0) && client == nil {
		return nil, errors.New("TAMS client is required unless dry-run is enabled without a TAMS Flow Profile")
	}
	if prober == nil {
		return nil, errors.New("media prober is required")
	}
	if config.SegmentDuration > 0 && segmenter == nil {
		return nil, errors.New("media segmenter is required when segment duration is set")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	run := config.Observability
	if run == nil {
		run = observability.New(uuid.NewString(), logger)
	}
	if _, err := uuid.Parse(run.RunID()); err != nil {
		return nil, fmt.Errorf("observability run ID must be a UUID: %w", err)
	}
	logger = run.Logger()
	if reporter == nil {
		reporter = progress.Discard{}
	}
	return &Pipeline{
		config: config, runID: run.RunID(), client: client, prober: prober, segmenter: segmenter, logger: logger,
		observability: run,
		reporter:      reporter, transfers: make(chan struct{}, config.Transfers),
		probes: make(chan struct{}, config.ProbeConcurrency), mediaProcesses: semaphore.NewWeighted(2),
		rollingRenders:              make(chan struct{}, 1),
		graphLocks:                  make(map[string]*graphLock),
		apiVersion:                  tams.APIVersion{Major: tams.SpecMajor, Minor: tams.SpecMinor},
		profileAssignments:          profileAssignments,
		profileCache:                make(map[string]tams.Profile),
		flowStatuses:                make(map[string]string),
		registrationRecoveryTimeout: retractionTimeout,
		verificationRecoveryTimeout: retractionTimeout,
	}, nil
}

func (p *Pipeline) ResultContract() ResultContract {
	return ResultContract{
		SchemaVersion:  ResultSchemaVersion,
		ToolVersion:    version.Version,
		ToolCommit:     version.SourceCommit(),
		ToolBuildDate:  version.BuildDate(),
		ProfileVersion: p.config.ProfileVersion,
		RunID:          p.runID,
	}
}

func (p *Pipeline) newBatch(results []Result) BatchResult {
	contract := p.ResultContract()
	return BatchResult{
		SchemaVersion: contract.SchemaVersion, ToolVersion: contract.ToolVersion,
		ToolCommit: contract.ToolCommit, ToolBuildDate: contract.ToolBuildDate,
		ProfileVersion: contract.ProfileVersion, RunID: contract.RunID, Results: results,
	}
}

func (p *Pipeline) Run(ctx context.Context, items []source.Item) (BatchResult, error) {
	return p.RunObserved(ctx, items, nil)
}

// RunObserved runs a batch and reports each input exactly once when that input
// reaches a terminal state. Observations are serialized in completion order;
// index is the input's zero-based position and remains stable even when inputs
// run concurrently. A caller can therefore durably append results without
// waiting for the whole batch or adding its own synchronization.
// Each Pipeline belongs to one invocation; create a new Pipeline to resume.
func (p *Pipeline) RunObserved(ctx context.Context, items []source.Item, observe ResultObserver) (BatchResult, error) {
	if len(items) == 0 {
		return p.newBatch([]Result{}), errors.New("no source items resolved")
	}
	if (p.config.FlowID != "" || p.config.SourceID != "") && len(items) != 1 {
		return p.failAll(items, errors.New("explicit flow-id and source-id may only be used with one resolved input"), observe)
	}
	if err := p.checkProbeToolchain(ctx); err != nil {
		return p.failAll(items, withFailure(
			FailureCodeMediaToolUnavailable, FailureMessageMediaToolUnavailable, true, err), observe)
	}
	staging, err := newStagingManager(p.config.TempDirectory, p.config.StagingByteBudget, nil)
	if err != nil {
		return p.failAll(items, fmt.Errorf("prepare staging: %w", err), observe)
	}
	p.staging = staging

	// The budgets an operator actually got, which is not always the ones they
	// think: several resolve from the machine or from each other.
	p.logger.Debug("ingest budgets",
		"concurrency", p.config.Concurrency, "transfers", p.config.Transfers,
		"probe_concurrency", p.config.ProbeConcurrency, "retries", p.config.Retries,
		"staging_bytes", staging.limit, "staging_filesystem_free_bytes", staging.initialFree)

	storageID := p.config.StorageID
	if p.config.DryRunMode == DryRunOff {
		resolvedStorageID, err := p.runStartupPreflight(ctx)
		if err != nil {
			return p.failAll(items, err, observe)
		}
		storageID = resolvedStorageID
	} else if len(p.profileAssignments) > 0 {
		if err := p.runFlowProfilePreflight(ctx); err != nil {
			return p.failAll(items, err, observe)
		}
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	results := make([]Result, len(items))
	completed := make([]bool, len(items))
	jobs := make(chan int)
	type indexedResult struct {
		index  int
		result Result
	}
	terminal := make(chan indexedResult)
	var workers sync.WaitGroup
	workerCount := min(p.config.Concurrency, len(items))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				inputCtx := withInputIndex(runCtx, index)
				if err := p.observeInputStarted(index); err != nil {
					terminal <- indexedResult{index: index, result: p.failedResult(items[index], err)}
					continue
				}
				if p.config.DryRunMode == DryRunOff {
					phases := []progress.Phase{progress.PhaseStore}
					if p.config.VerificationMode != VerificationNone {
						phases = append(phases, progress.PhaseVerify)
					}
					tracker := progress.NewTracker(p.reporter, progress.Scope{
						InputIndex: index,
						Input:      safeURI(items[index].URI),
					}, phases...)
					inputCtx = progress.WithTracker(inputCtx, tracker)
				}
				result, err := p.ingestOne(inputCtx, items[index], storageID)
				result.Profile = p.config.Profile
				result.ProfileVersion = p.config.ProfileVersion
				if err != nil {
					result.Input = safeURI(items[index].URI)
					result.Status = ResultStatusFailed
					result.Error = err.Error()
					result.Verification = p.verificationFailureStatus(err)
					result.Failure = describeRunFailure(result, err, ctx)
				} else if p.config.VerificationMode != VerificationNone && p.config.DryRunMode == DryRunOff {
					result.Verification = VerificationVerified
				} else if p.config.VerificationMode != VerificationNone {
					result.Verification = VerificationNotReached
				} else {
					result.Verification = VerificationNotRequested
				}
				if result.Flows == nil {
					result.Flows = []FlowResult{}
				}
				if observationErr := p.observeRemainingObjects(index, result.Flows); observationErr != nil {
					if err == nil {
						err = withFailure(FailureCodeOutputFailed, FailureMessageOutputFailed, false, observationErr)
					} else {
						err = errors.Join(err, observationErr)
					}
					result.Status = ResultStatusFailed
					result.Error = err.Error()
					result.Failure = describeRunFailure(result, err, ctx)
				}
				compactObjectResults(result.Flows)
				terminal <- indexedResult{index: index, result: result}
			}
		}()
	}

	go func() {
		for index := range len(items) {
			select {
			case <-runCtx.Done():
				close(jobs)
				workers.Wait()
				close(terminal)
				return
			case jobs <- index:
			}
		}
		close(jobs)
		workers.Wait()
		close(terminal)
	}()

	var observerErr error
	for item := range terminal {
		results[item.index] = item.result
		completed[item.index] = true
		if observe != nil {
			if err := observe(item.index, item.result); err != nil {
				if observerErr == nil {
					observerErr = fmt.Errorf("write terminal result for input %d: %w", item.index, err)
					cancel(observerErr)
				}
			}
		}
	}

	// Inputs which were not dispatched before cancellation still receive a
	// terminal result. This keeps graceful-interruption output complete.
	cause := context.Cause(runCtx)
	if cause == nil {
		cause = errors.New("input did not reach a terminal state")
	}
	for index := range results {
		if completed[index] {
			continue
		}
		result := p.failedResult(items[index], cause)
		results[index] = result
		if observe != nil {
			if err := observe(index, result); err != nil {
				if observerErr == nil {
					observerErr = fmt.Errorf("write terminal result for input %d: %w", index, err)
				}
			}
		}
	}

	batch := p.newBatch(results)
	for _, result := range results {
		if result.Status == ResultStatusFailed {
			batch.Failed++
		} else {
			batch.Succeeded++
		}
	}
	if observerErr != nil {
		return batch, observerErr
	}
	if err := ctx.Err(); err != nil {
		return batch, err
	}
	return batch, nil
}

func (p *Pipeline) failAll(items []source.Item, cause error, observe ResultObserver) (BatchResult, error) {
	results := make([]Result, len(items))
	for index, item := range items {
		results[index] = p.failedResult(item, cause)
	}
	batch := p.newBatch(results)
	batch.Failed = len(results)
	if observe != nil {
		for index, result := range results {
			if err := observe(index, result); err != nil {
				return batch, fmt.Errorf("write terminal result for input %d: %w", index, err)
			}
		}
	}
	return batch, cause
}

func (p *Pipeline) failedResult(item source.Item, cause error) Result {
	result := Result{
		Input: safeURI(item.URI), Profile: p.config.Profile, ProfileVersion: p.config.ProfileVersion,
		Status: ResultStatusFailed, Verification: p.verificationFailureStatus(cause),
		Flows: []FlowResult{}, Error: cause.Error(),
	}
	var classified *classifiedFailure
	if errors.As(cause, &classified) {
		result.Failure = DescribeFailure(result, cause)
	} else if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		result.Failure = interruptedFailure()
	} else {
		result.Failure = DescribeFailure(result, cause)
	}
	return result
}

func (p *Pipeline) verificationFailureStatus(err error) VerificationStatus {
	if p.config.VerificationMode == VerificationNone {
		return VerificationNotRequested
	}
	var verificationErr *VerificationError
	if !errors.As(err, &verificationErr) {
		return VerificationNotReached
	}
	if verificationErr.Stranded > 0 {
		return VerificationFailedStranded
	}
	return VerificationFailedRetracted
}

func (p *Pipeline) initialVerificationStatus() VerificationStatus {
	if p.config.VerificationMode != VerificationNone {
		return VerificationNotReached
	}
	return VerificationNotRequested
}

func (p *Pipeline) ingestOne(ctx context.Context, item source.Item, storageID string) (Result, error) {
	p.logger.Info("preparing input", "input", safeURI(item.URI))
	lease, required, available, err := p.staging.reserve(ctx, item, p.config)
	if err != nil {
		return Result{}, withFailure(FailureCodeStagingCapacity, FailureMessageStagingCapacity, true, err)
	}
	defer lease.release()
	p.logger.Debug("staging preflight", "input", safeURI(item.URI),
		"estimated_required_bytes", required, "available_bytes", available)
	staged, err := stage(ctx, item, p.config.TempDirectory, p.config.Retries, lease, p.observability)
	if err != nil {
		return Result{}, withFailure(FailureCodeSourceTransferFailed, FailureMessageSourceTransferFailed, true, err)
	}
	defer staged.cleanup()
	p.observability.Staged(staged.size)

	// The input's own probe is an ffprobe spawn like any other, so it draws on
	// the same budget; leaving it out let one process per concurrent input
	// escape the bound.
	probe, err := p.probeInput(ctx, staged.path)
	if err != nil {
		return Result{}, withFailure(FailureCodeMediaAnalysisFailed, FailureMessageMediaAnalysisFailed, true, err)
	}
	contentType, err := media.DetectContentType(staged.path)
	if err != nil {
		return Result{}, withFailure(FailureCodeMediaAnalysisFailed, FailureMessageMediaContainerUnknown, true, err)
	}
	writesOutput := ffmpegWritesOutput(p.config, probe)
	if writesOutput {
		if err := validateCustomOutputMetadata(probe, p.config.FFmpegArgs, p.config.FlowMetadata); err != nil {
			return Result{}, withFailure(FailureCodeMediaOptionsInvalid, FailureMessageMediaOptionsInvalid, true, err)
		}
	}
	segmentContainer := media.SourceSegmentContainer(probe.Format, contentType)
	if p.config.SegmentFormat == media.SegmentFormatMPEGTS {
		segmentContainer = media.SegmentContainer{Muxer: "mpegts", Extension: ".ts"}
		// The named/structured stream-copy policy can be validated from the
		// input. Explicit FFmpeg arguments make the treatment custom and may
		// deliberately transcode to a compatible output codec that does not yet
		// exist to probe, so that workflow owns its output metadata and policy.
		if len(p.config.FFmpegArgs) == 0 {
			if err := validateMPEGTSSegmentCodecs(probe); err != nil {
				return Result{}, withFailure(FailureCodeMediaUnsupported, FailureMessageMediaUnsupported, true, err)
			}
		}
	}
	if writesOutput {
		if err := validateMuxerArguments(p.config.FFmpegArgs, p.config.SegmentDuration > 0, segmentContainer.Muxer); err != nil {
			return Result{}, withFailure(FailureCodeMediaOptionsInvalid, FailureMessageMediaOptionsInvalid, true, err)
		}
	}
	ffmpegVersion, toolchainFingerprint := "", ""
	if writesOutput {
		ffmpegVersion, toolchainFingerprint, err = p.mediaToolchain(ctx)
		if err != nil {
			return Result{}, withFailure(FailureCodeMediaToolUnavailable, FailureMessageMediaToolUnavailable, true, err)
		}
	}

	profileKey := flowProfile(staged.sha256, p.config)
	flowID := p.config.FlowID
	sourceID := p.config.SourceID
	if sourceID == "" {
		sourceID = sourceIdentity(staged.sha256)
	}
	identity := media.Identity{
		FlowID: flowID, SourceID: sourceID, Label: generatedLabel(staged.sha256),
		URI: safeURI(item.URI), SHA256: staged.sha256, Size: staged.size,
		IngestProfile: p.config.Profile, IngestProfileVersion: p.config.ProfileVersion,
		FFmpegVersion: ffmpegVersion, MediaToolchain: toolchainFingerprint,
	}
	flow, flowInfo, err := media.BuildFlow(probe, identity, contentType, p.config.EssenceStorage)
	if err != nil {
		return Result{}, withFailure(FailureCodeMediaAnalysisFailed, FailureMessageMediaInvalidFlow, true, err)
	}
	if flowID == "" {
		mediaKey, err := mediaInterpretationFingerprint(flow, flowInfo)
		if err != nil {
			return Result{}, withFailure(FailureCodeMediaAnalysisFailed, FailureMessageMediaIdentityUnresolved, true,
				fmt.Errorf("derive media interpretation identity: %w", err))
		}
		flowID = generatedRootFlowID(profileKey, mediaKey)
	}
	// A locator is not part of generated identity. Consequently two resolved
	// items in this process can name the same graph when their staged bytes and
	// treatment agree. Serialize that graph from planning through registration:
	// deterministic IDs make the second worker a resume, not a concurrent
	// writer of the same Objects.
	releaseGraph := p.acquireGraph(flowID)
	defer releaseGraph()
	p.warnUnsupportedCodecs(item, flowInfo.UnsupportedCodecs)
	// Independent storage demultiplexes, so each essence owns its Media Objects
	// and is ingested as a Flow in its own right. AppNote 0006's
	// container_mapping describes essence inside a shared multiplex, which no
	// longer exists here, so the elemental Flows carry none.
	if p.config.EssenceStorage == media.EssenceStorageIndependent && len(flowInfo.Collected) > 0 {
		if !hasValidContainerOverride(p.config.FlowMetadata) && p.config.SegmentFormat.ContainerMIME() == "" {
			for _, essence := range flowInfo.Collected {
				if !essence.ContainerSupported {
					p.warnUnsupportedContainer(item, probe, essence.Role)
					break
				}
			}
		}
		result, ingestErr := p.ingestIndependently(ctx, item, staged, flow, flowInfo, flowID, storageID)
		result.FFmpegVersion = ffmpegVersion
		result.MediaToolchain = toolchainFingerprint
		return result, ingestErr
	}

	// Each collected essence gets its own Flow and Source: a video track and an
	// audio track are distinct essences, not alternative representations of one
	// another, so they are not editorially equivalent. TAMS derives the
	// counterpart source_collection from the Flow collection itself.
	collectedFlows := make([]collectedFlow, 0, len(flowInfo.Collected))
	collectionItems := make([]map[string]any, 0, len(flowInfo.Collected))
	for index, collected := range flowInfo.Collected {
		position := strconv.Itoa(index)
		collectedID := generatedChildFlowID(flowID, "collected", position)
		collectedSourceID := sourceIdentity(staged.sha256, "essence", position)
		collected.Flow["id"] = collectedID
		collected.Flow["source_id"] = collectedSourceID
		collectedFlows = append(collectedFlows, collectedFlow{
			id: collectedID, sourceID: collectedSourceID, role: collected.Role, flow: collected.Flow,
			containerMapping: collected.ContainerMapping,
		})
		collectionItem := map[string]any{"id": collectedID, "role": collected.Role}
		if collected.ContainerMapping != nil {
			collectionItem["container_mapping"] = collected.ContainerMapping
		}
		collectionItems = append(collectionItems, collectionItem)
	}

	mergeFlow(flow, p.config.FlowMetadata)
	flow["id"] = flowID
	flow["source_id"] = sourceID
	if hasValidContainerOverride(p.config.FlowMetadata) {
		flowInfo.ContainerSupported = true
	}
	if len(collectionItems) > 0 {
		flow["flow_collection"] = collectionItems
	}
	if p.config.SegmentDuration > 0 {
		flow["segment_duration"] = durationRational(p.config.SegmentDuration)
	}
	// Flow metadata is built from the source probe, before any segmentation has
	// run. When an explicit format changes the container the Segments are written
	// in, the Flow must declare what was actually written rather than what the
	// input happened to be.
	if container := p.config.SegmentFormat.ContainerMIME(); container != "" && p.config.SegmentDuration > 0 {
		flow["container"] = container
		flowInfo.ContentType = container
		flowInfo.ContainerSupported = true
	}
	if !flowInfo.ContainerSupported {
		p.warnUnsupportedContainer(item, probe, "")
	}
	if p.config.SegmentDuration > 0 && p.config.DryRunMode != DryRunFast {
		return p.ingestMuxedRolling(ctx, safeURI(item.URI), staged, flow, flowInfo,
			flowID, sourceID, storageID, collectedFlows, ffmpegVersion, toolchainFingerprint)
	}

	objects, objectCleanup, err := p.prepareObjects(ctx, flowID, staged, flowInfo)
	if err != nil {
		return Result{}, withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaPrepareFailed, true, err)
	}
	defer objectCleanup()
	if !staged.owned && p.config.SegmentDuration > 0 {
		if err := ensureStagedInputUnchanged(ctx, staged); err != nil {
			return Result{}, withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true, err)
		}
	}
	p.applyBitRates(flow, objects)

	result := Result{
		Input: safeURI(item.URI), Profile: p.config.Profile, ProfileVersion: p.config.ProfileVersion,
		FFmpegVersion: ffmpegVersion, MediaToolchain: toolchainFingerprint,
		RootFlowID: flowID, Bytes: staged.size, SHA256: staged.sha256,
		Status: ResultStatusPlanned, Verification: p.initialVerificationStatus(),
		Flows: make([]FlowResult, 0, len(collectedFlows)+1),
	}
	for _, collected := range collectedFlows {
		result.Flows = append(result.Flows, FlowResult{
			FlowID: collected.id, SourceID: collected.sourceID, Role: collected.role, Disposition: FlowPlanned, Kind: FlowKindEssence,
		})
	}
	rootKind := FlowKindEssence
	rootRole := flowPlanRole(rootKind, "single", stringField(flow, "format"))
	if len(collectedFlows) > 0 {
		rootKind, rootRole = FlowKindMuxed, ""
	}
	rootIndex := len(result.Flows)
	result.Flows = append(result.Flows, FlowResult{
		FlowID: flowID, SourceID: sourceID, Role: rootRole, Disposition: FlowPlanned, Kind: rootKind,
		Objects: make([]ObjectResult, len(objects)),
	})
	for index, object := range objects {
		result.Flows[rootIndex].Objects[index] = p.newObjectResult(object)
	}
	graph := flowGraph{
		flows:   make([]graphFlow, 0, len(collectedFlows)+1),
		storage: p.config.EssenceStorage,
	}
	for _, collected := range collectedFlows {
		graph.flows = append(graph.flows, graphFlow{
			id: collected.id, role: collected.role, flow: collected.flow,
			containerMapping: collected.containerMapping,
		})
	}
	parentRole := "single"
	if len(collectedFlows) > 0 {
		parentRole = "multi"
	}
	graph.flows = append(graph.flows, graphFlow{id: flowID, role: parentRole, flow: flow, ownsMedia: true})
	if len(collectedFlows) > 0 {
		graph.collectorID = flowID
	}
	if err := p.executeFlowPlan(ctx, safeURI(item.URI), graph, storageID, []flowRegistrationTarget{
		flowExecutionTarget(flowID, rootIndex, objects, ""),
	}, result.Flows); err != nil {
		return result, err
	}
	if p.config.DryRunMode != DryRunOff {
		return result, nil
	}

	result.Status = ResultStatusIngested
	allResumed := len(result.Flows[rootIndex].Objects) > 0
	for _, object := range result.Flows[rootIndex].Objects {
		if object.Status != ObjectStatusResumed {
			allResumed = false
			break
		}
	}
	if allResumed {
		result.Status = ResultStatusResumed
	}
	return result, nil
}

// hasValidContainerOverride makes an explicit --flow-metadata container the
// supported escape hatch for a format outside Tamsin's built-in profile. An
// invalid value does not suppress the warning that helps diagnose it.
func hasValidContainerOverride(flow tams.Flow) bool {
	value, ok := flow["container"].(string)
	if !ok {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(value)
	return err == nil && mediaType != "" && len(parameters) == 0
}

// validateFlowMetadataOverrides rejects fields whose meaning depends on the
// graph Tamsin is constructing. Applying one open-map override to a collector
// and several elemental Flows cannot safely redefine their identities or
// ownership relationships; silently overwriting the value later is equally
// misleading. JSON Pointers make the error actionable against the supplied
// document and New's caller reports it as a usage error.
func validateFlowMetadataOverrides(flow tams.Flow) error {
	for _, field := range []struct {
		name   string
		action string
	}{
		{name: "id", action: "use --flow-id"},
		{name: "source_id", action: "use --source-id"},
		{name: "format", action: "it is derived for each essence"},
		{name: "flow_collection", action: "it is built from the resolved Flow graph"},
		{name: "container_mapping", action: "it is built from the input track map"},
		{name: "collected_by", action: "it is service-managed collection metadata"},
		{name: "timerange", action: "it is service-managed Segment metadata"},
	} {
		if _, present := flow[field.name]; present {
			return fmt.Errorf("--flow-metadata /%s cannot be overridden: %s", field.name, field.action)
		}
	}
	return nil
}

// validateCustomOutputMetadata prevents an argument-driven transcode from
// retaining technical metadata derived from the input. An FFmpeg argument list
// is not a declarative output contract, and one open-map override cannot
// describe several different output essences. Until per-essence output probing
// is part of the treatment contract, custom rendering is therefore restricted
// to one essence whose output codec and parameters are explicit.
func validateCustomOutputMetadata(probe media.Probe, ffmpegArgs []string, metadata tams.Flow) error {
	if len(ffmpegArgs) == 0 {
		return nil
	}
	streams := 0
	for _, stream := range probe.Streams {
		if stream.Disposition.AttachedPicture == 0 {
			streams++
		}
	}
	if streams != 1 {
		return fmt.Errorf(
			"custom --ffmpeg-arg treatment requires exactly one essence, got %d; "+
				"transcode multi-stream media before ingest because one --flow-metadata document cannot describe each output essence",
			streams)
	}
	codec, codecSet := metadata["codec"].(string)
	_, parametersSet := metadata["essence_parameters"]
	if !codecSet || strings.TrimSpace(codec) == "" || !parametersSet {
		return errors.New(
			"custom --ffmpeg-arg treatment requires --flow-metadata with the output codec and complete essence_parameters; " +
				"input probe metadata cannot describe bytes that FFmpeg may transcode")
	}
	return nil
}

func (p *Pipeline) warnUnsupportedContainer(item source.Item, probe media.Probe, role string) {
	attributes := []any{
		"input", safeURI(item.URI),
		"ffprobe_format", probe.Format.Name,
		"fallback", "application/octet-stream",
	}
	if brand := strings.TrimSpace(probe.Format.Tags["major_brand"]); brand != "" {
		attributes = append(attributes, "major_brand", brand)
	}
	if role != "" {
		attributes = append(attributes, "essence", role)
	}
	p.logger.Warn("media container is outside Tamsin's supported profile; use --flow-metadata to override metadata, or choose MPEG-TS/whole-file storage", attributes...)
}

func (p *Pipeline) warnUnsupportedCodecs(item source.Item, codecs []media.UnsupportedCodec) {
	for _, codec := range codecs {
		p.logger.Warn("media codec is outside Tamsin's supported profile; TAMS requires a codec media type on elemental Flows; a single-essence workflow may supply one through --flow-metadata",
			"input", safeURI(item.URI),
			"ffprobe_codec", codec.Name,
			"stream_type", codec.StreamType,
			"stream_index", codec.StreamIndex,
		)
	}
}

type preparedObject struct {
	id     string
	path   string
	size   int64
	sha256 string
	// start is where the Segment sits on the Flow timeline, in nanoseconds. It
	// is kept so a batch can ask the store for just the Segments it wrote.
	start int64
	// duration is the Segment's own length in nanoseconds. It is kept because
	// the Flow's bit rate properties are defined over Segments, so they cannot
	// be worked out from the input alone.
	duration        int64
	timerange       string
	objectTimerange string
	tsOffset        string
}

// registerFlow creates one Flow and brings its Media Objects into the store:
// allocate, upload, register, then verify and retract on mismatch. Independent
// essence storage runs this once per essence, so it takes the Flow and the
// Objects belonging to it rather than reading them off a single ingest.

// ingestIndependently extracts each elementary stream into its own Media
// Objects and ingests it as a separate Flow, the arrangement AppNote 0001 leads
// with because it lets a consumer fetch one essence without the others.
func (p *Pipeline) ingestIndependently(ctx context.Context, item source.Item, staged stagedFile,
	collector tams.Flow, flowInfo media.FlowInfo, collectorID, storageID string) (Result, error) {
	if p.segmenter == nil {
		return Result{}, errors.New("independent essence storage requires a media segmenter")
	}
	if !staged.owned && p.config.SegmentDuration <= 0 {
		allowance := max(p.staging.limit, 1)
		if len(p.config.FFmpegArgs) == 0 {
			var err error
			allowance, err = percentageWithFloor(staged.size, segmentAllowancePercent, segmentAllowanceFloor)
			if err != nil {
				return Result{}, fmt.Errorf("estimate essence staging: %w", err)
			}
		}
		if err := staged.lease.reserveAdditional(ctx, allowance); err != nil {
			return Result{}, fmt.Errorf("reserve essence staging: %w", err)
		}
		p.logger.Debug("expanded staging reservation after probe", "input", safeURI(item.URI),
			"additional_bytes", allowance)
	}

	// The Multi-Flow records that these essences came from one input. AppNote
	// 0001 has a demultiplexed ingest create three Flows for a video and audio
	// stream: one per essence, and one collecting them. Without it the store
	// holds two unrelated Flows and nothing says they belong together.
	//
	// Explicit identifiers name the collector, because it is the input's Flow;
	// the essences are derived beneath it.
	collectorSourceID := p.config.SourceID
	if collectorSourceID == "" {
		collectorSourceID = sourceIdentity(staged.sha256)
	}
	if p.config.SegmentDuration > 0 && p.config.DryRunMode != DryRunFast {
		return p.ingestIndependentRolling(ctx, safeURI(item.URI), staged, collector, flowInfo,
			collectorID, collectorSourceID, storageID)
	}

	result := Result{
		Input: safeURI(item.URI), Profile: p.config.Profile, ProfileVersion: p.config.ProfileVersion,
		RootFlowID: collectorID,
		Bytes:      staged.size, SHA256: staged.sha256, Status: ResultStatusPlanned,
		Verification: p.initialVerificationStatus(),
		Flows: []FlowResult{{
			FlowID: collectorID, SourceID: collectorSourceID, Disposition: FlowPlanned, Kind: FlowKindCollection,
		}},
	}

	// Every essence is prepared before any is transferred, so the work an
	// operator is shown is the whole input's rather than growing as each essence
	// begins.
	type plannedFlow struct {
		id          string
		role        string
		flow        tams.Flow
		objects     []preparedObject
		resultIndex int
	}
	planned := make([]plannedFlow, 0, len(flowInfo.Collected))
	collectionItems := make([]map[string]any, 0, len(flowInfo.Collected))
	renderedByStream := make(map[int][]media.SegmentRecord)
	if p.config.DryRunMode != DryRunFast && len(flowInfo.Collected) >= multiOutputEssenceThreshold && len(p.config.FFmpegArgs) == 0 {
		streamIndices := make([]int, len(flowInfo.Collected))
		for index, essence := range flowInfo.Collected {
			streamIndices[index] = essence.StreamIndex
		}
		records, cleanup, err := p.renderSegments(ctx, staged, flowInfo, streamIndices, nil)
		if err != nil {
			return result, withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaStreamPrepareFailed, true,
				fmt.Errorf("prepare %d essences in one render: %w", len(streamIndices), err))
		}
		defer cleanup()
		for _, record := range records {
			renderedByStream[record.StreamIndex] = append(renderedByStream[record.StreamIndex], record)
		}
		for _, streamIndex := range streamIndices {
			if len(renderedByStream[streamIndex]) == 0 {
				return result, withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaStreamPrepareFailed, true,
					fmt.Errorf("multi-output renderer produced no objects for stream %d", streamIndex))
			}
		}
	}

	for index, essence := range flowInfo.Collected {
		position := strconv.Itoa(index)
		flowID := generatedChildFlowID(collectorID, "essence", position)
		sourceID := sourceIdentity(staged.sha256, "essence", position)
		flow := essence.Flow
		mergeFlow(flow, p.config.FlowMetadata)
		flow["id"] = flowID
		flow["source_id"] = sourceID
		if p.config.SegmentDuration > 0 {
			flow["segment_duration"] = durationRational(p.config.SegmentDuration)
		}
		if container := p.config.SegmentFormat.ContainerMIME(); container != "" {
			flow["container"] = container
		}
		// The stream's own index, not its position in the collection: anything
		// that is not essence has already been filtered out, so counting the
		// collection would demultiplex the wrong track. Its offset places it
		// where it starts relative to the container, which is what keeps the
		// essences in sync once each is a Flow of its own.
		objects, cleanup, err := p.prepareEssenceObjects(
			ctx, flowID, staged, flowInfo, essence.StreamIndex, p.config.Start+essence.Offset,
			renderedByStream[essence.StreamIndex])
		if err != nil {
			return result, withFailure(FailureCodeMediaPrepareFailed, FailureMessageMediaStreamPrepareFailed, true,
				fmt.Errorf("prepare essence %s: %w", essence.Role, err))
		}
		defer cleanup()
		p.applyBitRates(flow, objects)

		flowResult := FlowResult{
			FlowID: flowID, SourceID: sourceID, Role: essence.Role, Disposition: FlowPlanned, Kind: FlowKindEssence,
			Objects: make([]ObjectResult, len(objects)),
		}
		for objectIndex, object := range objects {
			flowResult.Objects[objectIndex] = p.newObjectResult(object)
		}
		resultIndex := len(result.Flows)
		result.Flows = append(result.Flows, flowResult)
		planned = append(planned, plannedFlow{
			id: flowID, role: essence.Role, flow: flow, objects: objects, resultIndex: resultIndex,
		})
		collectionItems = append(collectionItems, map[string]any{"id": flowID, "role": essence.Role})
	}
	if !staged.owned {
		if err := ensureStagedInputUnchanged(ctx, staged); err != nil {
			return result, withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true, err)
		}
	}

	// The collector references Media Objects through no Segments of its own, so
	// it declares no container: its media is reached through the essences it
	// collects, each of which owns theirs.
	mergeFlow(collector, p.config.FlowMetadata)
	collector["id"] = collectorID
	collector["source_id"] = collectorSourceID
	collector["flow_collection"] = collectionItems
	delete(collector, "container")
	graph := flowGraph{
		flows:       make([]graphFlow, 0, len(planned)+1),
		collectorID: collectorID,
		storage:     media.EssenceStorageIndependent,
	}
	targets := make([]flowRegistrationTarget, 0, len(planned))
	for _, entry := range planned {
		graph.flows = append(graph.flows, graphFlow{
			id: entry.id, role: entry.role, flow: entry.flow, ownsMedia: true,
		})
		targets = append(targets, flowExecutionTarget(entry.id, entry.resultIndex, entry.objects, entry.role))
	}
	graph.flows = append(graph.flows, graphFlow{id: collectorID, role: "multi", flow: collector})
	if err := p.executeFlowPlan(ctx, safeURI(item.URI), graph, storageID, targets, result.Flows); err != nil {
		return result, err
	}
	if p.config.DryRunMode != DryRunOff {
		return result, nil
	}

	result.Status = ResultStatusIngested
	resumed := true
	for _, flowResult := range result.Flows {
		for _, object := range flowResult.Objects {
			if object.Status != ObjectStatusResumed {
				resumed = false
			}
		}
	}
	if resumed && len(result.Flows) > 0 {
		result.Status = ResultStatusResumed
	}
	return result, nil
}

// expectTransfers seals one input's exact, per-phase work before any transfer
// starts. Store and verification describe the same logical Objects at distinct
// phases; they must never be added into a doubled Object or byte total.
//
// Independent essence ingest calls this only after every elemental Flow has
// been prepared, so a consumer can safely derive a percentage as soon as
// TotalsFinal becomes true.
func (p *Pipeline) expectTransfers(ctx context.Context, objects []preparedObject) {
	tracker := progress.FromContext(ctx)
	if tracker == nil {
		return
	}
	var bytes int64
	for _, object := range objects {
		bytes += object.size
	}
	p.setProgressTotals(tracker, progress.PhaseStore, len(objects), bytes)
	if p.config.VerificationMode != VerificationNone {
		p.setProgressTotals(tracker, progress.PhaseVerify, len(objects), bytes)
	}
}

func (p *Pipeline) setProgressTotals(tracker *progress.Tracker, phase progress.Phase, objects int, bytes int64) {
	if err := tracker.SetTotals(phase, objects, bytes, true); err != nil {
		p.logger.Error("invalid progress totals", "phase", phase, "objects", objects, "bytes", bytes, "error", err)
	}
}

func (p *Pipeline) advanceProgress(ctx context.Context, phase progress.Phase, objects int, bytes int64) {
	tracker := progress.FromContext(ctx)
	if tracker == nil {
		return
	}
	if err := tracker.Advance(phase, objects, bytes); err != nil {
		p.logger.Error("invalid progress completion", "phase", phase, "objects", objects, "bytes", bytes, "error", err)
	}
}

// probeInput measures a staged input under the global measurement budget.
func (p *Pipeline) probeInput(ctx context.Context, path string) (media.Probe, error) {
	release, err := p.acquireProbe(ctx)
	if err != nil {
		return media.Probe{}, err
	}
	defer release()
	probe, err := p.prober.Probe(ctx, path)
	if err != nil {
		return media.Probe{}, err
	}
	// A complete operator override replaces essence_parameters as one object,
	// so decoded input cadence cannot affect the resulting Flow. Avoid an
	// otherwise full-length video decode in that case; custom transcodes in
	// particular describe their output explicitly rather than spending CPU to
	// perfect metadata that belongs only to the input.
	if _, overridden := p.config.FlowMetadata["essence_parameters"]; !overridden {
		if presentation, ok := p.prober.(media.PresentationProber); ok {
			if err := presentation.ProbePresentation(ctx, path, &probe); err != nil {
				return media.Probe{}, err
			}
		}
	}
	return probe, nil
}

// checkAPIVersion refuses a store speaking a specification Tamsin was not
// written against.
//
// The service document is fetched anyway to confirm the store is reachable and
// the credentials work, and it carries the version it implements. Reading it
// costs nothing and turns an incompatible store into one clear sentence, rather
// than a confusing failure some way into an ingest when a request is rejected
// for a reason that looks like a bug.
//
// A newer minor revision is accepted, because minor revisions add to the API
// rather than change it, and a client that refused them would stand in the way
// of every service upgrade. An older one is allowed but said out loud.
func (p *Pipeline) checkAPIVersion(service map[string]any) error {
	assessment, err := AssessAPIVersion(service)
	if err != nil {
		return err
	}
	p.apiVersion = assessment.Version
	if assessment.Relationship == "older" {
		p.logger.Warn("store implements an older TAMS revision than tamsin targets",
			"store_api_version", assessment.StoreVersion,
			"tamsin_api_version", assessment.TargetVersion)
	}
	return nil
}

// assumedThroughput is the transfer rate a batch is sized against before any
// batch has been timed. It is deliberately pessimistic -- roughly eight
// megabits a second -- because the cost of guessing low is a few extra round
// trips, while the cost of guessing high is Media Objects collected before they
// could be registered.
const assumedThroughput = 1 << 20

// rejectUnusableMediaOptions refuses options that would silently do nothing.
//
// Without segmentation the staged file is uploaded as it stands. An FFmpeg
// argument list or a chosen Segment container can then only be honoured by
// re-encoding or remuxing the whole file, which is not what this path does.
// Ignoring them quietly is the worst option, because both feed the derived Flow
// identity: two ingests that differ only in an argument that had no effect
// would land on different Flows, and an argument list also leaves generation
// unset, implying a transcode that never happened.
func (p *Pipeline) rejectUnusableMediaOptions() error {
	var unusable []string
	if len(p.config.FFmpegArgs) > 0 {
		unusable = append(unusable, "--ffmpeg-arg")
	}
	if p.config.SegmentFormat.ContainerMIME() != "" {
		unusable = append(unusable, "--segment-format "+string(p.config.SegmentFormat))
	}
	if len(unusable) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s cannot take effect without segmentation, because the input is stored as it stands; "+
			"set --segment-duration, or remove the option",
		strings.Join(unusable, " and "))
}

// prepareEssenceObjects stages the Media Objects for a single elementary
// stream. Extraction always runs, even at a zero segment duration, because the
// essence has to be separated from the multiplex before it can be stored.
func (p *Pipeline) prepareEssenceObjects(ctx context.Context, flowID string, staged stagedFile, flowInfo media.FlowInfo,
	streamIndex int, start int64, rendered []media.SegmentRecord) ([]preparedObject, func(), error) {
	return p.prepareObjectsForStream(ctx, flowID, staged, flowInfo, streamIndex, start, rendered)
}

func (p *Pipeline) prepareObjects(ctx context.Context, flowID string, staged stagedFile, flowInfo media.FlowInfo) ([]preparedObject, func(), error) {
	return p.prepareObjectsForStream(ctx, flowID, staged, flowInfo, media.AllStreams, p.config.Start, nil)
}

func (p *Pipeline) renderSegments(ctx context.Context, staged stagedFile, flowInfo media.FlowInfo,
	streamIndices []int, additionalArgs []string) ([]media.SegmentRecord, func(), error) {
	var (
		records  []media.SegmentRecord
		recordMu sync.Mutex
	)
	cleanup, err := p.renderSegmentsTo(ctx, staged, flowInfo, streamIndices, additionalArgs, nil,
		func(record media.SegmentRecord) error {
			recordMu.Lock()
			records = append(records, record)
			recordMu.Unlock()
			return nil
		})
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if len(records) == 0 {
		cleanup()
		return nil, func() {}, errors.New("media renderer produced no objects")
	}
	return records, cleanup, nil
}

func (p *Pipeline) renderSegmentsTo(ctx context.Context, staged stagedFile, flowInfo media.FlowInfo,
	streamIndices []int, additionalArgs []string, window *media.SegmentStagingWindow,
	sink media.SegmentSink) (func(), error) {
	directory, err := os.MkdirTemp(p.config.TempDirectory, "tamsin-segments-")
	if err != nil {
		return func() {}, fmt.Errorf("create segment staging directory: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(directory)
		staged.lease.removeArtifact(directory)
	}
	segmentCtx, cancelSegment := context.WithCancel(ctx)
	releaseRolling := func() {}
	if window != nil {
		releaseRolling, err = p.acquireRollingRender(segmentCtx)
		if err != nil {
			cancelSegment()
			cleanup()
			return func() {}, err
		}
	}
	processWeight := int64(1)
	if len(additionalArgs) > 0 && window == nil {
		processWeight = 2
	}
	releaseProcess, err := p.acquireMediaProcess(segmentCtx, processWeight)
	if err != nil {
		releaseRolling()
		cancelSegment()
		cleanup()
		return func() {}, err
	}
	monitorDone := make(chan struct{})
	monitorResult := make(chan error, 1)
	go monitorStagingDirectory(segmentCtx, cancelSegment, staged.lease, directory, monitorDone, monitorResult)
	renderErr := p.segmenter.Segment(segmentCtx, media.SegmentRequest{
		Input: staged.path, Duration: p.config.SegmentDuration, Format: p.config.SegmentFormat,
		SourceContainer: flowInfo.SegmentContainer, StreamIndices: streamIndices,
		Directory: directory, AdditionalArgs: additionalArgs, StagingWindow: window,
	}, sink)
	releaseProcess()
	releaseRolling()
	close(monitorDone)
	monitorErr := <-monitorResult
	cancelSegment()
	if monitorErr == nil {
		size, sizeErr := directoryBytes(directory)
		if sizeErr != nil {
			monitorErr = fmt.Errorf("measure segment staging directory: %w", sizeErr)
		} else if capacityErr := staged.lease.setArtifact(directory, size); capacityErr != nil {
			monitorErr = fmt.Errorf("segment output exceeded staging capacity: %w", capacityErr)
		}
	}
	if monitorErr != nil {
		return cleanup, monitorErr
	}
	if renderErr != nil {
		return cleanup, renderErr
	}
	return cleanup, nil
}

// prepareObjectsForStream cuts one stream, or the whole input, into Media
// Objects and places them from start on the Flow timeline. The start is passed
// rather than read from the configuration because demultiplexed essences do not
// all begin together: each is placed where its own stream begins.
func (p *Pipeline) prepareObjectsForStream(ctx context.Context, flowID string, staged stagedFile, flowInfo media.FlowInfo,
	streamIndex int, start int64, rendered []media.SegmentRecord) ([]preparedObject, func(), error) {
	records := rendered
	cleanup := func() {}
	if p.config.SegmentDuration <= 0 && streamIndex == media.AllStreams {
		// The whole input becomes one Media Object and FFmpeg is never invoked,
		// so anything that only takes effect through FFmpeg would do nothing --
		// while still changing the Flow's generated identity and its generation,
		// which would then describe a treatment the bytes never received.
		if err := p.rejectUnusableMediaOptions(); err != nil {
			return nil, cleanup, withFailure(FailureCodeMediaOptionsIgnored, FailureMessageMediaOptionsIgnored, true, err)
		}
	}
	if p.config.DryRunMode == DryRunFast && (p.config.SegmentDuration > 0 || streamIndex != media.AllStreams) {
		return []preparedObject{}, cleanup, nil
	}
	if records == nil && (p.config.SegmentDuration > 0 || streamIndex != media.AllStreams) {
		var err error
		records, cleanup, err = p.renderSegments(ctx, staged, flowInfo, []int{streamIndex}, p.config.FFmpegArgs)
		if err != nil {
			return nil, func() {}, err
		}
	}
	if records == nil {
		records = []media.SegmentRecord{{StreamIndex: streamIndex, Path: staged.path}}
	}
	if len(records) == 0 {
		cleanup()
		return nil, func() {}, errors.New("media renderer produced no objects")
	}
	paths := make([]string, len(records))
	for index, record := range records {
		paths[index] = record.Path
	}

	objects := make([]preparedObject, 0, len(paths))
	// Measuring each Segment means a digest and, when segmenting, an ffprobe
	// subprocess. Spawning those serially dominates local cost: twelve probes
	// cost about a second, which is more than the segmentation that produced
	// them. They are independent, so they run concurrently.
	//
	// Only the measurement parallelises. Assigning positions on the Flow
	// timeline stays sequential below, because each Segment begins where the
	// previous one ended.
	type measurement struct {
		size        int64
		checksum    string
		objectStart int64
		duration    int64
	}
	measurements := make([]measurement, len(paths))
	probeSegments := len(paths) > 1 || streamIndex != media.AllStreams
	manifestTiming := len(records) > 0
	for _, record := range records {
		manifestTiming = manifestTiming && record.Timed
	}
	var anchorStart, manifestStart int64
	if manifestTiming {
		release, err := p.acquireProbe(ctx)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		probe, probeErr := p.prober.Probe(ctx, records[0].Path)
		release()
		if probeErr != nil {
			cleanup()
			return nil, func() {}, probeErr
		}
		parsedAnchor, _, err := media.ProbeTiming(probe)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		anchorStart = parsedAnchor
		manifestStart = records[0].Start
	}
	// Without segmentation the single Media Object is the staged file itself,
	// which staging has already read end to end to hash. Reading it a second
	// time only re-derives a digest that cannot have changed -- but only when
	// the file is ours. A local input is not, so it keeps the later digest,
	// which at least describes the file closer to when it is uploaded.
	reuseStagedDigest := staged.owned && len(paths) == 1 && paths[0] == staged.path

	group, groupCtx := errgroup.WithContext(ctx)
	// A day of ten-second Segments is over eight thousand measurements. Each
	// one needs a slot from the Pipeline-wide budget before it can do anything,
	// so creating them all up front only parks them on the same semaphore.
	group.SetLimit(max(p.config.ProbeConcurrency, 1))
	for index, path := range paths {
		group.Go(func() error {
			if reuseStagedDigest {
				measurements[index] = measurement{
					size: staged.size, checksum: staged.sha256,
					objectStart: flowInfo.Start, duration: flowInfo.Duration,
				}
				return nil
			}
			// The budget is Pipeline-wide, so concurrent Flows contend for the
			// same measurement slots rather than each getting a full allowance.
			release, err := p.acquireProbe(groupCtx)
			if err != nil {
				return err
			}
			defer release()

			size, checksum, err := digestFile(groupCtx, path)
			if err != nil {
				return err
			}
			entry := measurement{size: size, checksum: checksum, objectStart: flowInfo.Start, duration: flowInfo.Duration}
			if manifestTiming {
				manifestOffset, offsetErr := media.TimestampOffset(records[index].Start, manifestStart)
				if offsetErr != nil {
					return fmt.Errorf("calculate segment manifest offset: %w", offsetErr)
				}
				entry.objectStart, err = media.TimestampShift(anchorStart, manifestOffset)
				if err != nil {
					return fmt.Errorf("calculate segment object start: %w", err)
				}
				entry.duration, err = media.TimestampOffset(records[index].End, records[index].Start)
				if err != nil {
					return fmt.Errorf("calculate segment duration: %w", err)
				}
			} else if probeSegments {
				probe, probeErr := p.prober.Probe(groupCtx, path)
				if probeErr != nil {
					return probeErr
				}
				if entry.objectStart, entry.duration, err = media.ProbeTiming(probe); err != nil {
					return err
				}
			}
			measurements[index] = entry
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if !staged.owned && len(paths) == 1 && paths[0] == staged.path {
		measurement := measurements[0]
		if measurement.size != staged.size || measurement.checksum != staged.sha256 {
			cleanup()
			return nil, func() {}, fmt.Errorf(
				"local input changed after staging: expected %d bytes with SHA-256 %s, got %d bytes with SHA-256 %s",
				staged.size, staged.sha256, measurement.size, measurement.checksum)
		}
	}

	flowPosition := start
	for index, path := range paths {
		size, checksum := measurements[index].size, measurements[index].checksum
		objectStart, duration := measurements[index].objectStart, measurements[index].duration
		timerange, err := media.TimeRange(flowPosition, duration)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		objectTimerange, err := media.TimeRange(objectStart, duration)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		offset, err := media.TimestampOffset(flowPosition, objectStart)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		object := preparedObject{
			id: namedID("object", flowID, checksum, timerange), path: path, size: size, sha256: checksum,
			start: flowPosition, duration: duration, timerange: timerange, objectTimerange: objectTimerange,
		}
		if offset != 0 {
			object.tsOffset = media.Timestamp(offset)
		}
		objects = append(objects, object)
		flowPosition, err = media.TimestampShift(flowPosition, duration)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
	}
	return objects, cleanup, nil
}

// Directory scans grow with the Segment count. Four checks per second catches
// a runaway output promptly without turning a long, many-thousand-Segment
// transcode into a metadata polling workload of its own.
const stagingDirectoryPollInterval = 250 * time.Millisecond

// monitorStagingDirectory accounts for files written by FFmpeg, which cannot
// write through a Go io.Writer. The reservation made before the process starts
// is the primary bound; this monitor extends it when an output grows beyond its
// estimate and cancels the process immediately when no global capacity remains.
// The final scan is exact even when a short-lived test segmenter finishes
// between polling ticks.
func monitorStagingDirectory(ctx context.Context, cancel context.CancelFunc, lease *stagingLease,
	directory string, done <-chan struct{}, result chan<- error) {
	syncDirectory := func() error {
		size, err := directoryBytes(directory)
		if err != nil {
			return fmt.Errorf("measure segment staging directory: %w", err)
		}
		if err := lease.setArtifact(directory, size); err != nil {
			return fmt.Errorf("segment output exceeded staging capacity: %w", err)
		}
		return nil
	}
	ticker := time.NewTicker(stagingDirectoryPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			result <- syncDirectory()
			return
		case <-ticker.C:
			if err := syncDirectory(); err != nil {
				cancel()
				result <- err
				return
			}
		case <-ctx.Done():
			result <- nil
			return
		}
	}
}

func ensureStagedInputUnchanged(ctx context.Context, staged stagedFile) error {
	size, checksum, err := digestFile(ctx, staged.path)
	if err != nil {
		return fmt.Errorf("recheck local input after media processing: %w", err)
	}
	if size == staged.size && checksum == staged.sha256 {
		return nil
	}
	return fmt.Errorf(
		"local input changed after staging: expected %d bytes with SHA-256 %s, got %d bytes with SHA-256 %s",
		staged.size, staged.sha256, size, checksum)
}

func (p *Pipeline) verifyObject(ctx context.Context, expected preparedObject, segment tams.Segment) error {
	if len(segment.GetURLs) == 0 {
		return fmt.Errorf("segment %s has no download URL for verification", expected.id)
	}
	// A service may also list direct storage URLs that require separate credentials.
	// Prefer its presigned access route when one is available.
	download := segment.GetURLs[0]
	for _, candidate := range segment.GetURLs {
		if candidate.Presigned {
			download = candidate
			break
		}
	}
	size, checksum, err := p.client.DownloadDigest(ctx, download, expected.size)
	if err != nil {
		return fmt.Errorf("verify object %s: %w", expected.id, err)
	}
	if size != expected.size {
		return fmt.Errorf("object %s byte length mismatch: expected %d, got %d", expected.id, expected.size, size)
	}
	if checksum != expected.sha256 {
		return fmt.Errorf("object %s SHA-256 mismatch: expected %s, got %s", expected.id, expected.sha256, checksum)
	}
	return nil
}

func digestFile(ctx context.Context, filename string) (int64, string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, "", fmt.Errorf("open prepared object: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	size, err := copyContext(ctx, hash, file)
	if err != nil {
		return 0, "", fmt.Errorf("hash prepared object: %w", err)
	}
	return size, hex.EncodeToString(hash.Sum(nil)), nil
}

// copyBuffers recycles the staging buffers. Every byte Tamsin handles passes
// through copyContext at least three times — hashing, uploading, and verifying
// — so allocating a fresh buffer per copy made this the source of over ninety
// percent of the program's allocations, and the resulting garbage collection
// and zeroing showed up as a fifth of its CPU time.
var copyBuffers = sync.Pool{
	New: func() any {
		buffer := make([]byte, copyBufferSize)
		return &buffer
	},
}

const copyBufferSize = 128 << 10

// worthResuming reports whether a failed staging copy should be retried by
// reopening the source.
//
// A source that cannot be reopened and an exhausted allowance are the obvious
// nos. The one worth stating is a failure to write what was read: reopening a
// source cannot create disk space, so a staging volume that filled up would
// otherwise send the transfer round again, pull the same bytes over the
// network, and fail in the same place -- once for every attempt remaining.
func worthResuming(err error, canReopen bool, attempt, retries int) bool {
	if err == nil || !canReopen || attempt >= retries {
		return false
	}
	var destination destinationError
	return !errors.As(err, &destination)
}

// destinationError marks a failure to write bytes that were read successfully.
//
// The distinction decides whether retrying makes sense: reopening a source
// cannot create disk space, so a staging volume that filled up would otherwise
// send the transfer round again, download the same bytes, and fail in the same
// place -- once per remaining attempt.
type destinationError struct{ err error }

func (e destinationError) Error() string { return e.err.Error() }
func (e destinationError) Unwrap() error { return e.err }

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	pooled := copyBuffers.Get().(*[]byte)
	defer copyBuffers.Put(pooled)
	buffer := *pooled
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, destinationError{err: writeErr}
			}
			if written != read {
				return total, destinationError{err: io.ErrShortWrite}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, readErr
		}
	}
}

// flowProfile is everything about an ingest that changes what ends up in the
// store. Generated identifiers derive from it, so that the same input treated
// the same way keeps landing on the same Flow -- which is what makes a resume
// work -- while any difference that alters the result produces a new one.
//
// Essence storage is part of it because it decides whether one Flow or several
// are written and what shape the parent takes. Without it a muxed
// multi-essence Flow and the collector of a demultiplexed ingest would derive
// the same identifier for the same input: two incompatible Flows claiming one
// ID, one holding Media Objects and one holding none.
//
// The start is part of it because it decides where the media sits on the
// timeline. Without it a second ingest at a different --start appends to the
// Flow the first one made, instead of describing the placement that was asked
// for.
func flowProfile(digest string, config Config) string {
	return flowProfileForRendererEpoch(digest, config, rendererIdentityEpoch)
}

func flowProfileForRendererEpoch(digest string, config Config, rendererEpoch string) string {
	parts := []string{
		config.Profile,
		config.ProfileVersion,
		digest,
		strconv.FormatInt(int64(config.SegmentDuration), 10),
		string(config.SegmentFormat),
		string(config.EssenceStorage),
		strconv.FormatInt(config.Start, 10),
		strconv.Itoa(len(config.FFmpegArgs)),
	}
	parts = append(parts, config.FFmpegArgs...)
	parts = append(parts, config.TAMSFlowProfiles...)
	parts = append(parts, rendererEpoch)
	return identityFingerprint("media-treatment/v1", parts...)
}

// ffmpegWritesOutput distinguishes probing from treatment. FFmpeg always
// writes when segmentation is enabled. With segmentation disabled it writes
// only to separate a multi-essence input; a whole-file muxed (or single-stream)
// ingest uploads the source bytes directly and must not depend on FFmpeg's
// installed version.
func ffmpegWritesOutput(config Config, probe media.Probe) bool {
	if config.SegmentDuration > 0 {
		return true
	}
	if config.EssenceStorage != media.EssenceStorageIndependent {
		return false
	}
	essences := 0
	for _, stream := range probe.Streams {
		if stream.Disposition.AttachedPicture == 0 {
			essences++
		}
	}
	return essences > 1
}

func (p *Pipeline) mediaToolchain(ctx context.Context) (string, string, error) {
	p.toolchainOnce.Do(func() {
		if p.segmenter == nil {
			p.toolchainErr = errors.New("media segmenter is required when FFmpeg writes output")
			return
		}
		release, err := p.acquireMediaProcess(ctx, 1)
		if err != nil {
			p.toolchainErr = fmt.Errorf("wait to inspect FFmpeg version: %w", err)
			return
		}
		toolchainReport, err := p.segmenter.Version(ctx)
		release()
		p.toolchainErr = err
		if p.toolchainErr != nil {
			p.toolchainErr = fmt.Errorf("read FFmpeg version for media provenance: %w", p.toolchainErr)
			return
		}
		toolchainReport = strings.TrimSpace(strings.ReplaceAll(toolchainReport, "\r\n", "\n"))
		if toolchainReport == "" {
			p.toolchainErr = errors.New("read FFmpeg version for media provenance: empty version report")
			return
		}
		p.toolchainVersion, _, _ = strings.Cut(toolchainReport, "\n")
		if err := media.ValidateToolVersion(p.toolchainVersion, "FFmpeg"); err != nil {
			p.toolchainErr = err
			return
		}
		p.toolchainFingerprint = mediaToolchainFingerprint(
			p.config.Profile, p.config.ProfileVersion, toolchainReport)
	})
	return p.toolchainVersion, p.toolchainFingerprint, p.toolchainErr
}

func (p *Pipeline) checkProbeToolchain(ctx context.Context) error {
	release, err := p.acquireMediaProcess(ctx, 1)
	if err != nil {
		return fmt.Errorf("wait to inspect FFprobe version: %w", err)
	}
	report, versionErr := p.prober.Version(ctx)
	release()
	if versionErr != nil {
		return fmt.Errorf("read FFprobe version: %w", versionErr)
	}
	line, _, _ := strings.Cut(strings.TrimSpace(strings.ReplaceAll(report, "\r\n", "\n")), "\n")
	return media.ValidateToolVersion(line, "FFprobe")
}

func mediaToolchainFingerprint(profile, profileVersion, ffmpegReport string) string {
	return identityFingerprint("media-toolchain/v1", profile, profileVersion, ffmpegReport)
}

// sourceIdentity derives a Source identifier from the content it stands for.
//
// A Source is the content; Flows are representations of it. Deriving the
// identifier from the location instead meant that reusing a path or an S3 key
// for an unrelated programme handed the new content the old Source, asserting
// that the two are the same thing in different renditions. Nothing about a
// filename supports that claim, and a store cannot tell afterwards that it was
// made in error.
//
// Content is the safer basis because it errs the other way: the same bytes
// segmented differently share a Source, which is what makes several renditions
// of one input hang together, while different bytes never silently inherit one.
// A genuine re-encode does produce different bytes and so a different Source,
// which is what --source-id is for -- an operator asserting an equivalence they
// know about and Tamsin cannot see.
//
// The same essence keeps one Source whether it was stored inside a multiplex or
// demultiplexed alongside it, because those are two representations of the same
// content and that is precisely what a Source is for.
func sourceIdentity(digest string, parts ...string) string {
	return namedID(append([]string{"source", digest}, parts...)...)
}

func namedID(parts ...string) string {
	// Source and Object IDs predate the length-framed Flow recipe. Preserve
	// their encoding so this Flow fix does not rotate an unchanged Source or the
	// Objects beneath an explicit --flow-id and thereby break resume.
	return uuid.NewSHA1(idNamespace, []byte(strings.Join(parts, "\x00"))).String()
}

func generatedLabel(digest string) string {
	const prefix = "Tamsin "
	if len(digest) > 12 {
		digest = digest[:12]
	}
	return prefix + digest
}
func mergeFlow(destination, override tams.Flow) {
	for key, value := range override {
		if key == "tags" {
			existing, existingOK := destination[key].(map[string]any)
			incoming, incomingOK := value.(map[string]any)
			if existingOK && incomingOK {
				for tag, tagValue := range incoming {
					existing[tag] = tagValue
				}
				continue
			}
		}
		destination[key] = value
	}
}

func durationRational(duration time.Duration) map[string]any {
	numerator := int64(duration)
	denominator := int64(time.Second)
	divisor := greatestCommonDivisor(numerator, denominator)
	return map[string]any{"numerator": numerator / divisor, "denominator": denominator / divisor}
}

func greatestCommonDivisor(left, right int64) int64 {
	for right != 0 {
		left, right = right, left%right
	}
	if left < 0 {
		return -left
	}
	return left
}

func safeFilename(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "." || name == string(filepath.Separator) || name == "" {
		return "input.bin"
	}
	return name
}

func safeURI(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "<unknown-input>"
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<invalid-input>"
	}
	// Userinfo and query strings are the standard credential-bearing parts of a
	// signed URL; fragments are local-only state and can contain secrets too.
	// None is needed once the bytes have been staged, so provenance keeps the
	// stable locator (scheme, authority, and path) and drops them entirely. This
	// applies defensively to every scheme even though the built-in file and S3
	// resolvers already reject those components.
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

// SafeInputURI returns the representation safe for structured output. It
// retains only the canonical scheme, authority, and path; userinfo,
// query material, and fragments are never persisted.
func SafeInputURI(raw string) string { return safeURI(raw) }
