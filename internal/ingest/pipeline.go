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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/contracts"
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
const rendererIdentityEpoch = "1"

type TAMSClient interface {
	Service(context.Context) (map[string]any, error)
	StorageBackends(context.Context) ([]tams.StorageBackend, error)
	Flow(context.Context, string) (tams.Flow, error)
	PutFlow(context.Context, string, tams.Flow) (tams.Flow, error)
	AllocateStorage(context.Context, string, tams.StorageRequest) (tams.StorageResponse, error)
	RegisterSegment(context.Context, string, tams.SegmentRequest) error
	RegisterSegments(context.Context, string, []tams.SegmentRequest) error
	DeleteSegments(context.Context, string, tams.SegmentDeleteOptions) error
	ListSegments(context.Context, string, tams.SegmentListOptions) ([]tams.Segment, error)
	Object(context.Context, string) (tams.ObjectInfo, error)
	UploadFile(context.Context, tams.PresignedURL, string) (tams.UploadReceipt, error)
	DownloadDigest(context.Context, tams.PresignedURL) (int64, string, error)
}

// collectedFlow is a mono-essence Flow with its assigned identifier, ready to
// register ahead of the multi-essence Flow that collects it.
type collectedFlow struct {
	id               string
	sourceID         string
	role             string
	flow             tams.Flow
	containerMapping map[string]any
}

// graphFlow describes one member of the complete empty Flow graph which must
// exist before any Media Object is allocated. ownsMedia is a cross-Flow
// invariant: direct owners declare container, while association-only members
// do not.
type graphFlow struct {
	id               string
	role             string
	flow             tams.Flow
	ownsMedia        bool
	containerMapping map[string]any
}

type flowGraph struct {
	flows       []graphFlow
	collectorID string
	storage     media.EssenceStorage
}

type plannedFlowWrite struct {
	member    graphFlow
	effective tams.Flow
	existed   bool
	changed   bool
}

type Config struct {
	// Observability carries the CLI invocation's correlation ID and operational
	// metrics. When omitted, direct library callers receive an isolated run.
	Observability *observability.Run
	// Profile and ProfileVersion name the resolved media-treatment contract.
	// The CLI resolves named profiles and deliberate overrides before building
	// a Pipeline; direct callers that omit them are reported as custom@1.
	Profile           string
	ProfileVersion    string
	LifecycleObserver LifecycleObserver
	// RetainObjectResults opts direct library callers into an in-memory copy of
	// every terminal Object. The default retains only action-required recovery
	// identifiers; process consumers use lifecycle events or the durable journal
	// for clean per-Object detail.
	RetainObjectResults bool
	Concurrency         int
	// Transfers bounds Media Object uploads and verification downloads in
	// flight across the whole run, not per input. A single large input and a
	// thousand small ones should both saturate the same budget, which is why
	// this is separate from Concurrency: nesting one inside the other would
	// multiply, and bounding transfers by input count would leave one big file
	// entirely serial.
	Transfers int
	// ProbeConcurrency bounds media measurement — hashing a Segment and running
	// ffprobe over it — across the whole run. It is separate from Transfers
	// because the two contend for different resources: transfers wait on the
	// network, measurements spawn processes and read local disk, so the right
	// number for one is rarely right for the other.
	ProbeConcurrency int
	// Retries is the shared allowance for an operation that failed in a way
	// worth trying again, including resuming a broken source transfer. It is
	// the same number --retries gives the HTTP client, so an operator raising
	// or lowering it changes the whole run rather than one layer of it.
	Retries          int
	DryRun           bool
	Verify           bool
	DryRunMode       DryRunMode
	VerificationMode VerificationMode
	TempDirectory    string
	// StagingByteBudget is the global number of temporary bytes concurrent
	// inputs may reserve. Zero derives a safe budget from free space in
	// TempDirectory; negative values are invalid.
	StagingByteBudget int64
	SegmentDuration   time.Duration
	SegmentFormat     media.SegmentFormat
	EssenceStorage    media.EssenceStorage
	FFmpegArgs        []string
	Start             int64
	StorageID         string
	FlowID            string
	SourceID          string
	FlowMetadata      tams.Flow
}

type Pipeline struct {
	config Config
	runID  string
	// reporter presents progress to an operator. It is Discard unless the CLI
	// decided a terminal is watching, so the pipeline itself stays unaware of
	// whether anything is being rendered.
	reporter progress.Reporter
	// transfers is the global budget. Acquiring a slot is what bounds
	// concurrency; goroutines are cheap, waiting on the network is not.
	transfers chan struct{}
	// probes bounds media measurement globally. Limiting it per Flow multiplied
	// by the number of Flows prepared at once.
	probes chan struct{}
	// mediaProcesses is shared by FFprobe and FFmpeg. Ordinary probes and
	// stream-copy renders take one token; custom FFmpeg work takes both.
	mediaProcesses *semaphore.Weighted
	// graphLocks serializes inputs that converge on the same generated Flow
	// graph. URI-based identities happened to keep differently located copies
	// apart; content-based identities deliberately do not, so two workers in one
	// batch must not allocate and register the same deterministic Objects at the
	// same time.
	graphLocksMu sync.Mutex
	graphLocks   map[string]*graphLock
	// limits are what the store says about how long the things it hands out
	// last. Set once before any work starts, then only read.
	limits               tams.ServiceLimits
	client               TAMSClient
	prober               media.Prober
	segmenter            media.Segmenter
	logger               *slog.Logger
	baseLogger           *slog.Logger
	observability        *observability.Run
	ownsObservability    bool
	toolchainOnce        sync.Once
	toolchainVersion     string
	toolchainFingerprint string
	toolchainErr         error
	// One deadline covers recovery of an entire ambiguous bulk registration;
	// it is not renewed for each Object in the batch.
	registrationRecoveryTimeout time.Duration
	// One deadline covers retraction after ordinary verification failures. It
	// starts at the first failure and is shared by every Object in that batch,
	// so a stalled service cannot multiply shutdown time by Object count.
	verificationRecoveryTimeout time.Duration
	// staging is initialised at Run time so free-space preflight sees the
	// filesystem the job will actually use.
	staging *stagingManager
}

type ObjectStatus string

const (
	ObjectStatusPlanned                 ObjectStatus = "planned"
	ObjectStatusUploaded                ObjectStatus = "uploaded"
	ObjectStatusRegistered              ObjectStatus = "registered"
	ObjectStatusVerified                ObjectStatus = "verified"
	ObjectStatusResumed                 ObjectStatus = "resumed"
	ObjectStatusIngested                ObjectStatus = "ingested"
	ObjectStatusRetractionIndeterminate ObjectStatus = "registration-indeterminate"
	ObjectStatusRejected                ObjectStatus = "registration-rejected"
	ObjectStatusRetracted               ObjectStatus = "retracted"
	ObjectStatusStranded                ObjectStatus = "stranded"
)

type ObjectResult struct {
	ObjectID           string                   `json:"object_id"`
	Timerange          string                   `json:"timerange"`
	Bytes              int64                    `json:"bytes"`
	SHA256             string                   `json:"sha256"`
	Disposition        ObjectDisposition        `json:"disposition"`
	Verification       ObjectVerificationStatus `json:"verification_status"`
	VerificationMethod VerificationMethod       `json:"verification_method"`
	// Status remains an internal state-machine projection while v2 exposes
	// disposition and verification independently.
	Status   ObjectStatus `json:"-"`
	reported bool
}

type ObjectDisposition string

const (
	ObjectDispositionPlanned                   ObjectDisposition = "planned"
	ObjectDispositionUploaded                  ObjectDisposition = "uploaded"
	ObjectDispositionRegistrationIndeterminate ObjectDisposition = "registration_indeterminate"
	ObjectDispositionRegistered                ObjectDisposition = "registered"
	ObjectDispositionRejected                  ObjectDisposition = "rejected"
	ObjectDispositionIngested                  ObjectDisposition = "ingested"
	ObjectDispositionResumed                   ObjectDisposition = "resumed"
	ObjectDispositionRetracted                 ObjectDisposition = "retracted"
	ObjectDispositionStranded                  ObjectDisposition = "stranded"
	ObjectDispositionUnattempted               ObjectDisposition = "unattempted"
)

type ObjectVerificationStatus string

const (
	ObjectVerificationVerified     ObjectVerificationStatus = "verified"
	ObjectVerificationNotRequested ObjectVerificationStatus = "not_requested"
	ObjectVerificationNotReached   ObjectVerificationStatus = "not_reached"
	ObjectVerificationFailed       ObjectVerificationStatus = "failed"
)

type VerificationMethod string

const (
	VerificationMethodNone     VerificationMethod = "none"
	VerificationMethodStorage  VerificationMethod = "storage"
	VerificationMethodReadback VerificationMethod = "readback"
)

type ObjectSummary struct {
	Total            int   `json:"total"`
	Bytes            int64 `json:"bytes"`
	Ingested         int   `json:"ingested"`
	Resumed          int   `json:"resumed"`
	Rejected         int   `json:"rejected"`
	Retracted        int   `json:"retracted"`
	Stranded         int   `json:"stranded"`
	Unattempted      int   `json:"unattempted"`
	Verified         int   `json:"verified"`
	StorageVerified  int   `json:"storage_verified"`
	ReadbackVerified int   `json:"readback_verified"`
}

// FlowResult reports one Flow produced from an input. Every Flow appears once
// in Result.Flows; Result.RootFlowID points at the Flow which represents the
// input as a whole rather than duplicating it at the Result level.
type FlowResult struct {
	FlowID        string          `json:"flow_id"`
	SourceID      string          `json:"source_id"`
	Role          string          `json:"role,omitempty"`
	Disposition   FlowDisposition `json:"disposition"`
	ObjectSummary ObjectSummary   `json:"object_summary"`
	Objects       []ObjectResult  `json:"-"`
	// Kind prevents terminal consumers from inferring media ownership from an
	// empty role or Object count.
	Kind FlowKind `json:"kind"`
}

// FlowDisposition reports what this invocation knows about each planned Flow
// mutation. A failed PUT is indeterminate because TAMS may have committed the
// replacement before the response was lost; later graph members are then
// unattempted rather than misleadingly described as created.
type FlowDisposition string

const (
	FlowPlanned       FlowDisposition = "planned"
	FlowUnchanged     FlowDisposition = "unchanged"
	FlowWritten       FlowDisposition = "written"
	FlowIndeterminate FlowDisposition = "indeterminate"
	FlowUnattempted   FlowDisposition = "unattempted"
)

type ResultStatus string

const (
	ResultStatusPlanned  ResultStatus = "planned"
	ResultStatusIngested ResultStatus = "ingested"
	ResultStatusResumed  ResultStatus = "resumed"
	ResultStatusFailed   ResultStatus = "failed"
)

type VerificationStatus string

const (
	VerificationVerified        VerificationStatus = "verified"
	VerificationNotRequested    VerificationStatus = "not_requested"
	VerificationNotReached      VerificationStatus = "not_reached"
	VerificationFailedRetracted VerificationStatus = "failed_retracted"
	VerificationFailedStranded  VerificationStatus = "failed_stranded"

	// ResultSchemaVersion changes for any shape, enum, or required-field change:
	// the strict schema rejects unknown properties, so even an optional field
	// cannot be added under the same version. Profile versions are carried by
	// each result and batch independently of the executable and result schema.
	ResultSchemaVersion = "2.0"
)

// Failure is the stable, disclosure-safe terminal explanation shared by the
// journal, human receipt, and process event adapters. It never contains raw
// provider bodies, URLs, headers, FFmpeg commands, or arbitrary error text.
type Failure struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	ActionRequired bool   `json:"action_required"`
}

type Result struct {
	Input          string             `json:"input"`
	Profile        string             `json:"profile"`
	ProfileVersion string             `json:"profile_version"`
	FFmpegVersion  string             `json:"ffmpeg_version,omitempty"`
	MediaToolchain string             `json:"media_toolchain,omitempty"`
	RootFlowID     string             `json:"root_flow_id,omitempty"`
	Bytes          int64              `json:"bytes,omitempty"`
	SHA256         string             `json:"sha256,omitempty"`
	Status         ResultStatus       `json:"status"`
	Verification   VerificationStatus `json:"verification"`
	Flows          []FlowResult       `json:"flows"`
	Failure        *Failure           `json:"failure,omitempty"`
	// Error retains detailed in-process diagnostics for direct internal callers
	// and tests. It is deliberately excluded from every serialized or rendered
	// contract; external process consumers use Failure instead.
	Error string `json:"-"`
}

func (r *Result) rootFlow() *FlowResult {
	for index := range r.Flows {
		if r.Flows[index].FlowID == r.RootFlowID {
			return &r.Flows[index]
		}
	}
	return nil
}

type BatchResult struct {
	SchemaVersion  string   `json:"schema_version"`
	ToolVersion    string   `json:"tool_version"`
	ToolCommit     string   `json:"tool_commit"`
	ToolBuildDate  string   `json:"tool_build_date,omitempty"`
	ProfileVersion string   `json:"profile_version"`
	RunID          string   `json:"run_id"`
	Results        []Result `json:"results"`
	Succeeded      int      `json:"succeeded"`
	Failed         int      `json:"failed"`
}

type ResultContract struct {
	SchemaVersion  string
	ToolVersion    string
	ToolCommit     string
	ToolBuildDate  string
	ProfileVersion string
	RunID          string
}

type ResultObserver func(index int, result Result) error

func New(config Config, client TAMSClient, prober media.Prober, segmenter media.Segmenter, logger *slog.Logger, reporter progress.Reporter) (*Pipeline, error) {
	if config.DryRunMode == "" {
		if config.DryRun {
			config.DryRunMode = DryRunExact
		} else {
			config.DryRunMode = DryRunOff
		}
	}
	if err := config.DryRunMode.Validate(); err != nil {
		return nil, err
	}
	config.DryRun = config.DryRunMode != DryRunOff
	if config.VerificationMode == "" {
		if config.Verify {
			config.VerificationMode = VerificationReadback
		} else {
			config.VerificationMode = VerificationNone
		}
	}
	if err := config.VerificationMode.Validate(); err != nil {
		return nil, err
	}
	config.Verify = config.VerificationMode != VerificationNone
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
	if !config.DryRun && client == nil {
		return nil, errors.New("TAMS client is required unless dry-run is enabled")
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
	baseLogger := logger
	run := config.Observability
	ownsObservability := run == nil
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
		observability: run, baseLogger: baseLogger, ownsObservability: ownsObservability,
		reporter: reporter, transfers: make(chan struct{}, config.Transfers),
		probes: make(chan struct{}, config.ProbeConcurrency), mediaProcesses: semaphore.NewWeighted(2),
		graphLocks:                  make(map[string]*graphLock),
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
func (p *Pipeline) RunObserved(ctx context.Context, items []source.Item, observe ResultObserver) (BatchResult, error) {
	// Media provenance belongs to this invocation. A Pipeline is deliberately
	// reusable, but its configured executable may have been patched between
	// runs, and a transient version lookup failure in one run must not poison
	// every later run. mediaToolchain still uses sync.Once to keep the lookup to
	// one process when several inputs in this invocation need FFmpeg.
	defer p.resetMediaToolchain()
	// ResultContract may have been read before this call so a journal could make
	// its start record durable. Direct library callers that did not bind an
	// invocation-wide observer get a fresh isolated run on deliberate Pipeline
	// reuse. The CLI supplies its own observer, generated before source
	// resolution, and owns that invocation's lifetime itself.
	if p.ownsObservability {
		defer func() {
			p.observability = observability.New(uuid.NewString(), p.baseLogger)
			p.runID = p.observability.RunID()
			p.logger = p.observability.Logger()
		}()
	}
	if len(items) == 0 {
		return p.newBatch([]Result{}), errors.New("no source items resolved")
	}
	if (p.config.FlowID != "" || p.config.SourceID != "") && len(items) != 1 {
		return p.failAll(items, errors.New("explicit flow-id and source-id may only be used with one resolved input"), observe)
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
	if !p.config.DryRun {
		resolvedStorageID, err := p.runStartupPreflight(ctx)
		if err != nil {
			return p.failAll(items, err, observe)
		}
		storageID = resolvedStorageID
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
				if !p.config.DryRun {
					phases := []progress.Phase{progress.PhaseStore}
					if p.config.Verify {
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
				} else if p.config.Verify && !p.config.DryRun {
					result.Verification = VerificationVerified
				} else if p.config.Verify {
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
	// terminal result. This keeps the batch and a graceful-interruption journal
	// index-complete, while a hard kill retains every result synced before it.
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
		result.Failure = describeFailure(result, cause)
	} else if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		result.Failure = interruptedFailure()
	} else {
		result.Failure = describeFailure(result, cause)
	}
	return result
}

func (p *Pipeline) verificationFailureStatus(err error) VerificationStatus {
	if !p.config.Verify {
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
	if p.config.Verify {
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

	// Stream copy carries the coded essence through untouched, so the Flow is
	// the same generation as its source. An explicit FFmpeg profile may re-encode,
	// and nothing here can tell whether a given argument list does; rather than
	// assert a generation that might be wrong, leave it unset for the operator
	// to supply through --flow-metadata.
	if len(p.config.FFmpegArgs) == 0 {
		flow["generation"] = 0
		for _, collected := range flowInfo.Collected {
			collected.Flow["generation"] = 0
		}
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
	if p.config.DryRun {
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

type stagedFile struct {
	path   string
	size   int64
	sha256 string
	// owned reports whether the file at path was created by this run. A staged
	// copy of a remote input is ours alone and cannot change underneath us; a
	// local input belongs to whoever is running Tamsin and may be rewritten at
	// any point, so the two cannot be treated alike.
	owned   bool
	cleanup func()
	lease   *stagingLease
}

func stage(ctx context.Context, item source.Item, tempRoot string, retries int, lease *stagingLease,
	run *observability.Run) (stagedFile, error) {
	if item.Open == nil {
		return stagedFile{}, errors.New("source item has no opener")
	}
	input, err := item.Open(ctx)
	if err != nil {
		return stagedFile{}, err
	}

	if item.LocalPath != "" {
		hash := sha256.New()
		size, err := copyContext(ctx, hash, input)
		_ = input.Close()
		if err != nil {
			return stagedFile{}, fmt.Errorf("hash input %q: %w", item.LocalPath, err)
		}
		if item.Size >= 0 && size != item.Size {
			return stagedFile{}, fmt.Errorf("input %q changed size while being read: expected %d, got %d", item.LocalPath, item.Size, size)
		}
		return stagedFile{path: item.LocalPath, size: size, sha256: hex.EncodeToString(hash.Sum(nil)), cleanup: func() {}, lease: lease}, nil
	}

	directory, err := os.MkdirTemp(tempRoot, "tamsin-input-")
	if err != nil {
		_ = input.Close()
		return stagedFile{}, fmt.Errorf("create input staging directory: %w", err)
	}
	var stagedBytes int64
	cleanup := func() {
		_ = os.RemoveAll(directory)
		lease.subtract(stagedBytes)
		stagedBytes = 0
	}
	filename := filepath.Join(directory, safeFilename(item.Name))
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = input.Close()
		cleanup()
		return stagedFile{}, fmt.Errorf("create staged input: %w", err)
	}
	// A transfer that dies partway is resumed rather than restarted. Staging a
	// large input is often the longest part of an ingest, and losing an hour of
	// it to a dropped connection at the end is the difference between a retry
	// that costs seconds and one that costs the whole transfer again.
	hash := sha256.New()
	var (
		size     int64
		copyErr  error
		attempts int
	)
	for attempt := 0; ; attempt++ {
		attempts = attempt + 1
		var written int64
		written, copyErr = copyContext(ctx, io.MultiWriter(stagingWriter{lease: lease, destination: output}, hash), input)
		size += written
		stagedBytes = size
		_ = input.Close()
		if copyErr == nil {
			break
		}
		// A source that cannot be reopened, a cancelled run, and an exhausted
		// allowance all mean the failure stands.
		if ctx.Err() != nil || !worthResuming(copyErr, item.Reopen != nil, attempt, retries) {
			break
		}
		run.Retry(observability.OperationSourceTransfer, attempts+1, retries+1, 0, copyErr, 0)
		resumed, continuing, reopenErr := item.Reopen(ctx, size)
		if reopenErr != nil {
			copyErr = errors.Join(copyErr, reopenErr)
			break
		}
		if !continuing {
			// The source is answering from the beginning, so what has been
			// written is not a prefix of what is about to arrive and every
			// record of it has to go: the file, the digest, and the count.
			if _, err := output.Seek(0, io.SeekStart); err != nil {
				_ = resumed.Close()
				copyErr = errors.Join(copyErr, err)
				break
			}
			if err := output.Truncate(0); err != nil {
				_ = resumed.Close()
				copyErr = errors.Join(copyErr, err)
				break
			}
			hash.Reset()
			lease.subtract(size)
			size = 0
			stagedBytes = 0
		}
		input = resumed
	}
	closeErr := output.Close()
	if copyErr != nil {
		cleanup()
		return stagedFile{}, fmt.Errorf("stage input %s failed after %d attempt(s): %w", safeURI(item.URI), attempts, copyErr)
	}
	if closeErr != nil {
		cleanup()
		return stagedFile{}, fmt.Errorf("close staged input: %w", closeErr)
	}
	if item.Size >= 0 && size != item.Size {
		cleanup()
		return stagedFile{}, fmt.Errorf("input %s size mismatch: expected %d, got %d", safeURI(item.URI), item.Size, size)
	}
	return stagedFile{
		path: filename, size: size, sha256: hex.EncodeToString(hash.Sum(nil)),
		owned: true, cleanup: cleanup, lease: lease,
	}, nil
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
		if len(p.config.FFmpegArgs) == 0 {
			flow["generation"] = 0
		}
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
	if len(p.config.FFmpegArgs) == 0 {
		collector["generation"] = 0
	}

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
	if p.config.DryRun {
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
	if p.config.Verify {
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
	if assessment.Relationship == "unknown" {
		// The property is required, but a store that omits it is not thereby
		// unusable, and refusing to work with one would be a stricter rule than
		// the specification asks of a client.
		p.logger.Warn("could not determine the TAMS API version of this store", "cause", assessment.Warning)
	}
	if err != nil {
		return err
	}
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

// planFlowGraph resolves every final effective Flow before writing any of
// them. That means schema and association failures cannot leave a prefix of the
// graph in the service, and it makes preservation decisions from one coherent
// read of the graph rather than interleaving reads with replacements.
func (p *Pipeline) planFlowGraph(ctx context.Context, graph flowGraph) ([]plannedFlowWrite, error) {
	planned := make([]plannedFlowWrite, 0, len(graph.flows))
	for _, member := range graph.flows {
		plan, err := p.planFlowWrite(ctx, member)
		if err != nil {
			return nil, err
		}
		if err := contracts.ValidateFlow(plan.effective); err != nil {
			return nil, fmt.Errorf(
				"final Flow metadata for %s (%s) is not valid against pinned TAMS %d.%d at %w",
				member.id, member.role, tams.SpecMajor, tams.SpecMinor, err)
		}
		planned = append(planned, plan)
	}
	if err := validateFlowGraph(graph, planned); err != nil {
		return nil, fmt.Errorf("final Flow graph is not valid: %w", err)
	}
	return planned, nil
}

// planFlowWrite applies the ownership rule without mutating TAMS. A PUT
// replaces a Flow, so the final value must preserve everything this run does
// not own. The dry-run path has no store to read and validates the generated
// value as-is.
func (p *Pipeline) planFlowWrite(ctx context.Context, member graphFlow) (plannedFlowWrite, error) {
	plan := plannedFlowWrite{member: member, effective: member.flow, changed: true}
	if p.config.DryRun {
		return plan, nil
	}
	existing, err := p.client.Flow(ctx, member.id)
	if err != nil {
		var httpErr *tams.HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
			// Writing anyway would risk replacing metadata that is there but
			// could not be read, and that cannot be undone.
			return plannedFlowWrite{}, fmt.Errorf("read flow %s before planning the Flow graph: %w", member.id, err)
		}
		return plan, nil
	}

	plan.existed = true
	plan.effective = preserveForeignMetadata(existing, member.flow, p.config.FlowMetadata)
	plan.changed = !reflect.DeepEqual(plan.effective, existing)
	return plan, nil
}

// writeFlow commits one already validated member through the same
// ownership-aware path for elemental Flows and collectors alike.
func (p *Pipeline) writeFlow(ctx context.Context, plan plannedFlowWrite) error {
	if !plan.changed {
		p.logger.Debug("flow already describes this ingest", "flow_id", plan.member.id)
		return nil
	}
	action, verb := "creating", "create"
	if plan.existed {
		action, verb = "updating", "update"
	}
	p.logger.Info(action+" flow", "flow_id", plan.member.id, "role", plan.member.role)
	if _, err := p.client.PutFlow(ctx, plan.member.id, plan.effective); err != nil {
		return fmt.Errorf("%s flow %s: %w", verb, plan.member.id, err)
	}
	return nil
}

// commitFlowGraph writes children before their collector, as required by the
// Collection Item schema. TAMS has no transaction spanning Flow PUTs. A failed
// PUT can therefore leave an unavoidable partial metadata mutation (including
// the ambiguous case where the service committed but the response was lost),
// which is reported explicitly. No Object has been allocated at this point.
func (p *Pipeline) commitFlowGraphObserved(ctx context.Context, planned []plannedFlowWrite, results []FlowResult) error {
	for _, plan := range planned {
		if !plan.changed {
			setFlowDisposition(results, plan.member.id, FlowUnchanged)
		}
	}
	written := make([]string, 0, len(planned))
	for index, plan := range planned {
		if err := p.writeFlow(ctx, plan); err != nil {
			setFlowDisposition(results, plan.member.id, FlowIndeterminate)
			for _, pending := range planned[index+1:] {
				if pending.changed {
					setFlowDisposition(results, pending.member.id, FlowUnattempted)
				}
			}
			confirmed := "no earlier Flow write was required"
			if len(written) > 0 {
				confirmed = "Flows written before the failure: " + strings.Join(written, ", ")
			}
			return fmt.Errorf(
				"flow graph may be partially written (%s; the failing PUT may also have committed); no Media Objects were allocated: %w",
				confirmed, err)
		}
		if plan.changed {
			written = append(written, plan.member.id)
			setFlowDisposition(results, plan.member.id, FlowWritten)
		}
	}
	return nil
}

func setFlowDisposition(results []FlowResult, flowID string, disposition FlowDisposition) {
	for index := range results {
		if results[index].FlowID == flowID {
			results[index].Disposition = disposition
			return
		}
	}
}

func validateFlowGraph(graph flowGraph, planned []plannedFlowWrite) error {
	if len(planned) != len(graph.flows) || len(planned) == 0 {
		return errors.New("/ must contain every planned Flow")
	}
	byID := make(map[string]plannedFlowWrite, len(planned))
	for _, plan := range planned {
		member := plan.member
		if byID[member.id].member.id != "" {
			return fmt.Errorf("/id duplicates Flow %s", member.id)
		}
		byID[member.id] = plan
		if plan.effective["id"] != member.id {
			return fmt.Errorf("/id for Flow %s must equal its request identifier", member.id)
		}
		container, hasContainer := plan.effective["container"].(string)
		if member.ownsMedia && (!hasContainer || container == "") {
			return fmt.Errorf("/container for media-owning Flow %s is required", member.id)
		}
		if !member.ownsMedia {
			if _, present := plan.effective["container"]; present {
				return fmt.Errorf("/container for association-only Flow %s must be absent", member.id)
			}
		}
		if _, present := plan.effective["container_mapping"]; present {
			return fmt.Errorf("/container_mapping for Flow %s must be on its parent Collection Item", member.id)
		}
	}

	if graph.collectorID == "" {
		if len(planned) != 1 {
			return errors.New("/flow_collection is missing a collector for multiple Flows")
		}
		if _, present := planned[0].effective["flow_collection"]; present {
			return errors.New("/flow_collection must be absent for a single Flow")
		}
		return nil
	}

	collector, present := byID[graph.collectorID]
	if !present {
		return fmt.Errorf("/flow_collection collector %s is missing", graph.collectorID)
	}
	if collector.effective["format"] != "urn:x-nmos:format:multi" {
		return fmt.Errorf("/format for collector %s must be urn:x-nmos:format:multi", graph.collectorID)
	}
	items, ok := collector.effective["flow_collection"].([]map[string]any)
	if !ok {
		return fmt.Errorf("/flow_collection for collector %s must be an array", graph.collectorID)
	}
	expected := make([]graphFlow, 0, len(graph.flows)-1)
	for _, member := range graph.flows {
		if member.id != graph.collectorID {
			expected = append(expected, member)
			if _, present := byID[member.id].effective["flow_collection"]; present {
				return fmt.Errorf("/flow_collection must be absent from collected Flow %s", member.id)
			}
		}
	}
	if len(items) != len(expected) {
		return fmt.Errorf("/flow_collection has %d items, want %d", len(items), len(expected))
	}
	for index, member := range expected {
		item := items[index]
		base := fmt.Sprintf("/flow_collection/%d", index)
		if item["id"] != member.id {
			return fmt.Errorf("%s/id = %v, want %s", base, item["id"], member.id)
		}
		if item["role"] != member.role {
			return fmt.Errorf("%s/role = %v, want %s", base, item["role"], member.role)
		}
		mapping, hasMapping := item["container_mapping"]
		switch graph.storage {
		case media.EssenceStorageIndependent:
			if hasMapping {
				return fmt.Errorf("%s/container_mapping must be absent after demultiplexing", base)
			}
		case media.EssenceStorageMuxed, "":
			if member.containerMapping == nil || !hasMapping || !reflect.DeepEqual(mapping, member.containerMapping) {
				return fmt.Errorf("%s/container_mapping does not match the input track", base)
			}
		default:
			return fmt.Errorf("/ uses unsupported essence storage %q", graph.storage)
		}
	}
	return nil
}

// descriptiveFields are written when a Flow is created and then left alone.
//
// Everything else Tamsin generates describes the media -- codec, container,
// essence parameters, bit rates -- and has to stay accurate, so a later run
// updates it. These two describe the content to a person, and a person may well
// have improved on the neutral generated values. Overwriting a curated label on
// every resume would be its own kind of data loss.
//
// An operator can still set them deliberately through --flow-metadata, which is
// an instruction rather than a by-product.
var descriptiveFields = [...]string{"label", "description"}

// preserveForeignMetadata overlays what this run generated onto what the store
// already holds, keeping anything the run does not own.
//
// A tag Tamsin writes carries its own prefix, so one already in the store under
// that prefix but absent from this run is a leftover of Tamsin's own and is
// dropped. Anything else belongs to somebody, and is kept.
func preserveForeignMetadata(existing, generated, operatorOverrides tams.Flow) tams.Flow {
	merged := make(tams.Flow, len(existing)+len(generated))
	for key, value := range existing {
		merged[key] = value
	}
	// These fields describe which Flow owns Media Objects and how the graph is
	// connected. Their absence is meaningful, so merely overlaying generated
	// values would preserve a stale arrangement across a muxed/independent
	// rewrite. They are always Tamsin-owned and operator overrides are rejected.
	for _, field := range [...]string{"container", "flow_collection", "container_mapping"} {
		if _, generatedHere := generated[field]; !generatedHere {
			delete(merged, field)
		}
	}
	for key, value := range generated {
		merged[key] = value
	}
	for _, field := range descriptiveFields {
		if _, asked := operatorOverrides[field]; asked {
			continue
		}
		if value, present := existing[field]; present {
			merged[field] = value
		}
	}

	existingTags, hasExisting := existing["tags"].(map[string]any)
	if !hasExisting {
		return merged
	}
	generatedTags, _ := generated["tags"].(map[string]any)
	sources := make(map[string]struct{})
	for _, tagSet := range []map[string]any{existingTags, generatedTags} {
		collectProvenanceSources(sources, tagSet[media.ProvenanceSourcesTag])
		// Migrate the singular tag written by earlier builds when this Flow is
		// next touched, without losing where that ingest came from.
		collectProvenanceSources(sources, tagSet[media.TagPrefix+"source"])
	}
	tags := make(map[string]any, len(existingTags)+len(generatedTags))
	for name, value := range existingTags {
		if strings.HasPrefix(name, media.TagPrefix) {
			continue
		}
		tags[name] = value
	}
	for name, value := range generatedTags {
		tags[name] = value
	}
	delete(tags, media.TagPrefix+"source")
	if len(sources) > 0 {
		ordered := make([]string, 0, len(sources))
		for source := range sources {
			ordered = append(ordered, source)
		}
		sort.Strings(ordered)
		tags[media.ProvenanceSourcesTag] = ordered
	}
	merged["tags"] = tags
	return merged
}

func collectProvenanceSources(destination map[string]struct{}, value any) {
	add := func(source string) {
		if source = strings.TrimSpace(source); source != "" {
			destination[source] = struct{}{}
		}
	}
	switch sources := value.(type) {
	case string:
		add(sources)
	case []string:
		for _, source := range sources {
			add(source)
		}
	case []any:
		for _, source := range sources {
			if text, ok := source.(string); ok {
				add(text)
			}
		}
	}
}

// chunkSize decides how many Media Objects to commit together.
//
// The outer bound is the Object lifetime the store advertised: an Object is
// collected if it is not registered in time, and that clock does not care how
// many uploads run at once. commitChunk may divide this further into ready-worker
// microbatches so the shorter presigned-URL lifetime is honoured as well.
//
// Half the advertised lifetime is used, leaving the other half as margin for a
// batch that turns out slower than the one before it. A store that advertises
// no lifetime has not told us to divide the work, so it is not divided.
func (p *Pipeline) chunkSize(remaining []preparedObject, throughput float64) int {
	if p.limits.ObjectRegistration <= 0 {
		return len(remaining)
	}
	if throughput <= 0 {
		throughput = assumedThroughput
	}
	budget := p.limits.ObjectRegistration.Seconds() / 2 * throughput
	var bytes float64
	fits := 0
	for _, object := range remaining {
		bytes += float64(object.size)
		// At least one Object goes in every batch: an Object too large to fit
		// the budget on its own cannot be made smaller, and is warned about
		// when its upload is estimated.
		if fits > 0 && bytes > budget {
			break
		}
		fits++
	}
	return min(max(fits, 1), len(remaining))
}

// outlastsURL reports how long an upload is expected to take and whether that
// is longer than the URL it will be sent to remains valid.
//
// This is the case batching cannot help with. Committing in smaller batches
// bounds how long an Object waits before its turn, but a single Object large
// enough takes longer than its URL lasts however few of them are in flight, and
// the upload then fails against a URL that no retry can revive. Naming the
// Object beats leaving an expiry to be inferred from a rejected PUT.
//
// It answers false whenever it does not know: an unmeasured rate or an
// unadvertised lifetime is not evidence of a problem, and guessing would put a
// warning in front of an operator who can do nothing with it.
func outlastsURL(size int64, throughput float64, lifetime time.Duration) (time.Duration, bool) {
	if lifetime <= 0 || throughput <= 0 || size <= 0 {
		return 0, false
	}
	expected := time.Duration(float64(size) / throughput * float64(time.Second))
	return expected, expected > lifetime
}

// chunkTimerange covers the Segments in one registration operation, so an
// ambiguous write can be reconciled without listing the whole Flow.
func chunkTimerange(chunk []preparedObject) string {
	first, last := chunk[0].start, chunk[0].start+chunk[0].duration
	for _, object := range chunk[1:] {
		first = min(first, object.start)
		last = max(last, object.start+object.duration)
	}
	timerange, err := media.TimeRange(first, last-first)
	if err != nil {
		// Listing the whole Flow is wasteful but correct, and a batch that
		// cannot describe its own span is not a reason to fail an ingest.
		return ""
	}
	return timerange
}

// commitChunk brings one batch of Media Objects into the store: storage
// allocated, bytes uploaded, Segments registered, and each one verified or
// withdrawn before the next batch starts. Keeping that whole cycle inside one
// batch is what bounds how long an Object sits unregistered.
//
// It reports how long the batch took and how many bytes it moved, so the next
// can be sized from what this one achieved rather than from a guess.
func (p *Pipeline) commitChunk(ctx context.Context, flowID string, chunk []preparedObject,
	objectResults []ObjectResult, storageID string, throughput float64) (time.Duration, int64, error) {
	if p.limits.PresignedURL <= 0 {
		return p.commitReadyChunk(ctx, flowID, chunk, objectResults, storageID, throughput, nil)
	}

	// Storage allocation creates every PUT URL in its response. Reserve the
	// workers that will consume them first, and ask for no more URLs than can
	// begin immediately. Each microbatch completes registration and verification
	// before the next allocation, preserving the Object-registration state
	// machine as well as both advertised lifetimes.
	started := time.Now()
	var transferred int64
	for offset := 0; offset < len(chunk); {
		remaining := chunk[offset:]
		reservation, err := p.reserveTransferBatch(ctx, min(len(remaining), max(p.config.Transfers, 1)))
		if err != nil {
			return 0, 0, fmt.Errorf("wait for an upload worker: %w", err)
		}
		ready := remaining[:min(len(remaining), reservation.count)]
		_, bytes, err := p.commitReadyChunk(
			ctx, flowID, ready, objectResults, storageID, throughput, reservation)
		if err != nil {
			return 0, 0, err
		}
		transferred += bytes
		offset += len(ready)
	}
	return time.Since(started), transferred, nil
}

// commitReadyChunk consumes one batch whose upload workers are already
// reserved. A nil reservation retains the defensive fallback for tests and
// clients that have no advertised presigned-URL lifetime.
func (p *Pipeline) commitReadyChunk(ctx context.Context, flowID string, chunk []preparedObject,
	objectResults []ObjectResult, storageID string, throughput float64,
	reservation *transferReservation) (time.Duration, int64, error) {
	started := time.Now()
	if reservation != nil {
		defer reservation.releaseAll()
	}
	var transferred int64
	objectIDs := make([]string, 0, len(chunk))
	for _, object := range chunk {
		transferred += object.size
		objectIDs = append(objectIDs, object.id)
	}

	allocation, err := p.client.AllocateStorage(ctx, flowID, tams.StorageRequest{
		ObjectIDs: objectIDs, StorageID: storageID,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("allocate storage for %d objects: %w", len(chunk), err)
	}
	destinations := make(map[string]tams.PresignedURL, len(allocation.MediaObjects))
	var uploadStartBefore time.Time
	if p.limits.PresignedURL > 0 {
		// The service exposes a minimum duration, not an absolute expiry. Measure
		// it from response receipt; the schema asks services to leave grace for
		// URL generation and response latency.
		uploadStartBefore = time.Now().Add(p.limits.PresignedURL)
	}
	for _, allocated := range allocation.MediaObjects {
		allocated.PutURL.StartBefore = uploadStartBefore
		destinations[allocated.ObjectID] = allocated.PutURL
	}
	// Validate the complete response before starting any transfer. Returning
	// halfway through scheduling would let earlier goroutines outlive a failed
	// batch.
	for _, object := range chunk {
		destination, ok := destinations[object.id]
		if !ok || destination.URL == "" {
			return 0, 0, fmt.Errorf("storage allocation omitted object %s", object.id)
		}
	}

	// Batching bounds the time an Object waits in a queue, but not the time one
	// Object takes on its own. A large enough Media Object outlives the URL it
	// was given however small the batch is, and the upload then fails on a URL
	// no retry can revive, so it is worth saying which Object and why rather
	// than leaving an expiry to be diagnosed from a 403.
	for _, object := range chunk {
		if expected, oversized := outlastsURL(object.size, throughput, p.limits.PresignedURL); oversized {
			p.logger.Warn("media object may outlast the upload URL issued for it",
				"flow_id", flowID, "object_id", object.id, "bytes", object.size,
				"estimated_upload", expected.Round(time.Second), "url_lifetime", p.limits.PresignedURL)
		}
	}

	uploads, uploadCtx := errgroup.WithContext(ctx)
	// The limit bounds how many goroutines exist, not just how many are doing
	// something. Without it every Object in the batch got one immediately and
	// then queued on the transfer budget, so the goroutine count followed the
	// size of the job rather than the size of the allowance. The budget itself
	// is still taken inside, because it is shared across concurrent Flows while
	// this limit only governs one batch.
	uploadLimit := max(p.config.Transfers, 1)
	if reservation != nil {
		uploadLimit = reservation.count
	}
	uploads.SetLimit(uploadLimit)
	receipts := make(map[string]tams.UploadReceipt, len(chunk))
	var receiptsMu sync.Mutex
	for _, object := range chunk {
		destination := destinations[object.id]
		uploads.Go(func() error {
			if reservation == nil {
				release, err := p.acquireTransfer(uploadCtx)
				if err != nil {
					return err
				}
				defer release()
			}
			p.logger.Info("uploading object", "flow_id", flowID, "object_id", object.id, "bytes", object.size)
			receipt, err := p.client.UploadFile(uploadCtx, destination, object.path)
			if err != nil {
				return fmt.Errorf("upload object %s: %w", object.id, err)
			}
			if receipt.Bytes != object.size || receipt.SHA256 != object.sha256 {
				return withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true,
					fmt.Errorf("prepared object %s changed before upload: expected %d bytes with SHA-256 %s, transmitted %d bytes with SHA-256 %s",
						object.id, object.size, object.sha256, receipt.Bytes, receipt.SHA256))
			}
			if receipt.StorageSHA256 != "" && receipt.StorageSHA256 != object.sha256 {
				return fmt.Errorf("storage checksum for object %s is %s, expected SHA-256 %s",
					object.id, receipt.StorageSHA256, object.sha256)
			}
			receiptsMu.Lock()
			receipts[object.id] = receipt
			receiptsMu.Unlock()
			p.observability.Uploaded(object.size)
			p.advanceProgress(uploadCtx, progress.PhaseStore, 1, object.size)
			return nil
		})
	}
	if err := uploads.Wait(); err != nil {
		return 0, 0, err
	}
	if reservation != nil {
		reservation.releaseAll()
	}
	records := registrationRecords(chunk, registrationUploaded)
	for _, record := range records {
		setRegistrationState(record, registrationUploaded, objectResults)
	}

	requests := make([]tams.SegmentRequest, 0, len(chunk))
	for _, object := range chunk {
		requests = append(requests, tams.SegmentRequest{
			ObjectID: object.id, Timerange: object.timerange,
			ObjectTimerange: object.objectTimerange, TSOffset: object.tsOffset,
		})
	}
	for _, record := range records {
		setRegistrationState(record, registrationIndeterminate, objectResults)
	}
	registrationRecovered := false
	if err := p.client.RegisterSegments(ctx, flowID, requests); err != nil {
		if resolveErr := p.reconcileRegistrationError(ctx, flowID, records, objectResults, err); resolveErr != nil {
			return 0, 0, fmt.Errorf("register segments: %w", errors.Join(err, resolveErr))
		}
		// A complete readback (and verification, when enabled) proved that the
		// bulk POST committed before its response was lost.
		registrationRecovered = true
	} else {
		for _, record := range records {
			setRegistrationState(record, registrationRegistered, objectResults)
		}
	}

	// TAMS requires GET /objects/{objectId} to answer 404 until the Object is
	// registered against a Flow Segment, so uploaded bytes cannot be read back
	// before registration. With an advertised URL lifetime, verification takes
	// a transfer slot and then fetches one exact, fresh URL in verifyOne. The
	// per-record state machine still resolves every listing failure or omission
	// by retracting that known-registered Segment.
	if p.config.Verify && !registrationRecovered {
		readback := records
		if p.config.VerificationMode == VerificationAuto {
			readback = make([]*registrationRecord, 0, len(records))
			for _, record := range records {
				if receipts[record.object.id].StorageSHA256 == "" {
					readback = append(readback, record)
					continue
				}
				p.acceptStorageVerification(ctx, record, objectResults)
			}
		}
		if len(readback) > 0 && p.limits.PresignedURL > 0 {
			for _, record := range readback {
				record.segment = tams.Segment{
					ObjectID: record.object.id, Timerange: record.object.timerange,
				}
			}
			if err := p.verifyRegistrationRecords(ctx, flowID, readback, objectResults); err != nil {
				return 0, 0, err
			}
		} else if len(readback) > 0 {
			registered, err := p.client.ListSegments(ctx, flowID,
				tams.SegmentListOptions{Timerange: chunkTimerange(objectsFromRecords(readback)), IncludeDownloadURLs: true})
			if err != nil {
				return 0, 0, p.resolveRegisteredListingFailure(ctx, flowID, readback, objectResults, err)
			}
			visible := make([]*registrationRecord, 0, len(readback))
			missing := make([]*registrationRecord, 0)
			for _, record := range readback {
				segment := matchingSegment(registered, record.object.id, record.object.timerange)
				if segment == nil {
					missing = append(missing, record)
					continue
				}
				record.segment = *segment
				visible = append(visible, record)
			}
			if len(missing) > 0 {
				return 0, 0, p.resolveIncompleteRegisteredListing(ctx, flowID, visible, missing, objectResults)
			}
			if err := p.verifyRegistrationRecords(ctx, flowID, visible, objectResults); err != nil {
				return 0, 0, err
			}
		}
	}
	for _, object := range chunk {
		setObjectStatus(objectResults, object.id, ObjectStatusIngested)
	}
	completed := make(map[string]struct{}, len(chunk))
	for _, object := range chunk {
		completed[object.id] = struct{}{}
	}
	if err := p.observeObjectBatch(ctx, flowID, objectResults, completed); err != nil {
		return 0, 0, err
	}
	return time.Since(started), transferred, nil
}

// applyBitRates records what a reader will actually have to pull off the wire.
//
// The Flow's bit rate properties are defined over Segments rather than essence,
// so they can only be worked out once the Segments exist -- which is why this
// runs after preparation rather than when the Flow was built from the probe.
// max_bit_rate in particular is what sizes a receiver's buffer, so leaving it
// unset makes a Flow harder to play back than it needs to be.
func (p *Pipeline) applyBitRates(flow tams.Flow, objects []preparedObject) {
	if len(objects) == 0 {
		return
	}
	segments := make([]media.SegmentMeasurement, len(objects))
	for index, object := range objects {
		segments[index] = media.SegmentMeasurement{Bytes: object.size, Duration: object.duration}
	}
	average, peak, ok := media.SegmentBitRates(segments, p.config.SegmentDuration)
	if !ok {
		return
	}
	flow["avg_bit_rate"] = average
	flow["max_bit_rate"] = peak
}

// acquireProbe takes a slot from the global media-measurement budget.
func (p *Pipeline) acquireProbe(ctx context.Context) (func(), error) {
	select {
	case p.probes <- struct{}{}:
		releaseProcess, err := p.acquireMediaProcess(ctx, 1)
		if err != nil {
			<-p.probes
			return nil, err
		}
		return func() {
			releaseProcess()
			<-p.probes
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Pipeline) acquireMediaProcess(ctx context.Context, weight int64) (func(), error) {
	if err := p.mediaProcesses.Acquire(ctx, weight); err != nil {
		return nil, err
	}
	return func() { p.mediaProcesses.Release(weight) }, nil
}

// retractionTimeout bounds cleanup. Retraction runs detached from the caller's
// context so cancellation cannot skip it, which means it needs a deadline of
// its own or a wedged service could hang a run that is already finishing.
const retractionTimeout = 30 * time.Second

// acquireTransfer takes a slot from the global transfer budget, returning the
// release function. It respects cancellation so a failing sibling does not
// leave callers queued behind work that will be discarded.
func (p *Pipeline) acquireTransfer(ctx context.Context) (func(), error) {
	select {
	case p.transfers <- struct{}{}:
		return func() { <-p.transfers }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// transferReservation holds slots in the global transfer budget before a
// service is asked to generate upload URLs. An allocated URL is therefore
// handed only to work that can begin immediately, rather than to a goroutine
// queued behind an unrelated Flow's transfers.
type transferReservation struct {
	pipeline *Pipeline
	count    int
}

func (r *transferReservation) releaseAll() {
	if r == nil || r.pipeline == nil {
		return
	}
	for range r.count {
		<-r.pipeline.transfers
	}
	r.pipeline = nil
	r.count = 0
}

// reserveTransferBatch waits for one global transfer slot, then takes as many
// additional slots as are immediately free. Waiting for every desired slot
// would deadlock when two concurrent Flows each held part of the budget. The
// returned count is consequently the safe allocation batch size right now.
func (p *Pipeline) reserveTransferBatch(ctx context.Context, desired int) (*transferReservation, error) {
	desired = min(max(desired, 1), cap(p.transfers))
	select {
	case p.transfers <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	reserved := 1
	for reserved < desired {
		select {
		case p.transfers <- struct{}{}:
			reserved++
		default:
			return &transferReservation{pipeline: p, count: reserved}, nil
		}
	}
	return &transferReservation{pipeline: p, count: reserved}, nil
}

func (p *Pipeline) registerFlow(ctx context.Context, flowID string, objects []preparedObject, objectResults []ObjectResult, storageID string) error {
	// One listing answers the resume question for every Object. Asking per
	// Object cost a round trip each, which dominates on a high-latency link.
	// Download URLs are only wanted if a resumed Object will be verified. When
	// they are not, the service is spared signing one per Segment for a listing
	// that is only being asked which Objects exist.
	existing, err := p.client.ListSegments(ctx, flowID,
		tams.SegmentListOptions{
			// This first listing answers identity only. A verification worker asks
			// for its own URL after it holds a transfer slot.
			IncludeDownloadURLs: p.config.Verify && p.limits.PresignedURL <= 0,
		})
	if err != nil {
		return fmt.Errorf("list existing segments: %w", err)
	}
	throughput := float64(0)
	return p.registerPreparedObjects(ctx, flowID, objects, objectResults, storageID, existing, &throughput)
}

func (p *Pipeline) registerRollingChunk(ctx context.Context, flowID string, objects []preparedObject,
	objectResults []ObjectResult, storageID string, throughput *float64) error {
	existing, err := p.client.ListSegments(ctx, flowID, tams.SegmentListOptions{
		Timerange:           chunkTimerange(objects),
		IncludeDownloadURLs: p.config.Verify && p.limits.PresignedURL <= 0,
	})
	if err != nil {
		return fmt.Errorf("list existing segments for rolling batch: %w", err)
	}
	return p.registerPreparedObjects(ctx, flowID, objects, objectResults, storageID, existing, throughput)
}

func (p *Pipeline) registerPreparedObjects(ctx context.Context, flowID string, objects []preparedObject,
	objectResults []ObjectResult, storageID string, existing []tams.Segment, throughput *float64) error {
	if throughput == nil {
		throughput = new(float64)
	}

	missing := make([]preparedObject, 0, len(objects))
	var resumed []verificationTask
	for _, object := range objects {
		segment := matchingSegment(existing, object.id, object.timerange)
		if segment == nil {
			missing = append(missing, object)
			continue
		}
		// A resumed Object credits the upload it did not need to repeat. Its
		// verification is scheduled with the rest, so that unit is credited there.
		p.advanceProgress(ctx, progress.PhaseStore, 1, object.size)
		if p.config.Verify {
			resumed = append(resumed, verificationTask{object: object, segment: *segment})
		}
		setObjectStatus(objectResults, object.id, ObjectStatusResumed)
	}
	// Resumed Objects are checked before missing uploads. When URL lifetimes are
	// advertised, the tasks intentionally carry no URL: verifyOne refreshes each
	// only after its worker owns transfer capacity.
	outcomes, verifyErr := p.verifyAllWithOutcomes(ctx, flowID, resumed)
	for index, outcome := range outcomes {
		if outcome == outcomeVerified {
			setObjectVerification(objectResults, resumed[index].object.id,
				ObjectVerificationVerified, VerificationMethodReadback)
		}
	}
	if verifyErr != nil {
		for index, outcome := range outcomes {
			switch outcome {
			case outcomeRetracted:
				setObjectStatus(objectResults, resumed[index].object.id, ObjectStatusRetracted)
				setObjectVerification(objectResults, resumed[index].object.id,
					ObjectVerificationFailed, VerificationMethodReadback)
			case outcomeRetractionFailed:
				setObjectStatus(objectResults, resumed[index].object.id, ObjectStatusStranded)
				setObjectVerification(objectResults, resumed[index].object.id,
					ObjectVerificationFailed, VerificationMethodReadback)
			}
		}
		return verifyErr
	}
	if len(resumed) > 0 {
		completed := make(map[string]struct{}, len(resumed))
		for _, task := range resumed {
			completed[task.object.id] = struct{}{}
		}
		if err := p.observeObjectBatch(ctx, flowID, objectResults, completed); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}

	// Media Objects are committed in batches rather than all at once. A store
	// collects an Object that is not registered against a Segment in time, and
	// promises only five minutes, so allocating storage for a whole programme
	// and registering it an hour later is relying on a guarantee that was never
	// given. Each batch is allocated, uploaded, registered and verified before
	// the next begins, which keeps every Object's unregistered life to the
	// length of one batch.
	for offset := 0; offset < len(missing); {
		chunk := missing[offset:min(offset+p.chunkSize(missing[offset:], *throughput), len(missing))]
		elapsed, transferred, err := p.commitChunk(ctx, flowID, chunk, objectResults, storageID, *throughput)
		if err != nil {
			return err
		}
		if elapsed > 0 && transferred > 0 {
			*throughput = float64(transferred) / elapsed.Seconds()
		}
		offset += len(chunk)
	}

	for _, object := range missing {
		setObjectStatus(objectResults, object.id, ObjectStatusIngested)
	}
	return nil
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
	processWeight := int64(1)
	if len(additionalArgs) > 0 && window == nil {
		processWeight = 2
	}
	releaseProcess, err := p.acquireMediaProcess(segmentCtx, processWeight)
	if err != nil {
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
	size, checksum, err := p.client.DownloadDigest(ctx, segment.GetURLs[0])
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
		p.toolchainFingerprint = mediaToolchainFingerprint(
			p.config.Profile, p.config.ProfileVersion, toolchainReport)
	})
	return p.toolchainVersion, p.toolchainFingerprint, p.toolchainErr
}

func (p *Pipeline) resetMediaToolchain() {
	p.toolchainOnce = sync.Once{}
	p.toolchainVersion = ""
	p.toolchainFingerprint = ""
	p.toolchainErr = nil
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
func matchingSegment(segments []tams.Segment, objectID, timerange string) *tams.Segment {
	for index := range segments {
		if segments[index].ObjectID == objectID && segments[index].Timerange == timerange {
			return &segments[index]
		}
	}
	return nil
}

func (p *Pipeline) newObjectResult(object preparedObject) ObjectResult {
	verification := ObjectVerificationNotReached
	if !p.config.Verify {
		verification = ObjectVerificationNotRequested
	}
	return ObjectResult{
		ObjectID: object.id, Timerange: object.timerange, Bytes: object.size, SHA256: object.sha256,
		Status: ObjectStatusPlanned, Disposition: ObjectDispositionPlanned,
		Verification: verification, VerificationMethod: VerificationMethodNone,
	}
}

func setObjectStatus(objects []ObjectResult, objectID string, status ObjectStatus) {
	for index := range objects {
		if objects[index].ObjectID == objectID {
			objects[index].Status = status
			if disposition := objectDispositionForStatus(status); disposition != "" {
				objects[index].Disposition = disposition
			}
			return
		}
	}
}

func setObjectVerification(objects []ObjectResult, objectID string,
	status ObjectVerificationStatus, method VerificationMethod) {
	for index := range objects {
		if objects[index].ObjectID == objectID {
			objects[index].Verification = status
			objects[index].VerificationMethod = method
			return
		}
	}
}

func finalizeObjectResult(object *ObjectResult) {
	if object.Disposition == "" {
		object.Disposition = objectDispositionForStatus(object.Status)
		if object.Disposition == "" {
			object.Disposition = ObjectDispositionUnattempted
		}
	}
	if object.Verification == "" {
		object.Verification = ObjectVerificationNotReached
	}
	if object.VerificationMethod == "" {
		object.VerificationMethod = VerificationMethodNone
	}
}

func objectDispositionForStatus(status ObjectStatus) ObjectDisposition {
	switch status {
	case ObjectStatusPlanned:
		return ObjectDispositionPlanned
	case ObjectStatusUploaded:
		return ObjectDispositionUploaded
	case ObjectStatusRegistered, ObjectStatusVerified:
		return ObjectDispositionRegistered
	case ObjectStatusResumed:
		return ObjectDispositionResumed
	case ObjectStatusIngested:
		return ObjectDispositionIngested
	case ObjectStatusRejected:
		return ObjectDispositionRejected
	case ObjectStatusRetractionIndeterminate:
		return ObjectDispositionRegistrationIndeterminate
	case ObjectStatusRetracted:
		return ObjectDispositionRetracted
	case ObjectStatusStranded:
		return ObjectDispositionStranded
	default:
		return ""
	}
}

func compactObjectResults(flows []FlowResult) {
	for flowIndex := range flows {
		if flows[flowIndex].ObjectSummary.Total > 0 {
			continue
		}
		var summary ObjectSummary
		for objectIndex := range flows[flowIndex].Objects {
			object := &flows[flowIndex].Objects[objectIndex]
			finalizeObjectResult(object)
			addObjectSummary(&summary, *object)
		}
		flows[flowIndex].ObjectSummary = summary
	}
}

func addObjectSummary(summary *ObjectSummary, object ObjectResult) {
	if summary == nil {
		return
	}
	finalizeObjectResult(&object)
	summary.Total++
	summary.Bytes += object.Bytes
	switch object.Disposition {
	case ObjectDispositionIngested:
		summary.Ingested++
	case ObjectDispositionResumed:
		summary.Resumed++
	case ObjectDispositionRejected:
		summary.Rejected++
	case ObjectDispositionRetracted:
		summary.Retracted++
	case ObjectDispositionStranded, ObjectDispositionRegistrationIndeterminate:
		summary.Stranded++
	case ObjectDispositionUnattempted, ObjectDispositionPlanned, ObjectDispositionUploaded, ObjectDispositionRegistered:
		summary.Unattempted++
	}
	if object.Verification == ObjectVerificationVerified {
		summary.Verified++
		switch object.VerificationMethod {
		case VerificationMethodStorage:
			summary.StorageVerified++
		case VerificationMethodReadback:
			summary.ReadbackVerified++
		}
	}
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

// SafeInputURI returns the representation safe for result output and durable
// journals. It retains only the canonical scheme, authority, and path; userinfo,
// query material, and fragments are never persisted.
func SafeInputURI(raw string) string { return safeURI(raw) }
