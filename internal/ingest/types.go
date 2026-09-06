package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"golang.org/x/sync/semaphore"
)

type TAMSClient interface {
	Service(context.Context) (map[string]any, error)
	StorageBackends(context.Context) ([]tams.StorageBackend, error)
	Profile(context.Context, string) (tams.Profile, error)
	Flow(context.Context, string) (tams.Flow, error)
	PutFlow(context.Context, string, tams.Flow) (tams.Flow, error)
	AllocateStorage(context.Context, string, tams.StorageRequest) (tams.StorageResponse, error)
	RegisterSegment(context.Context, string, tams.SegmentRequest) error
	RegisterSegments(context.Context, string, []tams.SegmentRequest) error
	DeleteSegments(context.Context, string, tams.SegmentDeleteOptions) error
	ListSegments(context.Context, string, tams.SegmentListOptions) ([]tams.Segment, error)
	UploadFile(context.Context, tams.PresignedURL, string) (tams.UploadReceipt, error)
	DownloadDigest(context.Context, tams.PresignedURL, int64) (int64, string, error)
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
	profileID        string
}

type flowGraph struct {
	flows       []graphFlow
	collectorID string
	storage     media.EssenceStorage
}

type plannedFlowWrite struct {
	member    graphFlow
	effective tams.Flow
	request   tams.Flow
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
	// identifiers; process consumers use lifecycle events
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
	// TAMSFlowProfiles assigns immutable TAMS 8.2 Flow Profiles using
	// [format[:index]=]UUID selectors.
	TAMSFlowProfiles []string
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
	// rollingRenders prevents two live FFmpeg segmenters from each occupying
	// one media-process token and then both waiting forever for their sink's
	// nested FFprobe to acquire the other. A rolling render keeps one process
	// slot available for measurement while preserving the global two-process
	// ceiling.
	rollingRenders chan struct{}
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
	apiVersion           tams.APIVersion
	profileAssignments   []flowProfileAssignment
	profileMu            sync.Mutex
	profileCache         map[string]tams.Profile
	flowStatusMu         sync.Mutex
	flowStatuses         map[string]string
	client               TAMSClient
	prober               media.Prober
	segmenter            media.Segmenter
	logger               *slog.Logger
	observability        *observability.Run
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
	FlowID            string          `json:"flow_id"`
	SourceID          string          `json:"source_id"`
	Role              string          `json:"role,omitempty"`
	TAMSFlowProfileID string          `json:"tams_flow_profile_id,omitempty"`
	Disposition       FlowDisposition `json:"disposition"`
	ObjectSummary     ObjectSummary   `json:"object_summary"`
	Objects           []ObjectResult  `json:"-"`
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

	// ResultSchemaVersion identifies the terminal result vocabulary. Compatible
	// additions use a minor version; incompatible changes require a new major.
	// Profile versions are independent of the executable and result schema.
	ResultSchemaVersion = "2.1"
)

// Failure is the stable, disclosure-safe terminal explanation shared by the
// human receipt and process event adapters. It never contains raw
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
