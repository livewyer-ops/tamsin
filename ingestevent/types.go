package ingestevent

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// Protocol identifies the ingest event stream independently from any one
	// result or journal schema.
	Protocol = "tamsin.ingest.events"
	// ProtocolVersion follows major.minor compatibility. Version 2 separates
	// Object disposition from verification and keeps reducer memory bounded.
	ProtocolVersion = "2.0"
)

const (
	// DefaultMaxEventBytes is the advertised upper bound for a complete encoded
	// envelope, including its trailing newline. Object and Flow results are
	// deliberately split so ordinary records stay far below this limit.
	DefaultMaxEventBytes = 1 << 20
	// AdvertisedMaxEventBytesLimit prevents a producer from asking consumers to
	// treat arbitrarily large records as ordinary protocol events.
	AdvertisedMaxEventBytesLimit = 16 << 20
	// MinimumMaxEventBytes prevents a producer from advertising a limit too
	// small for the mandatory protocol envelope.
	MinimumMaxEventBytes = 512
	// Diagnostic fields are independently bounded because they originate near
	// error boundaries and must never become an unbounded provider-error tunnel.
	MaxDiagnosticCodeBytes    = 128
	MaxDiagnosticMessageBytes = 4096
	MaxDiagnosticHintBytes    = 4096
)

// Type names a semantic record in an ingest event stream.
type Type string

const (
	TypeHello                    Type = "hello"
	TypeRunStarted               Type = "run.started"
	TypeInputDeclared            Type = "input.declared"
	TypeManifestFinished         Type = "manifest.finished"
	TypeInputStarted             Type = "input.started"
	TypeFlowPlanned              Type = "flow.planned"
	TypeProgressSnapshot         Type = "progress.snapshot"
	TypeRetryScheduled           Type = "retry.scheduled"
	TypeDiagnostic               Type = "diagnostic"
	TypeObjectResult             Type = "object.result"
	TypeFlowResult               Type = "flow.result"
	TypeInputFinished            Type = "input.finished"
	TypeRunCancellationRequested Type = "run.cancellation_requested"
	TypeRunFinished              Type = "run.finished"
)

// Scope identifies the input and optional Flow/Object to which an event
// belongs. InputIndex is a pointer so input zero remains distinguishable from
// a run-scoped record.
type Scope struct {
	InputIndex *int   `json:"input_index,omitempty"`
	FlowID     string `json:"flow_id,omitempty"`
	ObjectID   string `json:"object_id,omitempty"`
}

// InputScope constructs the required scope for an input-scoped event.
func InputScope(index int) *Scope { return &Scope{InputIndex: &index} }

// FlowScope constructs a scope for an event about one Flow.
func FlowScope(index int, flowID string) *Scope {
	return &Scope{InputIndex: &index, FlowID: flowID}
}

// ObjectScope constructs a scope for an event about one Media Object.
func ObjectScope(index int, flowID, objectID string) *Scope {
	return &Scope{InputIndex: &index, FlowID: flowID, ObjectID: objectID}
}

// Envelope is one independently decodable NDJSON record. Payload remains raw
// at the transport boundary so a v2 consumer can safely skip event types it
// does not understand.
type Envelope struct {
	Protocol        string          `json:"protocol"`
	ProtocolVersion string          `json:"protocol_version"`
	Type            Type            `json:"type"`
	Seq             uint64          `json:"seq"`
	RunID           string          `json:"run_id"`
	EmittedAt       time.Time       `json:"emitted_at"`
	ElapsedMS       uint64          `json:"elapsed_ms"`
	Scope           *Scope          `json:"scope,omitempty"`
	Payload         json.RawMessage `json:"payload"`
}

// Event is a typed event payload accepted by Encoder.
type Event interface {
	EventType() Type
}

type Hello struct {
	ToolVersion          string   `json:"tool_version"`
	ToolCommit           string   `json:"tool_commit"`
	ToolBuildDate        string   `json:"tool_build_date,omitempty"`
	ResultSchemaVersion  string   `json:"result_schema_version"`
	ProfilePolicyVersion string   `json:"profile_policy_version"`
	MaxEventBytes        uint64   `json:"max_event_bytes"`
	Capabilities         []string `json:"capabilities"`
}

func (Hello) EventType() Type { return TypeHello }

type RunStarted struct {
	StartedAt        time.Time `json:"started_at"`
	Profile          string    `json:"profile,omitempty"`
	ProfileVersion   string    `json:"profile_version,omitempty"`
	DryRunMode       string    `json:"dry_run_mode,omitempty"`
	VerificationMode string    `json:"verification_mode,omitempty"`
	Concurrency      *uint64   `json:"concurrency,omitempty"`
	Transfers        *uint64   `json:"transfers,omitempty"`
	RequestedInputs  *uint64   `json:"requested_inputs,omitempty"`
}

func (RunStarted) EventType() Type { return TypeRunStarted }

// KnownInputCount marks requested_inputs as known, including a known count of
// zero. A nil pointer means discovery has not established the count yet.
func KnownInputCount(count uint64) *uint64 { return &count }

// KnownSetting marks an optional scalar setting as resolved. It is useful for
// booleans, where false must remain distinguishable from startup failure before
// the setting was resolved.
func KnownSetting[T any](value T) *T { return &value }

type InputDeclared struct {
	Input string `json:"input"`
}

func (InputDeclared) EventType() Type { return TypeInputDeclared }

type ManifestFinished struct {
	TotalInputs uint64 `json:"total_inputs"`
}

func (ManifestFinished) EventType() Type { return TypeManifestFinished }

type InputStarted struct {
	StartedAt time.Time `json:"started_at"`
}

func (InputStarted) EventType() Type { return TypeInputStarted }

type FlowKind string

const (
	FlowKindEssence    FlowKind = "essence"
	FlowKindCollection FlowKind = "collection"
	// FlowKindMuxed is a multi-essence Flow which directly owns Media Objects.
	// Collection is reserved for the empty association-only Flow used by
	// demuxed ingest.
	FlowKindMuxed FlowKind = "muxed"
)

type FlowPlanned struct {
	FlowID       string   `json:"flow_id"`
	SourceID     string   `json:"source_id"`
	Kind         FlowKind `json:"kind"`
	Role         string   `json:"role,omitempty"`
	Root         bool     `json:"root"`
	ParentFlowID string   `json:"parent_flow_id,omitempty"`
	Format       string   `json:"format,omitempty"`
	Container    string   `json:"container,omitempty"`
}

func (FlowPlanned) EventType() Type { return TypeFlowPlanned }

type ProgressPhase string

const (
	ProgressStore  ProgressPhase = "store"
	ProgressVerify ProgressPhase = "verify"
)

// ProgressSnapshot is cumulative for one input and phase. Totals may grow
// while TotalsFinal is false. Once true, totals cannot change; consumers must
// not present a percentage before that point.
type ProgressSnapshot struct {
	Revision         uint64        `json:"revision"`
	Phase            ProgressPhase `json:"phase"`
	TotalsFinal      bool          `json:"totals_final"`
	CompletedObjects uint64        `json:"completed_objects"`
	TotalObjects     uint64        `json:"total_objects"`
	CompletedBytes   uint64        `json:"completed_bytes"`
	TotalBytes       uint64        `json:"total_bytes"`
	ElapsedMS        uint64        `json:"elapsed_ms"`
}

func (ProgressSnapshot) EventType() Type { return TypeProgressSnapshot }

type RetryScheduled struct {
	Operation   string `json:"operation"`
	Attempt     uint64 `json:"attempt"`
	MaxAttempts uint64 `json:"max_attempts"`
	DelayMS     uint64 `json:"delay_ms"`
	StatusClass string `json:"status_class,omitempty"`
	ErrorClass  string `json:"error_class,omitempty"`
}

func (RetryScheduled) EventType() Type { return TypeRetryScheduled }

type Severity string

const (
	SeverityDebug   Severity = "debug"
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"

	// Stable diagnostic and input failure codes emitted by TAMSin. Consumers
	// may branch on these values while retaining the payload message as their
	// display fallback.
	DiagnosticCodeConfigInvalid  = "config.invalid"
	DiagnosticCodeObjectStranded = "object.stranded"
	InputErrorCodeRunInterrupted = "run.interrupted"
	InputErrorCodeIngestFailed   = "ingest.input_failed"
)

// Diagnostic carries a stable code and a safe display fallback. Message and
// Hint must already be stripped of credentials, signed URLs, headers, provider
// response bodies, and raw provider errors before construction.
type Diagnostic struct {
	Severity       Severity `json:"severity"`
	Code           string   `json:"code"`
	Message        string   `json:"message"`
	Hint           string   `json:"hint,omitempty"`
	ActionRequired bool     `json:"action_required"`
	Truncated      bool     `json:"truncated"`
}

func (Diagnostic) EventType() Type { return TypeDiagnostic }

// NewDiagnostic validates the stable branching fields and bounds display text
// without splitting UTF-8. It does not accept arbitrary structured fields or a
// raw error. Callers remain responsible for supplying credential-free text.
func NewDiagnostic(severity Severity, code, message, hint string, actionRequired bool) (Diagnostic, error) {
	if !validSeverity(severity) {
		return Diagnostic{}, fmt.Errorf("unknown diagnostic severity %q", severity)
	}
	if !codePattern.MatchString(code) || len(code) > MaxDiagnosticCodeBytes {
		return Diagnostic{}, fmt.Errorf("invalid diagnostic code %q", code)
	}
	if strings.TrimSpace(message) == "" {
		return Diagnostic{}, fmt.Errorf("diagnostic message is required")
	}
	message, messageTruncated := truncateUTF8(message, MaxDiagnosticMessageBytes)
	hint, hintTruncated := truncateUTF8(hint, MaxDiagnosticHintBytes)
	return Diagnostic{
		Severity: severity, Code: code, Message: message, Hint: hint,
		ActionRequired: actionRequired, Truncated: messageTruncated || hintTruncated,
	}, nil
}

func truncateUTF8(value string, maximum int) (string, bool) {
	clean := strings.ToValidUTF8(value, string(utf8.RuneError))
	truncated := clean != value
	if len(clean) <= maximum {
		return clean, truncated
	}
	cut := maximum
	for cut > 0 && !utf8.ValidString(clean[:cut]) {
		cut--
	}
	return clean[:cut], true
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

type ObjectResult struct {
	ObjectID           string                   `json:"object_id"`
	Timerange          string                   `json:"timerange"`
	Bytes              uint64                   `json:"bytes"`
	SHA256             string                   `json:"sha256"`
	Disposition        ObjectDisposition        `json:"disposition"`
	Verification       ObjectVerificationStatus `json:"verification_status"`
	VerificationMethod VerificationMethod       `json:"verification_method"`
}

func (ObjectResult) EventType() Type { return TypeObjectResult }

type FlowDisposition string

const (
	FlowPlannedDisposition FlowDisposition = "planned"
	FlowUnchanged          FlowDisposition = "unchanged"
	FlowWritten            FlowDisposition = "written"
	FlowIndeterminate      FlowDisposition = "indeterminate"
	FlowUnattempted        FlowDisposition = "unattempted"
)

type FlowResult struct {
	FlowID        string          `json:"flow_id"`
	SourceID      string          `json:"source_id"`
	Kind          FlowKind        `json:"kind"`
	Role          string          `json:"role,omitempty"`
	Disposition   FlowDisposition `json:"disposition"`
	ObjectSummary ObjectSummary   `json:"object_summary"`
}

type ObjectSummary struct {
	Total            uint64 `json:"total"`
	Bytes            uint64 `json:"bytes"`
	Ingested         uint64 `json:"ingested"`
	Resumed          uint64 `json:"resumed"`
	Rejected         uint64 `json:"rejected"`
	Retracted        uint64 `json:"retracted"`
	Stranded         uint64 `json:"stranded"`
	Unattempted      uint64 `json:"unattempted"`
	Verified         uint64 `json:"verified"`
	StorageVerified  uint64 `json:"storage_verified"`
	ReadbackVerified uint64 `json:"readback_verified"`
}

func (FlowResult) EventType() Type { return TypeFlowResult }

type InputStatus string

const (
	InputPlanned  InputStatus = "planned"
	InputIngested InputStatus = "ingested"
	InputResumed  InputStatus = "resumed"
	InputFailed   InputStatus = "failed"
)

type VerificationStatus string

const (
	VerificationVerified        VerificationStatus = "verified"
	VerificationNotRequested    VerificationStatus = "not_requested"
	VerificationNotReached      VerificationStatus = "not_reached"
	VerificationFailedRetracted VerificationStatus = "failed_retracted"
	VerificationFailedStranded  VerificationStatus = "failed_stranded"
)

type InputFinished struct {
	Input          string             `json:"input"`
	Profile        string             `json:"profile"`
	ProfileVersion string             `json:"profile_version"`
	FFmpegVersion  string             `json:"ffmpeg_version,omitempty"`
	MediaToolchain string             `json:"media_toolchain,omitempty"`
	RootFlowID     string             `json:"root_flow_id,omitempty"`
	Bytes          uint64             `json:"bytes,omitempty"`
	SHA256         string             `json:"sha256,omitempty"`
	Status         InputStatus        `json:"status"`
	Verification   VerificationStatus `json:"verification"`
	FlowCount      uint64             `json:"flow_count"`
	ObjectCount    uint64             `json:"object_count"`
	ErrorCode      string             `json:"error_code,omitempty"`
	Message        string             `json:"message,omitempty"`
}

func (InputFinished) EventType() Type { return TypeInputFinished }

type CancellationReason string

const (
	CancellationSignal       CancellationReason = "signal"
	CancellationParent       CancellationReason = "parent"
	CancellationDeadline     CancellationReason = "deadline"
	CancellationOutputClosed CancellationReason = "output_closed"
	CancellationInternal     CancellationReason = "internal"
)

type RunCancellationRequested struct {
	Reason CancellationReason `json:"reason"`
}

func (RunCancellationRequested) EventType() Type { return TypeRunCancellationRequested }

type RunOutcome string

const (
	RunSucceeded   RunOutcome = "succeeded"
	RunFailed      RunOutcome = "failed"
	RunPartial     RunOutcome = "partial"
	RunInterrupted RunOutcome = "interrupted"
)

type RunFinished struct {
	Outcome          RunOutcome `json:"outcome"`
	ExitCode         int        `json:"exit_code"`
	Total            uint64     `json:"total"`
	Succeeded        uint64     `json:"succeeded"`
	Failed           uint64     `json:"failed"`
	ElapsedMS        uint64     `json:"elapsed_ms"`
	BytesStaged      uint64     `json:"bytes_staged"`
	BytesUploaded    uint64     `json:"bytes_uploaded"`
	BytesVerified    uint64     `json:"bytes_verified"`
	Retries          uint64     `json:"retries"`
	ObjectsVerified  uint64     `json:"objects_verified"`
	ObjectsRetracted uint64     `json:"objects_retracted"`
	ObjectsStranded  uint64     `json:"objects_stranded"`
}

func (RunFinished) EventType() Type { return TypeRunFinished }
