package ingestevent

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
)

var (
	// ErrIncompleteStream means EOF arrived before the mandatory run.finished
	// record. A consumer must treat this as a crash, forced kill, or broken
	// transport rather than an ordinary ingest failure.
	ErrIncompleteStream = errors.New("ingest event stream ended before run.finished")
	// ErrAfterRunFinished identifies a record after the stream's exact-last
	// terminal record.
	ErrAfterRunFinished = errors.New("ingest event follows run.finished")
)

var (
	codePattern      = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)*$`)
	sha256Pattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// State is the replayable state produced by reducing a stream. Transient
// progress retains only the latest cumulative snapshot for each phase. Object
// records are summarized by default and retained only when requested.
type State struct {
	ProtocolVersion   string
	RunID             string
	Hello             *Hello
	Started           *RunStarted
	Manifest          *ManifestFinished
	Inputs            map[int]*InputState
	Cancellation      *RunCancellationRequested
	Finished          *RunFinished
	RetryCount        uint64
	Diagnostics       []Diagnostic
	UnknownEventCount uint64
	NextSequence      uint64
}

// InputState is the latest validated state for one declared input. ObjectResults
// is populated only when explicitly requested; ObjectSummaries remains bounded
// by Flow count under the default policy.
type InputState struct {
	Declared         *InputDeclared
	Started          *InputStarted
	PlannedFlows     map[string]FlowPlanned
	Progress         map[ProgressPhase]ProgressSnapshot
	ObjectResults    []ScopedObjectResult
	ObjectSummaries  map[string]ObjectSummary
	FlowResults      map[string]FlowResult
	Finished         *InputFinished
	RetryCount       uint64
	Diagnostics      []Diagnostic
	ProgressRevision uint64
}

// ScopedObjectResult is retained only when requested. Keeping the Flow beside
// each record preserves stream order and does not assume Object IDs are a
// suitable map key for every future protocol minor.
type ScopedObjectResult struct {
	FlowID string
	Result ObjectResult
}

// ObjectObserver receives each validated Object after reducer state and its
// sequence have been committed. It may call Snapshot; returning an error stops
// reduction but does not make the committed event replayable.
type ObjectObserver func(scope Scope, result ObjectResult) error

// ReducerOptions controls per-Object handling. Retention makes snapshots and
// memory proportional to Object count; an observer can persist Objects without
// retaining them in reducer state.
type ReducerOptions struct {
	RetainObjectResults bool
	ObjectObserver      ObjectObserver
}

// Reducer validates ordering and folds a complete v2 stream into a state which
// a forked UI can render without consulting stderr. By default its memory is
// proportional to inputs and Flows, not Media Objects. Callers that need every
// Object can opt into retention or stream them through ObjectObserver.
type Reducer struct {
	applyMu       sync.Mutex
	mu            sync.Mutex
	state         State
	lastElapsedMS uint64
	options       ReducerOptions
}

// NewReducer returns a reducer that retains input and Flow state plus cumulative
// Object summaries, but not individual Object records.
func NewReducer() *Reducer {
	return NewReducerWithOptions(ReducerOptions{})
}

// NewReducerWithOptions returns a reducer with an explicit Object policy.
func NewReducerWithOptions(options ReducerOptions) *Reducer {
	return &Reducer{state: State{Inputs: make(map[int]*InputState)}, options: options}
}

// Apply validates and reduces one envelope. Sequence is contiguous from zero,
// making a missing NDJSON record distinguishable from an uneventful interval.
func (r *Reducer) Apply(envelope Envelope) error {
	if r == nil {
		return errors.New("nil ingest event reducer")
	}
	// Keep callbacks in global event order without holding the state lock while
	// invoking caller code. An observer may therefore inspect Snapshot without
	// deadlocking the reducer.
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	observation, err := r.applyLocked(envelope)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if observation.observer != nil {
		if err := observation.observer(observation.scope, observation.result); err != nil {
			return fmt.Errorf("observe Object result: %w", err)
		}
	}
	return nil
}

type objectObservation struct {
	observer ObjectObserver
	scope    Scope
	result   ObjectResult
}

// applyLocked validates and commits one event while r.mu is held. Object
// observation happens afterwards: an observer error stops the caller but does
// not make an already validated event replayable under the same sequence.
func (r *Reducer) applyLocked(envelope Envelope) (objectObservation, error) {
	if r.state.Finished != nil {
		return objectObservation{}, ErrAfterRunFinished
	}
	if envelope.Protocol != Protocol {
		return objectObservation{}, fmt.Errorf("unsupported event protocol %q", envelope.Protocol)
	}
	if !compatibleVersion(envelope.ProtocolVersion) {
		return objectObservation{}, fmt.Errorf("unsupported %s protocol version %q", Protocol, envelope.ProtocolVersion)
	}
	if _, err := uuid.Parse(envelope.RunID); err != nil {
		return objectObservation{}, fmt.Errorf("event run_id must be a UUID: %w", err)
	}
	if envelope.EmittedAt.IsZero() {
		return objectObservation{}, errors.New("event emitted_at is required")
	}
	if envelope.Seq > 0 && envelope.ElapsedMS < r.lastElapsedMS {
		return objectObservation{}, errors.New("event elapsed_ms cannot decrease")
	}
	if envelope.Seq != r.state.NextSequence {
		return objectObservation{}, fmt.Errorf("event sequence is %d, want %d", envelope.Seq, r.state.NextSequence)
	}
	if envelope.Seq == 0 && envelope.Type != TypeHello {
		return objectObservation{}, fmt.Errorf("event sequence zero must be %q", TypeHello)
	}
	if !eventTypePattern.MatchString(string(envelope.Type)) {
		return objectObservation{}, fmt.Errorf("invalid event type %q", envelope.Type)
	}
	if envelope.Seq > 0 {
		if r.state.Hello == nil {
			return objectObservation{}, errors.New("event stream has no hello record")
		}
		if envelope.RunID != r.state.RunID {
			return objectObservation{}, fmt.Errorf("event run_id %s does not match stream run_id %s", envelope.RunID, r.state.RunID)
		}
	}
	if err := validateScope(envelope.Scope); err != nil {
		return objectObservation{}, err
	}
	if !isJSONObject(envelope.Payload) {
		return objectObservation{}, fmt.Errorf("%s payload must be a JSON object", envelope.Type)
	}

	event, known, err := DecodeEvent(envelope)
	if err != nil {
		return objectObservation{}, err
	}
	if !known {
		if envelope.Seq == 0 {
			return objectObservation{}, errors.New("hello event is not decodable")
		}
		r.state.UnknownEventCount++
		r.advance(envelope)
		return objectObservation{}, nil
	}
	if err := r.applyKnown(envelope.Scope, event); err != nil {
		return objectObservation{}, fmt.Errorf("%s event: %w", envelope.Type, err)
	}
	r.advance(envelope)
	if result, ok := event.(ObjectResult); ok && r.options.ObjectObserver != nil {
		return objectObservation{observer: r.options.ObjectObserver, scope: *cloneScope(envelope.Scope), result: result}, nil
	}
	return objectObservation{}, nil
}

// Finalize validates run.finished as the exact last graceful record. Call this
// when the decoder reaches EOF.
func (r *Reducer) Finalize() error {
	if r == nil {
		return ErrIncompleteStream
	}
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Finished == nil {
		return ErrIncompleteStream
	}
	return nil
}

// Snapshot returns an independent copy safe for a renderer to retain while
// another goroutine continues reducing subsequent records.
func (r *Reducer) Snapshot() State {
	if r == nil {
		return State{Inputs: make(map[int]*InputState)}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := r.state
	result.Hello = clonePtr(r.state.Hello)
	if result.Hello != nil {
		result.Hello.Capabilities = append([]string(nil), r.state.Hello.Capabilities...)
	}
	result.Started = clonePtr(r.state.Started)
	if result.Started != nil {
		result.Started.Concurrency = clonePtr(r.state.Started.Concurrency)
		result.Started.Transfers = clonePtr(r.state.Started.Transfers)
		result.Started.RequestedInputs = clonePtr(r.state.Started.RequestedInputs)
	}
	result.Manifest = clonePtr(r.state.Manifest)
	result.Cancellation = clonePtr(r.state.Cancellation)
	result.Finished = clonePtr(r.state.Finished)
	result.Diagnostics = append([]Diagnostic(nil), r.state.Diagnostics...)
	result.Inputs = make(map[int]*InputState, len(r.state.Inputs))
	for index, input := range r.state.Inputs {
		copyInput := *input
		copyInput.Declared = clonePtr(input.Declared)
		copyInput.Started = clonePtr(input.Started)
		copyInput.Finished = clonePtr(input.Finished)
		copyInput.PlannedFlows = cloneMap(input.PlannedFlows)
		copyInput.Progress = cloneMap(input.Progress)
		copyInput.ObjectResults = append([]ScopedObjectResult(nil), input.ObjectResults...)
		copyInput.ObjectSummaries = cloneMap(input.ObjectSummaries)
		copyInput.FlowResults = cloneMap(input.FlowResults)
		copyInput.Diagnostics = append([]Diagnostic(nil), input.Diagnostics...)
		result.Inputs[index] = &copyInput
	}
	return result
}

func (r *Reducer) applyKnown(scope *Scope, event Event) error {
	switch value := event.(type) {
	case Hello:
		return r.applyHello(scope, value)
	case RunStarted:
		return r.applyRunStarted(scope, value)
	case InputDeclared:
		return r.applyInputDeclared(scope, value)
	case ManifestFinished:
		return r.applyManifest(scope, value)
	case InputStarted:
		return r.applyInputStarted(scope, value)
	case FlowPlanned:
		return r.applyFlowPlanned(scope, value)
	case ProgressSnapshot:
		return r.applyProgress(scope, value)
	case RetryScheduled:
		return r.applyRetry(scope, value)
	case Diagnostic:
		return r.applyDiagnostic(scope, value)
	case ObjectResult:
		return r.applyObjectResult(scope, value)
	case FlowResult:
		return r.applyFlowResult(scope, value)
	case InputFinished:
		return r.applyInputFinished(scope, value)
	case RunCancellationRequested:
		return r.applyCancellation(scope, value)
	case RunFinished:
		return r.applyRunFinished(scope, value)
	default:
		return fmt.Errorf("unsupported typed payload %T", event)
	}
}

func (r *Reducer) applyHello(scope *Scope, value Hello) error {
	if r.state.Hello != nil || r.state.NextSequence != 0 {
		return errors.New("hello must occur exactly once at sequence zero")
	}
	if scope != nil {
		return errors.New("hello must be run-scoped")
	}
	if strings.TrimSpace(value.ToolVersion) == "" || strings.TrimSpace(value.ToolCommit) == "" ||
		strings.TrimSpace(value.ResultSchemaVersion) == "" || strings.TrimSpace(value.ProfilePolicyVersion) == "" {
		return errors.New("tool, result schema, and profile policy versions are required")
	}
	if value.MaxEventBytes < MinimumMaxEventBytes || value.MaxEventBytes > AdvertisedMaxEventBytesLimit {
		return fmt.Errorf("max_event_bytes must be between %d and %d", MinimumMaxEventBytes, AdvertisedMaxEventBytesLimit)
	}
	if value.Capabilities == nil {
		return errors.New("capabilities must be an array")
	}
	seen := make(map[string]struct{}, len(value.Capabilities))
	for _, capability := range value.Capabilities {
		if !codePattern.MatchString(capability) {
			return fmt.Errorf("invalid capability %q", capability)
		}
		if _, duplicate := seen[capability]; duplicate {
			return fmt.Errorf("duplicate capability %q", capability)
		}
		seen[capability] = struct{}{}
	}
	value.Capabilities = append([]string(nil), value.Capabilities...)
	r.state.Hello = clonePtr(&value)
	return nil
}

func (r *Reducer) applyRunStarted(scope *Scope, value RunStarted) error {
	if scope != nil {
		return errors.New("run.started must be run-scoped")
	}
	if r.state.Started != nil {
		return errors.New("run.started already occurred")
	}
	if value.StartedAt.IsZero() {
		return errors.New("started_at is required")
	}
	if (value.Profile == "") != (value.ProfileVersion == "") {
		return errors.New("profile and profile_version must occur together")
	}
	if value.Profile != "" && (strings.TrimSpace(value.Profile) == "" || strings.TrimSpace(value.ProfileVersion) == "") {
		return errors.New("resolved profile and profile_version cannot be blank")
	}
	if value.Concurrency != nil && *value.Concurrency == 0 {
		return errors.New("resolved concurrency must be greater than zero")
	}
	if value.Transfers != nil && *value.Transfers == 0 {
		return errors.New("resolved transfers must be greater than zero")
	}
	if value.DryRunMode != "" && !validDryRunMode(value.DryRunMode) {
		return fmt.Errorf("unknown dry_run_mode %q", value.DryRunMode)
	}
	if value.VerificationMode != "" && !validVerificationMode(value.VerificationMode) {
		return fmt.Errorf("unknown verification_mode %q", value.VerificationMode)
	}
	r.state.Started = clonePtr(&value)
	return nil
}

func (r *Reducer) applyInputDeclared(scope *Scope, value InputDeclared) error {
	index, err := r.requireInputScope(scope, false)
	if err != nil {
		return err
	}
	if r.state.Started == nil {
		return errors.New("input cannot be declared before run.started")
	}
	if r.state.Manifest != nil {
		return errors.New("input cannot be declared after manifest.finished")
	}
	if strings.TrimSpace(value.Input) == "" {
		return errors.New("input is required")
	}
	if _, duplicate := r.state.Inputs[index]; duplicate {
		return fmt.Errorf("input index %d is already declared", index)
	}
	r.state.Inputs[index] = newInputState(value, r.options.RetainObjectResults)
	return nil
}

func (r *Reducer) applyManifest(scope *Scope, value ManifestFinished) error {
	if scope != nil {
		return errors.New("manifest.finished must be run-scoped")
	}
	if r.state.Started == nil {
		return errors.New("manifest cannot finish before run.started")
	}
	if r.state.Manifest != nil {
		return errors.New("manifest.finished already occurred")
	}
	if value.TotalInputs != uint64(len(r.state.Inputs)) {
		return fmt.Errorf("total_inputs is %d, but %d inputs were declared", value.TotalInputs, len(r.state.Inputs))
	}
	for index := range len(r.state.Inputs) {
		if _, exists := r.state.Inputs[index]; !exists {
			return fmt.Errorf("input manifest is missing contiguous index %d", index)
		}
	}
	r.state.Manifest = clonePtr(&value)
	return nil
}

func (r *Reducer) applyInputStarted(scope *Scope, value InputStarted) error {
	input, _, err := r.requireDeclaredInput(scope, false)
	if err != nil {
		return err
	}
	if r.state.Manifest == nil {
		return errors.New("input cannot start before manifest.finished")
	}
	if input.Started != nil {
		return errors.New("input.started already occurred")
	}
	if value.StartedAt.IsZero() {
		return errors.New("started_at is required")
	}
	input.Started = clonePtr(&value)
	return nil
}

func (r *Reducer) applyFlowPlanned(scope *Scope, value FlowPlanned) error {
	input, _, err := r.requireActiveInput(scope, true)
	if err != nil {
		return err
	}
	if err := validateFlow(value.FlowID, value.SourceID, value.Kind, value.Role); err != nil {
		return err
	}
	if scope.FlowID != value.FlowID {
		return errors.New("scope flow_id does not match payload flow_id")
	}
	if (value.Format != "" && strings.TrimSpace(value.Format) == "") ||
		(value.Container != "" && strings.TrimSpace(value.Container) == "") {
		return errors.New("flow format and container cannot be blank")
	}
	if value.Root == (value.ParentFlowID != "") {
		return errors.New("flow plan must be either root or name one parent_flow_id")
	}
	if value.ParentFlowID == value.FlowID {
		return errors.New("flow cannot be its own parent")
	}
	if value.ParentFlowID != "" {
		if _, err := uuid.Parse(value.ParentFlowID); err != nil {
			return fmt.Errorf("parent_flow_id must be a UUID: %w", err)
		}
	}
	if _, duplicate := input.PlannedFlows[value.FlowID]; duplicate {
		return fmt.Errorf("flow %s was already planned", value.FlowID)
	}
	input.PlannedFlows[value.FlowID] = value
	return nil
}

func (r *Reducer) applyProgress(scope *Scope, value ProgressSnapshot) error {
	input, _, err := r.requireActiveInput(scope, false)
	if err != nil {
		return err
	}
	if !validProgressPhase(value.Phase) {
		return fmt.Errorf("unknown progress phase %q", value.Phase)
	}
	if value.Revision == 0 || value.Revision <= input.ProgressRevision {
		return errors.New("progress revision must increase for each input snapshot")
	}
	if value.CompletedObjects > value.TotalObjects || value.CompletedBytes > value.TotalBytes {
		return errors.New("completed progress cannot exceed its current totals")
	}
	previous, exists := input.Progress[value.Phase]
	if exists {
		if value.CompletedObjects < previous.CompletedObjects || value.CompletedBytes < previous.CompletedBytes ||
			value.TotalObjects < previous.TotalObjects || value.TotalBytes < previous.TotalBytes ||
			value.ElapsedMS < previous.ElapsedMS {
			return errors.New("cumulative progress counters and totals cannot decrease")
		}
		if previous.TotalsFinal && (!value.TotalsFinal || value.TotalObjects != previous.TotalObjects || value.TotalBytes != previous.TotalBytes) {
			return errors.New("final progress totals cannot reopen or change")
		}
	}
	input.Progress[value.Phase] = value
	input.ProgressRevision = value.Revision
	return nil
}

func (r *Reducer) applyRetry(scope *Scope, value RetryScheduled) error {
	if r.state.Started == nil {
		return errors.New("retry cannot precede run.started")
	}
	if !codePattern.MatchString(value.Operation) || len(value.Operation) > MaxDiagnosticCodeBytes ||
		value.Attempt < 2 || value.MaxAttempts < value.Attempt {
		return errors.New("operation and a valid retry attempt/max_attempts are required")
	}
	if value.StatusClass == "" && value.ErrorClass == "" {
		return errors.New("status_class or error_class is required")
	}
	if (value.StatusClass != "" && (!codePattern.MatchString(value.StatusClass) || len(value.StatusClass) > MaxDiagnosticCodeBytes)) ||
		(value.ErrorClass != "" && (!codePattern.MatchString(value.ErrorClass) || len(value.ErrorClass) > MaxDiagnosticCodeBytes)) {
		return errors.New("retry status_class and error_class must be stable codes")
	}
	if scope != nil {
		input, _, err := r.requireDeclaredInput(scope, false)
		if err != nil {
			return err
		}
		if input.Finished != nil {
			return errors.New("retry cannot follow input.finished")
		}
		input.RetryCount++
	}
	r.state.RetryCount++
	return nil
}

func (r *Reducer) applyDiagnostic(scope *Scope, value Diagnostic) error {
	if r.state.Started == nil {
		return errors.New("diagnostic cannot precede run.started")
	}
	if !validSeverity(value.Severity) || !codePattern.MatchString(value.Code) || strings.TrimSpace(value.Message) == "" {
		return errors.New("severity, stable code, and message are required")
	}
	if len(value.Code) > MaxDiagnosticCodeBytes || len(value.Message) > MaxDiagnosticMessageBytes || len(value.Hint) > MaxDiagnosticHintBytes {
		return errors.New("diagnostic exceeds its bounded code/message/hint fields")
	}
	if scope != nil {
		input, _, err := r.requireDeclaredInput(scope, scope.FlowID != "")
		if err != nil {
			return err
		}
		if input.Finished != nil {
			return errors.New("diagnostic cannot follow input.finished")
		}
		input.Diagnostics = append(input.Diagnostics, value)
	} else {
		r.state.Diagnostics = append(r.state.Diagnostics, value)
	}
	return nil
}

func (r *Reducer) applyObjectResult(scope *Scope, value ObjectResult) error {
	input, _, err := r.requireActiveInput(scope, true)
	if err != nil {
		return err
	}
	if scope.ObjectID == "" || scope.ObjectID != value.ObjectID {
		return errors.New("scope object_id must match payload object_id")
	}
	if _, err := uuid.Parse(value.ObjectID); err != nil {
		return errors.New("object_id must be a UUID")
	}
	if strings.TrimSpace(value.Timerange) == "" || !sha256Pattern.MatchString(value.SHA256) ||
		!validObjectDisposition(value.Disposition) || !validObjectVerification(value.Verification) ||
		!validVerificationMethod(value.VerificationMethod) {
		return errors.New("timerange, lowercase SHA-256, disposition, verification status, and verification method are required")
	}
	if value.Verification == ObjectVerificationVerified && value.VerificationMethod == VerificationMethodNone {
		return errors.New("verified Object requires storage or readback verification method")
	}
	if value.Verification != ObjectVerificationVerified && value.Verification != ObjectVerificationFailed &&
		value.VerificationMethod != VerificationMethodNone {
		return errors.New("object without a verification attempt must use method none")
	}
	if value.Verification == ObjectVerificationFailed && value.VerificationMethod != VerificationMethodReadback {
		return errors.New("failed Object verification requires readback method")
	}
	summary := input.ObjectSummaries[scope.FlowID]
	if err := addObjectToSummary(&summary, value); err != nil {
		return err
	}
	if input.ObjectResults != nil {
		input.ObjectResults = append(input.ObjectResults, ScopedObjectResult{FlowID: scope.FlowID, Result: value})
	}
	input.ObjectSummaries[scope.FlowID] = summary
	return nil
}

func (r *Reducer) applyFlowResult(scope *Scope, value FlowResult) error {
	input, _, err := r.requireActiveInput(scope, true)
	if err != nil {
		return err
	}
	if scope.FlowID != value.FlowID {
		return errors.New("scope flow_id does not match payload flow_id")
	}
	if err := validateFlow(value.FlowID, value.SourceID, value.Kind, value.Role); err != nil {
		return err
	}
	if !validFlowDisposition(value.Disposition) {
		return fmt.Errorf("unknown Flow disposition %q", value.Disposition)
	}
	if err := validateObjectSummary(value.ObjectSummary); err != nil {
		return err
	}
	if value.Kind == FlowKindCollection && value.ObjectSummary.Total != 0 {
		return errors.New("association-only collection Flow cannot own Objects")
	}
	if _, duplicate := input.FlowResults[value.FlowID]; duplicate {
		return fmt.Errorf("flow %s already has a terminal result", value.FlowID)
	}
	if planned, exists := input.PlannedFlows[value.FlowID]; exists {
		if planned.SourceID != value.SourceID || planned.Kind != value.Kind || planned.Role != value.Role {
			return fmt.Errorf("flow %s terminal identity differs from its plan", value.FlowID)
		}
	}
	if emitted := input.ObjectSummaries[value.FlowID]; emitted != value.ObjectSummary {
		return fmt.Errorf("flow %s Object summary does not match preceding object.result records", value.FlowID)
	}
	input.FlowResults[value.FlowID] = value
	return nil
}

func (r *Reducer) applyInputFinished(scope *Scope, value InputFinished) error {
	input, _, err := r.requireDeclaredInput(scope, false)
	if err != nil {
		return err
	}
	if r.state.Manifest == nil {
		return errors.New("input cannot finish before manifest.finished")
	}
	if input.Finished != nil {
		return errors.New("input.finished already occurred")
	}
	if input.Declared.Input != value.Input {
		return errors.New("payload input does not match input.declared")
	}
	if strings.TrimSpace(value.Profile) == "" || strings.TrimSpace(value.ProfileVersion) == "" ||
		!validInputStatus(value.Status) || !validVerification(value.Verification) {
		return errors.New("profile, profile_version, status, and verification are required")
	}
	if value.Status == InputFailed {
		if !codePattern.MatchString(value.ErrorCode) || len(value.ErrorCode) > MaxDiagnosticCodeBytes ||
			strings.TrimSpace(value.Message) == "" || len(value.Message) > MaxDiagnosticMessageBytes {
			return errors.New("failed input requires a stable error_code and message")
		}
	} else if value.ErrorCode != "" || value.Message != "" {
		return errors.New("successful input cannot carry error_code or message")
	}
	if input.Started == nil {
		if value.Status != InputFailed ||
			(value.Verification != VerificationNotReached && value.Verification != VerificationNotRequested) {
			return errors.New("undispatched input must finish as failed with verification not_reached or not_requested")
		}
	}
	if value.SHA256 != "" && !sha256Pattern.MatchString(value.SHA256) {
		return errors.New("sha256 must contain 64 lowercase hexadecimal characters")
	}
	if (value.FFmpegVersion == "") != (value.MediaToolchain == "") {
		return errors.New("ffmpeg_version and media_toolchain must occur together")
	}
	if value.MediaToolchain != "" && !sha256Pattern.MatchString(strings.TrimPrefix(value.MediaToolchain, "sha256:")) {
		return errors.New("media_toolchain must be a sha256 fingerprint")
	}
	var emittedObjects uint64
	for _, summary := range input.ObjectSummaries {
		emittedObjects += summary.Total
	}
	if value.FlowCount != uint64(len(input.FlowResults)) || value.ObjectCount != emittedObjects {
		return fmt.Errorf("terminal counts (%d Flows, %d Objects) do not match emitted results (%d, %d)",
			value.FlowCount, value.ObjectCount, len(input.FlowResults), emittedObjects)
	}
	if value.RootFlowID != "" {
		if _, exists := input.FlowResults[value.RootFlowID]; !exists {
			return errors.New("root_flow_id has no preceding flow.result")
		}
	}
	for flowID := range input.PlannedFlows {
		if _, exists := input.FlowResults[flowID]; !exists {
			return fmt.Errorf("planned flow %s has no terminal flow.result", flowID)
		}
	}
	if len(input.PlannedFlows) > 0 {
		rootID := ""
		for flowID, planned := range input.PlannedFlows {
			if planned.Root {
				if rootID != "" {
					return errors.New("input has more than one planned root flow")
				}
				rootID = flowID
				continue
			}
			parent, exists := input.PlannedFlows[planned.ParentFlowID]
			if !exists || !parent.Root {
				return fmt.Errorf("planned flow %s does not reference the planned root", flowID)
			}
		}
		if rootID == "" {
			return errors.New("planned flow graph has no root")
		}
		if value.RootFlowID != rootID {
			return fmt.Errorf("root_flow_id %s does not match planned root %s", value.RootFlowID, rootID)
		}
	}
	for flowID, flow := range input.FlowResults {
		if input.ObjectSummaries[flowID] != flow.ObjectSummary {
			return fmt.Errorf("flow %s terminal Object summary changed after flow.result", flowID)
		}
	}
	input.Finished = clonePtr(&value)
	return nil
}

func (r *Reducer) applyCancellation(scope *Scope, value RunCancellationRequested) error {
	if scope != nil {
		return errors.New("run.cancellation_requested must be run-scoped")
	}
	if r.state.Started == nil {
		return errors.New("cancellation cannot precede run.started")
	}
	if r.state.Cancellation != nil {
		return errors.New("cancellation was already requested")
	}
	if !validCancellationReason(value.Reason) {
		return fmt.Errorf("unknown cancellation reason %q", value.Reason)
	}
	r.state.Cancellation = clonePtr(&value)
	return nil
}

func (r *Reducer) applyRunFinished(scope *Scope, value RunFinished) error {
	if scope != nil {
		return errors.New("run.finished must be run-scoped")
	}
	if r.state.Started == nil || r.state.Manifest == nil {
		return errors.New("run cannot finish before run.started and manifest.finished")
	}
	if !validRunOutcome(value.Outcome) {
		return fmt.Errorf("unknown run outcome %q", value.Outcome)
	}
	if value.Total != uint64(len(r.state.Inputs)) || value.Total != r.state.Manifest.TotalInputs ||
		value.Succeeded+value.Failed != value.Total {
		return errors.New("run totals do not cover the declared manifest")
	}
	var succeeded, failed uint64
	for index, input := range r.state.Inputs {
		if input.Finished == nil {
			return fmt.Errorf("input %d has no terminal input.finished record", index)
		}
		if input.Finished.Status == InputFailed {
			failed++
		} else {
			succeeded++
		}
	}
	if value.Succeeded != succeeded || value.Failed != failed {
		return errors.New("run success/failure counts do not match input.finished records")
	}
	if value.Retries != r.state.RetryCount {
		return fmt.Errorf("run retries is %d, but %d retry.scheduled events were emitted", value.Retries, r.state.RetryCount)
	}
	if value.Outcome == RunSucceeded && (value.ExitCode != 0 || value.Failed != 0) {
		return errors.New("succeeded run requires exit_code zero and no failed inputs")
	}
	if value.Outcome != RunSucceeded && value.ExitCode == 0 {
		return errors.New("failed or interrupted run requires a non-zero exit_code")
	}
	if value.ExitCode < 0 || value.ExitCode > 255 {
		return errors.New("exit_code must be between 0 and 255")
	}
	if value.Outcome == RunInterrupted && r.state.Cancellation == nil {
		return errors.New("interrupted run requires run.cancellation_requested")
	}
	if value.Outcome == RunInterrupted && value.ExitCode != 8 {
		return errors.New("interrupted run requires exit_code 8")
	}
	if r.state.Cancellation != nil && value.Outcome != RunInterrupted {
		return errors.New("run with cancellation requested must finish as interrupted")
	}
	if value.Outcome == RunPartial && (value.Succeeded == 0 || value.Failed == 0) {
		return errors.New("partial run requires both succeeded and failed inputs")
	}
	if value.Succeeded > 0 && value.Failed > 0 &&
		value.Outcome != RunPartial && value.Outcome != RunInterrupted {
		return errors.New("run with successful and failed inputs must use partial or interrupted outcome")
	}
	if value.Outcome == RunFailed && value.Failed == 0 && !hasErrorDiagnostic(r.state.Diagnostics) {
		return errors.New("run-level failure after successful inputs requires an error diagnostic")
	}
	r.state.Finished = clonePtr(&value)
	return nil
}

func (r *Reducer) requireInputScope(scope *Scope, flowRequired bool) (int, error) {
	if scope == nil || scope.InputIndex == nil {
		return 0, errors.New("input_index scope is required")
	}
	if flowRequired && scope.FlowID == "" {
		return 0, errors.New("flow_id scope is required")
	}
	if !flowRequired && (scope.FlowID != "" || scope.ObjectID != "") {
		return 0, errors.New("event must be scoped only to its input")
	}
	return *scope.InputIndex, nil
}

func (r *Reducer) requireDeclaredInput(scope *Scope, flowAllowed bool) (*InputState, int, error) {
	if scope == nil || scope.InputIndex == nil {
		return nil, 0, errors.New("input_index scope is required")
	}
	if !flowAllowed && (scope.FlowID != "" || scope.ObjectID != "") {
		return nil, 0, errors.New("event must be scoped only to its input")
	}
	input, exists := r.state.Inputs[*scope.InputIndex]
	if !exists {
		return nil, 0, fmt.Errorf("input index %d was not declared", *scope.InputIndex)
	}
	return input, *scope.InputIndex, nil
}

func (r *Reducer) requireActiveInput(scope *Scope, flowRequired bool) (*InputState, int, error) {
	input, index, err := r.requireDeclaredInput(scope, flowRequired)
	if err != nil {
		return nil, 0, err
	}
	if input.Started == nil {
		return nil, 0, errors.New("event cannot precede input.started")
	}
	if input.Finished != nil {
		return nil, 0, errors.New("event cannot follow input.finished")
	}
	return input, index, nil
}

func (r *Reducer) advance(envelope Envelope) {
	if r.state.NextSequence == 0 {
		r.state.ProtocolVersion = envelope.ProtocolVersion
		r.state.RunID = envelope.RunID
	}
	r.lastElapsedMS = envelope.ElapsedMS
	r.state.NextSequence++
}

func newInputState(declared InputDeclared, retainObjects bool) *InputState {
	result := &InputState{
		Declared: clonePtr(&declared), PlannedFlows: make(map[string]FlowPlanned),
		Progress: make(map[ProgressPhase]ProgressSnapshot), ObjectSummaries: make(map[string]ObjectSummary),
		FlowResults: make(map[string]FlowResult),
	}
	if retainObjects {
		result.ObjectResults = make([]ScopedObjectResult, 0)
	}
	return result
}

func validateScope(scope *Scope) error {
	if scope == nil {
		return nil
	}
	if scope.InputIndex == nil && scope.FlowID == "" && scope.ObjectID == "" {
		return errors.New("scope cannot be empty")
	}
	if scope.InputIndex != nil && *scope.InputIndex < 0 {
		return errors.New("scope input_index cannot be negative")
	}
	if scope.FlowID != "" {
		if scope.InputIndex == nil {
			return errors.New("scope flow_id requires input_index")
		}
		if _, err := uuid.Parse(scope.FlowID); err != nil {
			return errors.New("scope flow_id must be a UUID")
		}
	}
	if scope.ObjectID != "" {
		if scope.FlowID == "" {
			return errors.New("scope object_id requires flow_id")
		}
		if _, err := uuid.Parse(scope.ObjectID); err != nil {
			return errors.New("scope object_id must be a UUID")
		}
	}
	return nil
}

func validateFlow(flowID, sourceID string, kind FlowKind, role string) error {
	if _, err := uuid.Parse(flowID); err != nil {
		return errors.New("flow_id must be a UUID")
	}
	if _, err := uuid.Parse(sourceID); err != nil {
		return errors.New("source_id must be a UUID")
	}
	if kind != FlowKindEssence && kind != FlowKindCollection && kind != FlowKindMuxed {
		return fmt.Errorf("unknown Flow kind %q", kind)
	}
	if kind == FlowKindEssence && strings.TrimSpace(role) == "" {
		return errors.New("essence Flow requires role")
	}
	if kind != FlowKindEssence && role != "" {
		return errors.New("collection or muxed Flow cannot have an essence role")
	}
	return nil
}

func compatibleVersion(version string) bool {
	major, minor, ok := strings.Cut(version, ".")
	if !ok || major != "2" || minor == "" {
		return false
	}
	_, err := strconv.ParseUint(minor, 10, 32)
	return err == nil
}

func isJSONObject(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return len(trimmed) >= 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}'
}

func validProgressPhase(value ProgressPhase) bool {
	switch value {
	case ProgressStore, ProgressVerify:
		return true
	default:
		return false
	}
}

func validSeverity(value Severity) bool {
	switch value {
	case SeverityDebug, SeverityInfo, SeverityWarning, SeverityError:
		return true
	default:
		return false
	}
}

func validObjectDisposition(value ObjectDisposition) bool {
	switch value {
	case ObjectDispositionPlanned, ObjectDispositionUploaded, ObjectDispositionRegistrationIndeterminate,
		ObjectDispositionRegistered, ObjectDispositionRejected, ObjectDispositionIngested,
		ObjectDispositionResumed, ObjectDispositionRetracted, ObjectDispositionStranded,
		ObjectDispositionUnattempted:
		return true
	default:
		return false
	}
}

func validDryRunMode(value string) bool {
	switch value {
	case "off", "fast", "exact":
		return true
	default:
		return false
	}
}

func validVerificationMode(value string) bool {
	switch value {
	case "auto", "readback", "none":
		return true
	default:
		return false
	}
}

func validObjectVerification(value ObjectVerificationStatus) bool {
	switch value {
	case ObjectVerificationVerified, ObjectVerificationNotRequested, ObjectVerificationNotReached,
		ObjectVerificationFailed:
		return true
	default:
		return false
	}
}

func validVerificationMethod(value VerificationMethod) bool {
	switch value {
	case VerificationMethodNone, VerificationMethodStorage, VerificationMethodReadback:
		return true
	default:
		return false
	}
}

func addObjectToSummary(summary *ObjectSummary, object ObjectResult) error {
	next := *summary
	add := func(name string, target *uint64, value uint64) error {
		total, carry := bits.Add64(*target, value, 0)
		if carry != 0 {
			return fmt.Errorf("object summary %s overflow", name)
		}
		*target = total
		return nil
	}
	if err := add("total", &next.Total, 1); err != nil {
		return err
	}
	if err := add("bytes", &next.Bytes, object.Bytes); err != nil {
		return err
	}
	switch object.Disposition {
	case ObjectDispositionIngested:
		if err := add("ingested", &next.Ingested, 1); err != nil {
			return err
		}
	case ObjectDispositionResumed:
		if err := add("resumed", &next.Resumed, 1); err != nil {
			return err
		}
	case ObjectDispositionRejected:
		if err := add("rejected", &next.Rejected, 1); err != nil {
			return err
		}
	case ObjectDispositionRetracted:
		if err := add("retracted", &next.Retracted, 1); err != nil {
			return err
		}
	case ObjectDispositionStranded, ObjectDispositionRegistrationIndeterminate:
		if err := add("stranded", &next.Stranded, 1); err != nil {
			return err
		}
	case ObjectDispositionPlanned, ObjectDispositionUploaded, ObjectDispositionRegistered, ObjectDispositionUnattempted:
		if err := add("unattempted", &next.Unattempted, 1); err != nil {
			return err
		}
	}
	if object.Verification == ObjectVerificationVerified {
		if err := add("verified", &next.Verified, 1); err != nil {
			return err
		}
		switch object.VerificationMethod {
		case VerificationMethodStorage:
			if err := add("storage-verified", &next.StorageVerified, 1); err != nil {
				return err
			}
		case VerificationMethodReadback:
			if err := add("readback-verified", &next.ReadbackVerified, 1); err != nil {
				return err
			}
		}
	}
	*summary = next
	return nil
}

func validateObjectSummary(summary ObjectSummary) error {
	checkedSum := func(values ...uint64) (uint64, bool) {
		var total uint64
		for _, value := range values {
			var carry uint64
			total, carry = bits.Add64(total, value, 0)
			if carry != 0 {
				return 0, false
			}
		}
		return total, true
	}
	dispositions, ok := checkedSum(summary.Ingested, summary.Resumed, summary.Rejected, summary.Retracted,
		summary.Stranded, summary.Unattempted)
	if !ok {
		return errors.New("object summary disposition counters overflow")
	}
	if dispositions != summary.Total {
		return errors.New("object summary dispositions must cover total")
	}
	verificationMethods, ok := checkedSum(summary.StorageVerified, summary.ReadbackVerified)
	if !ok {
		return errors.New("object summary verification counters overflow")
	}
	if verificationMethods != summary.Verified || summary.Verified > summary.Total {
		return errors.New("object summary verification methods must cover verified total")
	}
	return nil
}

func validFlowDisposition(value FlowDisposition) bool {
	switch value {
	case FlowPlannedDisposition, FlowUnchanged, FlowWritten, FlowIndeterminate, FlowUnattempted:
		return true
	default:
		return false
	}
}

func validInputStatus(value InputStatus) bool {
	switch value {
	case InputPlanned, InputIngested, InputResumed, InputFailed:
		return true
	default:
		return false
	}
}

func validVerification(value VerificationStatus) bool {
	switch value {
	case VerificationVerified, VerificationNotRequested, VerificationNotReached,
		VerificationFailedRetracted, VerificationFailedStranded:
		return true
	default:
		return false
	}
}

func validCancellationReason(value CancellationReason) bool {
	switch value {
	case CancellationSignal, CancellationParent, CancellationDeadline, CancellationOutputClosed, CancellationInternal:
		return true
	default:
		return false
	}
}

func validRunOutcome(value RunOutcome) bool {
	switch value {
	case RunSucceeded, RunFailed, RunPartial, RunInterrupted:
		return true
	default:
		return false
	}
}

func hasErrorDiagnostic(diagnostics []Diagnostic) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Severity == SeverityError {
			return true
		}
	}
	return false
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
