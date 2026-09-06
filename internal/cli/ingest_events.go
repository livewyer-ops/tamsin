package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/ingestevent"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/version"
)

const eventProgressInterval = 250 * time.Millisecond

type eventProgressKey struct {
	input int
	phase progress.Phase
}

type eventProgressState struct {
	at          time.Time
	revision    uint64
	totalsFinal bool
}

type pendingProgressEvent struct {
	scope *ingestevent.Scope
	event ingestevent.ProgressSnapshot
}

// terminalDecision is the single cancellation snapshot shared by every
// terminal projection. Once frozen, later cancellation belongs to the caller's
// shutdown of an already-terminalizing command and cannot rewrite an event
// stream which is already being completed.
type terminalDecision struct {
	interrupted bool
	cause       error
	reason      ingestevent.CancellationReason
}

// ingestEventOutput is the single serializer between a concurrent ingest and
// the process protocol. Lifecycle and terminal records are never dropped.
// Progress is cumulative and sampled at four updates per second per input and
// phase; sealing a total and completing a phase always bypass the sampler.
type ingestEventOutput struct {
	mu      sync.Mutex
	encoder *ingestevent.Encoder
	cancel  context.CancelCauseFunc
	done    chan struct{}
	now     func() time.Time
	started time.Time

	runStarted       bool
	manifest         bool
	cancellation     bool
	terminalFrozen   bool
	terminalDecision terminalDecision
	finished         bool
	exitCode         int
	closed           bool
	err              error

	declared        map[int]string
	startedInputs   map[int]bool
	terminal        map[int]ingestevent.InputStatus
	plannedFlows    map[int]map[string]ingestevent.FlowPlanned
	objectSummaries map[int]map[string]ingest.ObjectSummary
	retries         uint64

	// Progress is the only disposable event class. Reporters replace the latest
	// value in this bounded mailbox without taking mu or writing to the process
	// pipe. The flusher remains the sole Encoder caller by taking mu, while
	// lifecycle and terminal methods keep their synchronous write acknowledgement.
	progressMu      sync.Mutex
	progress        map[eventProgressKey]eventProgressState
	pendingProgress map[eventProgressKey]pendingProgressEvent
	progressStarted map[int]bool
	progressSealed  map[int]bool
	progressWake    chan struct{}
	progressStop    chan struct{}
	progressDone    chan struct{}
	progressStopped bool
}

func newIngestEventOutput(writer io.Writer, runID string, cancel context.CancelCauseFunc) (*ingestEventOutput, error) {
	now := time.Now
	output := &ingestEventOutput{
		cancel: cancel, done: make(chan struct{}), now: now, started: now(),
		declared: make(map[int]string), startedInputs: make(map[int]bool),
		terminal: make(map[int]ingestevent.InputStatus), plannedFlows: make(map[int]map[string]ingestevent.FlowPlanned),
		objectSummaries: make(map[int]map[string]ingest.ObjectSummary),
		progress:        make(map[eventProgressKey]eventProgressState), pendingProgress: make(map[eventProgressKey]pendingProgressEvent),
		progressStarted: make(map[int]bool), progressSealed: make(map[int]bool),
		progressWake: make(chan struct{}, 1), progressStop: make(chan struct{}), progressDone: make(chan struct{}),
	}
	encoder, err := ingestevent.NewEncoder(writer, runID)
	if err != nil {
		return nil, err
	}
	output.encoder = encoder
	output.mu.Lock()
	err = output.emitLocked(nil, ingestevent.Hello{
		ToolVersion: version.Version, ToolCommit: version.SourceCommit(), ToolBuildDate: version.BuildDate(),
		ResultSchemaVersion: ingest.ResultSchemaVersion, ProfilePolicyVersion: ingest.ProfilePolicyVersion,
		MaxEventBytes: ingestevent.DefaultMaxEventBytes,
		Capabilities: []string{
			"graceful_cancel", "live_object_results", "progress", "progress_coalescing", "retry_events", "terminal_results",
		},
	})
	output.mu.Unlock()
	if err != nil {
		return output, err
	}
	go output.flushProgressLoop()
	return output, nil
}

// finishBootstrapEvents gives argument, configuration, and other pre-RunE
// failures the same complete machine protocol as failures after media work has
// begun. It is called by Execute only for an ingest invocation explicitly
// configured for JSON.
func (a *application) finishBootstrapEvents(ctx context.Context, cause error, exitCode int) (int, error) {
	if a.events == nil {
		output, err := newIngestEventOutput(a.stdout, a.runID, nil)
		if err != nil {
			return ExitGeneral, err
		}
		a.events = output
	}
	resolvedCode, err := a.events.Finish(cause, exitCode, nil, observability.Snapshot{}, ctx)
	if err == nil {
		a.ingestTerminalFrozen = true
	}
	return resolvedCode, err
}

func (o *ingestEventOutput) Start(options *ingestFlagValues) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.startLocked(options)
}

func (o *ingestEventOutput) startLocked(options *ingestFlagValues) error {
	if o.runStarted {
		return nil
	}
	event := ingestevent.RunStarted{StartedAt: o.started.UTC()}
	if options != nil {
		event.Profile = options.profile
		event.ProfileVersion = options.profileVersion
		event.DryRunMode = options.dryRun
		event.VerificationMode = options.verify
		if options.concurrency > 0 {
			event.Concurrency = ingestevent.KnownSetting(uint64(options.concurrency))
		}
		transfers := options.transfers
		if transfers <= 0 {
			transfers = options.concurrency
		}
		if transfers > 0 {
			event.Transfers = ingestevent.KnownSetting(uint64(transfers))
		}
		event.RequestedInputs = ingestevent.KnownInputCount(uint64(len(options.inputs)))
	}
	if err := o.emitLocked(nil, event); err != nil {
		return err
	}
	o.runStarted = true
	return nil
}

func (o *ingestEventOutput) Declare(items []source.Item) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.startLocked(nil); err != nil {
		return err
	}
	if o.manifest {
		return errors.New("ingest event manifest is already sealed")
	}
	for index, item := range items {
		input := ingest.SafeInputURI(item.URI)
		if err := o.emitLocked(ingestevent.InputScope(index), ingestevent.InputDeclared{Input: input}); err != nil {
			return err
		}
		o.declared[index] = input
	}
	if err := o.emitLocked(nil, ingestevent.ManifestFinished{TotalInputs: uint64(len(items))}); err != nil {
		return err
	}
	o.manifest = true
	return nil
}

// InputStarted implements ingest.LifecycleObserver independently of progress,
// so --progress none changes no lifecycle semantics or timestamps.
func (o *ingestEventOutput) InputStarted(index int) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, declared := o.declared[index]; !declared {
		return o.failLocked(fmt.Errorf("input start references undeclared input %d", index))
	}
	return o.ensureInputStartedLocked(index)
}

// FlowPlanned publishes the validated, effective graph before remote mutation
// or Object transfer begins. Kind and media metadata come from the graph
// itself rather than being guessed later from an empty Result.Role.
func (o *ingestEventOutput) FlowPlanned(index int, plan ingest.FlowPlan) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, declared := o.declared[index]; !declared {
		return o.failLocked(fmt.Errorf("flow plan references undeclared input %d", index))
	}
	if err := o.ensureInputStartedLocked(index); err != nil {
		return err
	}
	kind := ingestevent.FlowKind(plan.Kind)
	event := ingestevent.FlowPlanned{
		FlowID: plan.FlowID, SourceID: plan.SourceID, Kind: kind, Role: plan.Role,
		Root: plan.Root, ParentFlowID: plan.ParentFlowID,
		Format: plan.Format, Container: plan.Container, TAMSFlowProfileID: plan.TAMSFlowProfileID,
	}
	if o.plannedFlows[index] == nil {
		o.plannedFlows[index] = make(map[string]ingestevent.FlowPlanned)
	}
	if _, duplicate := o.plannedFlows[index][plan.FlowID]; duplicate {
		return o.failLocked(fmt.Errorf("flow %s was planned twice", plan.FlowID))
	}
	if err := o.emitLocked(ingestevent.FlowScope(index, plan.FlowID), event); err != nil {
		return err
	}
	o.plannedFlows[index][plan.FlowID] = event
	return nil
}

// ObjectsCompleted publishes each terminal Media Object as soon as its commit
// batch reaches a durable disposition. The input terminal record later carries
// only compact counters, so long jobs do not wait until the end to expose
// recovery handles.
func (o *ingestEventOutput) ObjectsCompleted(index int, flowID string, objects []ingest.ObjectResult) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.objectsCompletedLocked(index, flowID, objects)
}

func (o *ingestEventOutput) objectsCompletedLocked(index int, flowID string, objects []ingest.ObjectResult) error {
	if _, declared := o.declared[index]; !declared {
		return o.failLocked(fmt.Errorf("object results reference undeclared input %d", index))
	}
	if err := o.ensureInputStartedLocked(index); err != nil {
		return err
	}
	if o.objectSummaries[index] == nil {
		o.objectSummaries[index] = make(map[string]ingest.ObjectSummary)
	}
	summary := o.objectSummaries[index][flowID]
	for _, object := range objects {
		if object.Bytes < 0 {
			return o.failLocked(fmt.Errorf("object %s has a negative byte count", object.ObjectID))
		}
		if err := o.emitLocked(ingestevent.ObjectScope(index, flowID, object.ObjectID), ingestevent.ObjectResult{
			ObjectID: object.ObjectID, Timerange: object.Timerange, Bytes: uint64(object.Bytes), SHA256: object.SHA256,
			Disposition:        ingestevent.ObjectDisposition(object.Disposition),
			Verification:       ingestevent.ObjectVerificationStatus(object.Verification),
			VerificationMethod: ingestevent.VerificationMethod(object.VerificationMethod),
		}); err != nil {
			return err
		}
		ingest.AccumulateObjectSummary(&summary, object)
	}
	o.objectSummaries[index][flowID] = summary
	return nil
}

// Report implements progress.Reporter. Optional snapshots cannot return an
// error through that interface, so an output failure is retained and cancels
// the operation context; RunObserved then enters its normal reconciliation
// path and the CLI reports the output failure as the primary error.
func (o *ingestEventOutput) Report(snapshot progress.Snapshot) {
	if err := snapshot.Validate(); err != nil {
		return
	}
	o.progressMu.Lock()
	index := snapshot.Scope.InputIndex
	if o.progressStopped || o.progressSealed[index] || !o.progressStarted[index] {
		o.progressMu.Unlock()
		return
	}

	// The initial verify tracker is an implementation detail. Store's matching
	// zero snapshot already tells a UI that analysis is under way; the first
	// sealed verify total is the useful verification event.
	if snapshot.Phase == progress.PhaseVerify && !snapshot.TotalsFinal &&
		snapshot.CompletedObjects == 0 && snapshot.CompletedBytes == 0 {
		o.progressMu.Unlock()
		return
	}
	key := eventProgressKey{input: index, phase: snapshot.Phase}
	previous := o.progress[key]
	now := o.now()
	complete := snapshot.TotalsFinal && snapshot.CompletedObjects == snapshot.TotalObjects &&
		snapshot.CompletedBytes == snapshot.TotalBytes
	phaseTransition := snapshot.TotalsFinal && !previous.totalsFinal
	if previous.revision > 0 && !complete && !phaseTransition && now.Sub(previous.at) < eventProgressInterval {
		o.progressMu.Unlock()
		return
	}
	if previous.revision > 0 && snapshot.Revision > 0 && snapshot.Revision <= previous.revision {
		o.progressMu.Unlock()
		return
	}
	phase := ingestevent.ProgressStore
	if snapshot.Phase == progress.PhaseVerify {
		phase = ingestevent.ProgressVerify
	}
	event := ingestevent.ProgressSnapshot{
		Revision: snapshot.Revision, Phase: phase, TotalsFinal: snapshot.TotalsFinal,
		CompletedObjects: uint64(snapshot.CompletedObjects), TotalObjects: uint64(snapshot.TotalObjects),
		CompletedBytes: uint64(snapshot.CompletedBytes), TotalBytes: uint64(snapshot.TotalBytes),
		ElapsedMS: durationMilliseconds(now.Sub(o.started)),
	}
	o.progress[key] = eventProgressState{at: now, revision: snapshot.Revision, totalsFinal: snapshot.TotalsFinal}
	o.pendingProgress[key] = pendingProgressEvent{scope: ingestevent.InputScope(index), event: event}
	o.progressMu.Unlock()
	select {
	case o.progressWake <- struct{}{}:
	default:
	}
}

func (o *ingestEventOutput) Close() {
	o.mu.Lock()
	o.sealAllProgressLocked()
	_ = o.flushProgressLocked(nil)
	o.stopProgressLocked()
	o.mu.Unlock()
	<-o.progressDone
}

func (o *ingestEventOutput) Retry(event observability.RetryEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil || o.finished {
		return
	}
	retry := ingestevent.RetryScheduled{
		Operation: event.Operation.String(), Attempt: uint64(event.Attempt), MaxAttempts: uint64(event.MaxAttempts),
		DelayMS: durationMilliseconds(event.Backoff), StatusClass: event.StatusClass, ErrorClass: event.ErrorClass,
	}
	if err := o.emitLocked(nil, retry); err == nil {
		o.retries++
	}
}

func (o *ingestEventOutput) Result(index int, result ingest.Result) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.resultLocked(index, result)
}

func (o *ingestEventOutput) resultLocked(index int, result ingest.Result) error {
	input, declared := o.declared[index]
	if !declared {
		return o.failLocked(fmt.Errorf("terminal result references undeclared input %d", index))
	}
	if _, duplicate := o.terminal[index]; duplicate {
		return o.failLocked(fmt.Errorf("input %d already has a terminal event", index))
	}
	o.sealProgressInputLocked(index)
	if err := o.flushProgressLocked(&index); err != nil {
		return err
	}
	if len(result.Flows) > 0 || result.Status != ingest.ResultStatusFailed {
		if err := o.ensureInputStartedLocked(index); err != nil {
			return err
		}
	}
	for _, flow := range result.Flows {
		planned, wasPlanned := o.plannedFlows[index][flow.FlowID]
		kind, role := eventFlowKind(result.RootFlowID, flow)
		if !wasPlanned {
			// A failure may produce terminal recovery handles before a complete
			// validated Flow plan existed. Report those terminal records without
			// inventing a flow.planned event after the fact.
			planned = ingestevent.FlowPlanned{Kind: kind, Role: role}
		}
		emitted := o.objectSummaries[index][flow.FlowID]
		// Result can be called directly by library tests and older adapters. In
		// production, ObjectsCompleted has already streamed every Object before
		// this terminal projection, keeping output state proportional to Flows.
		if emitted.Total == 0 && flow.ObjectSummary.Total > 0 && len(flow.Objects) > 0 {
			if err := o.objectsCompletedLocked(index, flow.FlowID, flow.Objects); err != nil {
				return err
			}
			emitted = o.objectSummaries[index][flow.FlowID]
		}
		if emitted != flow.ObjectSummary {
			return o.failLocked(fmt.Errorf("flow %s terminal Object summary does not match streamed Object results", flow.FlowID))
		}
		if err := o.emitLocked(ingestevent.FlowScope(index, flow.FlowID), ingestevent.FlowResult{
			FlowID: flow.FlowID, SourceID: flow.SourceID, Kind: planned.Kind, Role: planned.Role,
			TAMSFlowProfileID: flow.TAMSFlowProfileID,
			Disposition:       ingestevent.FlowDisposition(flow.Disposition), ObjectSummary: eventObjectSummary(flow.ObjectSummary),
		}); err != nil {
			return err
		}
	}

	status := ingestevent.InputStatus(result.Status)
	finished := ingestevent.InputFinished{
		Input: input, Profile: result.Profile, ProfileVersion: result.ProfileVersion,
		FFmpegVersion: result.FFmpegVersion, MediaToolchain: result.MediaToolchain,
		RootFlowID: result.RootFlowID, SHA256: result.SHA256, Status: status,
		Verification: ingestevent.VerificationStatus(result.Verification),
		FlowCount:    uint64(len(result.Flows)), ObjectCount: uint64(resultObjectCount(result)),
	}
	if result.Bytes > 0 {
		finished.Bytes = uint64(result.Bytes)
	}
	if status == ingestevent.InputFailed {
		code, message, action := inputFailure(result)
		diagnostic, err := ingestevent.NewDiagnostic(ingestevent.SeverityError, code, message, "", action)
		if err != nil {
			return o.failLocked(err)
		}
		if err := o.emitLocked(ingestevent.InputScope(index), diagnostic); err != nil {
			return err
		}
		finished.ErrorCode = code
		finished.Message = diagnostic.Message
	}
	if err := o.emitLocked(ingestevent.InputScope(index), finished); err != nil {
		return err
	}
	o.terminal[index] = status
	return nil
}

func eventObjectSummary(summary ingest.ObjectSummary) ingestevent.ObjectSummary {
	return ingestevent.ObjectSummary{
		Total: uint64(summary.Total), Bytes: nonnegativeUint64(summary.Bytes),
		Ingested: uint64(summary.Ingested), Resumed: uint64(summary.Resumed), Rejected: uint64(summary.Rejected),
		Retracted: uint64(summary.Retracted), Stranded: uint64(summary.Stranded), Unattempted: uint64(summary.Unattempted),
		Verified: uint64(summary.Verified), StorageVerified: uint64(summary.StorageVerified),
		ReadbackVerified: uint64(summary.ReadbackVerified),
	}
}

func (o *ingestEventOutput) Finish(cause error, exitCode int,
	options *ingestFlagValues, metrics observability.Snapshot, ctx context.Context) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.stopProgressLocked()
	if o.finished {
		return o.exitCode, o.err
	}
	if o.err != nil {
		return ExitGeneral, o.err
	}
	decision := o.freezeTerminalLocked(ctx)
	// Once cancellation is observed before the freeze point it owns the terminal
	// classification, even if another operation returned a more specific error
	// at the same time. Every later projection consumes this frozen decision.
	if decision.interrupted {
		exitCode = ExitInterrupted
		cause = decision.cause
	}
	if err := o.startLocked(options); err != nil {
		return ExitGeneral, err
	}
	if !o.manifest {
		if err := o.emitLocked(nil, ingestevent.ManifestFinished{TotalInputs: uint64(len(o.declared))}); err != nil {
			return ExitGeneral, err
		}
		o.manifest = true
	}
	o.sealAllProgressLocked()
	if err := o.flushProgressLocked(nil); err != nil {
		return ExitGeneral, err
	}

	profile, profileVersion := ingest.ProfileUnresolved, ingest.UnresolvedProfileVersion
	verification := ingestevent.VerificationNotReached
	if options != nil {
		profile, profileVersion = options.profile, options.profileVersion
		if ingest.VerificationMode(options.verify) == ingest.VerificationNone {
			verification = ingestevent.VerificationNotRequested
		}
	}
	// Declare creates contiguous indexes in source order. Synthesize terminal
	// records in that same order rather than exposing randomized map iteration
	// when several queued inputs never started.
	for index := range len(o.declared) {
		input := o.declared[index]
		if _, terminal := o.terminal[index]; terminal {
			continue
		}
		code, message, action := runFailure(exitCode)
		result := ingest.Result{
			Input: input, Profile: profile, ProfileVersion: profileVersion, Status: ingest.ResultStatusFailed,
			Verification: ingest.VerificationStatus(verification), Flows: []ingest.FlowResult{},
			Failure: &ingest.Failure{Code: code, Message: message, ActionRequired: action}, Error: message,
		}
		if code == ingest.FailureCodeInterrupted {
			result.Failure = ingest.DescribeInputInterruptedFailure()
			result.Error = result.Failure.Message
		}
		if err := o.resultLocked(index, result); err != nil {
			return ExitGeneral, err
		}
	}

	if cause != nil {
		code, message, action := runFailure(exitCode)
		diagnostic, err := ingestevent.NewDiagnostic(
			ingestevent.SeverityError, code, message, diagnosticHintFor(cause), action)
		if err != nil {
			return ExitGeneral, o.failLocked(err)
		}
		if err := o.emitLocked(nil, diagnostic); err != nil {
			return ExitGeneral, err
		}
	}

	var succeeded, failed uint64
	for _, status := range o.terminal {
		if status == ingestevent.InputFailed {
			failed++
		} else {
			succeeded++
		}
	}
	outcome := ingestevent.RunSucceeded
	switch {
	case exitCode == ExitInterrupted:
		outcome = ingestevent.RunInterrupted
	case succeeded > 0 && failed > 0:
		outcome = ingestevent.RunPartial
	case exitCode != ExitOK || failed > 0:
		outcome = ingestevent.RunFailed
	}
	// Events remain authoritative if the pipeline fails before returning results.
	finished := ingestevent.RunFinished{
		Outcome: outcome, ExitCode: exitCode, Total: uint64(len(o.declared)), Succeeded: succeeded, Failed: failed,
		ElapsedMS:   max(durationMilliseconds(metrics.Elapsed), durationMilliseconds(o.now().Sub(o.started))),
		BytesStaged: nonnegativeUint64(metrics.BytesStaged), BytesUploaded: nonnegativeUint64(metrics.BytesUploaded),
		BytesVerified: nonnegativeUint64(metrics.BytesVerified), Retries: o.retries,
		ObjectsVerified: nonnegativeUint64(metrics.Verified), ObjectsRetracted: nonnegativeUint64(metrics.Retracted),
		ObjectsStranded: nonnegativeUint64(metrics.Stranded),
	}
	if err := o.emitLocked(nil, finished); err != nil {
		return ExitGeneral, err
	}
	if err := o.encoder.Finalize(); err != nil {
		return ExitGeneral, o.failLocked(err)
	}
	o.finished = true
	o.exitCode = exitCode
	o.stopProgressLocked()
	o.closeDoneLocked()
	return exitCode, nil
}

func diagnosticHintFor(err error) string {
	var hinted interface{ DiagnosticHint() string }
	if errors.As(err, &hinted) {
		return hinted.DiagnosticHint()
	}
	return ""
}

func (o *ingestEventOutput) WatchCancellation(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			o.mu.Lock()
			_ = o.cancellationLocked(cancellationReason(ctx))
			o.mu.Unlock()
		case <-o.done:
		}
	}()
}

// FreezeTerminal chooses whether caller cancellation owns the run before any
// terminal projection is committed. It is safe to call more than once; the
// first call is the linearization point and all later calls return the same
// decision.
func (o *ingestEventOutput) FreezeTerminal(ctx context.Context) terminalDecision {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.freezeTerminalLocked(ctx)
}

func (o *ingestEventOutput) freezeTerminalLocked(ctx context.Context) terminalDecision {
	if o.terminalFrozen {
		return o.terminalDecision
	}
	decision := terminalDecisionFromContext(ctx)
	if o.cancellation {
		decision.interrupted = true
	}
	if decision.interrupted {
		if decision.cause == nil {
			decision.cause = context.Canceled
		}
		if decision.reason == "" {
			decision.reason = ingestevent.CancellationParent
		}
	}
	// Store the classification before any potentially blocking event write.
	// WatchCancellation observes terminalFrozen under the same mutex and cannot
	// append a contradictory cancellation after this point.
	o.terminalFrozen = true
	o.terminalDecision = decision
	if decision.interrupted && !o.cancellation {
		_ = o.recordCancellationLocked(decision.reason)
	}
	return decision
}

func terminalDecisionFromContext(ctx context.Context) terminalDecision {
	if ctx == nil || ctx.Err() == nil {
		return terminalDecision{}
	}
	cause := context.Cause(ctx)
	if cause == nil {
		cause = context.Canceled
	}
	return terminalDecision{interrupted: true, cause: cause, reason: cancellationReason(ctx)}
}

func (o *ingestEventOutput) cancellationLocked(reason ingestevent.CancellationReason) error {
	if o.cancellation || o.finished || o.terminalFrozen || o.err != nil {
		return o.err
	}
	return o.recordCancellationLocked(reason)
}

func (o *ingestEventOutput) recordCancellationLocked(reason ingestevent.CancellationReason) error {
	if err := o.startLocked(nil); err != nil {
		return err
	}
	if err := o.emitLocked(nil, ingestevent.RunCancellationRequested{Reason: reason}); err != nil {
		return err
	}
	o.cancellation = true
	return nil
}

func (o *ingestEventOutput) ensureInputStartedLocked(index int) error {
	if o.startedInputs[index] {
		return nil
	}
	if err := o.emitLocked(ingestevent.InputScope(index), ingestevent.InputStarted{StartedAt: o.now().UTC()}); err != nil {
		return err
	}
	o.startedInputs[index] = true
	o.progressMu.Lock()
	o.progressStarted[index] = true
	o.progressMu.Unlock()
	return nil
}

func (o *ingestEventOutput) emitLocked(scope *ingestevent.Scope, event ingestevent.Event) error {
	if o.err != nil {
		return o.err
	}
	if _, err := o.encoder.Emit(scope, event); err != nil {
		return o.failLocked(err)
	}
	return nil
}

func (o *ingestEventOutput) failLocked(err error) error {
	if err == nil {
		return nil
	}
	if o.err == nil {
		o.err = err
		if o.cancel != nil {
			o.cancel(fmt.Errorf("write ingest event stream: %w", err))
		}
		o.stopProgressLocked()
		o.closeDoneLocked()
	}
	return o.err
}

func (o *ingestEventOutput) closeDoneLocked() {
	if o.closed {
		return
	}
	close(o.done)
	o.closed = true
}

func (o *ingestEventOutput) Finished() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.finished
}

func (o *ingestEventOutput) Err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

func eventFlowKind(rootFlowID string, flow ingest.FlowResult) (ingestevent.FlowKind, string) {
	role := strings.TrimSpace(flow.Role)
	if flow.Kind != "" {
		return ingestevent.FlowKind(flow.Kind), role
	}
	if flow.FlowID == rootFlowID && flow.ObjectSummary.Total == 0 {
		return ingestevent.FlowKindCollection, ""
	}
	if role == "" || strings.EqualFold(role, "multi") {
		return ingestevent.FlowKindMuxed, ""
	}
	return ingestevent.FlowKindEssence, role
}

func resultObjectCount(result ingest.Result) int {
	count := 0
	for _, flow := range result.Flows {
		count += flow.ObjectSummary.Total
	}
	return count
}

func inputFailure(result ingest.Result) (code, message string, action bool) {
	failure := ingest.DescribeFailure(result, nil)
	if failure == nil {
		return "", "", false
	}
	if failure.Code != "" {
		return failure.Code, failure.Message, failure.ActionRequired
	}
	return "", "", false
}

// runFailure renders an exit code as the run-level event-stream failure. The
// exit code is this layer's own vocabulary, so the mapping lives here.
func runFailure(exitCode int) (code, message string, action bool) {
	switch exitCode {
	case ExitUsage:
		return ingest.FailureCodeConfigInvalid, ingest.FailureMessageConfigInvalid, true
	case ExitAuth:
		return ingest.FailureCodeAuthFailed, ingest.FailureMessageAuthFailed, true
	case ExitPartial:
		return ingest.FailureCodeInputFailed, ingest.FailureMessageInputFailed, false
	case ExitSource:
		return ingest.FailureCodeSourceFailed, ingest.FailureMessageSourceFailed, true
	case ExitMedia:
		return ingest.FailureCodeMediaFailed, ingest.FailureMessageMediaFailed, true
	case ExitRemote:
		return ingest.FailureCodeTAMSFailed, ingest.FailureMessageTAMSFailed, true
	case ExitInterrupted:
		return ingest.FailureCodeInterrupted, ingest.FailureMessageInterrupted, false
	default:
		return ingest.FailureCodeRunFailed, ingest.FailureMessageRunFailed, true
	}
}

func cancellationReason(ctx context.Context) ingestevent.CancellationReason {
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, ErrSignal):
		return ingestevent.CancellationSignal
	case errors.Is(cause, context.DeadlineExceeded):
		return ingestevent.CancellationDeadline
	default:
		return ingestevent.CancellationParent
	}
}

func durationMilliseconds(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration / time.Millisecond)
}

func nonnegativeUint64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}
