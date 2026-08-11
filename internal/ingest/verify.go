package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// verificationOutcome is the terminal state of one registered Media Object.
//
// Every Object that reaches the store must end in one of these. The failure
// this models is specific: Segments are registered in bulk before any is
// verified, so a mismatch found in one leaves the rest registered and unchecked
// unless something guarantees they are each resolved.
type verificationOutcome int

const (
	outcomeVerified verificationOutcome = iota
	outcomeRetracted
	outcomeRetractionFailed
)

// verificationTask pairs an Object with the Segment registered for it.
type verificationTask struct {
	object  preparedObject
	segment tams.Segment
}

// VerificationError preserves the safety-relevant terminal state of a failed
// verification. Callers must not have to parse prose to distinguish media that
// was withdrawn from media which is still referenced by a Flow and needs an
// operator.
type VerificationError struct {
	FlowID    string
	Total     int
	Retracted int
	Stranded  int
	Err       error
}

func (e *VerificationError) Error() string {
	failed := e.Retracted + e.Stranded
	if e.Stranded > 0 {
		return fmt.Sprintf("%d of %d segments failed verification and %d could not be retracted from flow %s: %v",
			failed, e.Total, e.Stranded, e.FlowID, e.Err)
	}
	return fmt.Sprintf("%d of %d segments failed verification and were retracted from flow %s: %v",
		e.Retracted, e.Total, e.FlowID, e.Err)
}

func (e *VerificationError) Unwrap() error { return e.Err }

// verifyAllWithOutcomes checks every registered Object and guarantees each
// reaches a terminal state, whatever happens to the others.
//
// The rule that shapes this: a checksum mismatch must not cancel its siblings'
// cleanup. Cancelling on first error is the natural way to write concurrent
// work, and it is wrong here — the siblings are already registered, so
// abandoning them leaves the Flow referencing bytes nobody checked. Failures
// are therefore collected rather than propagated, and every Object that did not
// verify is retracted before returning.
//
// Cancellation of the parent context is handled the same way: whatever has been
// registered is still retracted, on a detached context, because the Segments
// outlive the run that created them.
// The returned states let registration reconciliation and resume results report
// whether each Object verified, was retracted, or was stranded.
func (p *Pipeline) verifyAllWithOutcomes(ctx context.Context, flowID string, tasks []verificationTask) ([]verificationOutcome, error) {
	var (
		once        sync.Once
		recoveryCtx context.Context
		cancel      context.CancelFunc
	)
	recovery := func() context.Context {
		once.Do(func() {
			recoveryCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), p.verificationRecoveryTimeout)
		})
		return recoveryCtx
	}
	defer func() {
		// Do not start a timer for a batch with no verification failures.
		once.Do(func() {})
		if cancel != nil {
			cancel()
		}
	}()
	return p.verifyAllOutcomes(ctx, flowID, tasks, recovery)
}

// verifyAllWithinRecovery shares the caller's recovery deadline with both
// verification and retraction. It must not give each Object a fresh detached
// cleanup allowance.
func (p *Pipeline) verifyAllWithinRecovery(ctx context.Context, flowID string, tasks []verificationTask) ([]verificationOutcome, error) {
	return p.verifyAllOutcomes(ctx, flowID, tasks, func() context.Context { return ctx })
}

func (p *Pipeline) verifyAllOutcomes(ctx context.Context, flowID string, tasks []verificationTask,
	recovery func() context.Context) ([]verificationOutcome, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	outcomes := make([]verificationOutcome, len(tasks))
	failures := make([]error, len(tasks))

	// A fixed pool rather than a goroutine per Object. Each verification needs a
	// slot from the transfer budget before it can do anything, so creating one
	// per Object only parks them all on the same semaphore -- and a day of
	// ten-second Segments is nearly nine thousand of them.
	//
	// errgroup would express this more briefly but cancels its context when a
	// goroutine returns an error, and cancelling here is exactly wrong: a
	// mismatch in one Object must not abandon the retraction of its siblings.
	workers := min(max(p.config.Transfers, 1), len(tasks))
	pending := make(chan int)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for index := range pending {
				outcomes[index], failures[index] = p.verifyOneWithRetraction(ctx, recovery, flowID, tasks[index])
			}
		}()
	}
	for index := range tasks {
		pending <- index
	}
	close(pending)
	group.Wait()

	var (
		verified  int
		retracted int
		stranded  int
		collected []error
	)
	for index, outcome := range outcomes {
		switch outcome {
		case outcomeVerified:
			verified++
		case outcomeRetracted:
			retracted++
		case outcomeRetractionFailed:
			stranded++
		}
		if failures[index] != nil {
			collected = append(collected, failures[index])
		}
	}

	if len(collected) == 0 {
		return outcomes, nil
	}
	p.logger.Warn("verification did not complete for every object",
		"flow_id", flowID, "verified", verified, "retracted", retracted, "stranded", stranded)
	// A Segment that could not be retracted needs an operator, so it is named
	// first: the difference between "we cleaned up" and "you must" is the whole
	// point of tracking terminal state.
	if stranded > 0 {
		return outcomes, &VerificationError{
			FlowID: flowID, Total: len(tasks), Retracted: retracted, Stranded: stranded,
			Err: errors.Join(collected...),
		}
	}
	return outcomes, &VerificationError{
		FlowID: flowID, Total: len(tasks), Retracted: retracted,
		Err: errors.Join(collected...),
	}
}

// verifyOneWithRetraction resolves a single Object to a terminal state.
//
// An Object that cannot be checked at all — because the run was cancelled, or
// because a transfer slot never came free — is retracted rather than left
// registered. Unchecked is indistinguishable from corrupt from the store's
// point of view, and the safe reading is the pessimistic one.
func (p *Pipeline) verifyOneWithRetraction(ctx context.Context, recovery func() context.Context,
	flowID string, task verificationTask) (verificationOutcome, error) {
	release, err := p.acquireTransfer(ctx)
	if err != nil {
		cause := fmt.Errorf("object %s was registered but never verified: %w", task.object.id, err)
		return p.resolveVerificationFailure(recovery(), flowID, task.object, cause)
	}
	defer release()

	// A whole-Flow listing generates every GET URL before bounded verification
	// workers can consume them. Hold this worker's global slot first, then ask
	// for only this exact registered Segment. Nothing queues between issuance
	// and DownloadDigest, and a failed/omitted fresh listing still enters the P0
	// terminal-state path below.
	if p.limits.PresignedURL > 0 {
		segments, listErr := p.client.ListSegments(ctx, flowID, tams.SegmentListOptions{
			ObjectID: task.object.id, Timerange: task.object.timerange, IncludeDownloadURLs: true,
		})
		if listErr != nil {
			cause := fmt.Errorf("refresh download URL for object %s: %w", task.object.id, listErr)
			return p.resolveVerificationFailure(recovery(), flowID, task.object, cause)
		}
		segment := matchingSegment(segments, task.object.id, task.object.timerange)
		if segment == nil {
			cause := fmt.Errorf("registered segment %s was not returned by TAMS", task.object.id)
			return p.resolveVerificationFailure(recovery(), flowID, task.object, cause)
		}
		task.segment = *segment
		startBefore := time.Now().Add(p.limits.PresignedURL)
		for index := range task.segment.GetURLs {
			task.segment.GetURLs[index].StartBefore = startBefore
		}
	}

	if err := p.verifyObject(ctx, task.object, task.segment); err != nil {
		return p.resolveVerificationFailure(recovery(), flowID, task.object, err)
	}
	p.observability.Verification(task.object.size, observability.OutcomeVerified)
	p.advanceProgress(ctx, progress.PhaseVerify, 1, task.object.size)
	return outcomeVerified, nil
}

func (p *Pipeline) resolveVerificationFailure(ctx context.Context, flowID string,
	object preparedObject, cause error) (verificationOutcome, error) {
	return p.retractWithinRecovery(ctx, flowID, object, cause)
}

func (p *Pipeline) retractWithinRecovery(ctx context.Context, flowID string, object preparedObject, cause error) (verificationOutcome, error) {
	p.logger.Warn("retracting unverified segment",
		"flow_id", flowID, "object_id", object.id, "timerange", object.timerange, "cause", cause)
	if err := p.deleteRegisteredSegment(ctx, flowID, object); err != nil {
		p.observability.Verification(object.size, observability.OutcomeStranded)
		return outcomeRetractionFailed, fmt.Errorf(
			"%w; the unverified segment could not be retracted: %w", cause, err)
	}
	p.observability.Verification(object.size, observability.OutcomeRetracted)
	return outcomeRetracted, fmt.Errorf(
		"%w; retraction of the segment from flow %s completed", cause, flowID)
}
