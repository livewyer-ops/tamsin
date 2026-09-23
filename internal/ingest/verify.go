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
	// A Segment that could not be retracted needs an operator, so it is named
	// first: the difference between "we cleaned up" and "you must" is the whole
	// point of tracking terminal state.
	if e.Stranded > 0 {
		return fmt.Sprintf("%d of %d segments failed verification and %d could not be retracted from flow %s: %v",
			failed, e.Total, e.Stranded, e.FlowID, e.Err)
	}
	return fmt.Sprintf("%d of %d segments failed verification and were retracted from flow %s: %v",
		e.Retracted, e.Total, e.FlowID, e.Err)
}

func (e *VerificationError) Unwrap() error { return e.Err }

// verifyAll checks every registered Object, attempts retraction for each that
// fails verification, and records every terminal outcome in results. Failures
// must not cancel sibling cleanup. Within recovery, verification and
// retraction share the caller's recovery deadline rather than giving each
// Object a fresh detached cleanup allowance. Otherwise retraction shares one
// deadline, detached from parent cancellation, that starts at the first
// failure.
func (p *Pipeline) verifyAll(ctx context.Context, flowID string, objects []preparedObject,
	results []ObjectResult, withinRecovery bool) error {
	if len(objects) == 0 {
		return nil
	}
	recovery := func() context.Context { return ctx }
	if !withinRecovery {
		var (
			once        sync.Once
			recoveryCtx context.Context
			cancel      context.CancelFunc
		)
		recovery = func() context.Context {
			once.Do(func() {
				recoveryCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), p.recoveryTimeout)
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
	}

	outcomes := make([]verificationOutcome, len(objects))
	failures := make([]error, len(objects))

	// Bound goroutines by the transfer budget. Wait for every outcome without
	// cancelling siblings on error: they may still need to retract Segments.
	workers := min(max(p.config.Transfers, 1), len(objects))
	pending := make(chan int)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for index := range pending {
				outcomes[index], failures[index] = p.verifyOneWithRetraction(ctx, recovery, flowID, objects[index])
			}
		}()
	}
	for index := range objects {
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
		objectID := objects[index].id
		switch outcome {
		case outcomeVerified:
			verified++
			setObjectVerification(results, objectID, ObjectVerificationVerified, VerificationMethodReadback)
		case outcomeRetracted:
			retracted++
			setObjectDisposition(results, objectID, ObjectDispositionRetracted)
			setObjectVerification(results, objectID, ObjectVerificationFailed, VerificationMethodReadback)
		case outcomeRetractionFailed:
			stranded++
			setObjectDisposition(results, objectID, ObjectDispositionStranded)
			setObjectVerification(results, objectID, ObjectVerificationFailed, VerificationMethodReadback)
		}
		if failures[index] != nil {
			collected = append(collected, failures[index])
		}
	}

	if len(collected) == 0 {
		return nil
	}
	p.logger.Warn("verification did not complete for every object",
		"flow_id", flowID, "verified", verified, "retracted", retracted, "stranded", stranded)
	return &VerificationError{
		FlowID: flowID, Total: len(objects), Retracted: retracted, Stranded: stranded,
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
	flowID string, object preparedObject) (verificationOutcome, error) {
	release, err := p.acquireTransfer(ctx)
	if err != nil {
		cause := fmt.Errorf("object %s was registered but never verified: %w", object.id, err)
		return p.retractWithinRecovery(recovery(), flowID, object, cause)
	}
	defer release()

	// A whole-Flow listing generates every GET URL before bounded verification
	// workers can consume them. Hold this worker's global slot first, then ask
	// for only this exact registered Segment. Nothing queues between issuance
	// and DownloadDigest. A failed listing still requires retraction.
	segments, listErr := p.client.ListSegments(ctx, flowID, tams.SegmentListOptions{
		ObjectID: object.id, Timerange: object.timerange, IncludeDownloadURLs: true,
	})
	if listErr != nil {
		cause := fmt.Errorf("refresh download URL for object %s: %w", object.id, listErr)
		return p.retractWithinRecovery(recovery(), flowID, object, cause)
	}
	segment := matchingSegment(segments, object.id, object.timerange)
	if segment == nil {
		cause := fmt.Errorf("registered segment %s was not returned by TAMS", object.id)
		return p.retractWithinRecovery(recovery(), flowID, object, cause)
	}
	startBefore := time.Now().Add(p.limits.PresignedURL)
	for index := range segment.GetURLs {
		if segment.GetURLs[index].Presigned {
			segment.GetURLs[index].StartBefore = startBefore
		}
	}

	if err := p.verifyObject(ctx, object, *segment); err != nil {
		return p.retractWithinRecovery(recovery(), flowID, object, err)
	}
	p.observability.Verification(object.size, observability.OutcomeVerified)
	p.advanceProgress(ctx, progress.PhaseVerify, 1, object.size)
	return outcomeVerified, nil
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
