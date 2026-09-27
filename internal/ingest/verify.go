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
// Every new registration must be verified or retracted. Existing Segments may
// be preserved when the store cannot be read; a retry must not destroy media
// solely because it could not finish verification.
type verificationOutcome int

const (
	outcomeVerified verificationOutcome = iota
	outcomeRetracted
	outcomeRetractionFailed
	outcomePreserved
)

var errObjectIntegrity = errors.New("object integrity check failed")

// VerificationError preserves the safety-relevant terminal state of a failed
// verification. Callers must not have to parse prose to distinguish media that
// was withdrawn from media which is still referenced by a Flow and needs an
// operator.
type VerificationError struct {
	FlowID    string
	Total     int
	Retracted int
	Stranded  int
	Preserved int
	Err       error
}

func (e *VerificationError) Error() string {
	failed := e.Retracted + e.Stranded
	if e.Preserved > 0 {
		return fmt.Sprintf("verification incomplete for flow %s: %d existing segments preserved, %d retracted, %d could not be retracted: %v",
			e.FlowID, e.Preserved, e.Retracted, e.Stranded, e.Err)
	}
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

// verifyAll checks every registered Object and records its terminal outcome.
// Existing Segments are preserved unless their bytes prove corrupt. Failures
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
	existing := make(map[string]bool)
	for _, result := range results {
		if result.Disposition == ObjectDispositionResumed {
			existing[result.ObjectID] = true
		}
	}

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
				outcomes[index], failures[index] = p.verifyOneWithRetraction(ctx, recovery, flowID, objects[index], existing[objects[index].id])
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
		preserved int
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
		case outcomePreserved:
			preserved++
			setObjectVerification(results, objectID, ObjectVerificationNotReached, VerificationMethodReadback)
		}
		if failures[index] != nil {
			collected = append(collected, failures[index])
		}
	}

	if len(collected) == 0 {
		return nil
	}
	p.logger.Warn("verification did not complete for every object",
		"flow_id", flowID, "verified", verified, "retracted", retracted, "stranded", stranded, "preserved", preserved)
	return &VerificationError{
		FlowID: flowID, Total: len(objects), Retracted: retracted, Stranded: stranded, Preserved: preserved,
		Err: errors.Join(collected...),
	}
}

// verifyOneWithRetraction resolves a single Object to a terminal state.
//
// New registrations are retracted if they cannot be verified. An inability to
// read an existing Segment does not prove corruption and must not delete it.
func (p *Pipeline) verifyOneWithRetraction(ctx context.Context, recovery func() context.Context,
	flowID string, object preparedObject, existing bool) (verificationOutcome, error) {
	fail := func(cause error) (verificationOutcome, error) {
		if existing && (ctx.Err() != nil || !errors.Is(cause, errObjectIntegrity)) {
			return outcomePreserved, fmt.Errorf("existing segment %s preserved: %w", object.id, cause)
		}
		return p.retractWithinRecovery(recovery(), flowID, object, cause)
	}
	release, err := p.acquireTransfer(ctx)
	if err != nil {
		cause := fmt.Errorf("object %s was registered but never verified: %w", object.id, err)
		return fail(cause)
	}
	defer release()

	// A whole-Flow listing generates every GET URL before bounded verification
	// workers can consume them. Hold this worker's global slot first, then ask
	// for only this exact registered Segment. Nothing queues between issuance
	// and DownloadDigest.
	segments, listErr := p.client.ListSegments(ctx, flowID, tams.SegmentListOptions{
		ObjectID: object.id, Timerange: object.timerange, IncludeDownloadURLs: true, IncludeObjectTimerange: true,
	})
	if listErr != nil {
		cause := fmt.Errorf("refresh download URL for object %s: %w", object.id, listErr)
		return fail(cause)
	}
	segment := matchingSegment(segments, object.id, object.timerange)
	if segment == nil {
		cause := fmt.Errorf("registered segment %s was not returned by TAMS", object.id)
		return fail(cause)
	}
	if err := checkSegmentTiming(flowID, object, *segment); err != nil {
		return fail(err)
	}
	startBefore := time.Now().Add(p.limits.PresignedURL)
	for index := range segment.GetURLs {
		if segment.GetURLs[index].Presigned {
			segment.GetURLs[index].StartBefore = startBefore
		}
	}

	if err := p.verifyObject(ctx, object, *segment); err != nil {
		return fail(err)
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
