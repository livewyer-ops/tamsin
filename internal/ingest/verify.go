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
			if cancel != nil {
				cancel()
			}
		}()
	}

	outcomes := make([]verificationOutcome, len(objects))
	failures := make([]error, len(objects))
	existing := make(map[segmentKey]bool)
	for _, result := range results {
		if result.Disposition == ObjectDispositionResumed {
			existing[result.key()] = true
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
				outcomes[index], failures[index] = p.verifyOneWithRetraction(ctx, recovery, flowID, objects[index], existing[objects[index].key()])
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
		switch outcome {
		case outcomeVerified:
			verified++
			setObjectVerification(results, objects[index].key(), ObjectVerificationVerified, VerificationMethodReadback)
		case outcomeRetracted:
			retracted++
			setObjectDisposition(results, objects[index].key(), ObjectDispositionRetracted)
			setObjectVerification(results, objects[index].key(), ObjectVerificationFailed, VerificationMethodReadback)
		case outcomeRetractionFailed:
			stranded++
			setObjectDisposition(results, objects[index].key(), ObjectDispositionStranded)
			setObjectVerification(results, objects[index].key(), ObjectVerificationFailed, VerificationMethodReadback)
		case outcomePreserved:
			preserved++
			setObjectVerification(results, objects[index].key(), ObjectVerificationNotReached, VerificationMethodReadback)
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
	// for only this exact registered Segment.
	if err := p.readBackSegment(ctx, flowID, object.id, object); err != nil {
		return fail(err)
	}
	p.observability.Verification(object.size, observability.OutcomeVerified)
	p.advanceProgress(ctx, progress.PhaseVerify, 1, object.size)
	return outcomeVerified, nil
}

// readBackSegment lists the Segment registered under objectID at the
// Object's timerange and compares its timing and bytes with the Object's. The
// caller holds a transfer slot, so nothing queues between the URL's issuance
// and DownloadDigest. A large Object can outlive its URL's window; when the
// window closes under a retry, the Segment is listed once more for a fresh
// URL and checked again from the start.
func (p *Pipeline) readBackSegment(ctx context.Context, flowID, objectID string, object preparedObject) error {
	for attempt := 0; ; attempt++ {
		segments, err := p.client.ListSegments(ctx, flowID, tams.SegmentListOptions{
			ObjectID: objectID, Timerange: object.timerange, IncludeDownloadURLs: true, IncludeObjectTimerange: true,
		})
		if err != nil {
			return fmt.Errorf("refresh download URL for object %s: %w", objectID, err)
		}
		segment := matchingSegment(segments, objectID, object.timerange)
		if segment == nil {
			return fmt.Errorf("registered segment %s was not returned by TAMS", objectID)
		}
		if err := checkSegmentTiming(flowID, object, *segment); err != nil {
			return err
		}
		startBefore := time.Now().Add(p.limits.PresignedURL)
		for index := range segment.GetURLs {
			if segment.GetURLs[index].Presigned {
				segment.GetURLs[index].StartBefore = startBefore
			}
		}
		err = p.verifyObject(ctx, object, *segment)
		if attempt == 0 && errors.Is(err, tams.ErrPresignedURLExpired) && ctx.Err() == nil {
			p.logger.Debug("download URL expired during verification; listing again", "object_id", objectID)
			continue
		}
		return err
	}
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

func (p *Pipeline) verifyObject(ctx context.Context, expected preparedObject, segment tams.Segment) error {
	if len(segment.GetURLs) == 0 {
		return fmt.Errorf("segment %s has no download URL for verification", expected.id)
	}
	// A service may list several routes to one Object: a presigned one, a
	// direct storage URL that needs credentials of its own, an unsigned route
	// on the API origin. Presigned routes are tried first, then the rest, until
	// one can be read. A route that serves the wrong bytes or the wrong length
	// is an integrity failure, not a reason to try another.
	candidates := make([]tams.PresignedURL, 0, len(segment.GetURLs))
	for _, candidate := range segment.GetURLs {
		if candidate.Presigned {
			candidates = append(candidates, candidate)
		}
	}
	for _, candidate := range segment.GetURLs {
		if !candidate.Presigned {
			candidates = append(candidates, candidate)
		}
	}
	var unreadable []error
	for _, download := range candidates {
		size, checksum, err := p.client.DownloadDigest(ctx, download, expected.size)
		if err != nil {
			var sizeErr *tams.ObjectSizeError
			if errors.As(err, &sizeErr) {
				return fmt.Errorf("%w: verify object %s: %w", errObjectIntegrity, expected.id, err)
			}
			if ctx.Err() != nil || len(candidates) == 1 {
				return fmt.Errorf("verify object %s: %w", expected.id, err)
			}
			unreadable = append(unreadable, err)
			continue
		}
		if size != expected.size {
			return fmt.Errorf("%w: object %s byte length mismatch: expected %d, got %d", errObjectIntegrity, expected.id, expected.size, size)
		}
		if checksum != expected.sha256 {
			return fmt.Errorf("%w: object %s SHA-256 mismatch: expected %s, got %s", errObjectIntegrity, expected.id, expected.sha256, checksum)
		}
		return nil
	}
	return fmt.Errorf("verify object %s: none of %d advertised URLs could be read: %w", expected.id, len(candidates), errors.Join(unreadable...))
}
