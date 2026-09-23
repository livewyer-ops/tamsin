package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func (p *Pipeline) acceptStorageVerification(ctx context.Context, object preparedObject, results []ObjectResult) {
	setObjectDisposition(results, object.id, ObjectDispositionRegistered)
	setObjectVerification(results, object.id, ObjectVerificationVerified, VerificationMethodStorage)
	p.observability.Verification(object.size, observability.OutcomeVerified)
	p.advanceProgress(ctx, progress.PhaseVerify, 1, object.size)
}

// reconcileRegistrationError resolves an ambiguous or partial bulk POST on a
// single detached deadline. Parent cancellation cannot abandon Segments that
// may already be in the Flow, while the shared deadline prevents a batch of N
// Objects from turning into N consecutive cleanup timeouts.
//
// A typed partial result is authoritative and needs no read-after-write: the
// complement of FailedSegments is known to have registered and is rolled back.
// A transport error is indeterminate, so one readback identifies visible
// Segments. A complete readback proves the POST committed and may let ingest
// continue; an incomplete or failed readback resolves every uncertain Object.
func (p *Pipeline) reconcileRegistrationError(ctx context.Context, flowID string,
	objects []preparedObject, results []ObjectResult, registerErr error) error {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.recoveryTimeout)
	defer cancel()

	var partial *tams.PartialSegmentRegistrationError
	if errors.As(registerErr, &partial) {
		var registered, rejected []preparedObject
		for _, object := range objects {
			if requestWasRegistered(partial.RegisteredSegments, object) {
				setObjectDisposition(results, object.id, ObjectDispositionRegistered)
				registered = append(registered, object)
				continue
			}
			setObjectDisposition(results, object.id, ObjectDispositionRejected)
			rejected = append(rejected, object)
		}
		cleanupErr := p.retractRegisteredObjects(recoveryCtx, flowID, registered, results,
			"segment was committed by a partial bulk registration")
		resolution := fmt.Errorf("partial registration rejected %s; committed %s were rolled back",
			objectNames(rejected), objectNames(registered))
		return errors.Join(resolution, cleanupErr)
	}

	// Verification refreshes an exact URL after its worker holds transfer
	// capacity. Asking for URLs during ambiguity readback would age them in both
	// the recovery work and the bounded verification queue.
	listed, listErr := p.client.ListSegments(recoveryCtx, flowID, tams.SegmentListOptions{
		Timerange: chunkTimerange(objects),
	})
	if listErr != nil {
		cleanupErr := p.retractRegisteredObjects(recoveryCtx, flowID, objects, results,
			"bulk registration response and registration readback were both lost")
		resolution := fmt.Errorf("registration readback for %s failed: %w; every indeterminate segment was sent for retraction",
			objectNames(objects), listErr)
		return errors.Join(resolution, cleanupErr)
	}

	var visible, unresolved []preparedObject
	for _, object := range objects {
		if matchingSegment(listed, object.id, object.timerange) == nil {
			unresolved = append(unresolved, object)
			continue
		}
		setObjectDisposition(results, object.id, ObjectDispositionRegistered)
		visible = append(visible, object)
	}

	// Seeing every requested Segment turns a lost response into a known commit.
	// Verification, when enabled, is still required before treating it as a
	// success; without verification the completed registration is sufficient.
	if len(unresolved) == 0 {
		if p.config.VerificationMode == VerificationNone {
			return nil
		}
		return p.verifyAll(recoveryCtx, flowID, visible, results, true)
	}

	var resolutionErrs []error
	if p.config.VerificationMode != VerificationNone {
		if err := p.verifyAll(recoveryCtx, flowID, visible, results, true); err != nil {
			resolutionErrs = append(resolutionErrs, err)
		}
	} else if err := p.retractRegisteredObjects(recoveryCtx, flowID, visible, results,
		"segment registered as part of an incomplete batch"); err != nil {
		resolutionErrs = append(resolutionErrs, err)
	}
	if err := p.retractRegisteredObjects(recoveryCtx, flowID, unresolved, results,
		"segment registration remained indeterminate after readback"); err != nil {
		resolutionErrs = append(resolutionErrs, err)
	}
	resolutionErrs = append([]error{fmt.Errorf(
		"registration readback did not return %s; visible segments were resolved and every missing segment was sent for exact retraction",
		objectNames(unresolved))}, resolutionErrs...)
	return errors.Join(resolutionErrs...)
}

// retractRegisteredObjects uses a fixed worker pool and the caller's shared
// recovery context. Each error names the exact Object that needs an operator;
// successful siblings continue even after one cleanup fails.
func (p *Pipeline) retractRegisteredObjects(ctx context.Context, flowID string,
	objects []preparedObject, results []ObjectResult, cause string) error {
	if len(objects) == 0 {
		return nil
	}
	failures := make([]error, len(objects))
	workers := min(max(p.config.Transfers, 1), len(objects))
	// Buffer the complete batch so dispatch itself never waits behind a slow
	// deletion. The fixed worker count still bounds TAMS calls, while every
	// Object is made eligible for cleanup within the one shared deadline.
	pending := make(chan int, len(objects))
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for index := range pending {
				object := objects[index]
				outcome, err := p.retractWithinRecovery(ctx, flowID, object,
					fmt.Errorf("object %s: %s", object.id, cause))
				if outcome == outcomeRetractionFailed {
					setObjectDisposition(results, object.id, ObjectDispositionStranded)
					failures[index] = fmt.Errorf("object %s (%s) is stranded: %w",
						object.id, object.timerange, err)
					continue
				}
				setObjectDisposition(results, object.id, ObjectDispositionRetracted)
			}
		}()
	}
	for index := range objects {
		pending <- index
	}
	close(pending)
	group.Wait()
	var stranded []error
	for _, failure := range failures {
		if failure != nil {
			stranded = append(stranded, failure)
		}
	}
	if len(stranded) == 0 {
		return nil
	}
	return fmt.Errorf("%d segment(s) could not be retracted: %w", len(stranded), errors.Join(stranded...))
}

func requestWasRegistered(requests []tams.SegmentRequest, object preparedObject) bool {
	for _, request := range requests {
		if request.ObjectID == object.id && request.Timerange == object.timerange {
			return true
		}
	}
	return false
}

func objectNames(objects []preparedObject) string {
	if len(objects) == 0 {
		return "none"
	}
	names := make([]string, len(objects))
	for index, object := range objects {
		names[index] = object.id
	}
	return strings.Join(names, ", ")
}

// deleteRegisteredSegment retracts exactly one Object's Segment, never every
// Segment sharing its timerange.
func (p *Pipeline) deleteRegisteredSegment(ctx context.Context, flowID string, object preparedObject) error {
	return p.client.DeleteSegments(ctx, flowID, tams.SegmentDeleteOptions{
		ObjectID: object.id, Timerange: object.timerange,
	})
}
