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

// registrationState records the fate of every Object after its bytes have
// reached storage. The states are deliberately more precise than a successful
// or failed batch: after a bulk POST loses its response, different Objects in
// that one request can require different recovery actions.
type registrationState string

const (
	registrationUploaded      registrationState = "uploaded"
	registrationIndeterminate registrationState = "registration-indeterminate"
	registrationRegistered    registrationState = "registered"
	registrationRejected      registrationState = "registration-rejected"
	registrationVerified      registrationState = "verified"
	registrationRetracted     registrationState = "retracted"
	registrationStranded      registrationState = "stranded"
)

type registrationRecord struct {
	object  preparedObject
	segment tams.Segment
	state   registrationState
}

func registrationRecords(chunk []preparedObject, state registrationState) []*registrationRecord {
	records := make([]*registrationRecord, len(chunk))
	for index, object := range chunk {
		records[index] = &registrationRecord{object: object, state: state}
	}
	return records
}

func setRegistrationState(record *registrationRecord, state registrationState, results []ObjectResult) {
	record.state = state
	setObjectStatus(results, record.object.id, ObjectStatus(state))
}

func (p *Pipeline) acceptStorageVerification(ctx context.Context, record *registrationRecord, results []ObjectResult) {
	setRegistrationState(record, registrationVerified, results)
	setObjectVerification(results, record.object.id, ObjectVerificationVerified, VerificationMethodStorage)
	p.observability.Verification(record.object.size, observability.OutcomeVerified)
	p.advanceProgress(ctx, progress.PhaseVerify, 1, record.object.size)
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
	records []*registrationRecord, results []ObjectResult, registerErr error) error {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.registrationRecoveryTimeout)
	defer cancel()

	var partial *tams.PartialSegmentRegistrationError
	if errors.As(registerErr, &partial) {
		var registered, rejected []*registrationRecord
		for _, record := range records {
			if requestWasRegistered(partial.RegisteredSegments, record.object) {
				setRegistrationState(record, registrationRegistered, results)
				registered = append(registered, record)
				continue
			}
			setRegistrationState(record, registrationRejected, results)
			rejected = append(rejected, record)
		}
		cleanupErr := p.retractRegistrationRecords(recoveryCtx, flowID, registered, results,
			"segment was committed by a partial bulk registration")
		resolution := fmt.Errorf("partial registration rejected %s; committed %s were rolled back",
			recordNames(rejected), recordNames(registered))
		return errors.Join(resolution, cleanupErr)
	}

	listed, listErr := p.client.ListSegments(recoveryCtx, flowID, tams.SegmentListOptions{
		Timerange: chunkTimerange(objectsFromRecords(records)),
		// Verification refreshes an exact URL after its worker holds transfer
		// capacity. Asking for URLs during ambiguity readback would age them in
		// both the recovery work and the bounded verification queue.
		IncludeDownloadURLs: p.config.VerificationMode != VerificationNone && p.limits.PresignedURL <= 0,
	})
	if listErr != nil {
		cleanupErr := p.retractRegistrationRecords(recoveryCtx, flowID, records, results,
			"bulk registration response and registration readback were both lost")
		resolution := fmt.Errorf("registration readback for %s failed: %w; every indeterminate segment was sent for retraction",
			recordNames(records), listErr)
		return errors.Join(resolution, cleanupErr)
	}

	var visible, unresolved []*registrationRecord
	for _, record := range records {
		segment := matchingSegment(listed, record.object.id, record.object.timerange)
		if segment == nil {
			unresolved = append(unresolved, record)
			continue
		}
		record.segment = *segment
		setRegistrationState(record, registrationRegistered, results)
		visible = append(visible, record)
	}

	// Seeing every requested Segment turns a lost response into a known commit.
	// Verification, when enabled, is still required before treating it as a
	// success; without verification the completed registration is sufficient.
	if len(unresolved) == 0 {
		if p.config.VerificationMode == VerificationNone {
			return nil
		}
		return p.verifyRegistrationRecordsWithinRecovery(recoveryCtx, flowID, visible, results)
	}

	var resolutionErrs []error
	if p.config.VerificationMode != VerificationNone {
		if err := p.verifyRegistrationRecordsWithinRecovery(recoveryCtx, flowID, visible, results); err != nil {
			resolutionErrs = append(resolutionErrs, err)
		}
	} else if err := p.retractRegistrationRecords(recoveryCtx, flowID, visible, results,
		"segment registered as part of an incomplete batch"); err != nil {
		resolutionErrs = append(resolutionErrs, err)
	}
	if err := p.retractRegistrationRecords(recoveryCtx, flowID, unresolved, results,
		"segment registration remained indeterminate after readback"); err != nil {
		resolutionErrs = append(resolutionErrs, err)
	}
	resolutionErrs = append([]error{fmt.Errorf(
		"registration readback did not return %s; visible segments were resolved and every missing segment was sent for exact retraction",
		recordNames(unresolved))}, resolutionErrs...)
	return errors.Join(resolutionErrs...)
}

// resolveRegisteredListingFailure handles a failure after a successful bulk
// POST. Every record is known to be registered, so failure to obtain fresh
// verification URLs must roll all of them back rather than leaving unchecked
// references in the Flow.
func (p *Pipeline) resolveRegisteredListingFailure(ctx context.Context, flowID string,
	records []*registrationRecord, results []ObjectResult, listErr error) error {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.registrationRecoveryTimeout)
	defer cancel()
	cleanupErr := p.retractRegistrationRecords(recoveryCtx, flowID, records, results,
		"fresh verification URLs could not be listed after registration")
	return errors.Join(fmt.Errorf("read registered segments for %s: %w; every registered segment was sent for retraction",
		recordNames(records), listErr), cleanupErr)
}

// resolveIncompleteRegisteredListing verifies every visible Segment and
// retracts every known-registered Segment omitted from the fresh listing. It
// never returns at the first omission.
func (p *Pipeline) resolveIncompleteRegisteredListing(ctx context.Context, flowID string,
	visible, missing []*registrationRecord, results []ObjectResult) error {
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.registrationRecoveryTimeout)
	defer cancel()
	var failures []error
	if err := p.verifyRegistrationRecordsWithinRecovery(recoveryCtx, flowID, visible, results); err != nil {
		failures = append(failures, err)
	}
	if err := p.retractRegistrationRecords(recoveryCtx, flowID, missing, results,
		"registered segment was omitted from the fresh verification listing"); err != nil {
		failures = append(failures, err)
	}
	failures = append([]error{fmt.Errorf(
		"fresh registration listing omitted %s; visible segments were verified and every omitted segment was sent for retraction",
		recordNames(missing))}, failures...)
	return errors.Join(failures...)
}

func (p *Pipeline) verifyRegistrationRecords(ctx context.Context, flowID string,
	records []*registrationRecord, results []ObjectResult) error {
	return p.verifyRegistrationRecordsWithPolicy(ctx, flowID, records, results, false)
}

func (p *Pipeline) verifyRegistrationRecordsWithinRecovery(ctx context.Context, flowID string,
	records []*registrationRecord, results []ObjectResult) error {
	return p.verifyRegistrationRecordsWithPolicy(ctx, flowID, records, results, true)
}

func (p *Pipeline) verifyRegistrationRecordsWithPolicy(ctx context.Context, flowID string,
	records []*registrationRecord, results []ObjectResult, sharedRecoveryDeadline bool) error {
	tasks := make([]verificationTask, len(records))
	for index, record := range records {
		tasks[index] = verificationTask{object: record.object, segment: record.segment}
	}
	var (
		outcomes []verificationOutcome
		err      error
	)
	if sharedRecoveryDeadline {
		outcomes, err = p.verifyAllWithinRecovery(ctx, flowID, tasks)
	} else {
		outcomes, err = p.verifyAllWithOutcomes(ctx, flowID, tasks)
	}
	for index, outcome := range outcomes {
		switch outcome {
		case outcomeVerified:
			setRegistrationState(records[index], registrationVerified, results)
			setObjectVerification(results, records[index].object.id, ObjectVerificationVerified, VerificationMethodReadback)
		case outcomeRetracted:
			setRegistrationState(records[index], registrationRetracted, results)
			setObjectVerification(results, records[index].object.id, ObjectVerificationFailed, VerificationMethodReadback)
		case outcomeRetractionFailed:
			setRegistrationState(records[index], registrationStranded, results)
			setObjectVerification(results, records[index].object.id, ObjectVerificationFailed, VerificationMethodReadback)
		}
	}
	return err
}

// retractRegistrationRecords uses a fixed worker pool and the caller's shared
// recovery context. Each error names the exact Object that needs an operator;
// successful siblings continue even after one cleanup fails.
func (p *Pipeline) retractRegistrationRecords(ctx context.Context, flowID string,
	records []*registrationRecord, results []ObjectResult, cause string) error {
	if len(records) == 0 {
		return nil
	}
	failures := make([]error, len(records))
	workers := min(max(p.config.Transfers, 1), len(records))
	// Buffer the complete batch so dispatch itself never waits behind a slow
	// deletion. The fixed worker count still bounds TAMS calls, while every
	// Object is made eligible for cleanup within the one shared deadline.
	pending := make(chan int, len(records))
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for index := range pending {
				record := records[index]
				outcome, err := p.retractWithinRecovery(ctx, flowID, record.object,
					fmt.Errorf("object %s: %s", record.object.id, cause))
				if outcome == outcomeRetractionFailed {
					setRegistrationState(record, registrationStranded, results)
					failures[index] = fmt.Errorf("object %s (%s) is stranded: %w",
						record.object.id, record.object.timerange, err)
					continue
				}
				setRegistrationState(record, registrationRetracted, results)
			}
		}()
	}
	for index := range records {
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

func objectsFromRecords(records []*registrationRecord) []preparedObject {
	objects := make([]preparedObject, len(records))
	for index, record := range records {
		objects[index] = record.object
	}
	return objects
}

func recordNames(records []*registrationRecord) string {
	if len(records) == 0 {
		return "none"
	}
	names := make([]string, len(records))
	for index, record := range records {
		names[index] = record.object.id
	}
	return strings.Join(names, ", ")
}

// deleteRegisteredSegment is the single adaptation point for the terminal,
// exact-object deletion phase stacked immediately below this change.
func (p *Pipeline) deleteRegisteredSegment(ctx context.Context, flowID string, object preparedObject) error {
	return p.client.DeleteSegments(ctx, flowID, tams.SegmentDeleteOptions{
		ObjectID: object.id, Timerange: object.timerange,
	})
}
