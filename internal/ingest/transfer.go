package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"golang.org/x/sync/errgroup"
)

// chunkSize decides how many Media Objects to commit together.
//
// The outer bound is the Object lifetime the store advertised: an Object is
// collected if it is not registered in time, and that clock does not care how
// many uploads run at once. commitChunk may divide this further into ready-worker
// microbatches so the shorter presigned-URL lifetime is honoured as well.
//
// Half the advertised lifetime is used, leaving the other half as margin for a
// batch that turns out slower than the one before it. A store that advertises
// no lifetime has not told us to divide the work, so it is not divided.
func (p *Pipeline) chunkSize(remaining []preparedObject, throughput float64) int {
	if p.limits.ObjectRegistration <= 0 {
		return len(remaining)
	}
	if throughput <= 0 {
		throughput = assumedThroughput
	}
	budget := p.limits.ObjectRegistration.Seconds() / 2 * throughput
	var bytes float64
	fits := 0
	for _, object := range remaining {
		bytes += float64(object.size)
		// At least one Object goes in every batch: an Object too large to fit
		// the budget on its own cannot be made smaller, and is warned about
		// when its upload is estimated.
		if fits > 0 && bytes > budget {
			break
		}
		fits++
	}
	return min(max(fits, 1), len(remaining))
}

// outlastsRegistration estimates whether upload alone exceeds the Object's
// registration lifetime. URL expiry only limits when a transfer may start.
func outlastsRegistration(size int64, throughput float64, lifetime time.Duration) (time.Duration, bool) {
	if lifetime <= 0 || throughput <= 0 || size <= 0 {
		return 0, false
	}
	expected := time.Duration(float64(size) / throughput * float64(time.Second))
	return expected, expected > lifetime
}

// chunkTimerange covers the Segments in one registration operation, so an
// ambiguous write can be reconciled without listing the whole Flow.
func chunkTimerange(chunk []preparedObject) string {
	first, last := chunk[0].start, chunk[0].start+chunk[0].duration
	for _, object := range chunk[1:] {
		first = min(first, object.start)
		last = max(last, object.start+object.duration)
	}
	timerange, err := media.TimeRange(first, last-first)
	if err != nil {
		// Listing the whole Flow is wasteful but correct, and a batch that
		// cannot describe its own span is not a reason to fail an ingest.
		return ""
	}
	return timerange
}

// commitChunk brings one batch of Media Objects into the store: storage
// allocated, bytes uploaded, Segments registered, and each one verified or
// withdrawn before the next batch starts. Keeping that whole cycle inside one
// batch is what bounds how long an Object sits unregistered.
//
// It reports how long the batch took and how many bytes it moved, so the next
// can be sized from what this one achieved rather than from a guess.
func (p *Pipeline) commitChunk(ctx context.Context, flowID string, chunk []preparedObject,
	objectResults []ObjectResult, storageID string, throughput float64) (time.Duration, int64, error) {
	if p.limits.PresignedURL <= 0 {
		return p.commitReadyChunk(ctx, flowID, chunk, objectResults, storageID, throughput, nil)
	}

	// Storage allocation creates every PUT URL in its response. Reserve the
	// workers that will consume them first, and ask for no more URLs than can
	// begin immediately. Each microbatch completes registration and verification
	// before the next allocation, preserving the Object-registration state
	// machine as well as both advertised lifetimes.
	started := time.Now()
	var transferred int64
	for offset := 0; offset < len(chunk); {
		remaining := chunk[offset:]
		reservation, err := p.reserveTransferBatch(ctx, min(len(remaining), max(p.config.Transfers, 1)))
		if err != nil {
			return 0, 0, fmt.Errorf("wait for an upload worker: %w", err)
		}
		ready := remaining[:min(len(remaining), reservation.count)]
		_, bytes, err := p.commitReadyChunk(
			ctx, flowID, ready, objectResults, storageID, throughput, reservation)
		if err != nil {
			return 0, 0, err
		}
		transferred += bytes
		offset += len(ready)
	}
	return time.Since(started), transferred, nil
}

// commitReadyChunk consumes one batch whose upload workers are already
// reserved. A nil reservation retains the defensive fallback for tests and
// clients that have no advertised presigned-URL lifetime.
func (p *Pipeline) commitReadyChunk(ctx context.Context, flowID string, chunk []preparedObject,
	objectResults []ObjectResult, storageID string, throughput float64,
	reservation *transferReservation) (time.Duration, int64, error) {
	started := time.Now()
	if reservation != nil {
		defer reservation.releaseAll()
	}
	var transferred int64
	objectIDs := make([]string, 0, len(chunk))
	for _, object := range chunk {
		transferred += object.size
		objectIDs = append(objectIDs, object.id)
	}

	allocation, err := p.client.AllocateStorage(ctx, flowID, tams.StorageRequest{
		ObjectIDs: objectIDs, StorageID: storageID,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("allocate storage for %d objects: %w", len(chunk), err)
	}
	destinations := make(map[string]tams.PresignedURL, len(allocation.MediaObjects))
	var uploadStartBefore time.Time
	if p.limits.PresignedURL > 0 {
		// The service exposes a minimum duration, not an absolute expiry. Measure
		// it from response receipt; the schema asks services to leave grace for
		// URL generation and response latency.
		uploadStartBefore = time.Now().Add(p.limits.PresignedURL)
	}
	for _, allocated := range allocation.MediaObjects {
		// Before 8.2 the allocation response did not identify presigned URLs.
		if !p.apiVersion.AtLeast(8, 2) || allocated.Presigned != nil && *allocated.Presigned {
			allocated.PutURL.StartBefore = uploadStartBefore
		}
		destinations[allocated.ObjectID] = allocated.PutURL
	}
	// Validate the complete response before starting any transfer. Returning
	// halfway through scheduling would let earlier goroutines outlive a failed
	// batch.
	for _, object := range chunk {
		destination, ok := destinations[object.id]
		if !ok || destination.URL == "" {
			return 0, 0, fmt.Errorf("storage allocation omitted object %s", object.id)
		}
	}

	// A single large Object may exceed the registration window even with no
	// queue. Warn without changing the selected media treatment.
	for _, object := range chunk {
		if expected, oversized := outlastsRegistration(object.size, throughput, p.limits.ObjectRegistration); oversized {
			p.logger.Warn("media object upload may exceed its registration lifetime",
				"flow_id", flowID, "object_id", object.id, "bytes", object.size,
				"estimated_upload", expected.Round(time.Second), "registration_lifetime", p.limits.ObjectRegistration)
		}
	}

	uploads, uploadCtx := errgroup.WithContext(ctx)
	// The limit bounds how many goroutines exist, not just how many are doing
	// something. Without it every Object in the batch got one immediately and
	// then queued on the transfer budget, so the goroutine count followed the
	// size of the job rather than the size of the allowance. The budget itself
	// is still taken inside, because it is shared across concurrent Flows while
	// this limit only governs one batch.
	uploadLimit := max(p.config.Transfers, 1)
	if reservation != nil {
		uploadLimit = reservation.count
	}
	uploads.SetLimit(uploadLimit)
	receipts := make(map[string]tams.UploadReceipt, len(chunk))
	var receiptsMu sync.Mutex
	for _, object := range chunk {
		destination := destinations[object.id]
		uploads.Go(func() error {
			if reservation == nil {
				release, err := p.acquireTransfer(uploadCtx)
				if err != nil {
					return err
				}
				defer release()
			}
			p.logger.Info("uploading object", "flow_id", flowID, "object_id", object.id, "bytes", object.size)
			receipt, err := p.client.UploadFile(uploadCtx, destination, object.path)
			if err != nil {
				return fmt.Errorf("upload object %s: %w", object.id, err)
			}
			if receipt.Bytes != object.size || receipt.SHA256 != object.sha256 {
				return withFailure(FailureCodeSourceChanged, FailureMessageSourceChanged, true,
					fmt.Errorf("prepared object %s changed before upload: expected %d bytes with SHA-256 %s, transmitted %d bytes with SHA-256 %s",
						object.id, object.size, object.sha256, receipt.Bytes, receipt.SHA256))
			}
			if receipt.StorageSHA256 != "" && receipt.StorageSHA256 != object.sha256 {
				return fmt.Errorf("storage checksum for object %s is %s, expected SHA-256 %s",
					object.id, receipt.StorageSHA256, object.sha256)
			}
			receiptsMu.Lock()
			receipts[object.id] = receipt
			receiptsMu.Unlock()
			p.observability.Uploaded(object.size)
			p.advanceProgress(uploadCtx, progress.PhaseStore, 1, object.size)
			return nil
		})
	}
	if err := uploads.Wait(); err != nil {
		return 0, 0, err
	}
	if reservation != nil {
		reservation.releaseAll()
	}
	records := registrationRecords(chunk, registrationUploaded)
	for _, record := range records {
		setRegistrationState(record, registrationUploaded, objectResults)
	}

	requests := make([]tams.SegmentRequest, 0, len(chunk))
	for _, object := range chunk {
		requests = append(requests, tams.SegmentRequest{
			ObjectID: object.id, Timerange: object.timerange,
			ObjectTimerange: object.objectTimerange, TSOffset: object.tsOffset,
		})
	}
	for _, record := range records {
		setRegistrationState(record, registrationIndeterminate, objectResults)
	}
	registrationRecovered := false
	if err := p.client.RegisterSegments(ctx, flowID, requests); err != nil {
		if resolveErr := p.reconcileRegistrationError(ctx, flowID, records, objectResults, err); resolveErr != nil {
			return 0, 0, fmt.Errorf("register segments: %w", errors.Join(err, resolveErr))
		}
		// A complete readback (and verification, when enabled) proved that the
		// bulk POST committed before its response was lost.
		registrationRecovered = true
	} else {
		for _, record := range records {
			setRegistrationState(record, registrationRegistered, objectResults)
		}
	}

	// TAMS requires GET /objects/{objectId} to answer 404 until the Object is
	// registered against a Flow Segment, so uploaded bytes cannot be read back
	// before registration. With an advertised URL lifetime, verification takes
	// a transfer slot and then fetches one exact, fresh URL in verifyOne. The
	// per-record state machine still resolves every listing failure or omission
	// by retracting that known-registered Segment.
	if p.config.VerificationMode != VerificationNone && !registrationRecovered {
		readback := records
		if p.config.VerificationMode == VerificationAuto {
			readback = make([]*registrationRecord, 0, len(records))
			for _, record := range records {
				if receipts[record.object.id].StorageSHA256 == "" {
					readback = append(readback, record)
					continue
				}
				p.acceptStorageVerification(ctx, record, objectResults)
			}
		}
		if len(readback) > 0 && p.limits.PresignedURL > 0 {
			for _, record := range readback {
				record.segment = tams.Segment{
					ObjectID: record.object.id, Timerange: record.object.timerange,
				}
			}
			if err := p.verifyRegistrationRecords(ctx, flowID, readback, objectResults); err != nil {
				return 0, 0, err
			}
		} else if len(readback) > 0 {
			registered, err := p.client.ListSegments(ctx, flowID,
				tams.SegmentListOptions{Timerange: chunkTimerange(objectsFromRecords(readback)), IncludeDownloadURLs: true})
			if err != nil {
				return 0, 0, p.resolveRegisteredListingFailure(ctx, flowID, readback, objectResults, err)
			}
			visible := make([]*registrationRecord, 0, len(readback))
			missing := make([]*registrationRecord, 0)
			for _, record := range readback {
				segment := matchingSegment(registered, record.object.id, record.object.timerange)
				if segment == nil {
					missing = append(missing, record)
					continue
				}
				record.segment = *segment
				visible = append(visible, record)
			}
			if len(missing) > 0 {
				return 0, 0, p.resolveIncompleteRegisteredListing(ctx, flowID, visible, missing, objectResults)
			}
			if err := p.verifyRegistrationRecords(ctx, flowID, visible, objectResults); err != nil {
				return 0, 0, err
			}
		}
	}
	for _, object := range chunk {
		setObjectStatus(objectResults, object.id, ObjectStatusIngested)
	}
	completed := make(map[string]struct{}, len(chunk))
	for _, object := range chunk {
		completed[object.id] = struct{}{}
	}
	if err := p.observeObjectBatch(ctx, flowID, objectResults, completed); err != nil {
		return 0, 0, err
	}
	return time.Since(started), transferred, nil
}

// applyBitRates records what a reader will actually have to pull off the wire.
//
// The Flow's bit rate properties are defined over Segments rather than essence,
// so they can only be worked out once the Segments exist -- which is why this
// runs after preparation rather than when the Flow was built from the probe.
// max_bit_rate in particular is what sizes a receiver's buffer, so leaving it
// unset makes a Flow harder to play back than it needs to be.
func (p *Pipeline) applyBitRates(flow tams.Flow, objects []preparedObject) {
	if len(objects) == 0 {
		return
	}
	segments := make([]media.SegmentMeasurement, len(objects))
	for index, object := range objects {
		segments[index] = media.SegmentMeasurement{Bytes: object.size, Duration: object.duration}
	}
	average, peak, ok := media.SegmentBitRates(segments, p.config.SegmentDuration)
	if !ok {
		return
	}
	flow["avg_bit_rate"] = average
	flow["max_bit_rate"] = peak
}

// acquireProbe takes a slot from the global media-measurement budget.
func (p *Pipeline) acquireProbe(ctx context.Context) (func(), error) {
	select {
	case p.probes <- struct{}{}:
		releaseProcess, err := p.acquireMediaProcess(ctx, 1)
		if err != nil {
			<-p.probes
			return nil, err
		}
		return func() {
			releaseProcess()
			<-p.probes
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Pipeline) acquireMediaProcess(ctx context.Context, weight int64) (func(), error) {
	if err := p.mediaProcesses.Acquire(ctx, weight); err != nil {
		return nil, err
	}
	return func() { p.mediaProcesses.Release(weight) }, nil
}

func (p *Pipeline) acquireRollingRender(ctx context.Context) (func(), error) {
	select {
	case p.rollingRenders <- struct{}{}:
		return func() { <-p.rollingRenders }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// retractionTimeout bounds cleanup. Retraction runs detached from the caller's
// context so cancellation cannot skip it, which means it needs a deadline of
// its own or a wedged service could hang a run that is already finishing.
const retractionTimeout = 30 * time.Second

// acquireTransfer takes a slot from the global transfer budget, returning the
// release function. It respects cancellation so a failing sibling does not
// leave callers queued behind work that will be discarded.
func (p *Pipeline) acquireTransfer(ctx context.Context) (func(), error) {
	select {
	case p.transfers <- struct{}{}:
		return func() { <-p.transfers }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// transferReservation holds slots in the global transfer budget before a
// service is asked to generate upload URLs. An allocated URL is therefore
// handed only to work that can begin immediately, rather than to a goroutine
// queued behind an unrelated Flow's transfers.
type transferReservation struct {
	pipeline *Pipeline
	count    int
}

func (r *transferReservation) releaseAll() {
	if r == nil || r.pipeline == nil {
		return
	}
	for range r.count {
		<-r.pipeline.transfers
	}
	r.pipeline = nil
	r.count = 0
}

// reserveTransferBatch waits for one global transfer slot, then takes as many
// additional slots as are immediately free. Waiting for every desired slot
// would deadlock when two concurrent Flows each held part of the budget. The
// returned count is consequently the safe allocation batch size right now.
func (p *Pipeline) reserveTransferBatch(ctx context.Context, desired int) (*transferReservation, error) {
	desired = min(max(desired, 1), cap(p.transfers))
	select {
	case p.transfers <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	reserved := 1
	for reserved < desired {
		select {
		case p.transfers <- struct{}{}:
			reserved++
		default:
			return &transferReservation{pipeline: p, count: reserved}, nil
		}
	}
	return &transferReservation{pipeline: p, count: reserved}, nil
}

func (p *Pipeline) registerFlow(ctx context.Context, flowID string, objects []preparedObject, objectResults []ObjectResult, storageID string) error {
	// One listing answers the resume question for every Object. Asking per
	// Object cost a round trip each, which dominates on a high-latency link.
	// Download URLs are only wanted if a resumed Object will be verified. When
	// they are not, the service is spared signing one per Segment for a listing
	// that is only being asked which Objects exist.
	existing, err := p.client.ListSegments(ctx, flowID,
		tams.SegmentListOptions{
			// This first listing answers identity only. A verification worker asks
			// for its own URL after it holds a transfer slot.
			IncludeDownloadURLs: p.config.VerificationMode != VerificationNone && p.limits.PresignedURL <= 0,
		})
	if err != nil {
		return fmt.Errorf("list existing segments: %w", err)
	}
	throughput := float64(0)
	return p.registerPreparedObjects(ctx, flowID, objects, objectResults, storageID, existing, &throughput)
}

func (p *Pipeline) registerRollingChunk(ctx context.Context, flowID string, objects []preparedObject,
	objectResults []ObjectResult, storageID string, throughput *float64) error {
	existing, err := p.client.ListSegments(ctx, flowID, tams.SegmentListOptions{
		Timerange:           chunkTimerange(objects),
		IncludeDownloadURLs: p.config.VerificationMode != VerificationNone && p.limits.PresignedURL <= 0,
	})
	if err != nil {
		return fmt.Errorf("list existing segments for rolling batch: %w", err)
	}
	return p.registerPreparedObjects(ctx, flowID, objects, objectResults, storageID, existing, throughput)
}

func (p *Pipeline) registerPreparedObjects(ctx context.Context, flowID string, objects []preparedObject,
	objectResults []ObjectResult, storageID string, existing []tams.Segment, throughput *float64) error {
	if throughput == nil {
		throughput = new(float64)
	}

	missing := make([]preparedObject, 0, len(objects))
	var resumed []verificationTask
	for _, object := range objects {
		segment := matchingSegment(existing, object.id, object.timerange)
		if segment == nil {
			missing = append(missing, object)
			continue
		}
		// A resumed Object credits the upload it did not need to repeat. Its
		// verification is scheduled with the rest, so that unit is credited there.
		p.advanceProgress(ctx, progress.PhaseStore, 1, object.size)
		if p.config.VerificationMode != VerificationNone {
			resumed = append(resumed, verificationTask{object: object, segment: *segment})
		}
		setObjectStatus(objectResults, object.id, ObjectStatusResumed)
	}
	// Resumed Objects are checked before missing uploads. When URL lifetimes are
	// advertised, the tasks intentionally carry no URL: verifyOne refreshes each
	// only after its worker owns transfer capacity.
	outcomes, verifyErr := p.verifyAllWithOutcomes(ctx, flowID, resumed)
	for index, outcome := range outcomes {
		if outcome == outcomeVerified {
			setObjectVerification(objectResults, resumed[index].object.id,
				ObjectVerificationVerified, VerificationMethodReadback)
		}
	}
	if verifyErr != nil {
		for index, outcome := range outcomes {
			switch outcome {
			case outcomeRetracted:
				setObjectStatus(objectResults, resumed[index].object.id, ObjectStatusRetracted)
				setObjectVerification(objectResults, resumed[index].object.id,
					ObjectVerificationFailed, VerificationMethodReadback)
			case outcomeRetractionFailed:
				setObjectStatus(objectResults, resumed[index].object.id, ObjectStatusStranded)
				setObjectVerification(objectResults, resumed[index].object.id,
					ObjectVerificationFailed, VerificationMethodReadback)
			}
		}
		return verifyErr
	}
	if len(resumed) > 0 {
		completed := make(map[string]struct{}, len(resumed))
		for _, task := range resumed {
			completed[task.object.id] = struct{}{}
		}
		if err := p.observeObjectBatch(ctx, flowID, objectResults, completed); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if err := p.setFlowStatus(ctx, flowID, flowStatusIngesting); err != nil {
		return fmt.Errorf("mark Flow ingesting before Object allocation: %w", err)
	}

	// Media Objects are committed in batches rather than all at once. A store
	// collects an Object that is not registered against a Segment in time, and
	// promises only five minutes, so allocating storage for a whole programme
	// and registering it an hour later is relying on a guarantee that was never
	// given. Each batch is allocated, uploaded, registered and verified before
	// the next begins, which keeps every Object's unregistered life to the
	// length of one batch.
	for offset := 0; offset < len(missing); {
		chunk := missing[offset:min(offset+p.chunkSize(missing[offset:], *throughput), len(missing))]
		elapsed, transferred, err := p.commitChunk(ctx, flowID, chunk, objectResults, storageID, *throughput)
		if err != nil {
			return err
		}
		if elapsed > 0 && transferred > 0 {
			*throughput = float64(transferred) / elapsed.Seconds()
		}
		offset += len(chunk)
	}

	for _, object := range missing {
		setObjectStatus(objectResults, object.id, ObjectStatusIngested)
	}
	return nil
}
