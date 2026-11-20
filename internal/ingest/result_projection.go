package ingest

import (
	"fmt"

	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/livewyer-ops/tamsin/internal/tamstime"
)

func checkSegmentTiming(flowID string, object preparedObject, segment tams.Segment) error {
	if !tamstime.EqualTimestamps(segment.TSOffset, object.tsOffset) ||
		(segment.ObjectTimerange != "" && !tamstime.EqualTimeRanges(segment.ObjectTimerange, object.objectTimerange)) {
		return withFailure(FailureCodeSegmentConflict, FailureMessageSegmentConflict, true,
			fmt.Errorf("flow %s segment %s at %s records different media timing; use a new Flow ID",
				flowID, segment.ObjectID, segment.Timerange))
	}
	return nil
}

// matchingSegment compares timeranges by value: a service may return the
// instantaneous range [t] as [t_t], or otherwise respell what was registered.
func matchingSegment(segments []tams.Segment, objectID, timerange string) *tams.Segment {
	for index := range segments {
		if segments[index].ObjectID == objectID && tamstime.EqualTimeRanges(segments[index].Timerange, timerange) {
			return &segments[index]
		}
	}
	return nil
}

type segmentKey struct {
	objectID  string
	timerange string
}

func (o preparedObject) key() segmentKey { return segmentKey{o.id, o.timerange} }
func (o *ObjectResult) key() segmentKey  { return segmentKey{o.ObjectID, o.Timerange} }

// renameObjectResult follows an Object whose identifier the service replaced
// during allocation, so its dispositions keep landing on the same result.
func renameObjectResult(objects []ObjectResult, old segmentKey, newID string) {
	for index := range objects {
		if objects[index].key() == old {
			objects[index].ObjectID = newID
			return
		}
	}
}

func (p *Pipeline) newObjectResult(object preparedObject) ObjectResult {
	verification := ObjectVerificationNotReached
	if p.config.VerificationMode == VerificationNone {
		verification = ObjectVerificationNotRequested
	}
	return ObjectResult{
		ObjectID: object.id, Timerange: object.timerange, Bytes: object.size, SHA256: object.sha256,
		Disposition:  ObjectDispositionPlanned,
		Verification: verification, VerificationMethod: VerificationMethodNone,
	}
}

func setObjectDisposition(objects []ObjectResult, key segmentKey, disposition ObjectDisposition) {
	for index := range objects {
		if objects[index].key() == key {
			objects[index].Disposition = disposition
			return
		}
	}
}

func setObjectVerification(objects []ObjectResult, key segmentKey,
	status ObjectVerificationStatus, method VerificationMethod,
) {
	for index := range objects {
		if objects[index].key() == key {
			objects[index].Verification = status
			objects[index].VerificationMethod = method
			return
		}
	}
}

// completedStatus reports a successful ingest as resumed only when it has
// Objects and every one of them was already in the store.
func completedStatus(flows []FlowResult) ResultStatus {
	total, resumed := 0, 0
	for _, flow := range flows {
		total += flow.ObjectSummary.Total
		resumed += flow.ObjectSummary.Resumed
	}
	if total > 0 && resumed == total {
		return ResultStatusResumed
	}
	return ResultStatusIngested
}

func compactObjectResults(flows []FlowResult) {
	for flowIndex := range flows {
		if flows[flowIndex].ObjectSummary.Total > 0 {
			continue
		}
		var summary ObjectSummary
		for _, object := range flows[flowIndex].Objects {
			AccumulateObjectSummary(&summary, object)
		}
		flows[flowIndex].ObjectSummary = summary
	}
}
