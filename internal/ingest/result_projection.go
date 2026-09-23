package ingest

import "github.com/livewyer-ops/tamsin/internal/tams"

func matchingSegment(segments []tams.Segment, objectID, timerange string) *tams.Segment {
	for index := range segments {
		if segments[index].ObjectID == objectID && segments[index].Timerange == timerange {
			return &segments[index]
		}
	}
	return nil
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

func setObjectDisposition(objects []ObjectResult, objectID string, disposition ObjectDisposition) {
	for index := range objects {
		if objects[index].ObjectID == objectID {
			objects[index].Disposition = disposition
			return
		}
	}
}

func setObjectVerification(objects []ObjectResult, objectID string,
	status ObjectVerificationStatus, method VerificationMethod,
) {
	for index := range objects {
		if objects[index].ObjectID == objectID {
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
