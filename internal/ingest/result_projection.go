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
		Status: ObjectStatusPlanned, Disposition: ObjectDispositionPlanned,
		Verification: verification, VerificationMethod: VerificationMethodNone,
	}
}

func setObjectStatus(objects []ObjectResult, objectID string, status ObjectStatus) {
	for index := range objects {
		if objects[index].ObjectID == objectID {
			objects[index].Status = status
			if disposition := objectDispositionForStatus(status); disposition != "" {
				objects[index].Disposition = disposition
			}
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

func finalizeObjectResult(object *ObjectResult) {
	if object.Disposition == "" {
		object.Disposition = objectDispositionForStatus(object.Status)
		if object.Disposition == "" {
			object.Disposition = ObjectDispositionUnattempted
		}
	}
	if object.Verification == "" {
		object.Verification = ObjectVerificationNotReached
	}
	if object.VerificationMethod == "" {
		object.VerificationMethod = VerificationMethodNone
	}
}

func objectDispositionForStatus(status ObjectStatus) ObjectDisposition {
	switch status {
	case ObjectStatusPlanned:
		return ObjectDispositionPlanned
	case ObjectStatusUploaded:
		return ObjectDispositionUploaded
	case ObjectStatusRegistered, ObjectStatusVerified:
		return ObjectDispositionRegistered
	case ObjectStatusResumed:
		return ObjectDispositionResumed
	case ObjectStatusIngested:
		return ObjectDispositionIngested
	case ObjectStatusRejected:
		return ObjectDispositionRejected
	case ObjectStatusRetractionIndeterminate:
		return ObjectDispositionRegistrationIndeterminate
	case ObjectStatusRetracted:
		return ObjectDispositionRetracted
	case ObjectStatusStranded:
		return ObjectDispositionStranded
	default:
		return ""
	}
}

func compactObjectResults(flows []FlowResult) {
	for flowIndex := range flows {
		if flows[flowIndex].ObjectSummary.Total > 0 {
			continue
		}
		var summary ObjectSummary
		for objectIndex := range flows[flowIndex].Objects {
			object := &flows[flowIndex].Objects[objectIndex]
			finalizeObjectResult(object)
			AccumulateObjectSummary(&summary, *object)
		}
		flows[flowIndex].ObjectSummary = summary
	}
}
