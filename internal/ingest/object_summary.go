package ingest

// AccumulateObjectSummary adds one terminal Object to a bounded Flow summary.
// Keeping this projection in one place prevents human and event
// pipeline result paths from assigning different meanings to a disposition.
func AccumulateObjectSummary(summary *ObjectSummary, object ObjectResult) {
	if summary == nil {
		return
	}
	finalizeObjectResult(&object)
	summary.Total++
	summary.Bytes += object.Bytes
	switch object.Disposition {
	case ObjectDispositionIngested:
		summary.Ingested++
	case ObjectDispositionResumed:
		summary.Resumed++
	case ObjectDispositionRejected:
		summary.Rejected++
	case ObjectDispositionRetracted:
		summary.Retracted++
	case ObjectDispositionStranded, ObjectDispositionRegistrationIndeterminate:
		summary.Stranded++
	case ObjectDispositionUnattempted, ObjectDispositionPlanned, ObjectDispositionUploaded, ObjectDispositionRegistered:
		summary.Unattempted++
	}
	if object.Verification == ObjectVerificationVerified {
		summary.Verified++
		switch object.VerificationMethod {
		case VerificationMethodStorage:
			summary.StorageVerified++
		case VerificationMethodReadback:
			summary.ReadbackVerified++
		}
	}
}
