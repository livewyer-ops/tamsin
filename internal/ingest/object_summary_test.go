package ingest

import "testing"

func TestAccumulateObjectSummaryClassifiesEveryTerminalProjection(t *testing.T) {
	t.Parallel()

	objects := []ObjectResult{
		{Bytes: 1, Disposition: ObjectDispositionIngested, Verification: ObjectVerificationVerified, VerificationMethod: VerificationMethodStorage},
		{Bytes: 2, Disposition: ObjectDispositionResumed, Verification: ObjectVerificationVerified, VerificationMethod: VerificationMethodReadback},
		{Bytes: 3, Disposition: ObjectDispositionRejected},
		{Bytes: 4, Disposition: ObjectDispositionRetracted},
		{Bytes: 5, Disposition: ObjectDispositionStranded},
		{Bytes: 6, Disposition: ObjectDispositionRegistrationIndeterminate},
		{Bytes: 7, Disposition: ObjectDispositionPlanned},
		{Bytes: 8, Disposition: ObjectDispositionUploaded},
		{Bytes: 9, Disposition: ObjectDispositionRegistered},
		{Bytes: 10, Disposition: ObjectDispositionUnattempted},
	}
	var summary ObjectSummary
	for _, object := range objects {
		AccumulateObjectSummary(&summary, object)
	}
	want := ObjectSummary{
		Total: 10, Bytes: 55, Ingested: 1, Resumed: 1, Rejected: 1, Retracted: 1,
		Stranded: 2, Unattempted: 4, Verified: 2, StorageVerified: 1, ReadbackVerified: 1,
	}
	if summary != want {
		t.Fatalf("Object summary = %#v, want %#v", summary, want)
	}
}
