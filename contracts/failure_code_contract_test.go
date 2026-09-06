package contracts_test

import (
	"testing"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/ingestevent"
)

func TestPublishedFailureCodesAreStable(t *testing.T) {
	t.Parallel()
	for _, code := range []struct{ got, want string }{
		{ingest.FailureCodeConfigInvalid, "config.invalid"},
		{ingest.FailureCodeAuthFailed, "authentication.failed"},
		{ingest.FailureCodeInputFailed, "ingest.input_failures"},
		{ingest.FailureCodeSourceFailed, "source.failed"},
		{ingest.FailureCodeMediaFailed, "media.failed"},
		{ingest.FailureCodeTAMSFailed, "tams.failed"},
		{ingest.FailureCodeInterrupted, "run.interrupted"},
		{ingest.FailureCodeRunFailed, "run.failed"},
		{ingest.FailureCodeInputFailedGeneric, "ingest.input_failed"},
		{ingest.FailureCodeVerificationNotReached, "verification.not_reached"},
		{ingest.FailureCodeVerificationStranded, "verification.stranded"},
		{ingest.FailureCodeVerificationRetracted, "verification.retracted"},
		{ingest.FailureCodeFlowIndeterminate, "flow.indeterminate"},
		{ingest.FailureCodeObjectStranded, "object.stranded"},
		{ingest.FailureCodeObjectIndeterminate, "object.indeterminate"},
		{ingest.FailureCodeHTTPRequestFailed, "tams.request_failed"},
		{ingest.FailureCodeOutputFailed, "output.failed"},
		{ingest.FailureCodePreflightFailed, "tams.preflight_failed"},
		{ingest.FailureCodeStorageUnavailable, "tams.storage_unavailable"},
		{ingest.FailureCodeFlowPlanFailed, "flow.plan_failed"},
		{ingest.FailureCodeFlowWriteFailed, "flow.write_failed"},
		{ingest.FailureCodeTAMSRegistrationFailed, "tams.registration_failed"},
		{ingest.FailureCodeStagingCapacity, "staging.capacity"},
		{ingest.FailureCodeSourceTransferFailed, "source.transfer_failed"},
		{ingest.FailureCodeSourceChanged, "source.changed"},
		{ingest.FailureCodeMediaAnalysisFailed, "media.analysis_failed"},
		{ingest.FailureCodeMediaUnsupported, "media.unsupported"},
		{ingest.FailureCodeMediaOptionsInvalid, "media.options_invalid"},
		{ingest.FailureCodeMediaOptionsIgnored, "media.options_ignored"},
		{ingest.FailureCodeMediaToolUnavailable, "media.tool_unavailable"},
		{ingest.FailureCodeMediaPrepareFailed, "media.prepare_failed"},
	} {
		if code.got != code.want {
			t.Errorf("failure code changed from %q to %q", code.want, code.got)
		}
	}
}

func TestEventCodesMatchFailureCodes(t *testing.T) {
	t.Parallel()
	for _, pair := range [][2]string{
		{ingestevent.DiagnosticCodeConfigInvalid, ingest.FailureCodeConfigInvalid},
		{ingestevent.DiagnosticCodeObjectStranded, ingest.FailureCodeObjectStranded},
		{ingestevent.InputErrorCodeRunInterrupted, ingest.FailureCodeInterrupted},
		{ingestevent.InputErrorCodeIngestFailed, ingest.FailureCodeInputFailedGeneric},
	} {
		if pair[0] != pair[1] {
			t.Errorf("event code %q does not match failure code %q", pair[0], pair[1])
		}
	}
}
