package ingest

import (
	"context"
	"errors"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

// Stable failure codes and their safe operator-facing messages. These are the
// wire contract: they appear in the process event stream,
// are pinned by contract tests, and must never carry store-supplied text.
const (
	FailureCodeConfigInvalid          = "config.invalid"
	FailureCodeAuthFailed             = "authentication.failed"
	FailureCodeInputFailed            = "ingest.input_failures"
	FailureCodeSourceFailed           = "source.failed"
	FailureCodeMediaFailed            = "media.failed"
	FailureCodeTAMSFailed             = "tams.failed"
	FailureCodeInterrupted            = "run.interrupted"
	FailureCodeRunFailed              = "run.failed"
	FailureCodeVerificationNotReached = "verification.not_reached"
	FailureCodeInputFailedGeneric     = "ingest.input_failed"
	FailureCodeFlowIndeterminate      = "flow.indeterminate"
	FailureCodeObjectStranded         = "object.stranded"
	FailureCodeObjectIndeterminate    = "object.indeterminate"
	FailureCodeVerificationStranded   = "verification.stranded"
	FailureCodeVerificationRetracted  = "verification.retracted"
	FailureCodeHTTPRequestFailed      = "tams.request_failed"
	FailureCodeOutputFailed           = "output.failed"
	FailureCodePreflightFailed        = "tams.preflight_failed"
	FailureCodeStorageUnavailable     = "tams.storage_unavailable"
	FailureCodeFlowPlanFailed         = "flow.plan_failed"
	FailureCodeFlowWriteFailed        = "flow.write_failed"
	FailureCodeTAMSRegistrationFailed = "tams.registration_failed"
	FailureCodeStagingCapacity        = "staging.capacity"
	FailureCodeSourceTransferFailed   = "source.transfer_failed"
	FailureCodeMediaAnalysisFailed    = "media.analysis_failed"
	FailureCodeMediaUnsupported       = "media.unsupported"
	FailureCodeMediaOptionsInvalid    = "media.options_invalid"
	FailureCodeMediaToolUnavailable   = "media.tool_unavailable"
	FailureCodeMediaPrepareFailed     = "media.prepare_failed"
	FailureCodeSourceChanged          = "source.changed"
	FailureCodeMediaOptionsIgnored    = "media.options_ignored"
	FailureCodeStreamUnavailable      = "source.stream_unavailable"

	FailureMessageConfigInvalid            = "The command arguments or configuration are invalid."
	FailureMessageAuthFailed               = "Authentication did not complete successfully."
	FailureMessageInputFailed              = "One or more inputs did not complete successfully."
	FailureMessageSourceFailed             = "Input resolution or transfer did not complete successfully."
	FailureMessageMediaFailed              = "Media analysis or transformation did not complete successfully."
	FailureMessageTAMSFailed               = "The TAMS operation did not complete successfully."
	FailureMessageInterrupted              = "The ingest was interrupted while cleanup was in progress."
	FailureMessageRunFailed                = "The ingest run did not complete successfully."
	FailureMessageInputInterrupt           = "The input did not finish before the run was interrupted."
	FailureMessageFlowIndeterminate        = "A Flow update may have committed before its response was lost."
	FailureMessageObjectStranded           = "A registered media object could not be retracted."
	FailureMessageObjectIndeterminate      = "A media object's registration state is indeterminate."
	FailureMessageVerificationStranded     = "Verification failed and registered media remains stranded."
	FailureMessageVerificationRetracted    = "Verification failed; unverified media was retracted."
	FailureMessageVerificationNotReached   = "The input failed before verification completed."
	FailureMessageInputFailedGeneric       = "The input did not complete successfully."
	FailureMessageHTTPRequestFailed        = "A TAMS operation did not complete successfully."
	FailureMessageOutputFailed             = "The process event stream could not be written."
	FailureMessagePreflightFailed          = "The TAMS service preflight did not complete successfully."
	FailureMessagePreflightIncompatible    = "The TAMS service is not compatible with this ingest."
	FailureMessagePreflightTimedOut        = "The TAMS preflight timed out."
	FailureMessageTransferLifetimeInvalid  = "The TAMS service did not advertise usable transfer lifetimes."
	FailureMessageStorageUnavailable       = "No usable TAMS storage backend was selected."
	FailureMessageFlowPlanFailed           = "The final Flow graph is not valid or could not be read."
	FailureMessageFlowWriteFailed          = "The Flow graph could not be committed completely."
	FailureMessageTAMSRegistrationFailed   = "Media Object registration did not complete successfully."
	FailureMessageRunInterrupted           = "The run was interrupted."
	FailureMessageStagingCapacity          = "The input could not reserve enough staging capacity."
	FailureMessageSourceTransferFailed     = "The input could not be read completely."
	FailureMessageMediaAnalysisFailed      = "Media analysis did not complete successfully."
	FailureMessageMediaContainerUnknown    = "The input container could not be identified."
	FailureMessageMediaUnsupported         = "The input codecs are not supported by the MPEG-TS segment policy."
	FailureMessageMediaOptionsInvalid      = "The FFmpeg options conflict with the selected media treatment."
	FailureMessageMediaToolUnavailable     = "The configured media toolchain is unavailable."
	FailureMessageMediaInvalidFlow         = "The input could not be described as a valid Flow."
	FailureMessageMediaIdentityUnresolved  = "The media interpretation identity could not be derived."
	FailureMessageMediaPrepareFailed       = "Media Objects could not be prepared."
	FailureMessageSourceChanged            = "The input changed while it was being ingested."
	FailureMessageMediaStreamPrepareFailed = "An elemental media stream could not be prepared."
	FailureMessageMediaOptionsIgnored      = "Media options cannot take effect when storing the source without segmentation."
	FailureMessageStreamUnavailable        = "The input cannot be streamed as requested; use --input-mode=auto or stage."
)

type classifiedFailure struct {
	Failure
	err error
}

func (e *classifiedFailure) Error() string { return e.err.Error() }
func (e *classifiedFailure) Unwrap() error { return e.err }

func withFailure(code, message string, actionRequired bool, err error) error {
	if err == nil {
		return nil
	}
	var existing *classifiedFailure
	if errors.As(err, &existing) {
		return err
	}
	return &classifiedFailure{
		Failure: Failure{Code: code, Message: message, ActionRequired: actionRequired},
		err:     err,
	}
}

// DescribeFailure maps a terminal result and optional root error to the stable
// failure contract exposed to outputs and diagnostics.
func DescribeFailure(result Result, cause error) *Failure {
	if terminal := terminalStateFailure(result); terminal != nil {
		return terminal
	}
	if cause != nil {
		var classified *classifiedFailure
		if errors.As(cause, &classified) {
			failure := classified.Failure
			return &failure
		}
		var httpError *tams.HTTPError
		if errors.As(cause, &httpError) {
			return &Failure{Code: FailureCodeHTTPRequestFailed, Message: FailureMessageHTTPRequestFailed}
		}
	}
	if result.Failure != nil {
		failure := *result.Failure
		return &failure
	}
	if result.Verification == VerificationNotReached {
		return &Failure{Code: FailureCodeVerificationNotReached, Message: FailureMessageVerificationNotReached, ActionRequired: false}
	}
	return &Failure{Code: FailureCodeInputFailedGeneric, Message: FailureMessageInputFailedGeneric}
}

// DescribeInputInterruptedFailure returns the stable input-local interruption
// projection for run-level interruption. Diagnostics use a different message even
// though they share the same failure code.
func DescribeInputInterruptedFailure() *Failure {
	return &Failure{Code: FailureCodeInterrupted, Message: FailureMessageInputInterrupt, ActionRequired: false}
}

func terminalStateFailure(result Result) *Failure {
	for _, flow := range result.Flows {
		if flow.Disposition == FlowIndeterminate {
			return &Failure{
				Code: FailureCodeFlowIndeterminate, Message: FailureMessageFlowIndeterminate, ActionRequired: true,
			}
		}
		for _, object := range flow.Objects {
			switch object.Disposition {
			case ObjectDispositionStranded:
				return &Failure{
					Code: FailureCodeObjectStranded, Message: FailureMessageObjectStranded, ActionRequired: true,
				}
			case ObjectDispositionRegistrationIndeterminate:
				return &Failure{
					Code: FailureCodeObjectIndeterminate, Message: FailureMessageObjectIndeterminate, ActionRequired: true,
				}
			}
		}
	}
	if result.Verification == VerificationFailedStranded {
		return &Failure{
			Code: FailureCodeVerificationStranded, Message: FailureMessageVerificationStranded, ActionRequired: true,
		}
	}
	if result.Verification == VerificationFailedRetracted {
		return &Failure{
			Code: FailureCodeVerificationRetracted, Message: FailureMessageVerificationRetracted,
		}
	}
	return nil
}

// describeRunFailure adds terminal-run context (for example interruption) on top
// of per-input failure classification.
func describeRunFailure(result Result, cause error, runCtx context.Context) *Failure {
	// Recovery truth is more important than why scheduling stopped. A signal
	// must never erase an indeterminate or stranded terminal state which still
	// requires operator action.
	if terminal := terminalStateFailure(result); terminal != nil {
		return terminal
	}
	var classified *classifiedFailure
	if errors.As(cause, &classified) && classified.Code == FailureCodeOutputFailed {
		failure := classified.Failure
		return &failure
	}
	if runCtx != nil && runCtx.Err() != nil {
		return interruptedFailure()
	}
	return DescribeFailure(result, cause)
}

func interruptedFailure() *Failure {
	return DescribeInputInterruptedFailure()
}
