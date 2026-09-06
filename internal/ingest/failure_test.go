package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

func TestDescribeFailureDoesNotExposeUntrustedTAMSErrorDetails(t *testing.T) {
	t.Parallel()
	const toxic = "peer-response-top-secret"
	cause := &tams.HTTPError{
		Method: http.MethodGet, URL: "https://user:password@example.test/service?token=top-secret",
		StatusCode: http.StatusInternalServerError, Status: "500 Provider top-secret reason", Body: toxic,
	}
	result := Result{
		Input: "file:///input.ts", Profile: ProfileEssenceSegments, ProfileVersion: "1",
		Status: ResultStatusFailed, Verification: VerificationNotReached, Flows: []FlowResult{}, Error: cause.Error(),
	}
	result.Failure = DescribeFailure(result, cause)
	if result.Failure == nil || result.Failure.Code != FailureCodeHTTPRequestFailed ||
		result.Failure.Message != FailureMessageHTTPRequestFailed {
		t.Fatalf("unsafe or unstable classification: %#v", result.Failure)
	}

	serialized, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{toxic, "password", "top-secret", "Provider"} {
		if strings.Contains(string(serialized), forbidden) {
			t.Errorf("serialized Result leaked %q: %s", forbidden, serialized)
		}
	}
	if strings.Contains(string(serialized), `"error"`) {
		t.Fatalf("serialized Result exposed the raw implementation error: %s", serialized)
	}
}

func TestDescribeRunFailureDistinguishesOperationTimeoutFromRunCancellation(t *testing.T) {
	t.Parallel()
	result := Result{Status: ResultStatusFailed, Verification: VerificationNotReached, Flows: []FlowResult{}}
	if failure := describeRunFailure(result, context.DeadlineExceeded, context.Background()); failure.Code == FailureCodeInterrupted {
		t.Fatalf("a child operation deadline was mislabeled as whole-run interruption: %#v", failure)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if failure := describeRunFailure(result, context.DeadlineExceeded, canceled); failure.Code != FailureCodeInterrupted {
		t.Fatalf("parent cancellation was not authoritative: %#v", failure)
	}
	outputFailure := withFailure(FailureCodeOutputFailed, FailureMessageOutputFailed, false, context.Canceled)
	if failure := describeRunFailure(result, outputFailure, canceled); failure.Code != FailureCodeOutputFailed {
		t.Fatalf("event-sink cancellation lost its typed cause: %#v", failure)
	}
}

func TestDescribeInputInterruptedFailure(t *testing.T) {
	t.Parallel()
	failure := DescribeInputInterruptedFailure()
	if failure == nil || failure.Code != FailureCodeInterrupted ||
		failure.Message != FailureMessageInputInterrupt || failure.ActionRequired {
		t.Fatalf("unexpected input interruption failure: %#v", failure)
	}
}

func TestFailedResultPreservesTypedOperationTimeout(t *testing.T) {
	t.Parallel()
	pipeline := &Pipeline{config: Config{Profile: ProfileEssenceSegments, ProfileVersion: "1", VerificationMode: VerificationReadback}}
	cause := withFailure(FailureCodePreflightFailed, FailureMessagePreflightTimedOut, true, context.DeadlineExceeded)
	result := pipeline.failedResult(source.Item{URI: "file:///input.ts"}, cause)
	if result.Failure == nil || result.Failure.Code != FailureCodePreflightFailed || !result.Failure.ActionRequired {
		t.Fatalf("typed child deadline was mislabeled: %#v", result.Failure)
	}
}

func TestCancellationDoesNotEraseStrandedTerminalState(t *testing.T) {
	t.Parallel()
	result := Result{
		Status: ResultStatusFailed, Verification: VerificationFailedStranded,
		Flows: []FlowResult{{Objects: []ObjectResult{{Status: ObjectStatusStranded}}}},
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	failure := describeRunFailure(result, context.Canceled, canceled)
	if failure.Code != FailureCodeObjectStranded || !failure.ActionRequired {
		t.Fatalf("cancellation erased terminal recovery truth: %#v", failure)
	}
}
