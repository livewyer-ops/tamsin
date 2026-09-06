package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/ingestevent"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/source"
)

type switchableErrorEventWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	fail   atomic.Bool
}

func (writer *switchableErrorEventWriter) Write(payload []byte) (int, error) {
	if writer.fail.Load() {
		return 0, io.ErrClosedPipe
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.Write(payload)
}

const (
	eventTestRunID    = "0a853551-fb19-40d6-8f17-15dd3562e6d4"
	eventTestFlowID   = "7b0d2aec-1868-56ad-879d-95e35ed75e4c"
	eventTestSourceID = "29b5961b-22de-4b61-b137-013b70d20b54"
	eventTestObjectA  = "19e919cf-183a-40bd-b9e5-8c8b361f6728"
	eventTestDigest   = "ee79eb8b2ecda8115fe773d6469d7c26f99f35c9a555b3b4c72b05b8089ace5c"
)

func TestIngestEventOutputSeparatesProgressAndTerminalRecords(t *testing.T) {
	t.Parallel()
	var outputBytes bytes.Buffer
	output, err := newIngestEventOutput(&outputBytes, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	options := &ingestFlagValues{
		profile: ingest.ProfileEssenceSegments, profileVersion: "1",
		verify: string(ingest.VerificationReadback), concurrency: 1, transfers: 1, inputs: []string{"input.ts"},
	}
	if err := output.Start(options); err != nil {
		t.Fatal(err)
	}
	if err := output.Declare([]source.Item{{URI: "HTTPS://user:secret@example.test/input.ts?token=secret#fragment"}}); err != nil {
		t.Fatal(err)
	}
	if err := output.InputStarted(0); err != nil {
		t.Fatal(err)
	}

	instant := output.started.Add(time.Second)
	output.now = func() time.Time { return instant }
	scope := progress.Scope{InputIndex: 0, Input: "input.ts"}
	for _, snapshot := range []progress.Snapshot{
		{Scope: scope, Phase: progress.PhaseStore, Revision: 1},
		{Scope: scope, Phase: progress.PhaseStore, Revision: 2, TotalObjects: 1, TotalBytes: 10, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseVerify, Revision: 3, TotalObjects: 1, TotalBytes: 10, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseStore, Revision: 4, CompletedObjects: 1, TotalObjects: 1, CompletedBytes: 10, TotalBytes: 10, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseVerify, Revision: 5, CompletedObjects: 1, TotalObjects: 1, CompletedBytes: 10, TotalBytes: 10, TotalsFinal: true},
	} {
		output.Report(snapshot)
	}
	output.Retry(observability.RetryEvent{Operation: observability.OperationObjectUpload, Attempt: 2, MaxAttempts: 3})
	result := ingest.Result{
		Input: "input.ts", Profile: ingest.ProfileEssenceSegments, ProfileVersion: "1",
		RootFlowID: eventTestFlowID, Status: ingest.ResultStatusIngested, Verification: ingest.VerificationVerified,
		Flows: []ingest.FlowResult{{
			FlowID: eventTestFlowID, SourceID: eventTestSourceID, Disposition: ingest.FlowWritten,
			ObjectSummary: ingest.ObjectSummary{Total: 1, Bytes: 10, Ingested: 1, Verified: 1, ReadbackVerified: 1},
			Objects: []ingest.ObjectResult{{
				ObjectID: eventTestObjectA, Timerange: "[0:0_1:0)", Bytes: 10, SHA256: eventTestDigest,
				Disposition: ingest.ObjectDispositionIngested, Verification: ingest.ObjectVerificationVerified,
				VerificationMethod: ingest.VerificationMethodReadback,
			}},
		}},
	}
	if err := output.Result(0, result); err != nil {
		t.Fatal(err)
	}
	metrics := observability.Snapshot{Elapsed: time.Second, BytesStaged: 10, BytesUploaded: 10, BytesVerified: 10, Retries: 1, Verified: 1}
	if code, err := output.Finish(nil, ExitOK, options, metrics, context.Background()); err != nil || code != ExitOK {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}

	stream := decodeCLIIngestEventStream(t, outputBytes.Bytes())
	input := stream.state.Inputs[0]
	if input == nil || input.Finished == nil || input.Progress[ingestevent.ProgressStore].CompletedObjects != 1 ||
		input.Progress[ingestevent.ProgressVerify].CompletedObjects != 1 || len(input.ObjectResults) != 1 {
		t.Fatalf("incomplete event projection: %#v", input)
	}
	if stream.state.RetryCount != 1 || stream.state.Finished == nil || stream.state.Finished.ObjectsVerified != 1 {
		t.Fatalf("retry or terminal metrics were lost: %#v", stream.state)
	}
	if strings.Contains(outputBytes.String(), "secret") || strings.ContainsAny(outputBytes.String(), "\r\x1b") {
		t.Fatalf("machine stream contains secret or terminal control bytes: %q", outputBytes.String())
	}
}

func TestRunFailureMapsEveryExitCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		exitCode    int
		wantCode    string
		wantMessage string
		wantAction  bool
	}{
		{"usage", ExitUsage, ingest.FailureCodeConfigInvalid, ingest.FailureMessageConfigInvalid, true},
		{"auth", ExitAuth, ingest.FailureCodeAuthFailed, ingest.FailureMessageAuthFailed, true},
		{"partial", ExitPartial, ingest.FailureCodeInputFailed, ingest.FailureMessageInputFailed, false},
		{"source", ExitSource, ingest.FailureCodeSourceFailed, ingest.FailureMessageSourceFailed, true},
		{"media", ExitMedia, ingest.FailureCodeMediaFailed, ingest.FailureMessageMediaFailed, true},
		{"remote", ExitRemote, ingest.FailureCodeTAMSFailed, ingest.FailureMessageTAMSFailed, true},
		{"interrupted", ExitInterrupted, ingest.FailureCodeInterrupted, ingest.FailureMessageInterrupted, false},
		{"unrecognised", 99, ingest.FailureCodeRunFailed, ingest.FailureMessageRunFailed, true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			code, message, action := runFailure(testCase.exitCode)
			if code != testCase.wantCode || message != testCase.wantMessage || action != testCase.wantAction {
				t.Fatalf("runFailure(%d) = %q, %q, %t", testCase.exitCode, code, message, action)
			}
		})
	}
}

func TestInputFailurePrefersActionRequiredTerminalState(t *testing.T) {
	t.Parallel()
	result := ingest.Result{
		Status: ingest.ResultStatusFailed, Verification: ingest.VerificationFailedStranded,
		Failure: &ingest.Failure{Code: ingest.FailureCodeInterrupted, Message: ingest.FailureMessageRunInterrupted},
		Flows:   []ingest.FlowResult{{Objects: []ingest.ObjectResult{{Status: ingest.ObjectStatusStranded}}}},
	}
	code, _, action := inputFailure(result)
	if code != ingest.FailureCodeObjectStranded || !action {
		t.Fatalf("generic cancellation erased stranded terminal truth: code=%q action=%t", code, action)
	}
}

func TestIngestEventProgressWriterFailureCancelsTheRun(t *testing.T) {
	t.Parallel()
	writer := &switchableErrorEventWriter{}
	runCtx, cancel := context.WithCancelCause(context.Background())
	output, err := newIngestEventOutput(writer, eventTestRunID, cancel)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	if err := output.Declare([]source.Item{{URI: "file:///broken-pipe.ts"}}); err != nil {
		t.Fatal(err)
	}
	if err := output.InputStarted(0); err != nil {
		t.Fatal(err)
	}
	writer.fail.Store(true)
	output.Report(progress.Snapshot{
		Scope: progress.Scope{InputIndex: 0, Input: "file:///broken-pipe.ts"},
		Phase: progress.PhaseStore, Revision: 1,
	})
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("a broken event stream did not cancel ingest work")
	}
	if cause := context.Cause(runCtx); cause == nil || !strings.Contains(cause.Error(), "write ingest event stream") {
		t.Fatalf("cancellation cause = %v", cause)
	}
	if err := output.Err(); err == nil || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("output error = %v", err)
	}
}

func TestIngestEventInterruptionCompletesQueuedInput(t *testing.T) {
	t.Parallel()
	var outputBytes bytes.Buffer
	output, err := newIngestEventOutput(&outputBytes, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	options := &ingestFlagValues{
		profile: ingest.ProfileEssenceSegments, profileVersion: "1",
		verify: string(ingest.VerificationNone), concurrency: 1, transfers: 1, inputs: []string{"done.ts", "queued.ts"},
	}
	if err := output.Start(options); err != nil {
		t.Fatal(err)
	}
	if err := output.Declare([]source.Item{{URI: "file:///done.ts"}, {URI: "file:///queued.ts"}}); err != nil {
		t.Fatal(err)
	}
	if err := output.Result(0, ingest.Result{
		Profile: ingest.ProfileEssenceSegments, ProfileVersion: "1",
		Status: ingest.ResultStatusIngested, Verification: ingest.VerificationNotRequested, Flows: []ingest.FlowResult{},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrSignal)
	if code, err := output.Finish(context.Canceled, ExitInterrupted, options, observability.Snapshot{}, ctx); err != nil || code != ExitInterrupted {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}

	state := decodeCLIIngestEventStream(t, outputBytes.Bytes()).state
	queued := state.Inputs[1]
	if state.Cancellation == nil || state.Cancellation.Reason != ingestevent.CancellationSignal ||
		state.Finished == nil || state.Finished.Outcome != ingestevent.RunInterrupted ||
		queued == nil || queued.Started != nil || queued.Finished == nil ||
		queued.Finished.ErrorCode != ingest.FailureCodeInterrupted {
		t.Fatalf("interrupted state is incomplete: %#v", state)
	}
}

func TestIngestEventTerminalFreezeIgnoresLaterCancellation(t *testing.T) {
	t.Parallel()
	var outputBytes bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	output, err := newIngestEventOutput(&outputBytes, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	output.WatchCancellation(ctx)
	decision := output.FreezeTerminal(ctx)
	cancel()
	if decision.interrupted {
		t.Fatalf("live context froze as interrupted: %+v", decision)
	}
	if code, err := output.Finish(nil, ExitOK, nil, observability.Snapshot{}, ctx); err != nil || code != ExitOK {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}
	state := decodeCLIIngestEventStream(t, outputBytes.Bytes()).state
	if state.Cancellation != nil || state.Finished == nil || state.Finished.Outcome != ingestevent.RunSucceeded {
		t.Fatalf("later cancellation rewrote terminal state: %#v", state)
	}
}
