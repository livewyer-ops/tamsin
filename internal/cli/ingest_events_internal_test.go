package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/ingestevent"
	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/source"
)

type blockingEventWriter struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

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

func newBlockingEventWriter() *blockingEventWriter {
	return &blockingEventWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (writer *blockingEventWriter) Write(payload []byte) (int, error) {
	if writer.block.Load() {
		writer.once.Do(func() { close(writer.entered) })
		<-writer.release
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.Write(payload)
}

func (writer *blockingEventWriter) Bytes() []byte {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return bytes.Clone(writer.buffer.Bytes())
}

func closeOnce(channel chan struct{}) {
	select {
	case <-channel:
	default:
		close(channel)
	}
}

const (
	eventTestRunID    = "0a853551-fb19-40d6-8f17-15dd3562e6d4"
	eventTestFlowID   = "7b0d2aec-1868-56ad-879d-95e35ed75e4c"
	eventTestSourceID = "29b5961b-22de-4b61-b137-013b70d20b54"
	eventTestObjectA  = "19e919cf-183a-40bd-b9e5-8c8b361f6728"
	eventTestObjectB  = "28cc963c-51e5-4bb6-a4e4-8f9409b2dc59"
	eventTestDigest   = "ee79eb8b2ecda8115fe773d6469d7c26f99f35c9a555b3b4c72b05b8089ace5c"
)

func TestIngestEventOutputSeparatesProgressPhasesAndTerminalRecords(t *testing.T) {
	t.Parallel()
	var stream bytes.Buffer
	output, err := newIngestEventOutput(&stream, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
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

	// Hold the sampler clock still. Intermediate updates are coalesced, while
	// sealing and completion snapshots must still be emitted.
	instant := output.started.Add(time.Second)
	output.now = func() time.Time { return instant }
	scope := progress.Scope{InputIndex: 0, Input: "input.ts"}
	for _, snapshot := range []progress.Snapshot{
		{Scope: scope, Phase: progress.PhaseStore, Revision: 1},
		{Scope: scope, Phase: progress.PhaseVerify, Revision: 2}, // suppressed initial verify
		{Scope: scope, Phase: progress.PhaseStore, Revision: 3, TotalObjects: 2, TotalBytes: 20, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseStore, Revision: 4, CompletedObjects: 1, TotalObjects: 2, CompletedBytes: 10, TotalBytes: 20, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseVerify, Revision: 5, TotalObjects: 2, TotalBytes: 20, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseStore, Revision: 6, CompletedObjects: 2, TotalObjects: 2, CompletedBytes: 20, TotalBytes: 20, TotalsFinal: true},
		{Scope: scope, Phase: progress.PhaseVerify, Revision: 7, CompletedObjects: 2, TotalObjects: 2, CompletedBytes: 20, TotalBytes: 20, TotalsFinal: true},
	} {
		output.Report(snapshot)
	}
	output.Retry(observability.RetryEvent{
		Operation: observability.OperationObjectUpload, Attempt: 2, MaxAttempts: 3,
		StatusClass: "server_error", ErrorClass: "none", Backoff: 50 * time.Millisecond,
	})
	result := ingest.Result{
		Profile: ingest.ProfileEssenceSegments, ProfileVersion: "1",
		RootFlowID: eventTestFlowID, Bytes: 20, SHA256: eventTestDigest,
		Status: ingest.ResultStatusIngested, Verification: ingest.VerificationVerified,
		Flows: []ingest.FlowResult{{
			FlowID: eventTestFlowID, SourceID: eventTestSourceID, Disposition: ingest.FlowWritten,
			ObjectSummary: ingest.ObjectSummary{
				Total: 2, Bytes: 20, Ingested: 2, Verified: 2, ReadbackVerified: 2,
			},
			Objects: []ingest.ObjectResult{
				{ObjectID: eventTestObjectA, Timerange: "[0:0_1:0)", Bytes: 10, SHA256: eventTestDigest,
					Disposition: ingest.ObjectDispositionIngested, Verification: ingest.ObjectVerificationVerified,
					VerificationMethod: ingest.VerificationMethodReadback},
				{ObjectID: eventTestObjectB, Timerange: "[1:0_2:0)", Bytes: 10, SHA256: eventTestDigest,
					Disposition: ingest.ObjectDispositionIngested, Verification: ingest.ObjectVerificationVerified,
					VerificationMethod: ingest.VerificationMethodReadback},
			},
		}},
	}
	if err := output.Result(0, result); err != nil {
		t.Fatal(err)
	}
	metrics := observability.Snapshot{
		Elapsed: time.Second, BytesStaged: 20, BytesUploaded: 20, BytesVerified: 20, Retries: 1, Verified: 2,
	}
	if code, err := output.Finish(nil, nil, ExitOK, options, metrics, context.Background()); err != nil || code != ExitOK {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}

	state, err := ingestevent.ReduceWithOptions(bytes.NewReader(stream.Bytes()), ingestevent.ReducerOptions{RetainObjectResults: true})
	if err != nil {
		t.Fatal(err)
	}
	input := state.Inputs[0]
	if input == nil || input.Finished == nil || input.Progress[ingestevent.ProgressStore].CompletedObjects != 2 ||
		input.Progress[ingestevent.ProgressVerify].CompletedObjects != 2 {
		t.Fatalf("incomplete reduced input state: %#v", input)
	}
	if input.Progress[ingestevent.ProgressStore].Revision != 6 {
		t.Fatalf("intermediate store update was not coalesced: %#v", input.Progress[ingestevent.ProgressStore])
	}
	if state.RetryCount != 1 || state.Finished.Retries != 1 || state.Finished.ObjectsVerified != 2 {
		t.Fatalf("retry or terminal metrics were lost: %#v", state.Finished)
	}
	if strings.Contains(stream.String(), "secret") || strings.ContainsAny(stream.String(), "\r\x1b") {
		t.Fatalf("machine stream contains secret or terminal control bytes: %q", stream.String())
	}
}

// Exit codes are this layer's vocabulary and the failure codes are the ingest
// package's published contract, so the mapping between them is the seam where a
// silent renumbering would change what every consumer of the event stream and
// the journal reads for the same run.
func TestRunFailureMapsEveryExitCodeToItsPublishedFailure(t *testing.T) {
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

func TestPublicEventFailureCodesMatchRuntimeOutput(t *testing.T) {
	t.Parallel()

	interrupted, _, _ := runFailure(ExitInterrupted)
	if interrupted != ingestevent.InputErrorCodeRunInterrupted {
		t.Fatalf("interrupted runtime code = %q, public event code = %q",
			interrupted, ingestevent.InputErrorCodeRunInterrupted)
	}

	generic := ingest.Result{
		Status: ingest.ResultStatusFailed, Verification: ingest.VerificationNotRequested,
		Flows: []ingest.FlowResult{},
	}
	failed, _, _ := inputFailure(generic)
	if failed != ingestevent.InputErrorCodeIngestFailed {
		t.Fatalf("generic input runtime code = %q, public event code = %q",
			failed, ingestevent.InputErrorCodeIngestFailed)
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

func TestIngestEventProgressMailboxDoesNotBlockWorkersAndKeepsLatest(t *testing.T) {
	t.Parallel()
	writer := newBlockingEventWriter()
	output, err := newIngestEventOutput(writer, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	t.Cleanup(func() { closeOnce(writer.release) })
	if err := output.Start(&ingestFlagValues{profile: ingest.ProfileEssenceSegments, profileVersion: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := output.Declare([]source.Item{{URI: "file:///slow-consumer.ts"}}); err != nil {
		t.Fatal(err)
	}
	if err := output.InputStarted(0); err != nil {
		t.Fatal(err)
	}

	var tick atomic.Int64
	base := output.started.Add(time.Second)
	output.now = func() time.Time { return base.Add(time.Duration(tick.Add(1)) * time.Second) }
	writer.block.Store(true)
	scope := progress.Scope{InputIndex: 0, Input: "file:///slow-consumer.ts"}
	output.Report(progress.Snapshot{Scope: scope, Phase: progress.PhaseStore, Revision: 1})
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("progress flusher did not reach the deliberately slow writer")
	}

	reported := make(chan struct{})
	go func() {
		output.Report(progress.Snapshot{
			Scope: scope, Phase: progress.PhaseStore, Revision: 2,
			TotalObjects: 100, TotalBytes: 100, TotalsFinal: true,
		})
		for completed := 1; completed <= 100; completed++ {
			output.Report(progress.Snapshot{
				Scope: scope, Phase: progress.PhaseStore, Revision: uint64(completed + 2),
				CompletedObjects: completed, TotalObjects: 100,
				CompletedBytes: int64(completed), TotalBytes: 100, TotalsFinal: true,
			})
		}
		close(reported)
	}()
	select {
	case <-reported:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("progress reporting blocked behind a slow process consumer")
	}

	closeOnce(writer.release)
	writer.block.Store(false)
	result := ingest.Result{
		Input: "file:///slow-consumer.ts", Profile: ingest.ProfileEssenceSegments, ProfileVersion: "1",
		Status: ingest.ResultStatusIngested, Verification: ingest.VerificationNotRequested, Flows: []ingest.FlowResult{},
	}
	if err := output.Result(0, result); err != nil {
		t.Fatal(err)
	}
	// Reports admitted after the terminal boundary must not reappear after
	// input.finished or run.finished.
	output.Report(progress.Snapshot{
		Scope: scope, Phase: progress.PhaseStore, Revision: 103,
		CompletedObjects: 100, TotalObjects: 100, CompletedBytes: 100, TotalBytes: 100, TotalsFinal: true,
	})
	if code, err := output.Finish(nil, nil, ExitOK, nil, observability.Snapshot{}, context.Background()); err != nil || code != ExitOK {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}
	<-output.progressDone

	decoder := ingestevent.NewDecoder(bytes.NewReader(writer.Bytes()))
	var progressEvents []ingestevent.ProgressSnapshot
	var inputFinishedSeq uint64
	for {
		envelope, err := decoder.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		event, known, err := ingestevent.DecodeEvent(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if !known {
			continue
		}
		switch value := event.(type) {
		case ingestevent.ProgressSnapshot:
			if inputFinishedSeq > 0 {
				t.Fatalf("progress sequence %d followed terminal input sequence %d", envelope.Seq, inputFinishedSeq)
			}
			progressEvents = append(progressEvents, value)
		case ingestevent.InputFinished:
			inputFinishedSeq = envelope.Seq
		}
	}
	if len(progressEvents) != 2 {
		t.Fatalf("slow consumer received %d progress events, want first and latest: %#v", len(progressEvents), progressEvents)
	}
	latest := progressEvents[len(progressEvents)-1]
	if latest.Revision != 102 || latest.CompletedObjects != 100 || latest.CompletedBytes != 100 {
		t.Fatalf("coalesced progress is not the latest cumulative value: %#v", latest)
	}
	if _, err := ingestevent.Reduce(bytes.NewReader(writer.Bytes())); err != nil {
		t.Fatalf("coalesced stream does not reduce: %v", err)
	}
}

func TestIngestEventLifecycleWritesRemainAcknowledged(t *testing.T) {
	t.Parallel()
	writer := newBlockingEventWriter()
	output, err := newIngestEventOutput(writer, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	t.Cleanup(func() { closeOnce(writer.release) })
	if err := output.Declare([]source.Item{{URI: "file:///acknowledged.ts"}}); err != nil {
		t.Fatal(err)
	}
	if err := output.InputStarted(0); err != nil {
		t.Fatal(err)
	}
	writer.block.Store(true)
	returned := make(chan error, 1)
	go func() {
		returned <- output.FlowPlanned(0, ingest.FlowPlan{
			FlowID: eventTestFlowID, SourceID: eventTestSourceID, Kind: ingest.FlowKindCollection, Root: true,
		})
	}()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("lifecycle write did not reach the slow writer")
	}
	select {
	case err := <-returned:
		t.Fatalf("FlowPlanned returned before its line was written: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	closeOnce(writer.release)
	writer.block.Store(false)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	if err := output.Result(0, ingest.Result{
		Input: "file:///acknowledged.ts", Profile: ingest.ProfileEssenceSegments, ProfileVersion: "1",
		RootFlowID: eventTestFlowID, Status: ingest.ResultStatusIngested, Verification: ingest.VerificationNotRequested,
		Flows: []ingest.FlowResult{{FlowID: eventTestFlowID, SourceID: eventTestSourceID, Disposition: ingest.FlowWritten}},
	}); err != nil {
		t.Fatal(err)
	}
	if code, err := output.Finish(nil, nil, ExitOK, nil, observability.Snapshot{}, context.Background()); err != nil || code != ExitOK {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}
}

func TestIngestEventProgressWriterFailureCancelsAndStopsFlusher(t *testing.T) {
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
		t.Fatal("a broken progress pipe did not cancel in-scope ingest work")
	}
	if cause := context.Cause(runCtx); cause == nil || !strings.Contains(cause.Error(), "write ingest event stream") {
		t.Fatalf("cancellation cause = %v", cause)
	}
	select {
	case <-output.progressDone:
	case <-time.After(time.Second):
		t.Fatal("progress flusher did not terminate after its writer failed")
	}
	if err := output.Err(); err == nil || !strings.Contains(err.Error(), "closed pipe") {
		t.Fatalf("output error = %v", err)
	}
}

func TestIngestEventOutputInterruptionCompletesUndispatchedInputs(t *testing.T) {
	t.Parallel()
	var stream bytes.Buffer
	output, err := newIngestEventOutput(&stream, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
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
	if code, err := output.Finish(nil, context.Canceled, ExitInterrupted, options, observability.Snapshot{}, ctx); err != nil || code != ExitInterrupted {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}

	state, err := ingestevent.Reduce(bytes.NewReader(stream.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if state.Cancellation == nil || state.Cancellation.Reason != ingestevent.CancellationSignal ||
		state.Finished == nil || state.Finished.Outcome != ingestevent.RunInterrupted ||
		state.Finished.Succeeded != 1 || state.Finished.Failed != 1 {
		t.Fatalf("interrupted state is incomplete: %#v", state)
	}
	queued := state.Inputs[1]
	if queued == nil || queued.Started != nil || queued.Finished == nil ||
		queued.Finished.Status != ingestevent.InputFailed || queued.Finished.Verification != ingestevent.VerificationNotRequested ||
		queued.Finished.ErrorCode != ingest.FailureCodeInterrupted ||
		queued.Finished.Message != ingest.FailureMessageInputInterrupt {
		t.Fatalf("undispatched input did not get one terminal state: %#v", queued)
	}
}

func TestIngestEventOutputCompletesUndispatchedInputsInManifestOrder(t *testing.T) {
	t.Parallel()
	var stream bytes.Buffer
	output, err := newIngestEventOutput(&stream, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	const inputs = 64
	items := make([]source.Item, inputs)
	for index := range items {
		items[index].URI = fmt.Sprintf("file:///input-%03d.ts", index)
	}
	if err := output.Declare(items); err != nil {
		t.Fatal(err)
	}
	if code, err := output.Finish(nil, errors.New("source failed"), ExitSource, nil,
		observability.Snapshot{}, context.Background()); err != nil || code != ExitSource {
		t.Fatalf("Finish() = code %d, error %v", code, err)
	}
	output.Close()

	decoder := ingestevent.NewDecoder(bytes.NewReader(stream.Bytes()))
	nextInput := 0
	for {
		envelope, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if envelope.Type != ingestevent.TypeInputFinished {
			continue
		}
		if envelope.Scope == nil || envelope.Scope.InputIndex == nil || *envelope.Scope.InputIndex != nextInput {
			t.Fatalf("terminal input %d scope = %#v", nextInput, envelope.Scope)
		}
		nextInput++
	}
	if nextInput != inputs {
		t.Fatalf("terminal inputs = %d, want %d", nextInput, inputs)
	}
	state, err := ingestevent.Reduce(bytes.NewReader(stream.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	for index, input := range state.Inputs {
		if input.Finished.Profile != ingest.ProfileUnresolved || input.Finished.ProfileVersion != ingest.UnresolvedProfileVersion {
			t.Fatalf("input %d pre-resolution profile = %s@%s, want unresolved@0", index, input.Finished.Profile, input.Finished.ProfileVersion)
		}
	}
}

func TestIngestEventTerminalFreezeIgnoresLaterCancellation(t *testing.T) {
	t.Parallel()
	var stream bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	output, err := newIngestEventOutput(&stream, eventTestRunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(output.Close)
	output.WatchCancellation(ctx)

	decision := output.FreezeTerminal(ctx)
	if decision.interrupted {
		t.Fatalf("live context froze as interrupted: %+v", decision)
	}
	cancel()
	if code, err := output.Finish(nil, nil, ExitOK, nil, observability.Snapshot{}, ctx); err != nil || code != ExitOK {
		t.Fatalf("Finish() after post-freeze cancellation = code %d, error %v", code, err)
	}

	state, err := ingestevent.Reduce(bytes.NewReader(stream.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if state.Cancellation != nil || state.Finished == nil ||
		state.Finished.Outcome != ingestevent.RunSucceeded || state.Finished.ExitCode != ExitOK {
		t.Fatalf("post-freeze cancellation rewrote terminal state: %#v", state)
	}
}

func TestIngestEventCancellationRaceHasOneConsistentTerminalDecision(t *testing.T) {
	t.Parallel()
	for iteration := range 50 {
		var stream bytes.Buffer
		ctx, cancel := context.WithCancel(context.Background())
		output, err := newIngestEventOutput(&stream, eventTestRunID, nil)
		if err != nil {
			t.Fatal(err)
		}
		output.WatchCancellation(ctx)
		start := make(chan struct{})
		canceled := make(chan struct{})
		go func() {
			<-start
			cancel()
			close(canceled)
		}()
		close(start)
		decision := output.FreezeTerminal(ctx)
		<-canceled
		requestedCode := ExitOK
		if decision.interrupted {
			requestedCode = ExitInterrupted
		}
		code, err := output.Finish(nil, decision.cause, requestedCode, nil, observability.Snapshot{}, ctx)
		if err != nil {
			t.Fatalf("iteration %d Finish(): %v", iteration, err)
		}
		state, err := ingestevent.Reduce(bytes.NewReader(stream.Bytes()))
		if err != nil {
			t.Fatalf("iteration %d reduce: %v\n%s", iteration, err, stream.String())
		}
		interrupted := state.Cancellation != nil
		if interrupted != decision.interrupted || state.Finished == nil || state.Finished.ExitCode != code ||
			(code == ExitInterrupted) != decision.interrupted {
			t.Fatalf("iteration %d terminal decision diverged: decision=%+v code=%d state=%#v", iteration, decision, code, state)
		}
		output.Close()
	}
}
