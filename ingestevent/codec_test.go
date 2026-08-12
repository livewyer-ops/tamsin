package ingestevent

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	testRunID    = "0a853551-fb19-40d6-8f17-15dd3562e6d4"
	testFlowID   = "7b0d2aec-1868-56ad-879d-95e35ed75e4c"
	testSourceID = "29b5961b-22de-4b61-b137-013b70d20b54"
	testObjectID = "19e919cf-183a-40bd-b9e5-8c8b361f6728"
	testObjectB  = "28cc963c-51e5-4bb6-a4e4-8f9409b2dc59"
	testDigest   = "ee79eb8b2ecda8115fe773d6469d7c26f99f35c9a555b3b4c72b05b8089ace5c"
)

var testStartedAt = time.Date(2026, time.August, 9, 8, 35, 46, 0, time.UTC)

type flushingBuffer struct {
	bytes.Buffer
	flushes int
}

func (b *flushingBuffer) Flush() error {
	b.flushes++
	return nil
}

type futureEvent struct {
	Blob string `json:"blob"`
}

func (futureEvent) EventType() Type { return "ui.hint" }

func testHello(maxBytes uint64) Hello {
	return Hello{
		ToolVersion: "v1.0.0", ToolCommit: "abc123", ResultSchemaVersion: "2.1",
		ProfilePolicyVersion: "1", MaxEventBytes: maxBytes,
		Capabilities: []string{"progress", "terminal_results", "graceful_cancel"},
	}
}

func deterministicEncoder(t *testing.T, sink io.Writer) *Encoder {
	t.Helper()
	step := 0
	clock := func() time.Time {
		value := testStartedAt.Add(time.Duration(step) * 10 * time.Millisecond)
		step++
		return value
	}
	encoder, err := newEncoder(sink, testRunID, clock)
	if err != nil {
		t.Fatal(err)
	}
	return encoder
}

func mustEmit(t *testing.T, encoder *Encoder, scope *Scope, event Event) Envelope {
	t.Helper()
	envelope, err := encoder.Emit(scope, event)
	if err != nil {
		t.Fatalf("Emit(%s): %v", event.EventType(), err)
	}
	return envelope
}

func TestEncoderPublishesFlushableReplayableProcessStream(t *testing.T) {
	t.Parallel()
	sink := &flushingBuffer{}
	encoder := deterministicEncoder(t, sink)
	concurrency, transfers := uint64(2), uint64(8)
	events := []struct {
		scope *Scope
		event Event
	}{
		{event: testHello(DefaultMaxEventBytes)},
		{event: RunStarted{
			StartedAt: testStartedAt, Profile: "essence-segments", ProfileVersion: "1",
			DryRunMode: "off", VerificationMode: "readback", Concurrency: &concurrency, Transfers: &transfers,
			RequestedInputs: KnownInputCount(1),
		}},
		{scope: InputScope(0), event: InputDeclared{Input: "file:///tmp/first-ingest.ts"}},
		{event: ManifestFinished{TotalInputs: 1}},
		{scope: InputScope(0), event: InputStarted{StartedAt: testStartedAt.Add(time.Second)}},
		{scope: FlowScope(0, testFlowID), event: FlowPlanned{
			FlowID: testFlowID, SourceID: testSourceID, Kind: FlowKindMuxed, Root: true,
			Format: "urn:x-nmos:format:multi", Container: "video/mp2t",
		}},
		{scope: InputScope(0), event: ProgressSnapshot{
			Revision: 1, Phase: ProgressStore, CompletedObjects: 0, TotalObjects: 1,
			CompletedBytes: 0, TotalBytes: 1514152, ElapsedMS: 100,
		}},
		{scope: InputScope(0), event: ProgressSnapshot{
			Revision: 2, Phase: ProgressStore, TotalsFinal: true, CompletedObjects: 1, TotalObjects: 1,
			CompletedBytes: 1514152, TotalBytes: 1514152, ElapsedMS: 200,
		}},
		{scope: InputScope(0), event: ProgressSnapshot{
			Revision: 3, Phase: ProgressVerify, TotalsFinal: true, CompletedObjects: 1, TotalObjects: 1,
			CompletedBytes: 1514152, TotalBytes: 1514152, ElapsedMS: 250,
		}},
		{scope: InputScope(0), event: RetryScheduled{
			Operation: "object_upload", Attempt: 2, MaxAttempts: 3, DelayMS: 50,
			StatusClass: "server_error", ErrorClass: "none",
		}},
		{scope: InputScope(0), event: Diagnostic{
			Severity: SeverityInfo, Code: "transfer.recovered", Message: "Transfer recovered after retry.",
			ActionRequired: false, Truncated: false,
		}},
		{scope: ObjectScope(0, testFlowID, testObjectID), event: ObjectResult{
			ObjectID: testObjectID, Timerange: "0:0_1:0", Bytes: 1514152, SHA256: testDigest,
			Disposition: ObjectDispositionIngested, Verification: ObjectVerificationVerified,
			VerificationMethod: VerificationMethodReadback,
		}},
		{scope: FlowScope(0, testFlowID), event: FlowResult{
			FlowID: testFlowID, SourceID: testSourceID, Kind: FlowKindMuxed,
			Disposition: FlowWritten, ObjectSummary: ObjectSummary{
				Total: 1, Bytes: 1514152, Ingested: 1, Verified: 1, ReadbackVerified: 1,
			},
		}},
		{scope: InputScope(0), event: InputFinished{
			Input: "file:///tmp/first-ingest.ts", Profile: "essence-segments", ProfileVersion: "1",
			RootFlowID: testFlowID, Bytes: 1419776, SHA256: testDigest,
			Status: InputIngested, Verification: VerificationVerified, FlowCount: 1, ObjectCount: 1,
		}},
		{event: RunFinished{
			Outcome: RunSucceeded, ExitCode: 0, Total: 1, Succeeded: 1, ElapsedMS: 2783,
			BytesStaged: 1419776, BytesUploaded: 1514152, BytesVerified: 1514152,
			Retries: 1, ObjectsVerified: 1,
		}},
	}
	for index, item := range events {
		envelope := mustEmit(t, encoder, item.scope, item.event)
		if envelope.Protocol != Protocol || envelope.ProtocolVersion != ProtocolVersion || envelope.RunID != testRunID {
			t.Fatalf("envelope %d lost protocol identity: %#v", index, envelope)
		}
		if envelope.Seq != uint64(index) || envelope.EmittedAt.IsZero() || envelope.ElapsedMS != uint64(index+1)*10 {
			t.Fatalf("envelope %d sequence/time = %d/%s/%d", index, envelope.Seq, envelope.EmittedAt, envelope.ElapsedMS)
		}
	}
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	if sink.flushes != len(events) {
		t.Fatalf("flushes = %d, want exactly one per event", sink.flushes)
	}
	if lines := strings.Count(sink.String(), "\n"); lines != len(events) {
		t.Fatalf("NDJSON lines = %d, want %d", lines, len(events))
	}

	state, err := ReduceWithOptions(strings.NewReader(sink.String()), ReducerOptions{RetainObjectResults: true})
	if err != nil {
		t.Fatal(err)
	}
	if state.Finished == nil || state.Finished.Outcome != RunSucceeded || state.NextSequence != uint64(len(events)) {
		t.Fatalf("unexpected reduced run state: %#v", state)
	}
	input := state.Inputs[0]
	if input == nil || input.Finished == nil || len(input.FlowResults) != 1 || len(input.ObjectResults) != 1 ||
		input.Progress[ProgressStore].CompletedObjects != 1 || input.RetryCount != 1 {
		t.Fatalf("unexpected reduced input state: %#v", input)
	}
}

func TestReducerObjectRetentionIsExplicitAndDefaultStateIsBounded(t *testing.T) {
	t.Parallel()
	const objectCount = 1000
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{
		StartedAt: testStartedAt, Profile: "essence-segments", ProfileVersion: "1",
		DryRunMode: "off", VerificationMode: "auto",
	})
	mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///long-programme.ts"})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
	mustEmit(t, encoder, InputScope(0), InputStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, FlowScope(0, testFlowID), FlowPlanned{
		FlowID: testFlowID, SourceID: testSourceID, Kind: FlowKindMuxed, Root: true,
	})
	for index := range objectCount {
		objectID := fmt.Sprintf("00000000-0000-4000-8000-%012x", index)
		mustEmit(t, encoder, ObjectScope(0, testFlowID, objectID), ObjectResult{
			ObjectID: objectID, Timerange: fmt.Sprintf("%d:0_%d:0", index, index+1), Bytes: 1, SHA256: testDigest,
			Disposition: ObjectDispositionIngested, Verification: ObjectVerificationVerified,
			VerificationMethod: VerificationMethodStorage,
		})
	}
	summary := ObjectSummary{
		Total: objectCount, Bytes: objectCount, Ingested: objectCount,
		Verified: objectCount, StorageVerified: objectCount,
	}
	mustEmit(t, encoder, FlowScope(0, testFlowID), FlowResult{
		FlowID: testFlowID, SourceID: testSourceID, Kind: FlowKindMuxed,
		Disposition: FlowWritten, ObjectSummary: summary,
	})
	mustEmit(t, encoder, InputScope(0), InputFinished{
		Input: "file:///long-programme.ts", Profile: "essence-segments", ProfileVersion: "1",
		RootFlowID: testFlowID, Status: InputIngested, Verification: VerificationVerified,
		FlowCount: 1, ObjectCount: objectCount,
	})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunSucceeded, Total: 1, Succeeded: 1})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}

	bounded, err := Reduce(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if bounded.Inputs[0].ObjectResults != nil || bounded.Inputs[0].ObjectSummaries[testFlowID] != summary {
		t.Fatalf("default reducer retained per-Object state: %#v", bounded.Inputs[0])
	}

	observed := 0
	retained, err := ReduceWithOptions(bytes.NewReader(sink.Bytes()), ReducerOptions{
		RetainObjectResults: true,
		ObjectObserver: func(scope Scope, result ObjectResult) error {
			if scope.FlowID != testFlowID || result.ObjectID == "" {
				t.Fatalf("unexpected observed Object: scope=%#v result=%#v", scope, result)
			}
			observed++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed != objectCount || len(retained.Inputs[0].ObjectResults) != objectCount {
		t.Fatalf("opt-in Object projection observed=%d retained=%d, want %d", observed, len(retained.Inputs[0].ObjectResults), objectCount)
	}
}

func TestStartupFailureDoesNotRequireResolvedIngestContract(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	diagnostic, err := NewDiagnostic(SeverityError, DiagnosticCodeConfigInvalid, "Configuration is invalid.", "Correct the named setting.", true)
	if err != nil {
		t.Fatal(err)
	}
	mustEmit(t, encoder, nil, diagnostic)
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 0})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunFailed, ExitCode: 2})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	state, err := Reduce(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if state.Started.Profile != "" || state.Manifest.TotalInputs != 0 || state.Finished.Outcome != RunFailed || len(state.Diagnostics) != 1 {
		t.Fatalf("startup failure was not preserved: %#v", state)
	}
}

func TestTerminalRecordsDoNotRequireFlowPlanned(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///muxed.ts"})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
	mustEmit(t, encoder, InputScope(0), InputStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, ObjectScope(0, testFlowID, testObjectID), ObjectResult{
		ObjectID: testObjectID, Timerange: "0:0_1:0", Bytes: 10, SHA256: testDigest,
		Disposition: ObjectDispositionIngested, Verification: ObjectVerificationNotRequested,
		VerificationMethod: VerificationMethodNone,
	})
	mustEmit(t, encoder, FlowScope(0, testFlowID), FlowResult{
		FlowID: testFlowID, SourceID: testSourceID, Kind: FlowKindMuxed, Disposition: FlowWritten,
		ObjectSummary: ObjectSummary{Total: 1, Bytes: 10, Ingested: 1},
	})
	mustEmit(t, encoder, InputScope(0), InputFinished{
		Input: "file:///muxed.ts", Profile: "preserve", ProfileVersion: "1", RootFlowID: testFlowID,
		SHA256: testDigest, Status: InputIngested, Verification: VerificationNotRequested, FlowCount: 1, ObjectCount: 1,
	})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunSucceeded, Total: 1, Succeeded: 1})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
}

func TestObjectObserverCanInspectCommittedReducerState(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///muxed.ts"})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
	mustEmit(t, encoder, InputScope(0), InputStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, ObjectScope(0, testFlowID, testObjectID), ObjectResult{
		ObjectID: testObjectID, Timerange: "0:0_1:0", Bytes: 10, SHA256: testDigest,
		Disposition: ObjectDispositionIngested, Verification: ObjectVerificationNotRequested,
		VerificationMethod: VerificationMethodNone,
	})

	var reducer *Reducer
	observed := false
	reducer = NewReducerWithOptions(ReducerOptions{ObjectObserver: func(scope Scope, result ObjectResult) error {
		state := reducer.Snapshot()
		if scope.FlowID != testFlowID || result.ObjectID != testObjectID ||
			state.Inputs[0].ObjectSummaries[testFlowID].Total != 1 {
			return errors.New("observer did not see its committed Object")
		}
		observed = true
		return nil
	}})
	done := make(chan error, 1)
	go func() {
		decoder := NewDecoder(bytes.NewReader(sink.Bytes()))
		for {
			envelope, err := decoder.Decode()
			if errors.Is(err, io.EOF) {
				done <- nil
				return
			}
			if err != nil {
				done <- err
				return
			}
			if err := reducer.Apply(envelope); err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Object observer deadlocked while inspecting reducer state")
	}
	if !observed {
		t.Fatal("Object observer was not called")
	}
}

func TestReducerRejectsObjectSummaryOverflowWithoutConsumingSequence(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///muxed.ts"})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
	mustEmit(t, encoder, InputScope(0), InputStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, ObjectScope(0, testFlowID, testObjectID), ObjectResult{
		ObjectID: testObjectID, Timerange: "0:0_1:0", Bytes: ^uint64(0), SHA256: testDigest,
		Disposition: ObjectDispositionIngested, Verification: ObjectVerificationNotRequested,
		VerificationMethod: VerificationMethodNone,
	})
	if _, err := encoder.Emit(ObjectScope(0, testFlowID, testObjectB), ObjectResult{
		ObjectID: testObjectB, Timerange: "1:0_2:0", Bytes: 1, SHA256: testDigest,
		Disposition: ObjectDispositionIngested, Verification: ObjectVerificationNotRequested,
		VerificationMethod: VerificationMethodNone,
	}); err == nil || !strings.Contains(err.Error(), "bytes overflow") {
		t.Fatalf("overflowing Object summary error = %v", err)
	}
	// A semantic rejection must leave both the reducer and sequence usable.
	mustEmit(t, encoder, FlowScope(0, testFlowID), FlowResult{
		FlowID: testFlowID, SourceID: testSourceID, Kind: FlowKindMuxed, Disposition: FlowWritten,
		ObjectSummary: ObjectSummary{Total: 1, Bytes: ^uint64(0), Ingested: 1},
	})
	if _, err := encoder.Emit(FlowScope(0, testSourceID), FlowResult{
		FlowID: testSourceID, SourceID: testSourceID, Kind: FlowKindMuxed, Disposition: FlowWritten,
		ObjectSummary: ObjectSummary{Total: 0, Ingested: ^uint64(0), Resumed: 1},
	}); err == nil || !strings.Contains(err.Error(), "counters overflow") {
		t.Fatalf("overflowing terminal summary error = %v", err)
	}
}

func TestUndispatchedInputCanFinishDuringGracefulCancellation(t *testing.T) {
	t.Parallel()
	for _, verification := range []VerificationStatus{VerificationNotReached, VerificationNotRequested} {
		t.Run(string(verification), func(t *testing.T) {
			sink := &bytes.Buffer{}
			encoder := deterministicEncoder(t, sink)
			mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
			mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
			mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///queued.ts"})
			mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
			mustEmit(t, encoder, nil, RunCancellationRequested{Reason: CancellationSignal})
			mustEmit(t, encoder, InputScope(0), InputFinished{
				Input: "file:///queued.ts", Profile: "essence-segments", ProfileVersion: "1", Status: InputFailed,
				Verification: verification, ErrorCode: InputErrorCodeRunInterrupted, Message: "Interrupted before dispatch.",
			})
			mustEmit(t, encoder, nil, RunFinished{Outcome: RunInterrupted, ExitCode: 8, Total: 1, Failed: 1})
			if err := encoder.Finalize(); err != nil {
				t.Fatal(err)
			}
			state, err := Reduce(bytes.NewReader(sink.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if state.Inputs[0].Started != nil || state.Inputs[0].Finished == nil {
				t.Fatalf("undispatched input state = %#v", state.Inputs[0])
			}
		})
	}
}

func TestCumulativeProgressCannotDecreaseOrReopenFinalTotals(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///input.ts"})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
	mustEmit(t, encoder, InputScope(0), InputStarted{StartedAt: testStartedAt})
	first := ProgressSnapshot{
		Revision: 1, Phase: ProgressStore, TotalsFinal: true, CompletedObjects: 1, TotalObjects: 2,
		CompletedBytes: 100, TotalBytes: 200, ElapsedMS: 10,
	}
	firstEnvelope := mustEmit(t, encoder, InputScope(0), first)
	bad := first
	bad.Revision = 2
	bad.CompletedBytes = 99
	if _, err := encoder.Emit(InputScope(0), bad); err == nil || !strings.Contains(err.Error(), "cannot decrease") {
		t.Fatalf("decreasing snapshot error = %v", err)
	}
	bad = first
	bad.Revision = 2
	bad.TotalsFinal = false
	if _, err := encoder.Emit(InputScope(0), bad); err == nil || !strings.Contains(err.Error(), "cannot reopen") {
		t.Fatalf("reopened totals error = %v", err)
	}
	good := first
	good.Revision = 2
	good.CompletedObjects, good.CompletedBytes, good.ElapsedMS = 2, 200, 20
	if envelope := mustEmit(t, encoder, InputScope(0), good); envelope.Seq != firstEnvelope.Seq+1 {
		t.Fatalf("invalid snapshots consumed sequence: got %d after %d", envelope.Seq, firstEnvelope.Seq)
	}
	mustEmit(t, encoder, InputScope(0), InputFinished{
		Input: "file:///input.ts", Profile: "essence-segments", ProfileVersion: "1", Status: InputFailed,
		Verification: VerificationNotReached, ErrorCode: InputErrorCodeIngestFailed, Message: "Ingest failed.",
	})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunFailed, ExitCode: 1, Total: 1, Failed: 1})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownMinorEventAndLargeRecordAreForwardCompatible(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, nil, futureEvent{Blob: strings.Repeat("x", 70*1024)})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 0})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunSucceeded})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	state, err := Reduce(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if state.UnknownEventCount != 1 {
		t.Fatalf("unknown event count = %d", state.UnknownEventCount)
	}
}

func TestDiagnosticConstructorBoundsUTF8AndMarksTruncation(t *testing.T) {
	t.Parallel()
	diagnostic, err := NewDiagnostic(
		SeverityError, DiagnosticCodeObjectStranded, strings.Repeat("界", MaxDiagnosticMessageBytes),
		"Inspect the run journal.", true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !diagnostic.Truncated || !diagnostic.ActionRequired || len(diagnostic.Message) > MaxDiagnosticMessageBytes || !utf8.ValidString(diagnostic.Message) {
		t.Fatalf("unbounded or invalid diagnostic: %#v", diagnostic)
	}
	if _, err := NewDiagnostic(Severity("fatal"), DiagnosticCodeObjectStranded, "message", "", true); err == nil {
		t.Fatal("constructor accepted unknown severity")
	}
	if _, err := NewDiagnostic(SeverityError, "Not Stable", "message", "", true); err == nil {
		t.Fatal("constructor accepted an unstable code")
	}
}

func TestAdvertisedMaximumRejectsOversizedRecordWithoutConsumingSequence(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(1024))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	if _, err := encoder.Emit(InputScope(0), InputDeclared{Input: strings.Repeat("x", 2048)}); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized event error = %v", err)
	}
	diagnostic, err := NewDiagnostic(SeverityError, "input.too_large", "Input declaration exceeds the event limit.", "Use a shorter locator.", true)
	if err != nil {
		t.Fatal(err)
	}
	mustEmit(t, encoder, nil, diagnostic)
	manifest := mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 0})
	if manifest.Seq != 3 {
		t.Fatalf("oversized event consumed sequence: manifest seq = %d", manifest.Seq)
	}
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunFailed, ExitCode: 1})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
}

func TestDecoderEnforcesHelloAdvertisedMaximumWithoutScannerLimit(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(1024))
	stream := append([]byte(nil), sink.Bytes()...)
	stream = append(stream, []byte(`{"protocol":"tamsin.ingest.events","payload":{"blob":"`)...)
	stream = append(stream, strings.Repeat("x", 2048)...)
	stream = append(stream, []byte(`"}}`+"\n")...)
	decoder := NewDecoder(bytes.NewReader(stream))
	if _, err := decoder.Decode(); err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	if _, err := decoder.Decode(); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized decode error = %v", err)
	}
}

func TestRunLevelFailureCanFollowSuccessfulInputs(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, InputScope(0), InputDeclared{Input: "file:///dry-run.ts"})
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 1})
	mustEmit(t, encoder, InputScope(0), InputStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, InputScope(0), InputFinished{
		Input: "file:///dry-run.ts", Profile: "essence-segments", ProfileVersion: "1",
		Status: InputPlanned, Verification: VerificationNotRequested,
	})
	diagnostic, err := NewDiagnostic(
		SeverityError, "journal.close_failed", "The result journal could not be finalized.", "Inspect storage health.", true,
	)
	if err != nil {
		t.Fatal(err)
	}
	mustEmit(t, encoder, nil, diagnostic)
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunFailed, ExitCode: 1, Total: 1, Succeeded: 1})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	state, err := Reduce(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if state.Finished.Outcome != RunFailed || len(state.Diagnostics) != 1 {
		t.Fatalf("run-level failure was lost: %#v", state)
	}
}

func TestEncoderSerializesConcurrentPublishers(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	const count = 50
	var wg sync.WaitGroup
	errorsSeen := make(chan error, count)
	for index := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := encoder.Emit(nil, Diagnostic{
				Severity: SeverityInfo, Code: "worker.ready", Message: fmt.Sprintf("Worker %d is ready.", index),
			})
			errorsSeen <- err
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	mustEmit(t, encoder, nil, ManifestFinished{TotalInputs: 0})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunSucceeded})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	state, err := Reduce(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != count+4 {
		t.Fatalf("next sequence = %d, want %d", state.NextSequence, count+4)
	}
}

func TestRunFinishedIsExactLastAndMissingTerminalIsNotGraceful(t *testing.T) {
	t.Parallel()
	sink := &bytes.Buffer{}
	encoder := deterministicEncoder(t, sink)
	mustEmit(t, encoder, nil, testHello(DefaultMaxEventBytes))
	mustEmit(t, encoder, nil, RunStarted{StartedAt: testStartedAt})
	mustEmit(t, encoder, nil, ManifestFinished{})
	mustEmit(t, encoder, nil, RunFinished{Outcome: RunSucceeded})
	if _, err := encoder.Emit(nil, Diagnostic{Severity: SeverityInfo, Code: "too.late", Message: "Too late."}); !errors.Is(err, ErrAfterRunFinished) {
		t.Fatalf("post-terminal error = %v", err)
	}
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}

	incomplete := deterministicEncoder(t, &bytes.Buffer{})
	mustEmit(t, incomplete, nil, testHello(DefaultMaxEventBytes))
	if err := incomplete.Finalize(); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("incomplete finalize error = %v", err)
	}
	if err := incomplete.Finalize(); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("repeated incomplete finalize error = %v", err)
	}
	data := strings.Split(strings.TrimSpace(sink.String()), "\n")
	if len(data) < 2 {
		t.Fatal("expected complete test stream")
	}
	if _, err := Reduce(strings.NewReader(strings.Join(data[:len(data)-1], "\n"))); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("truncated reduction error = %v", err)
	}
}
