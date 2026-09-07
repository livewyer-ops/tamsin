package ingestevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

const testRunID = "d1f5657c-cee0-5ac4-a0a5-c78ba122fa39"

type flushingBuffer struct {
	bytes.Buffer
	flushes int
}

func (b *flushingBuffer) Flush() error {
	b.flushes++
	return nil
}

func testEncoder(t *testing.T, sink io.Writer) *Encoder {
	t.Helper()
	now := time.Date(2026, time.August, 9, 8, 35, 46, 0, time.UTC)
	encoder, err := newEncoder(sink, testRunID, func() time.Time {
		now = now.Add(time.Millisecond)
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoder
}

func emitHello(t *testing.T, encoder *Encoder, limit uint64) {
	t.Helper()
	if _, err := encoder.Emit(nil, Hello{
		ToolVersion: "1.0.0", ToolCommit: "abc", ResultSchemaVersion: "2.1",
		ProfilePolicyVersion: "1", MaxEventBytes: limit, Capabilities: []string{},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEncoderWritesOrderedFlushableNDJSON(t *testing.T) {
	t.Parallel()
	sink := &flushingBuffer{}
	encoder := testEncoder(t, sink)
	emitHello(t, encoder, DefaultMaxEventBytes)
	if _, err := encoder.Emit(InputScope(0), InputDeclared{Input: "file:///input.ts"}); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Emit(nil, RunFinished{Outcome: RunSucceeded, ExitCode: 0}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	first, _, _ := bytes.Cut(sink.Bytes(), []byte("\n"))
	assertJSONEqual(t, first, `{"protocol":"tamsin.ingest.events","protocol_version":"2.1","type":"hello","seq":0,"run_id":"d1f5657c-cee0-5ac4-a0a5-c78ba122fa39","emitted_at":"2026-08-09T08:35:46.002Z","elapsed_ms":1,"payload":{"tool_version":"1.0.0","tool_commit":"abc","result_schema_version":"2.1","profile_policy_version":"1","max_event_bytes":1048576,"capabilities":[]}}`)
	if sink.flushes != 3 {
		t.Fatalf("flushes = %d, want 3", sink.flushes)
	}
	decoder := json.NewDecoder(bytes.NewReader(sink.Bytes()))
	for sequence := uint64(0); sequence < 3; sequence++ {
		var envelope Envelope
		if err := decoder.Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Seq != sequence || envelope.RunID != testRunID || envelope.Protocol != Protocol {
			t.Fatalf("envelope %d = %#v", sequence, envelope)
		}
	}
}

func TestEncoderEnforcesProcessBoundaries(t *testing.T) {
	t.Parallel()
	encoder := testEncoder(t, io.Discard)
	if _, err := encoder.Emit(nil, RunStarted{}); err == nil {
		t.Fatal("stream started without hello")
	}
	emitHello(t, encoder, DefaultMaxEventBytes)
	if _, err := encoder.Emit(nil, Hello{}); err == nil {
		t.Fatal("second hello succeeded")
	}
	if err := encoder.Finalize(); err == nil {
		t.Fatal("incomplete stream finalised")
	}

	encoder = testEncoder(t, io.Discard)
	emitHello(t, encoder, DefaultMaxEventBytes)
	if _, err := encoder.Emit(nil, RunFinished{Outcome: RunFailed, ExitCode: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Emit(nil, Diagnostic{}); err == nil {
		t.Fatal("event after run.finished succeeded")
	}
}

func TestEncoderRejectsOversizedRecordWithoutConsumingSequence(t *testing.T) {
	t.Parallel()
	var sink bytes.Buffer
	encoder := testEncoder(t, &sink)
	emitHello(t, encoder, MinimumMaxEventBytes)
	if _, err := encoder.Emit(InputScope(0), InputDeclared{Input: string(bytes.Repeat([]byte{'x'}, 1024))}); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized error = %v", err)
	}
	envelope, err := encoder.Emit(nil, RunFinished{Outcome: RunFailed, ExitCode: 1})
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Seq != 1 {
		t.Fatalf("sequence after rejection = %d", envelope.Seq)
	}
}

func TestEncoderSerialisesConcurrentPublishers(t *testing.T) {
	t.Parallel()
	var sink bytes.Buffer
	encoder := testEncoder(t, &sink)
	emitHello(t, encoder, DefaultMaxEventBytes)
	const workers = 32
	var group sync.WaitGroup
	for index := range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := encoder.Emit(InputScope(index), InputDeclared{Input: "input"}); err != nil {
				t.Errorf("Emit() error = %v", err)
			}
		}()
	}
	group.Wait()
	if _, err := encoder.Emit(nil, RunFinished{Outcome: RunSucceeded}); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(sink.Bytes()))
	for sequence := uint64(0); sequence < workers+2; sequence++ {
		var envelope Envelope
		if err := decoder.Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Seq != sequence {
			t.Fatalf("sequence = %d, want %d", envelope.Seq, sequence)
		}
	}
}

func TestDiagnosticIsBoundedAndValidated(t *testing.T) {
	t.Parallel()
	diagnostic, err := NewDiagnostic(SeverityError, "config.invalid", strings.Repeat("é", 3000), "fix it", true)
	if err != nil {
		t.Fatal(err)
	}
	if !diagnostic.Truncated || len(diagnostic.Message) > MaxDiagnosticMessageBytes || !utf8.ValidString(diagnostic.Message) {
		t.Fatalf("bounded diagnostic = %#v", diagnostic)
	}
	if _, err := NewDiagnostic("other", "bad code", "message", "", false); err == nil {
		t.Fatal("invalid diagnostic succeeded")
	}
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var values [2]any
	for index, data := range []string{string(got), want} {
		decoder := json.NewDecoder(strings.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&values[index]); err != nil {
			t.Fatalf("decode JSON: %v", err)
		}
	}
	if !reflect.DeepEqual(values[0], values[1]) {
		t.Fatalf("JSON = %s\nwant %s", got, want)
	}
}

func TestEventPayloadJSON(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, time.August, 9, 8, 35, 46, 0, time.UTC)
	for _, test := range []struct {
		name  string
		event Event
		want  string
	}{
		{"hello", Hello{ToolVersion: "1.0.0", ToolCommit: "abc", ToolBuildDate: "2026-08-09T08:00:00Z", ResultSchemaVersion: "2.1", ProfilePolicyVersion: "1", MaxEventBytes: DefaultMaxEventBytes, Capabilities: []string{}},
			`{"tool_version":"1.0.0","tool_commit":"abc","tool_build_date":"2026-08-09T08:00:00Z","result_schema_version":"2.1","profile_policy_version":"1","max_event_bytes":1048576,"capabilities":[]}`},
		{"run.started", RunStarted{StartedAt: started, Profile: "preserve", ProfileVersion: "1", DryRunMode: "exact", VerificationMode: "auto", Concurrency: KnownSetting(uint64(2)), Transfers: KnownSetting(uint64(4)), RequestedInputs: KnownInputCount(0)},
			`{"started_at":"2026-08-09T08:35:46Z","profile":"preserve","profile_version":"1","dry_run_mode":"exact","verification_mode":"auto","concurrency":2,"transfers":4,"requested_inputs":0}`},
		{"input.declared", InputDeclared{Input: "file:///input.ts"},
			`{"input":"file:///input.ts"}`},
		{"manifest.finished", ManifestFinished{},
			`{"total_inputs":0}`},
		{"input.started", InputStarted{StartedAt: started},
			`{"started_at":"2026-08-09T08:35:46Z"}`},
		{"flow.planned", FlowPlanned{FlowID: "flow", SourceID: "source", Kind: FlowKindCollection, Root: true, Format: "urn:x-nmos:format:multi"},
			`{"flow_id":"flow","source_id":"source","kind":"collection","root":true,"format":"urn:x-nmos:format:multi"}`},
		{"progress.snapshot", ProgressSnapshot{Revision: 1, Phase: ProgressStore},
			`{"revision":1,"phase":"store","totals_final":false,"completed_objects":0,"total_objects":0,"completed_bytes":0,"total_bytes":0,"elapsed_ms":0}`},
		{"retry.scheduled", RetryScheduled{Operation: "object_upload", Attempt: 2, MaxAttempts: 3, DelayMS: 100, StatusClass: "server_error", ErrorClass: "none"},
			`{"operation":"object_upload","attempt":2,"max_attempts":3,"delay_ms":100,"status_class":"server_error","error_class":"none"}`},
		{"diagnostic", Diagnostic{Severity: SeverityError, Code: DiagnosticCodeConfigInvalid, Message: "Configuration is invalid.", Hint: "Correct it."},
			`{"severity":"error","code":"config.invalid","message":"Configuration is invalid.","hint":"Correct it.","action_required":false,"truncated":false}`},
		{"object.result", ObjectResult{ObjectID: "object", Timerange: "[0:0_1:0)", Bytes: 9007199254740993, SHA256: "digest", Disposition: ObjectDispositionIngested, Verification: ObjectVerificationVerified, VerificationMethod: VerificationMethodReadback},
			`{"object_id":"object","timerange":"[0:0_1:0)","bytes":9007199254740993,"sha256":"digest","disposition":"ingested","verification_status":"verified","verification_method":"readback"}`},
		{"flow.result", FlowResult{FlowID: "flow", SourceID: "source", Kind: FlowKindEssence, Role: "video", TAMSFlowProfileID: "profile", Disposition: FlowWritten},
			`{"flow_id":"flow","source_id":"source","kind":"essence","role":"video","tams_flow_profile_id":"profile","disposition":"written","object_summary":{"total":0,"bytes":0,"ingested":0,"resumed":0,"rejected":0,"retracted":0,"stranded":0,"unattempted":0,"verified":0,"storage_verified":0,"readback_verified":0}}`},
		{"input.finished", InputFinished{Input: "file:///input.ts", Profile: "preserve", ProfileVersion: "1", Status: InputFailed, Verification: VerificationNotReached, ErrorCode: InputErrorCodeRunInterrupted, Message: "Interrupted before dispatch."},
			`{"input":"file:///input.ts","profile":"preserve","profile_version":"1","status":"failed","verification":"not_reached","flow_count":0,"object_count":0,"error_code":"run.interrupted","message":"Interrupted before dispatch."}`},
		{"run.cancellation_requested", RunCancellationRequested{Reason: CancellationSignal},
			`{"reason":"signal"}`},
		{"run.finished", RunFinished{Outcome: RunPartial, ExitCode: 4, Total: 2, Succeeded: 1, Failed: 1},
			`{"outcome":"partial","exit_code":4,"total":2,"succeeded":1,"failed":1,"elapsed_ms":0,"bytes_staged":0,"bytes_uploaded":0,"bytes_verified":0,"retries":0,"objects_verified":0,"objects_retracted":0,"objects_stranded":0}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if string(test.event.EventType()) != test.name {
				t.Fatalf("event type = %q, want %q", test.event.EventType(), test.name)
			}
			got, err := json.Marshal(test.event)
			if err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, got, test.want)
		})
	}
}

func TestEventScopeAndOptionalSettingsJSON(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value any
		want  string
	}{
		{(*Scope)(nil), `null`},
		{InputScope(0), `{"input_index":0}`},
		{FlowScope(0, "flow"), `{"input_index":0,"flow_id":"flow"}`},
		{ObjectScope(0, "flow", "object"), `{"input_index":0,"flow_id":"flow","object_id":"object"}`},
		{RunStarted{}, `{"started_at":"0001-01-01T00:00:00Z"}`},
		{InputFinished{Input: "file:///input.ts", Profile: "demux", ProfileVersion: "1", FFmpegVersion: "ffmpeg version 5.1", MediaToolchain: "toolchain", RootFlowID: "flow", Bytes: 42, SHA256: "digest", Status: InputIngested, Verification: VerificationVerified, FlowCount: 3, ObjectCount: 2},
			`{"input":"file:///input.ts","profile":"demux","profile_version":"1","ffmpeg_version":"ffmpeg version 5.1","media_toolchain":"toolchain","root_flow_id":"flow","bytes":42,"sha256":"digest","status":"ingested","verification":"verified","flow_count":3,"object_count":2}`},
		{FlowPlanned{FlowID: "flow", SourceID: "source", Kind: FlowKindEssence, Role: "video", ParentFlowID: "parent", Container: "video/mp2t", TAMSFlowProfileID: "profile"},
			`{"flow_id":"flow","source_id":"source","kind":"essence","role":"video","root":false,"parent_flow_id":"parent","container":"video/mp2t","tams_flow_profile_id":"profile"}`},
	} {
		got, err := json.Marshal(test.value)
		if err != nil {
			t.Fatal(err)
		}
		assertJSONEqual(t, got, test.want)
	}
}
