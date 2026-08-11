package contracts_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/ingestevent"
)

const (
	eventRunID    = "0a853551-fb19-40d6-8f17-15dd3562e6d4"
	eventFlowID   = "7b0d2aec-1868-56ad-879d-95e35ed75e4c"
	eventSourceID = "29b5961b-22de-4b61-b137-013b70d20b54"
	eventObjectID = "19e919cf-183a-40bd-b9e5-8c8b361f6728"
	eventSHA256   = "ee79eb8b2ecda8115fe773d6469d7c26f99f35c9a555b3b4c72b05b8089ace5c"
)

func emitContractEvent(t *testing.T, encoder *ingestevent.Encoder, scope *ingestevent.Scope, event ingestevent.Event) {
	t.Helper()
	if _, err := encoder.Emit(scope, event); err != nil {
		t.Fatalf("emit %s: %v", event.EventType(), err)
	}
}

func validateEventStream(t *testing.T, schema interface{ Validate(any) error }, stream []byte) {
	t.Helper()
	for index, line := range strings.Split(strings.TrimSpace(string(stream)), "\n") {
		var record any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode event line %d: %v", index, err)
		}
		if err := schema.Validate(record); err != nil {
			t.Fatalf("event line %d does not satisfy published schema: %v\n%s", index, err, line)
		}
	}
}

func TestPublishedIngestEventSchemaAcceptsEveryRuntimePayload(t *testing.T) {
	t.Parallel()
	schema := compileTamsinSchema(t, "ingest-events-v2.json")
	sink := &bytes.Buffer{}
	encoder, err := ingestevent.NewEncoder(sink, eventRunID)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, time.August, 9, 8, 35, 46, 0, time.UTC)
	concurrency, transfers := uint64(2), uint64(8)
	emitContractEvent(t, encoder, nil, ingestevent.Hello{
		ToolVersion: "v1.0.0", ToolCommit: "abc123", ToolBuildDate: "2026-08-09T08:00:00Z",
		ResultSchemaVersion: "2.0", ProfilePolicyVersion: "1", MaxEventBytes: ingestevent.DefaultMaxEventBytes,
		Capabilities: []string{"progress", "terminal_results", "graceful_cancel"},
	})
	emitContractEvent(t, encoder, nil, ingestevent.RunStarted{
		StartedAt: started, Profile: "essence-segments", ProfileVersion: "1", DryRunMode: "off", VerificationMode: "readback",
		Concurrency: &concurrency, Transfers: &transfers, RequestedInputs: ingestevent.KnownInputCount(1),
	})
	emitContractEvent(t, encoder, ingestevent.InputScope(0), ingestevent.InputDeclared{Input: "file:///tmp/first-ingest.ts"})
	emitContractEvent(t, encoder, nil, ingestevent.ManifestFinished{TotalInputs: 1})
	emitContractEvent(t, encoder, ingestevent.InputScope(0), ingestevent.InputStarted{StartedAt: started})
	emitContractEvent(t, encoder, ingestevent.FlowScope(0, eventFlowID), ingestevent.FlowPlanned{
		FlowID: eventFlowID, SourceID: eventSourceID, Kind: ingestevent.FlowKindMuxed, Root: true,
		Format: "urn:x-nmos:format:multi", Container: "video/mp2t",
	})
	emitContractEvent(t, encoder, ingestevent.InputScope(0), ingestevent.ProgressSnapshot{
		Revision: 1, Phase: ingestevent.ProgressStore, TotalsFinal: true, CompletedObjects: 1, TotalObjects: 1,
		CompletedBytes: 1514152, TotalBytes: 1514152, ElapsedMS: 1200,
	})
	emitContractEvent(t, encoder, ingestevent.InputScope(0), ingestevent.RetryScheduled{
		Operation: "object_upload", Attempt: 2, MaxAttempts: 3, DelayMS: 100,
		StatusClass: "server_error", ErrorClass: "none",
	})
	diagnostic, err := ingestevent.NewDiagnostic(
		ingestevent.SeverityWarning, "transfer.recovered", "Transfer recovered after retry.", "No action is required.", false,
	)
	if err != nil {
		t.Fatal(err)
	}
	emitContractEvent(t, encoder, ingestevent.InputScope(0), diagnostic)
	emitContractEvent(t, encoder, ingestevent.ObjectScope(0, eventFlowID, eventObjectID), ingestevent.ObjectResult{
		ObjectID: eventObjectID, Timerange: "0:0_1:0", Bytes: 1514152, SHA256: eventSHA256,
		Disposition: ingestevent.ObjectDispositionIngested, Verification: ingestevent.ObjectVerificationVerified,
		VerificationMethod: ingestevent.VerificationMethodReadback,
	})
	emitContractEvent(t, encoder, ingestevent.FlowScope(0, eventFlowID), ingestevent.FlowResult{
		FlowID: eventFlowID, SourceID: eventSourceID, Kind: ingestevent.FlowKindMuxed,
		Disposition: ingestevent.FlowWritten, ObjectSummary: ingestevent.ObjectSummary{
			Total: 1, Bytes: 1514152, Ingested: 1, Verified: 1, ReadbackVerified: 1,
		},
	})
	emitContractEvent(t, encoder, ingestevent.InputScope(0), ingestevent.InputFinished{
		Input: "file:///tmp/first-ingest.ts", Profile: "essence-segments", ProfileVersion: "1",
		FFmpegVersion: "ffmpeg version 7.0", MediaToolchain: "sha256:" + eventSHA256,
		RootFlowID: eventFlowID, Bytes: 1419776, SHA256: eventSHA256, Status: ingestevent.InputIngested,
		Verification: ingestevent.VerificationVerified, FlowCount: 1, ObjectCount: 1,
	})
	emitContractEvent(t, encoder, nil, ingestevent.RunFinished{
		Outcome: ingestevent.RunSucceeded, Total: 1, Succeeded: 1, ElapsedMS: 2783,
		BytesStaged: 1419776, BytesUploaded: 1514152, BytesVerified: 1514152,
		Retries: 1, ObjectsVerified: 1,
	})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	validateEventStream(t, schema, sink.Bytes())

	interruptedSink := &bytes.Buffer{}
	interrupted, err := ingestevent.NewEncoder(interruptedSink, eventRunID)
	if err != nil {
		t.Fatal(err)
	}
	emitContractEvent(t, interrupted, nil, ingestevent.Hello{
		ToolVersion: "v1.0.0", ToolCommit: "abc123", ResultSchemaVersion: "2.0", ProfilePolicyVersion: "1",
		MaxEventBytes: ingestevent.DefaultMaxEventBytes, Capabilities: []string{"graceful_cancel"},
	})
	emitContractEvent(t, interrupted, nil, ingestevent.RunStarted{StartedAt: started})
	emitContractEvent(t, interrupted, ingestevent.InputScope(0), ingestevent.InputDeclared{Input: "file:///tmp/queued.ts"})
	emitContractEvent(t, interrupted, nil, ingestevent.ManifestFinished{TotalInputs: 1})
	emitContractEvent(t, interrupted, nil, ingestevent.RunCancellationRequested{Reason: ingestevent.CancellationSignal})
	emitContractEvent(t, interrupted, ingestevent.InputScope(0), ingestevent.InputFinished{
		Input: "file:///tmp/queued.ts", Profile: "essence-segments", ProfileVersion: "1", Status: ingestevent.InputFailed,
		Verification: ingestevent.VerificationNotReached, ErrorCode: ingestevent.InputErrorCodeRunInterrupted, Message: "Interrupted before dispatch.",
	})
	emitContractEvent(t, interrupted, nil, ingestevent.RunFinished{
		Outcome: ingestevent.RunInterrupted, ExitCode: 8, Total: 1, Failed: 1,
	})
	if err := interrupted.Finalize(); err != nil {
		t.Fatal(err)
	}
	validateEventStream(t, schema, interruptedSink.Bytes())
}

func TestPublishedSchemaAllowsStartupFailureAndCompatibleMinorExtensions(t *testing.T) {
	t.Parallel()
	schema := compileTamsinSchema(t, "ingest-events-v2.json")
	sink := &bytes.Buffer{}
	encoder, err := ingestevent.NewEncoder(sink, eventRunID)
	if err != nil {
		t.Fatal(err)
	}
	emitContractEvent(t, encoder, nil, ingestevent.Hello{
		ToolVersion: "v1.0.0", ToolCommit: "abc123", ResultSchemaVersion: "2.0", ProfilePolicyVersion: "1",
		MaxEventBytes: ingestevent.DefaultMaxEventBytes, Capabilities: []string{},
	})
	emitContractEvent(t, encoder, nil, ingestevent.RunStarted{StartedAt: time.Now().UTC()})
	diagnostic, err := ingestevent.NewDiagnostic(ingestevent.SeverityError, ingestevent.DiagnosticCodeConfigInvalid, "Configuration is invalid.", "Correct it.", true)
	if err != nil {
		t.Fatal(err)
	}
	emitContractEvent(t, encoder, nil, diagnostic)
	emitContractEvent(t, encoder, nil, ingestevent.ManifestFinished{})
	emitContractEvent(t, encoder, nil, ingestevent.RunFinished{Outcome: ingestevent.RunFailed, ExitCode: 2})
	if err := encoder.Finalize(); err != nil {
		t.Fatal(err)
	}
	validateEventStream(t, schema, sink.Bytes())

	future := map[string]any{
		"protocol": "tamsin.ingest.events", "protocol_version": "2.7", "type": "ui.hint", "seq": 9,
		"run_id": eventRunID, "emitted_at": "2026-08-09T08:35:46Z", "elapsed_ms": 10,
		"payload": map[string]any{"new_field": true}, "future_envelope_field": "ignored",
	}
	if err := validate(t, schema, future); err != nil {
		t.Fatalf("compatible v2 extension was rejected: %v", err)
	}
	delete(future, "emitted_at")
	if err := validate(t, schema, future); err == nil {
		t.Fatal("schema accepted an event without emitted_at")
	}
	future["emitted_at"] = "2026-08-09T08:35:46Z"
	future["protocol"] = "another.protocol"
	if err := validate(t, schema, future); err == nil {
		t.Fatal("schema accepted the wrong process protocol")
	}
}
