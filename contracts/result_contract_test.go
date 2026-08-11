package contracts_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/resultjournal"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const tamsinSchemaBase = "https://raw.githubusercontent.com/livewyer-ops/tamsin/main/contracts/tamsin/"

type journalBuffer struct{ bytes.Buffer }

func (*journalBuffer) Sync() error { return nil }

func compileTamsinSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	for _, filename := range []string{
		"batch-result-v1.json", "batch-result-v2.json", "doctor-report-v1.json",
		"ingest-journal-v1.json", "ingest-journal-v2.json", "ingest-events-v1.json", "ingest-events-v2.json",
	} {
		file, err := os.Open("tamsin/" + filename)
		if err != nil {
			t.Fatalf("open schema %s: %v", filename, err)
		}
		document, err := jsonschema.UnmarshalJSON(file)
		_ = file.Close()
		if err != nil {
			t.Fatalf("decode schema %s: %v", filename, err)
		}
		if err := compiler.AddResource(tamsinSchemaBase+filename, document); err != nil {
			t.Fatalf("add schema %s: %v", filename, err)
		}
	}
	schema, err := compiler.Compile(tamsinSchemaBase + name)
	if err != nil {
		t.Fatalf("compile schema %s: %v", name, err)
	}
	return schema
}

func validate(t *testing.T, schema *jsonschema.Schema, value any) error {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal instance: %v", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode instance: %v", err)
	}
	return schema.Validate(instance)
}

func contractFixture() ingest.BatchResult {
	rootID := "f3b1a8de-6c1e-4a0b-9d2f-1c7e5a904bb1"
	return ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: ingest.ResultProfileVersion, RunID: "d1f5657c-cee0-5ac4-a0a5-c78ba122fa39",
		Succeeded: 1,
		Results: []ingest.Result{{
			Input: "file:///programme.ts", Profile: ingest.ProfileEditorial, ProfileVersion: ingest.ProfileVersion,
			RootFlowID: rootID, Bytes: 991017,
			SHA256: "77145c94c11f3754207499158df22406e1fe7635553c1c86dc5e881dfeb32016",
			Status: ingest.ResultStatusIngested, Verification: ingest.VerificationVerified,
			Flows: []ingest.FlowResult{
				{
					FlowID: "b3391e64-e245-46d7-8496-0eebfde13950", SourceID: "9a2c4e60-71bd-4f3a-8e15-2d6b0c8a7f43", Kind: ingest.FlowKindEssence, Role: "video", Disposition: ingest.FlowWritten,
				},
				{
					FlowID: rootID, SourceID: "29b5961b-22de-4b61-b137-013b70d20b54", Kind: ingest.FlowKindMuxed, Disposition: ingest.FlowWritten,
					ObjectSummary: ingest.ObjectSummary{
						Total: 1, Bytes: 991017, Ingested: 1, Verified: 1, ReadbackVerified: 1,
					},
					Objects: []ingest.ObjectResult{{
						ObjectID: "19e919cf-183a-40bd-b9e5-8c8b361f6728", Timerange: "0:0_1:0", Bytes: 991017,
						SHA256:      "77145c94c11f3754207499158df22406e1fe7635553c1c86dc5e881dfeb32016",
						Disposition: ingest.ObjectDispositionIngested, Verification: ingest.ObjectVerificationVerified,
						VerificationMethod: ingest.VerificationMethodReadback,
					}},
				},
			},
		}},
	}
}

func failedContractResult() ingest.Result {
	return ingest.Result{
		Input: "s3://incoming/programme.ts", Profile: ingest.ProfilePreserve, ProfileVersion: "1",
		Status: ingest.ResultStatusFailed, Verification: ingest.VerificationNotReached,
		Flows: []ingest.FlowResult{}, Failure: &ingest.Failure{
			Code: ingest.FailureCodeHTTPRequestFailed, Message: ingest.FailureMessageHTTPRequestFailed, ActionRequired: false,
		},
	}
}

func TestPublishedBatchResultSchemaAcceptsTheRuntimeContract(t *testing.T) {
	t.Parallel()
	schema := compileTamsinSchema(t, "batch-result-v2.json")
	fixture := contractFixture()
	if err := validate(t, schema, fixture); err != nil {
		t.Fatalf("runtime result does not satisfy its published schema: %v", err)
	}

	encoded, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var invalid map[string]any
	if err := json.Unmarshal(encoded, &invalid); err != nil {
		t.Fatal(err)
	}
	delete(invalid, "tool_commit")
	if err := validate(t, schema, invalid); err == nil {
		t.Fatal("schema accepted a batch without source/build commit provenance")
	}
	invalid["tool_commit"] = fixture.ToolCommit
	invalid["results"].([]any)[0].(map[string]any)["verification"] = "probably"
	if err := validate(t, schema, invalid); err == nil {
		t.Fatal("schema accepted an unknown verification terminal state")
	}
	provenance := invalid["results"].([]any)[0].(map[string]any)
	provenance["verification"] = "verified"
	provenance["profile"] = "editorial"
	provenance["profile_version"] = "1"
	provenance["ffmpeg_version"] = "ffmpeg version 7.0"
	provenance["media_toolchain"] = "sha256:77145c94c11f3754207499158df22406e1fe7635553c1c86dc5e881dfeb32016"
	if err := validate(t, schema, invalid); err != nil {
		t.Fatalf("schema rejected per-input profile/toolchain provenance: %v", err)
	}
	delete(provenance, "media_toolchain")
	if err := validate(t, schema, invalid); err == nil {
		t.Fatal("schema accepted ffmpeg_version without its media_toolchain fingerprint")
	}
	provenance["media_toolchain"] = "sha256:77145c94c11f3754207499158df22406e1fe7635553c1c86dc5e881dfeb32016"
	delete(provenance, "profile")
	if err := validate(t, schema, invalid); err == nil {
		t.Fatal("schema accepted a result without its resolved profile")
	}

	failed := ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: "d1f5657c-cee0-5ac4-a0a5-c78ba122fa39",
		Failed: 1, Results: []ingest.Result{failedContractResult()},
	}
	if err := validate(t, schema, failed); err != nil {
		t.Fatalf("pre-Flow terminal failure does not satisfy result schema: %v", err)
	}

	failedJSON, err := json.Marshal(failed)
	if err != nil {
		t.Fatal(err)
	}
	var failedValue map[string]any
	if err := json.Unmarshal(failedJSON, &failedValue); err != nil {
		t.Fatal(err)
	}
	failedResult := failedValue["results"].([]any)[0].(map[string]any)
	failure := failedResult["failure"]
	delete(failedResult, "failure")
	if err := validate(t, schema, failedValue); err == nil {
		t.Fatal("schema accepted a failed result without its structured failure")
	}
	failedResult["failure"] = failure
	failedResult["error"] = "raw provider response must not be serializable"
	if err := validate(t, schema, failedValue); err == nil {
		t.Fatal("schema accepted the removed raw error field")
	}
	delete(failedResult, "error")
	failureValue := failure.(map[string]any)
	delete(failureValue, "action_required")
	if err := validate(t, schema, failedValue); err == nil {
		t.Fatal("schema accepted a failure without action_required")
	}
	failureValue["action_required"] = false
	failureValue["code"] = "Provider Error!"
	if err := validate(t, schema, failedValue); err == nil {
		t.Fatal("schema accepted an unstable failure code")
	}
	failureValue["code"] = ingest.FailureCodeHTTPRequestFailed
	failureValue["message"] = strings.Repeat("x", 4097)
	if err := validate(t, schema, failedValue); err == nil {
		t.Fatal("schema accepted an unbounded failure message")
	}
	failureValue["message"] = ingest.FailureMessageHTTPRequestFailed

	var successValue map[string]any
	if err := json.Unmarshal(encoded, &successValue); err != nil {
		t.Fatal(err)
	}
	successValue["results"].([]any)[0].(map[string]any)["failure"] = failure
	if err := validate(t, schema, successValue); err == nil {
		t.Fatal("schema accepted a failure on a successful result")
	}
}

func TestPublishedJournalSchemaAcceptsEveryDurableRecord(t *testing.T) {
	t.Parallel()
	schema := compileTamsinSchema(t, "ingest-journal-v2.json")
	fixture := contractFixture()
	sink := &journalBuffer{}
	journal, err := resultjournal.New(sink, ingest.ResultContract{
		SchemaVersion: fixture.SchemaVersion, ToolVersion: fixture.ToolVersion, ToolCommit: fixture.ToolCommit,
		ProfileVersion: fixture.ProfileVersion, RunID: fixture.RunID,
	}, []string{fixture.Results[0].Input, failedContractResult().Input})
	if err != nil {
		t.Fatal(err)
	}
	for _, flow := range fixture.Results[0].Flows {
		if len(flow.Objects) == 0 {
			continue
		}
		if err := journal.WriteObjectBatch(0, flow.FlowID, flow.Objects); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.WriteResult(0, fixture.Results[0]); err != nil {
		t.Fatal(err)
	}
	failed := failedContractResult()
	if err := journal.WriteResult(1, failed); err != nil {
		t.Fatal(err)
	}
	fixture.Results = append(fixture.Results, failed)
	fixture.Failed = 1
	if err := journal.WriteSummary(fixture, nil, false); err != nil {
		t.Fatal(err)
	}
	for index, line := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
		var record any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode journal line %d: %v", index, err)
		}
		if err := validate(t, schema, record); err != nil {
			t.Fatalf("journal line %d does not satisfy its published schema: %v\n%s", index, err, line)
		}
	}
}

func TestResultFixtureHasOneRootReferenceAndNoDuplicateFlows(t *testing.T) {
	t.Parallel()
	for _, result := range contractFixture().Results {
		seen := make(map[string]bool, len(result.Flows))
		roots := 0
		for _, flow := range result.Flows {
			if seen[flow.FlowID] {
				t.Fatalf("Flow %s occurs more than once", flow.FlowID)
			}
			seen[flow.FlowID] = true
			if flow.FlowID == result.RootFlowID {
				roots++
			}
		}
		if roots != 1 {
			t.Fatalf("root_flow_id %s resolves %d times", result.RootFlowID, roots)
		}
	}
}
