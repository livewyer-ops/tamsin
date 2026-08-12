package resultjournal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/source"
)

const testRunID = "d1f5657c-cee0-5ac4-a0a5-c78ba122fa39"

type syncBuffer struct {
	bytes.Buffer
	syncs  int
	failAt int
}

func (b *syncBuffer) Sync() error {
	b.syncs++
	if b.failAt > 0 && b.syncs == b.failAt {
		return errors.New("disk refused sync")
	}
	return nil
}

func testContract() ingest.ResultContract {
	return ingest.ResultContract{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: testRunID,
	}
}

func terminalResult(input string) ingest.Result {
	return ingest.Result{
		Input: input, Profile: ingest.ProfileCustom, ProfileVersion: "1",
		Status: ingest.ResultStatusPlanned, Verification: ingest.VerificationNotRequested,
		Flows: []ingest.FlowResult{},
	}
}

func TestJournalSyncsEveryTerminalResultAndSummary(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	// Completion order need not be input order; the explicit zero-based index
	// makes replay deterministic without delaying the first durable record.
	if err := journal.WriteResult(1, terminalResult("second")); err != nil {
		t.Fatal(err)
	}
	if sink.syncs != 2 || strings.Count(sink.String(), "\n") != 2 {
		t.Fatalf("first result was not independently durable: syncs=%d data=%q", sink.syncs, sink.String())
	}
	if err := journal.WriteResult(0, terminalResult("first")); err != nil {
		t.Fatal(err)
	}
	batch := ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: testRunID,
		Results: []ingest.Result{terminalResult("first"), terminalResult("second")}, Succeeded: 2,
	}
	if err := journal.WriteSummary(batch, nil, false); err != nil {
		t.Fatal(err)
	}
	if sink.syncs != 4 || strings.Count(sink.String(), "\n") != 4 {
		t.Fatalf("start, two results and summary need four durable records: syncs=%d data=%q", sink.syncs, sink.String())
	}

	lines := strings.Split(strings.TrimSpace(sink.String()), "\n")
	var start, first, second, final map[string]any
	for index, target := range []any{&start, &first, &second, &final} {
		if err := json.Unmarshal([]byte(lines[index]), target); err != nil {
			t.Fatalf("decode line %d: %v", index, err)
		}
	}
	if start["record_type"] != "start" || start["total"] != float64(2) || first["index"] != float64(1) || second["index"] != float64(0) {
		t.Fatalf("journal lost completion indexes: %s", sink.String())
	}
	if start["run_id"] != testRunID || first["run_id"] != testRunID || second["run_id"] != testRunID || final["run_id"] != testRunID {
		t.Fatalf("records do not share the invocation ID: %s", sink.String())
	}
	if final["record_type"] != "summary" {
		t.Fatalf("last record is not the summary: %#v", final)
	}
}

func TestJournalSyncsOneCompleteObjectBatch(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"input"})
	if err != nil {
		t.Fatal(err)
	}
	objects := []ingest.ObjectResult{
		{
			ObjectID: "19e919cf-183a-40bd-b9e5-8c8b361f6728", Timerange: "0:0_1:0", Bytes: 10,
			SHA256: strings.Repeat("1", 64), Disposition: ingest.ObjectDispositionIngested,
			Verification: ingest.ObjectVerificationVerified, VerificationMethod: ingest.VerificationMethodStorage,
		},
		{
			ObjectID: "29e919cf-183a-40bd-b9e5-8c8b361f6728", Timerange: "1:0_2:0", Bytes: 20,
			SHA256: strings.Repeat("2", 64), Disposition: ingest.ObjectDispositionIngested,
			Verification: ingest.ObjectVerificationVerified, VerificationMethod: ingest.VerificationMethodReadback,
		},
	}
	if err := journal.WriteObjectBatch(0, "7b0d2aec-1868-56ad-879d-95e35ed75e4c", objects); err != nil {
		t.Fatal(err)
	}
	if sink.syncs != 2 || strings.Count(sink.String(), "\n") != 3 {
		t.Fatalf("two Object records should add one batch sync: syncs=%d data=%q", sink.syncs, sink.String())
	}
	for index, line := range strings.Split(strings.TrimSpace(sink.String()), "\n")[1:] {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode Object line %d: %v", index, err)
		}
		if record["record_type"] != "object" || record["index"] != float64(0) {
			t.Fatalf("unexpected Object record %d: %#v", index, record)
		}
	}
}

func TestJournalMarksGracefulInterruption(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"input"})
	if err != nil {
		t.Fatal(err)
	}
	result := terminalResult("input")
	result.Status = ingest.ResultStatusFailed
	result.Error = context.Canceled.Error()
	if err := journal.WriteResult(0, result); err != nil {
		t.Fatal(err)
	}
	batch := ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: testRunID,
		Results: []ingest.Result{result}, Failed: 1,
	}
	// The caller may observe cancellation after every input reached a terminal
	// state, leaving no Pipeline error. The explicit run signal is authoritative.
	if err := journal.WriteSummary(batch, nil, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sink.String(), `"outcome":"interrupted"`) {
		t.Fatalf("graceful cancellation was not identified: %s", sink.String())
	}
}

func TestJournalDoesNotTreatChildDeadlineAsRunInterruption(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"input"})
	if err != nil {
		t.Fatal(err)
	}
	result := terminalResult("input")
	result.Status = ingest.ResultStatusFailed
	result.Failure = &ingest.Failure{Code: ingest.FailureCodePreflightFailed, Message: ingest.FailureMessagePreflightTimedOut, ActionRequired: true}
	if err := journal.WriteResult(0, result); err != nil {
		t.Fatal(err)
	}
	batch := ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: testRunID,
		Results: []ingest.Result{result}, Failed: 1,
	}
	if err := journal.WriteSummary(batch, context.DeadlineExceeded, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sink.String(), `"outcome":"failed"`) || strings.Contains(sink.String(), `"outcome":"interrupted"`) {
		t.Fatalf("child deadline was mislabeled as interruption: %s", sink.String())
	}
}

func TestJournalSerializesOnlyTheStableFailureContract(t *testing.T) {
	t.Parallel()
	const toxicImplementationError = "peer-response-top-secret"
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"input"})
	if err != nil {
		t.Fatal(err)
	}
	result := terminalResult("input")
	result.Status = ingest.ResultStatusFailed
	result.Error = toxicImplementationError
	result.Failure = &ingest.Failure{
		Code: ingest.FailureCodeHTTPRequestFailed, Message: ingest.FailureMessageHTTPRequestFailed, ActionRequired: true,
	}
	if err := journal.WriteResult(0, result); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sink.String(), toxicImplementationError) || strings.Contains(sink.String(), `"error"`) {
		t.Fatalf("journal serialized a raw implementation error: %s", sink.String())
	}
	if !strings.Contains(sink.String(), `"failure":{"code":"`+ingest.FailureCodeHTTPRequestFailed+`","message":"`+ingest.FailureMessageHTTPRequestFailed+`","action_required":true}`) {
		t.Fatalf("journal omitted the stable failure contract: %s", sink.String())
	}
}

func TestJournalRefusesToClaimAnIncompleteRun(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.WriteResult(1, terminalResult("second")); err != nil {
		t.Fatal(err)
	}
	batch := ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: testRunID,
		Results: []ingest.Result{terminalResult("first"), terminalResult("second")}, Succeeded: 2,
	}
	if err := journal.WriteSummary(batch, nil, false); err == nil || !strings.Contains(err.Error(), "1 terminal inputs") {
		t.Fatalf("summary error = %v, want incomplete journal", err)
	}
	if strings.Contains(sink.String(), `"record_type":"summary"`) {
		t.Fatalf("incomplete journal claimed a summary: %s", sink.String())
	}
}

func TestJournalSurfacesSyncFailureBeforeAcceptingRecord(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{failAt: 2}
	journal, err := New(sink, testContract(), []string{"input"})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.WriteResult(0, terminalResult("input")); err == nil || !strings.Contains(err.Error(), "disk refused sync") {
		t.Fatalf("WriteResult() error = %v, want sync failure", err)
	}
	// Once bytes were written but could not be synced their durability is
	// unknowable. Refuse further records rather than risk a duplicate index or a
	// summary which falsely claims the run is complete.
	if err := journal.WriteResult(0, terminalResult("input")); err == nil || !strings.Contains(err.Error(), "previously failed") {
		t.Fatalf("retry error = %v, want poisoned journal", err)
	}
	if sink.syncs != 2 {
		t.Fatalf("syncs = %d, want no write after failure", sink.syncs)
	}
}

func TestJournalRejectsDuplicateIndexAndPostSummaryWrites(t *testing.T) {
	t.Parallel()
	sink := &syncBuffer{}
	journal, err := New(sink, testContract(), []string{"input"})
	if err != nil {
		t.Fatal(err)
	}
	result := terminalResult("input")
	if err := journal.WriteResult(0, terminalResult("different")); err == nil || !strings.Contains(err.Error(), "start manifest") {
		t.Fatalf("manifest mismatch error = %v", err)
	}
	if err := journal.WriteResult(0, result); err != nil {
		t.Fatal(err)
	}
	if err := journal.WriteResult(0, result); err == nil || !strings.Contains(err.Error(), "already contains") {
		t.Fatalf("duplicate error = %v", err)
	}
	batch := ingest.BatchResult{
		SchemaVersion: ingest.ResultSchemaVersion, ToolVersion: "v1.2.3", ToolCommit: "abc123",
		ProfileVersion: "1", RunID: testRunID,
		Results: []ingest.Result{result}, Succeeded: 1,
	}
	if err := journal.WriteSummary(batch, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := journal.WriteResult(1, result); err == nil || !strings.Contains(err.Error(), "summary has already") {
		t.Fatalf("post-summary error = %v", err)
	}
}

func TestOpenRefusesEveryExistingPath(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "results.jsonl")
	original := []byte("do not overwrite")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err := Open(path, testContract(), []source.Item{{URI: "file:///input.ts"}})
	if journal != nil {
		_ = journal.Close()
	}
	if err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("Open() error = %v, want existing-path refusal", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(got, original) {
		t.Fatalf("existing journal changed: data=%q err=%v", got, readErr)
	}
}

func TestOpenRefusesExistingSymlink(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("protected"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "results.jsonl")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	journal, err := Open(path, testContract(), []source.Item{{URI: "file:///input.ts"}})
	if journal != nil {
		_ = journal.Close()
	}
	if err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("Open() error = %v, want symlink refusal", err)
	}
	if got, readErr := os.ReadFile(target); readErr != nil || string(got) != "protected" {
		t.Fatalf("symlink target changed: data=%q err=%v", got, readErr)
	}
}

func TestFailedOpenRemovesIncompleteJournalAndAllowsRetry(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "results.jsonl")
	invalid := testContract()
	invalid.RunID = "not-a-uuid"
	journal, err := Open(path, invalid, []source.Item{{URI: "file:///input.ts"}})
	if journal != nil {
		_ = journal.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "run ID") {
		t.Fatalf("Open() error = %v, want invalid contract refusal", err)
	}
	if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed Open left an incomplete journal: %v", statErr)
	}

	journal, err = Open(path, testContract(), []source.Item{{URI: "file:///input.ts"}})
	if err != nil {
		t.Fatalf("retry Open() after corrected configuration: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDurablyStartsWithCredentialFreeIndexedManifest(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "results.jsonl")
	journal, err := Open(path, testContract(), []source.Item{
		{URI: "HTTPS://alice:secret@example.test/programme.ts?token=secret"},
		{URI: "file:///safe/input.ts"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a process which stops before its first input reaches a terminal
	// result: the independently synced start record still identifies all work.
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "\n") != 1 || !strings.Contains(string(data), `"record_type":"start"`) ||
		!strings.Contains(string(data), `"total":2`) || !strings.Contains(string(data), `"index":0`) ||
		!strings.Contains(string(data), `"index":1`) || strings.Contains(string(data), "alice") ||
		strings.Contains(string(data), "secret") || strings.Contains(string(data), "token") ||
		!strings.Contains(string(data), `"input":"https://example.test/programme.ts"`) {
		t.Fatalf("unsafe or incomplete start record: %s", data)
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %v, want 0600", info.Mode().Perm())
	}
}
