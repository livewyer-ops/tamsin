package presentation

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/observability"
)

const (
	testRunID      = "00000000-0000-4000-8000-000000000000"
	testCollection = "11111111-1111-4111-8111-111111111111"
	testVideo      = "22222222-2222-4222-8222-222222222222"
	testAudio      = "33333333-3333-4333-8333-333333333333"
	testSource     = "44444444-4444-4444-8444-444444444444"
	testStranded   = "55555555-5555-4555-8555-555555555555"
	testRetracted  = "66666666-6666-4666-8666-666666666666"
	testUncertain  = "77777777-7777-4777-8777-777777777777"
)

func TestWriteHumanIsLineOriented(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	metrics := observability.Snapshot{Elapsed: 2782 * time.Millisecond, BytesUploaded: 1514152, BytesVerified: 1514152}
	if err := WriteHuman(&output, successBatch(), metrics, HumanOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"INGESTED AND VERIFIED", "first-ingest.ts", testCollection, "essence-segments@1", testRunID} {
		if !strings.Contains(output.String(), text) {
			t.Errorf("receipt omitted %q:\n%s", text, output.String())
		}
	}
	if strings.ContainsAny(output.String(), "\t\x1b\r") {
		t.Fatalf("plain receipt contains terminal controls: %q", output.String())
	}
}

func TestFailureReceiptRetainsOperationalIdentifiers(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	if err := WriteHuman(&output, failureBatch(), observability.Snapshot{}, HumanOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, identifier := range []string{testCollection, testVideo, testStranded, testRetracted, testUncertain, testRunID} {
		if !strings.Contains(output.String(), identifier) {
			t.Errorf("failure receipt omitted identifier %s", identifier)
		}
	}
	normalized := strings.Join(strings.Fields(output.String()), " ")
	for _, phrase := range []string{"ACTION REQUIRED", "indeterminate", "before retrying", "unverified"} {
		if !strings.Contains(strings.ToLower(normalized), strings.ToLower(phrase)) {
			t.Errorf("failure receipt omitted actionable phrase %q", phrase)
		}
	}
}

func TestFailureReceiptUsesOnlyTheStableFailureContract(t *testing.T) {
	t.Parallel()
	const toxicImplementationError = "peer-response-top-secret"
	batch := ingest.BatchResult{
		RunID: testRunID,
		Results: []ingest.Result{{
			Input: "file:///input.ts", Profile: ingest.ProfileEssenceSegments, ProfileVersion: "1",
			Status: ingest.ResultStatusFailed, Verification: ingest.VerificationNotReached, Flows: []ingest.FlowResult{},
			Failure: &ingest.Failure{
				Code: ingest.FailureCodeHTTPRequestFailed, Message: ingest.FailureMessageHTTPRequestFailed, ActionRequired: true,
			},
			Error: toxicImplementationError,
		}},
		Failed: 1,
	}
	var output bytes.Buffer
	if err := WriteHuman(&output, batch, observability.Snapshot{}, HumanOptions{Verbose: true}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), toxicImplementationError) {
		t.Fatalf("human receipt reproduced an arbitrary implementation error: %s", output.String())
	}
	for _, safe := range []string{
		"INGEST FAILED - ACTION REQUIRED",
		ingest.FailureMessageHTTPRequestFailed,
		"failure code " + ingest.FailureCodeHTTPRequestFailed,
	} {
		if !strings.Contains(output.String(), safe) {
			t.Errorf("human receipt omitted stable failure detail %q: %s", safe, output.String())
		}
	}
}

func TestHumanReceiptRejectsPercentEncodedTerminalControls(t *testing.T) {
	t.Parallel()
	batch := successBatch()
	batch.Results[0].Input = "file:///tmp/programme-%1B%5B31m.ts"
	var output bytes.Buffer
	if err := WriteHuman(&output, batch, observability.Snapshot{}, HumanOptions{Verbose: true}); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(output.String(), "\x1b\r") {
		t.Fatalf("human receipt contains injected terminal controls: %q", output.String())
	}
}

func TestConciseReceiptGroupsRatherThanRepeatsInputMetadata(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	batch := successBatch()
	if err := WriteHuman(&output, batch, observability.Snapshot{}, HumanOptions{}); err != nil {
		t.Fatal(err)
	}
	for value, wantCount := range map[string]int{
		"first-ingest.ts":       1,
		"essence-segments@1":    1,
		strings.Repeat("a", 64): 1,
	} {
		if got := strings.Count(output.String(), value); got != wantCount {
			t.Errorf("receipt contains %q %d times, want %d", value, got, wantCount)
		}
	}
	for _, internal := range []string{"root=true", "objects=0", "disposition=", "source="} {
		if strings.Contains(output.String(), internal) {
			t.Errorf("concise receipt leaked record-oriented field %q", internal)
		}
	}
}

func TestVerboseExpandsProvenanceAndObjects(t *testing.T) {
	t.Parallel()
	batch := successBatch()
	batch.ToolBuildDate = "2026-08-09T10:35:49Z"
	batch.Results[0].FFmpegVersion = "ffmpeg version 6.1.1"
	batch.Results[0].MediaToolchain = "sha256:" + strings.Repeat("e", 64)

	var concise, verbose bytes.Buffer
	if err := WriteHuman(&concise, batch, observability.Snapshot{}, HumanOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := WriteHuman(&verbose, batch, observability.Snapshot{BytesStaged: 1419776}, HumanOptions{Verbose: true}); err != nil {
		t.Fatal(err)
	}
	for _, detail := range []string{
		"file:///tmp/tamsintest/first-ingest.ts",
		"source " + testSource,
		"disposition written",
		"verified object aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"FFmpeg ffmpeg version 6.1.1",
		"media toolchain sha256:" + strings.Repeat("e", 64),
		"result schema 1.0",
		"tool 0.1.0@" + strings.Repeat("c", 40),
		"tool build 2026-08-09T10:35:49Z",
		"staged 1.35 MiB",
	} {
		if !strings.Contains(verbose.String(), detail) {
			t.Errorf("verbose receipt omitted %q\n%s", detail, verbose.String())
		}
		if strings.Contains(concise.String(), detail) {
			t.Errorf("concise receipt included verbose detail %q", detail)
		}
	}
}

func TestQuietSuppressesOnlyCleanSuccess(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	if err := WriteHuman(&output, successBatch(), observability.Snapshot{}, HumanOptions{Quiet: true}); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("quiet clean success wrote %q", output.String())
	}

	warning := successBatch()
	warning.Results[0].Verification = ingest.VerificationNotRequested
	output.Reset()
	if err := WriteHuman(&output, warning, observability.Snapshot{}, HumanOptions{Quiet: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "VERIFICATION NOT REQUESTED") {
		t.Fatalf("quiet hid a warning: %q", output.String())
	}

	output.Reset()
	if err := WriteHuman(&output, failureBatch(), observability.Snapshot{}, HumanOptions{Quiet: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), testStranded) {
		t.Fatalf("quiet hid a stranded Object: %q", output.String())
	}
}

func TestColorDecoratesHeadingsOnly(t *testing.T) {
	t.Parallel()
	var plain, colored bytes.Buffer
	batch := successBatch()
	if err := WriteHuman(&plain, batch, observability.Snapshot{}, HumanOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := WriteHuman(&colored, batch, observability.Snapshot{}, HumanOptions{Color: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(colored.String(), "\x1b[1;32mINGESTED AND VERIFIED\x1b[0m") {
		t.Fatalf("success heading was not coloured: %q", colored.String())
	}
	stripped := strings.NewReplacer("\x1b[1;32m", "", "\x1b[1;33m", "", "\x1b[1;31m", "", "\x1b[0m", "").Replace(colored.String())
	if stripped != plain.String() {
		t.Fatal("colour changed semantic output")
	}
}

func TestWriteHumanReportsWriterFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("output closed")
	err := WriteHuman(failingWriter{err: want}, successBatch(), observability.Snapshot{}, HumanOptions{})
	if !errors.Is(err, want) {
		t.Fatalf("WriteHuman error = %v, want %v", err, want)
	}
}

func TestEmptyBatchStillHasPermanentFooter(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	batch := ingest.BatchResult{RunID: testRunID}
	if err := WriteHuman(&output, batch, observability.Snapshot{}, HumanOptions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "COMPLETE  0 inputs succeeded") || !strings.Contains(output.String(), testRunID) {
		t.Fatalf("empty batch receipt = %q", output.String())
	}
}

func TestHumanUnits(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		bytes int64
		want  string
	}{
		{bytes: 1023, want: "1023 B"},
		{bytes: 1024, want: "1 KiB"},
		{bytes: 10 * 1024 * 1024, want: "10 MiB"},
		{bytes: 100 * 1024 * 1024, want: "100 MiB"},
		{bytes: 1514152, want: "1.44 MiB"},
	} {
		if got := humanBytes(testCase.bytes); got != testCase.want {
			t.Errorf("humanBytes(%d) = %q, want %q", testCase.bytes, got, testCase.want)
		}
	}
	for _, testCase := range []struct {
		duration time.Duration
		want     string
	}{
		{duration: 782*time.Millisecond + 400*time.Microsecond, want: "782ms"},
		{duration: 2782 * time.Millisecond, want: "2.8s"},
		{duration: 63*time.Second + 400*time.Millisecond, want: "1m3s"},
	} {
		if got := humanDuration(testCase.duration); got != testCase.want {
			t.Errorf("humanDuration(%s) = %q, want %q", testCase.duration, got, testCase.want)
		}
	}
}

func successBatch() ingest.BatchResult {
	return ingest.BatchResult{
		SchemaVersion: "1.0", ToolVersion: "0.1.0", ToolCommit: strings.Repeat("c", 40),
		ProfileVersion: "1", RunID: testRunID, Succeeded: 1,
		Results: []ingest.Result{{
			Input: "file:///tmp/tamsintest/first-ingest.ts", Profile: "essence-segments", ProfileVersion: "1",
			RootFlowID: testCollection, Bytes: 1419776, SHA256: strings.Repeat("a", 64),
			Status: ingest.ResultStatusIngested, Verification: ingest.VerificationVerified,
			Flows: []ingest.FlowResult{
				{FlowID: testCollection, SourceID: testSource, Disposition: ingest.FlowWritten},
				{FlowID: testVideo, SourceID: "88888888-8888-4888-8888-888888888888", Role: "video", Disposition: ingest.FlowWritten, Objects: []ingest.ObjectResult{
					{ObjectID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Timerange: "0:0_1:25", Bytes: 500000, SHA256: strings.Repeat("1", 64), Disposition: ingest.ObjectDispositionRegistered, Verification: ingest.ObjectVerificationVerified},
					{ObjectID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Timerange: "1:25_2:25", Bytes: 500000, SHA256: strings.Repeat("2", 64), Disposition: ingest.ObjectDispositionRegistered, Verification: ingest.ObjectVerificationVerified},
				}},
				{FlowID: testAudio, SourceID: "99999999-9999-4999-8999-999999999999", Role: "audio", Disposition: ingest.FlowWritten, Objects: []ingest.ObjectResult{
					{ObjectID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Timerange: "0:0_2:25", Bytes: 514152, SHA256: strings.Repeat("3", 64), Disposition: ingest.ObjectDispositionRegistered, Verification: ingest.ObjectVerificationVerified},
				}},
			},
		}},
	}
}

func failureBatch() ingest.BatchResult {
	return ingest.BatchResult{
		SchemaVersion: "1.0", ToolVersion: "0.1.0", ToolCommit: strings.Repeat("d", 40),
		ProfileVersion: "1", RunID: testRunID, Failed: 1,
		Results: []ingest.Result{{
			Input: "file:///srv/media/failure.ts", Profile: "mpegts-segments", ProfileVersion: "1",
			RootFlowID: testCollection, Bytes: 3_145_728, SHA256: strings.Repeat("f", 64),
			Status: ingest.ResultStatusFailed, Verification: ingest.VerificationFailedStranded,
			Failure: &ingest.Failure{
				Code: ingest.FailureCodeFlowIndeterminate, Message: ingest.FailureMessageFlowIndeterminate, ActionRequired: true,
			},
			Error: "verification failed after a storage timeout; one object could not be retracted",
			Flows: []ingest.FlowResult{
				{FlowID: testCollection, SourceID: testSource, Disposition: ingest.FlowIndeterminate},
				{FlowID: testVideo, SourceID: "88888888-8888-4888-8888-888888888888", Role: "video", Disposition: ingest.FlowWritten, Objects: []ingest.ObjectResult{
					{ObjectID: testStranded, Timerange: "0:0_1:25", Bytes: 1_048_576, SHA256: strings.Repeat("5", 64), Disposition: ingest.ObjectDispositionStranded},
					{ObjectID: testRetracted, Timerange: "1:25_2:25", Bytes: 1_048_576, SHA256: strings.Repeat("6", 64), Disposition: ingest.ObjectDispositionRetracted},
					{ObjectID: testUncertain, Timerange: "2:25_3:25", Bytes: 1_048_576, SHA256: strings.Repeat("7", 64), Disposition: ingest.ObjectDispositionRegistrationIndeterminate},
				}},
			},
		}},
	}
}

type failingWriter struct{ err error }

func (writer failingWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestObjectLabelsPreserveReceiptWording(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		disposition  ingest.ObjectDisposition
		verification ingest.ObjectVerificationStatus
		want         string
	}{
		{"", "", ""},
		{ingest.ObjectDispositionPlanned, "", "planned"},
		{ingest.ObjectDispositionUnattempted, "", "planned"},
		{ingest.ObjectDispositionUploaded, "", "uploaded"},
		{ingest.ObjectDispositionRegistered, "", "registered"},
		{ingest.ObjectDispositionRegistered, ingest.ObjectVerificationVerified, "verified"},
		{ingest.ObjectDispositionRegistrationIndeterminate, "", "registration-indeterminate"},
		{ingest.ObjectDispositionRejected, "", "registration-rejected"},
		{ingest.ObjectDispositionIngested, ingest.ObjectVerificationVerified, "ingested"},
		{ingest.ObjectDispositionResumed, ingest.ObjectVerificationVerified, "resumed"},
		{ingest.ObjectDispositionRetracted, "", "retracted"},
		{ingest.ObjectDispositionStranded, "", "stranded"},
	} {
		object := ingest.ObjectResult{Disposition: test.disposition, Verification: test.verification}
		if got := objectLabel(object); got != test.want {
			t.Errorf("objectLabel(%#v) = %q, want %q", object, got, test.want)
		}
	}
}
