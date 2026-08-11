package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRunObservedFlushesEveryIndexOnGracefulInterruption(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	items := make([]source.Item, 4)
	for index := range items {
		filename := filepath.Join(directory, string(rune('a'+index))+".mp4")
		if err := os.WriteFile(filename, []byte{byte(index)}, 0o600); err != nil {
			t.Fatal(err)
		}
		items[index] = localSource(filename)
	}
	pipeline, err := New(Config{Concurrency: 1, DryRun: true}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(map[int]Result, len(items))
	order := make([]int, 0, len(items))
	batch, runErr := pipeline.RunObserved(ctx, items, func(index int, result Result) error {
		if _, duplicate := observed[index]; duplicate {
			t.Fatalf("input %d observed twice", index)
		}
		observed[index] = result
		order = append(order, index)
		if len(order) == 1 {
			cancel()
		}
		return nil
	})
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("RunObserved() error = %v, want context cancellation", runErr)
	}
	if len(observed) != len(items) {
		t.Fatalf("observed %d of %d terminal results: order=%v", len(observed), len(items), order)
	}
	if len(batch.Results) != len(items) || batch.Succeeded+batch.Failed != len(items) {
		t.Fatalf("interrupted batch is not index-complete: %#v", batch)
	}
	for index, result := range batch.Results {
		if observedResult, ok := observed[index]; !ok || observedResult.Input != result.Input || observedResult.Status != result.Status {
			t.Fatalf("observer and final result differ at %d: observed=%#v final=%#v", index, observedResult, result)
		}
		if result.Flows == nil {
			t.Fatalf("terminal result %d has a null Flow inventory", index)
		}
	}
}

func TestRunObservedReportsEveryInputWhenStorePreflightFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	items := make([]source.Item, 2)
	for index := range items {
		filename := filepath.Join(directory, string(rune('a'+index))+".mp4")
		if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
			t.Fatal(err)
		}
		items[index] = localSource(filename)
	}
	client := newFakeClient()
	client.backends = nil
	pipeline, err := New(Config{Concurrency: 2, Verify: true}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var indexes []int
	batch, runErr := pipeline.RunObserved(context.Background(), items, func(index int, result Result) error {
		indexes = append(indexes, index)
		if result.Status != ResultStatusFailed || result.Verification != VerificationNotReached || result.Error == "" {
			t.Fatalf("preflight result %d is not terminal: %#v", index, result)
		}
		return nil
	})
	if runErr == nil {
		t.Fatal("store preflight unexpectedly succeeded")
	}
	if len(indexes) != len(items) || batch.Failed != len(items) {
		t.Fatalf("preflight observed indexes=%v batch=%#v", indexes, batch)
	}
	assertBatchResultSchema(t, batch)
}

func TestRunObservedReportsEveryInputWhenStagingSetupFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	blocked := filepath.Join(directory, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	items := make([]source.Item, 2)
	for index, name := range []string{"first.mp4", "second.mp4"} {
		filename := filepath.Join(directory, name)
		if err := os.WriteFile(filename, []byte{byte(index)}, 0o600); err != nil {
			t.Fatal(err)
		}
		items[index] = localSource(filename)
	}
	pipeline, err := New(Config{
		Concurrency: 2, DryRun: true, Verify: true, TempDirectory: filepath.Join(blocked, "staging"),
	}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}

	var indexes []int
	batch, runErr := pipeline.RunObserved(context.Background(), items, func(index int, result Result) error {
		indexes = append(indexes, index)
		if result.Status != ResultStatusFailed || result.Verification != VerificationNotReached || result.Flows == nil {
			t.Fatalf("staging failure result %d is not terminal: %#v", index, result)
		}
		return nil
	})
	if runErr == nil || !strings.Contains(runErr.Error(), "prepare staging") {
		t.Fatalf("RunObserved() error = %v, want attributed staging failure", runErr)
	}
	if len(indexes) != len(items) || batch.Failed != len(items) {
		t.Fatalf("staging failure observed indexes=%v batch=%#v", indexes, batch)
	}
	assertBatchResultSchema(t, batch)
}

func TestResultContractCarriesTheSelectedProfileVersion(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "input.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := New(Config{
		Profile: ProfilePreserve, ProfileVersion: "1",
		Concurrency: 1, DryRun: true, Verify: true, SegmentFormat: media.SegmentFormatSource,
		EssenceStorage: media.EssenceStorageMuxed,
	}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.ProfileVersion != "1" || pipeline.ResultContract().ProfileVersion != batch.ProfileVersion {
		t.Fatalf("selected profile version was not propagated: %#v", batch)
	}
	if batch.Results[0].Verification != VerificationNotReached {
		t.Fatalf("dry-run verification = %q, want not_reached", batch.Results[0].Verification)
	}
	assertBatchResultSchema(t, batch)
}

func TestResultDropsAuthenticatedHTTPCredentialMaterialWithUppercaseScheme(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "input.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	item := localSource(filename)
	item.URI = "HTTPS://alice:secret@example.test/programme.mp4?token=secret"
	pipeline, err := New(Config{Concurrency: 1, DryRun: true}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{item})
	if err != nil {
		t.Fatal(err)
	}
	input := batch.Results[0].Input
	if strings.Contains(input, "alice") || strings.Contains(input, "secret") || strings.Contains(input, "token") ||
		input != "https://example.test/programme.mp4" {
		t.Fatalf("authenticated input leaked in result: %q", input)
	}
	assertBatchResultSchema(t, batch)
}

func TestReusedPipelineGetsDistinctRunIDs(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "input.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := New(Config{Concurrency: 1, DryRun: true}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID == "" || second.RunID == "" || first.RunID == second.RunID {
		t.Fatalf("run IDs are not invocation-specific: first=%q second=%q", first.RunID, second.RunID)
	}
}

type versionResult struct {
	value string
	err   error
}

type sequencedVersionSegmenter struct {
	versions []versionResult
	calls    int
}

func (s *sequencedVersionSegmenter) Segment(ctx context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	return fakeSegmenter{}.Segment(ctx, request, sink)
}

func (s *sequencedVersionSegmenter) Version(context.Context) (string, error) {
	result := s.versions[min(s.calls, len(s.versions)-1)]
	s.calls++
	return result.value, result.err
}

func TestReusedPipelineRefreshesMediaToolchainPerInvocation(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name          string
		versions      []versionResult
		firstVersion  string
		firstFailed   bool
		secondVersion string
	}{
		{
			name:         "installed tool changed",
			versions:     []versionResult{{value: "ffmpeg version 6.1"}, {value: "ffmpeg version 7.0"}},
			firstVersion: "ffmpeg version 6.1", secondVersion: "ffmpeg version 7.0",
		},
		{
			name:        "transient lookup failure",
			versions:    []versionResult{{err: errors.New("temporary executable error")}, {value: "ffmpeg version 7.0"}},
			firstFailed: true, secondVersion: "ffmpeg version 7.0",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			filename := filepath.Join(t.TempDir(), "input.mp4")
			if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			segmenter := &sequencedVersionSegmenter{versions: testCase.versions}
			pipeline, err := New(Config{
				Concurrency: 1, DryRun: true, SegmentDuration: time.Second,
				EssenceStorage: media.EssenceStorageMuxed,
			}, nil, fakeProber{}, segmenter, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}

			first, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			if got := first.Results[0].FFmpegVersion; got != testCase.firstVersion {
				t.Fatalf("first FFmpeg version = %q, want %q", got, testCase.firstVersion)
			}
			if got := first.Results[0].Status == ResultStatusFailed; got != testCase.firstFailed {
				t.Fatalf("first run failed = %t, want %t: %#v", got, testCase.firstFailed, first.Results[0])
			}

			second, err := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
			if err != nil {
				t.Fatal(err)
			}
			if second.Results[0].Status == ResultStatusFailed || second.Results[0].FFmpegVersion != testCase.secondVersion {
				t.Fatalf("second run retained stale toolchain state: %#v", second.Results[0])
			}
			if segmenter.calls != 2 {
				t.Fatalf("FFmpeg version calls = %d, want one per invocation", segmenter.calls)
			}
		})
	}
}

func TestVerificationStatusUsesTypedVerificationErrorOnly(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{Verify: true}, nil, fakeProber{}, nil, discardLogger(), nil); err == nil {
		// A non-dry pipeline correctly requires a client; reconstruct this focused
		// state-machine fixture as a dry run without changing Verify.
		t.Fatal("New unexpectedly accepted a missing client")
	}
	pipeline, err := New(Config{Verify: true, DryRun: true}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := pipeline.verificationFailureStatus(errors.New("registration cleanup failed")); got != VerificationNotReached {
		t.Fatalf("non-verification cleanup produced verification %q", got)
	}
	if got := pipeline.verificationFailureStatus(&VerificationError{Retracted: 1, Err: errors.New("mismatch")}); got != VerificationFailedRetracted {
		t.Fatalf("typed retraction produced verification %q", got)
	}
	if got := pipeline.verificationFailureStatus(&VerificationError{Stranded: 1, Err: errors.New("delete failed")}); got != VerificationFailedStranded {
		t.Fatalf("typed stranding produced verification %q", got)
	}
}

func TestRunObservedStopsSchedulingButStillReportsEveryTerminalResultAfterOutputFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	items := make([]source.Item, 3)
	for index := range items {
		filename := filepath.Join(directory, string(rune('a'+index))+".mp4")
		if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
			t.Fatal(err)
		}
		items[index] = localSource(filename)
	}
	pipeline, err := New(Config{Concurrency: 1, DryRun: true}, nil, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	batch, runErr := pipeline.RunObserved(context.Background(), items, func(int, Result) error {
		writes++
		return errors.New("journal disk full")
	})
	if runErr == nil || !containsAll(runErr.Error(), "terminal result", "journal disk full") {
		// The observer error itself is returned; cancellation is only the
		// mechanism used to stop additional ingest work.
		t.Fatalf("RunObserved() error = %v, want attributed journal failure", runErr)
	}
	if writes != len(items) {
		t.Fatalf("observer called %d times after its first failure, want every one of %d terminal results", writes, len(items))
	}
	if len(batch.Results) != len(items) {
		t.Fatalf("batch lost indexes after observer failure: %#v", batch)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}

func assertBatchResultSchema(t *testing.T, batch BatchResult) {
	t.Helper()
	const schemaURL = "https://raw.githubusercontent.com/livewyer-ops/tamsin/main/contracts/tamsin/batch-result-v2.json"
	file, err := os.Open("../../contracts/tamsin/batch-result-v2.json")
	if err != nil {
		t.Fatalf("open published result schema: %v", err)
	}
	document, err := jsonschema.UnmarshalJSON(file)
	_ = file.Close()
	if err != nil {
		t.Fatalf("decode published result schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaURL, document); err != nil {
		t.Fatalf("load published result schema: %v", err)
	}
	schema, err := compiler.Compile(schemaURL)
	if err != nil {
		t.Fatalf("compile published result schema: %v", err)
	}
	encoded, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal runtime batch: %v", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode runtime batch: %v", err)
	}
	if err := schema.Validate(instance); err != nil {
		t.Fatalf("runtime batch does not satisfy the published result schema: %v\n%s", err, encoded)
	}
}
