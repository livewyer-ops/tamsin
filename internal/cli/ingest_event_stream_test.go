package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/ingestevent"
)

// cliIngestEventStream is the consumer-facing view of a completed `--format
// json` ingest. Tests deliberately exercise the public decoder and reducer
// rather than inspecting JSON fields ad hoc: a forked UI must be able to
// replay the same bytes without relying on stderr or event timing.
type cliIngestEventStream struct {
	state     ingestevent.State
	envelopes []ingestevent.Envelope
	events    []ingestevent.Event
}

func decodeCLIIngestEventStream(t *testing.T, output []byte) cliIngestEventStream {
	t.Helper()
	if len(output) == 0 {
		t.Fatal("ingest event stream is empty")
	}
	if bytes.Contains(output, []byte{'\r'}) || bytes.Contains(output, []byte("\x1b[")) {
		t.Fatalf("structured stdout contains terminal control bytes: %q", output)
	}

	state, err := ingestevent.ReduceWithOptions(bytes.NewReader(output), ingestevent.ReducerOptions{RetainObjectResults: true})
	if err != nil {
		t.Fatalf("reduce ingest event stream: %v\n%s", err, output)
	}

	decoder := ingestevent.NewDecoder(bytes.NewReader(output))
	stream := cliIngestEventStream{state: state}
	for {
		envelope, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode ingest event %d: %v\n%s", len(stream.envelopes), err, output)
		}
		event, known, err := ingestevent.DecodeEvent(envelope)
		if err != nil {
			t.Fatalf("decode %s payload: %v", envelope.Type, err)
		}
		if !known {
			t.Fatalf("CLI emitted unknown v2 event type %q", envelope.Type)
		}
		stream.envelopes = append(stream.envelopes, envelope)
		stream.events = append(stream.events, event)
	}

	if len(stream.envelopes) == 0 || stream.envelopes[0].Type != ingestevent.TypeHello {
		t.Fatalf("first event is not hello: %#v", stream.envelopes)
	}
	last := stream.envelopes[len(stream.envelopes)-1]
	if last.Type != ingestevent.TypeRunFinished {
		t.Fatalf("last event type = %q, want %q", last.Type, ingestevent.TypeRunFinished)
	}
	if state.NextSequence != uint64(len(stream.envelopes)) {
		t.Fatalf("reduced sequence = %d, decoded events = %d", state.NextSequence, len(stream.envelopes))
	}
	if strings.Count(string(output), "\n") != len(stream.envelopes) {
		t.Fatalf("stdout is not one newline-terminated JSON object per event: %q", output)
	}
	return stream
}

func (s cliIngestEventStream) eventsOfType(eventType ingestevent.Type) []ingestevent.Event {
	result := make([]ingestevent.Event, 0)
	for index, envelope := range s.envelopes {
		if envelope.Type == eventType {
			result = append(result, s.events[index])
		}
	}
	return result
}

func TestJSONIngestCancellationOwnsTheTerminalExitCode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	code := Execute(ctx, []string{
		"ingest", "--format", "json", "--dry-run", "--input", filepath.Join(t.TempDir(), "missing.ts"),
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitInterrupted {
		t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, ExitInterrupted, stdout.String(), stderr.String())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	if stream.state.Cancellation == nil || stream.state.Finished == nil ||
		stream.state.Finished.Outcome != ingestevent.RunInterrupted || stream.state.Finished.ExitCode != code {
		t.Fatalf("cancellation and terminal state diverged: %#v", stream.state)
	}
}

func TestHumanIngestCallerCancellationOwnsWrappedSourceExit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	code := Execute(ctx, []string{
		"ingest", "--format", "human", "--dry-run", "--input", filepath.Join(t.TempDir(), "missing.ts"),
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitInterrupted {
		t.Fatalf("caller cancellation exit = %d, want %d; stdout=%s stderr=%s", code, ExitInterrupted, stdout.String(), stderr.String())
	}
}

func TestJSONIngestArgumentFailureUsesUsageExitCode(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"ingest", "--format", "json", "first.ts", "https://tams.example.test", "extra",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, ExitUsage, stdout.String(), stderr.String())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	if stream.state.Finished == nil || stream.state.Finished.ExitCode != code ||
		stream.state.Finished.Outcome != ingestevent.RunFailed {
		t.Fatalf("argument failure terminal state = %#v", stream.state.Finished)
	}
}

func TestJSONIngestUnknownFlagIsStructuredRegardlessOfFlagOrder(t *testing.T) {
	t.Parallel()
	for _, arguments := range [][]string{
		{"ingest", "--format", "json", "--unknown-output-test-flag"},
		{"ingest", "--unknown-output-test-flag", "--format", "json"},
		{"ingest", "--unknown-output-test-flag", "--format=json"},
	} {
		arguments := arguments
		t.Run(strings.Join(arguments, "_"), func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
			if code != ExitUsage {
				t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, ExitUsage, stdout.String(), stderr.String())
			}
			stream := decodeCLIIngestEventStream(t, stdout.Bytes())
			if stream.state.Finished == nil || stream.state.Finished.ExitCode != code {
				t.Fatalf("unknown-flag stream terminal = %#v", stream.state.Finished)
			}
			if stderr.Len() != 0 {
				t.Fatalf("JSON bootstrap failure emitted an unstructured footer: %q", stderr.String())
			}
		})
	}
}

func TestLastExplicitHumanFormatSuppressesBootstrapJSON(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"ingest", "--format", "json", "--unknown-output-test-flag", "--format", "human",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage || stdout.Len() != 0 || !strings.Contains(stderr.String(), "unknown flag") {
		t.Fatalf("last explicit human format was not authoritative: exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestRequestedIngestFormatUsesLastFlagBeforeDoubleDash(t *testing.T) {
	t.Parallel()
	tests := []struct {
		arguments []string
		found     bool
		json      bool
	}{
		{arguments: []string{"ingest", "--format", "json"}, found: true, json: true},
		{arguments: []string{"ingest", "--format=json"}, found: true, json: true},
		{arguments: []string{"--format", "json", "ingest", "--unknown"}, found: true, json: true},
		{arguments: []string{"ingest", "--format", "json", "--format=human"}, found: true},
		{arguments: []string{"ingest", "--format", "human", "--format=json"}, found: true, json: true},
		{arguments: []string{"ingest", "--", "--format", "json"}},
	}
	for _, testCase := range tests {
		found, jsonRequested := requestedIngestFormat(testCase.arguments)
		if found != testCase.found || jsonRequested != testCase.json {
			t.Errorf("requestedIngestFormat(%q) = (%t, %t), want (%t, %t)",
				testCase.arguments, found, jsonRequested, testCase.found, testCase.json)
		}
	}
}
