package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/ingestevent"
)

type cliObjectEvent struct {
	FlowID string
	Result ingestevent.ObjectResult
}

type cliInputEventState struct {
	Declared      *ingestevent.InputDeclared
	Started       *ingestevent.InputStarted
	PlannedFlows  map[string]ingestevent.FlowPlanned
	Progress      map[ingestevent.ProgressPhase]ingestevent.ProgressSnapshot
	ObjectResults []cliObjectEvent
	FlowResults   map[string]ingestevent.FlowResult
	Finished      *ingestevent.InputFinished
	Diagnostics   []ingestevent.Diagnostic
	RetryCount    uint64
}

type cliEventState struct {
	RunID        string
	NextSequence uint64
	Hello        *ingestevent.Hello
	Started      *ingestevent.RunStarted
	Manifest     *ingestevent.ManifestFinished
	Inputs       map[int]*cliInputEventState
	Cancellation *ingestevent.RunCancellationRequested
	Finished     *ingestevent.RunFinished
	Diagnostics  []ingestevent.Diagnostic
	RetryCount   uint64
}

// cliIngestEventStream is a small test-only projection of a completed JSON
// ingest. Production exposes NDJSON, not a Go consumer SDK; these assertions
// check the wire records the CLI itself promises.
type cliIngestEventStream struct {
	state     cliEventState
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

	decoder := json.NewDecoder(bytes.NewReader(output))
	stream := cliIngestEventStream{state: cliEventState{Inputs: make(map[int]*cliInputEventState)}}
	for {
		var envelope ingestevent.Envelope
		err := decoder.Decode(&envelope)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode ingest event %d: %v\n%s", len(stream.envelopes), err, output)
		}
		if envelope.Protocol != ingestevent.Protocol || envelope.ProtocolVersion != ingestevent.ProtocolVersion {
			t.Fatalf("event %d has protocol %q version %q", len(stream.envelopes), envelope.Protocol, envelope.ProtocolVersion)
		}
		if envelope.Seq != uint64(len(stream.envelopes)) {
			t.Fatalf("event sequence = %d, want %d", envelope.Seq, len(stream.envelopes))
		}
		if stream.state.RunID == "" {
			stream.state.RunID = envelope.RunID
		} else if envelope.RunID != stream.state.RunID {
			t.Fatalf("event run_id = %q, want %q", envelope.RunID, stream.state.RunID)
		}
		event := decodeCLIEvent(t, envelope)
		stream.envelopes = append(stream.envelopes, envelope)
		stream.events = append(stream.events, event)
		stream.apply(t, envelope, event)
	}

	if len(stream.envelopes) == 0 || stream.envelopes[0].Type != ingestevent.TypeHello {
		t.Fatalf("first event is not hello: %#v", stream.envelopes)
	}
	last := stream.envelopes[len(stream.envelopes)-1]
	if last.Type != ingestevent.TypeRunFinished {
		t.Fatalf("last event type = %q, want %q", last.Type, ingestevent.TypeRunFinished)
	}
	if stream.state.NextSequence != uint64(len(stream.envelopes)) {
		t.Fatalf("projected sequence = %d, decoded events = %d", stream.state.NextSequence, len(stream.envelopes))
	}
	if strings.Count(string(output), "\n") != len(stream.envelopes) {
		t.Fatalf("stdout is not one newline-terminated JSON object per event: %q", output)
	}
	return stream
}

func decodeCLIEvent(t *testing.T, envelope ingestevent.Envelope) ingestevent.Event {
	t.Helper()
	var event ingestevent.Event
	switch envelope.Type {
	case ingestevent.TypeHello:
		event = &ingestevent.Hello{}
	case ingestevent.TypeRunStarted:
		event = &ingestevent.RunStarted{}
	case ingestevent.TypeInputDeclared:
		event = &ingestevent.InputDeclared{}
	case ingestevent.TypeManifestFinished:
		event = &ingestevent.ManifestFinished{}
	case ingestevent.TypeInputStarted:
		event = &ingestevent.InputStarted{}
	case ingestevent.TypeFlowPlanned:
		event = &ingestevent.FlowPlanned{}
	case ingestevent.TypeProgressSnapshot:
		event = &ingestevent.ProgressSnapshot{}
	case ingestevent.TypeRetryScheduled:
		event = &ingestevent.RetryScheduled{}
	case ingestevent.TypeDiagnostic:
		event = &ingestevent.Diagnostic{}
	case ingestevent.TypeObjectResult:
		event = &ingestevent.ObjectResult{}
	case ingestevent.TypeFlowResult:
		event = &ingestevent.FlowResult{}
	case ingestevent.TypeInputFinished:
		event = &ingestevent.InputFinished{}
	case ingestevent.TypeRunCancellationRequested:
		event = &ingestevent.RunCancellationRequested{}
	case ingestevent.TypeRunFinished:
		event = &ingestevent.RunFinished{}
	default:
		t.Fatalf("CLI emitted unknown event type %q", envelope.Type)
	}
	if err := json.Unmarshal(envelope.Payload, event); err != nil {
		t.Fatalf("decode %s payload: %v", envelope.Type, err)
	}
	return event
}

func (s *cliIngestEventStream) apply(t *testing.T, envelope ingestevent.Envelope, event ingestevent.Event) {
	t.Helper()
	s.state.NextSequence++
	switch value := event.(type) {
	case *ingestevent.Hello:
		s.state.Hello = value
	case *ingestevent.RunStarted:
		s.state.Started = value
	case *ingestevent.ManifestFinished:
		s.state.Manifest = value
	case *ingestevent.RunCancellationRequested:
		s.state.Cancellation = value
	case *ingestevent.RunFinished:
		s.state.Finished = value
	default:
		if envelope.Scope == nil || envelope.Scope.InputIndex == nil {
			if diagnostic, ok := event.(*ingestevent.Diagnostic); ok {
				s.state.Diagnostics = append(s.state.Diagnostics, *diagnostic)
				return
			}
			if _, ok := event.(*ingestevent.RetryScheduled); ok {
				s.state.RetryCount++
				return
			}
			t.Fatalf("%s event has no input scope", envelope.Type)
		}
	}

	if envelope.Scope == nil || envelope.Scope.InputIndex == nil {
		return
	}
	index := *envelope.Scope.InputIndex
	input := s.state.Inputs[index]
	if input == nil {
		input = &cliInputEventState{
			PlannedFlows: make(map[string]ingestevent.FlowPlanned),
			Progress:     make(map[ingestevent.ProgressPhase]ingestevent.ProgressSnapshot),
			FlowResults:  make(map[string]ingestevent.FlowResult),
		}
		s.state.Inputs[index] = input
	}
	switch value := event.(type) {
	case *ingestevent.InputDeclared:
		input.Declared = value
	case *ingestevent.InputStarted:
		input.Started = value
	case *ingestevent.FlowPlanned:
		input.PlannedFlows[value.FlowID] = *value
	case *ingestevent.ProgressSnapshot:
		input.Progress[value.Phase] = *value
	case *ingestevent.RetryScheduled:
		input.RetryCount++
		s.state.RetryCount++
	case *ingestevent.Diagnostic:
		input.Diagnostics = append(input.Diagnostics, *value)
	case *ingestevent.ObjectResult:
		input.ObjectResults = append(input.ObjectResults, cliObjectEvent{FlowID: envelope.Scope.FlowID, Result: *value})
	case *ingestevent.FlowResult:
		input.FlowResults[value.FlowID] = *value
	case *ingestevent.InputFinished:
		input.Finished = value
	}
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
		"ingest", "--format", "json", "--dry-run=fast", "--input", filepath.Join(t.TempDir(), "missing.ts"),
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
		"ingest", "--format", "human", "--dry-run=fast", "--input", filepath.Join(t.TempDir(), "missing.ts"),
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
