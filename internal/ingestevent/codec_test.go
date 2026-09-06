package ingestevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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
