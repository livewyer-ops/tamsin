package ingestevent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrEventTooLarge = errors.New("encoded ingest event exceeds advertised max_event_bytes")

type flushWriter interface {
	Flush() error
}

// Encoder serialises one process-wide NDJSON stream. Emit is safe for
// concurrent callers.
type Encoder struct {
	mu            sync.Mutex
	sink          io.Writer
	flusher       flushWriter
	runID         string
	now           func() time.Time
	started       time.Time
	nextSequence  uint64
	lastElapsedMS uint64
	maxEventBytes uint64
	failed        error
	finished      bool
	final         bool
	finalErr      error
}

func NewEncoder(sink io.Writer, runID string) (*Encoder, error) {
	return newEncoder(sink, runID, time.Now)
}

func newEncoder(sink io.Writer, runID string, now func() time.Time) (*Encoder, error) {
	if sink == nil {
		return nil, errors.New("ingest event sink is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("ingest event run ID must be a UUID: %w", err)
	}
	if now == nil {
		return nil, errors.New("ingest event clock is required")
	}
	started := now()
	if started.IsZero() {
		return nil, errors.New("ingest event clock returned a zero time")
	}
	encoder := &Encoder{sink: sink, runID: runID, now: now, started: started}
	if flusher, ok := sink.(flushWriter); ok {
		encoder.flusher = flusher
	}
	return encoder, nil
}

// Emit writes and flushes one event record. Ordering beyond the process-level
// hello and terminal boundaries belongs to the CLI state machine that produces
// the events.
func (e *Encoder) Emit(scope *Scope, event Event) (Envelope, error) {
	if e == nil {
		return Envelope{}, errors.New("nil ingest event encoder")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.final {
		return Envelope{}, errors.New("ingest event encoder is finalised")
	}
	if e.finished {
		return Envelope{}, errors.New("ingest event follows run.finished")
	}
	if e.failed != nil {
		return Envelope{}, fmt.Errorf("ingest event encoder previously failed: %w", e.failed)
	}
	if event == nil {
		return Envelope{}, errors.New("ingest event payload is required")
	}
	if e.nextSequence == 0 && event.EventType() != TypeHello {
		return Envelope{}, errors.New("first ingest event must be hello")
	}
	if e.nextSequence > 0 && event.EventType() == TypeHello {
		return Envelope{}, errors.New("hello may only be the first ingest event")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", event.EventType(), err)
	}
	emittedAt := e.now()
	elapsed := emittedAt.Sub(e.started)
	var elapsedMS uint64
	if elapsed > 0 {
		elapsedMS = uint64(elapsed / time.Millisecond)
	}
	if elapsedMS < e.lastElapsedMS {
		elapsedMS = e.lastElapsedMS
	}
	envelope := Envelope{
		Protocol: Protocol, ProtocolVersion: ProtocolVersion, Type: event.EventType(),
		Seq: e.nextSequence, RunID: e.runID, EmittedAt: emittedAt.UTC(), ElapsedMS: elapsedMS,
		Scope: cloneScope(scope), Payload: payload,
	}
	var line bytes.Buffer
	jsonEncoder := json.NewEncoder(&line)
	jsonEncoder.SetEscapeHTML(false)
	if err := jsonEncoder.Encode(envelope); err != nil {
		return Envelope{}, fmt.Errorf("marshal %s envelope: %w", envelope.Type, err)
	}
	limit := e.maxEventBytes
	if hello, ok := helloPayload(event); ok {
		limit = hello.MaxEventBytes
	}
	if limit > 0 && uint64(line.Len()) > limit {
		return Envelope{}, fmt.Errorf("%w: sequence %d is %d bytes, limit is %d", ErrEventTooLarge, envelope.Seq, line.Len(), limit)
	}
	if err := writeAll(e.sink, line.Bytes()); err != nil {
		e.failed = fmt.Errorf("write event sequence %d: %w", envelope.Seq, err)
		return Envelope{}, e.failed
	}
	if e.flusher != nil {
		if err := e.flusher.Flush(); err != nil {
			e.failed = fmt.Errorf("flush event sequence %d: %w", envelope.Seq, err)
			return Envelope{}, e.failed
		}
	}
	e.nextSequence++
	e.lastElapsedMS = elapsedMS
	if hello, ok := helloPayload(event); ok {
		e.maxEventBytes = hello.MaxEventBytes
	}
	if event.EventType() == TypeRunFinished {
		e.finished = true
	}
	return envelope, nil
}

// Finalize checks that the producer emitted the terminal record. It never
// closes the caller-owned writer.
func (e *Encoder) Finalize() error {
	if e == nil {
		return errors.New("nil ingest event encoder")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.final {
		return e.finalErr
	}
	e.final = true
	switch {
	case e.failed != nil:
		e.finalErr = fmt.Errorf("ingest event encoder previously failed: %w", e.failed)
	case !e.finished:
		e.finalErr = errors.New("ingest event stream ended before run.finished")
	}
	return e.finalErr
}

func helloPayload(event Event) (Hello, bool) {
	switch value := event.(type) {
	case Hello:
		return value, true
	case *Hello:
		if value != nil {
			return *value, true
		}
	}
	return Hello{}, false
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func cloneScope(scope *Scope) *Scope {
	if scope == nil {
		return nil
	}
	result := *scope
	result.InputIndex = clonePtr(scope.InputIndex)
	return &result
}
