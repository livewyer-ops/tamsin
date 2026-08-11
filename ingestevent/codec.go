package ingestevent

import (
	"bufio"
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

// Publisher is the narrow boundary the CLI can give to ingest adapters and
// presentation code. The ingest package itself does not need to import this
// package, avoiding a protocol/domain dependency cycle.
type Publisher interface {
	Emit(scope *Scope, event Event) (Envelope, error)
}

type flushWriter interface {
	Flush() error
}

// Encoder serializes one process-wide NDJSON stream. Emit is safe for
// concurrent callers: sequence allocation, semantic validation, serialization,
// write, and flush happen under one lock.
type Encoder struct {
	mu            sync.Mutex
	sink          io.Writer
	flusher       flushWriter
	runID         string
	now           func() time.Time
	started       time.Time
	lastElapsedMS uint64
	reducer       *Reducer
	failed        error
	final         bool
	finalErr      error
}

// NewEncoder starts an empty stream for runID. It intentionally requires no
// ingest ResultContract, profile, or input manifest, allowing configuration and
// startup failures to remain representable. The first Emit must be Hello.
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
	encoder := &Encoder{sink: sink, runID: runID, now: now, started: started, reducer: NewReducer()}
	if flusher, ok := sink.(flushWriter); ok {
		encoder.flusher = flusher
	}
	return encoder, nil
}

// Emit writes and immediately flushes one independently valid JSON record.
// Semantic errors do not poison the stream and may be corrected by the caller;
// a write/flush error does poison it because partial delivery is unknowable.
func (e *Encoder) Emit(scope *Scope, event Event) (Envelope, error) {
	if e == nil {
		return Envelope{}, errors.New("nil ingest event encoder")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.final {
		return Envelope{}, errors.New("ingest event encoder is finalized")
	}
	if e.failed != nil {
		return Envelope{}, fmt.Errorf("ingest event encoder previously failed: %w", e.failed)
	}
	if event == nil {
		return Envelope{}, errors.New("ingest event payload is required")
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
		Seq: e.reducer.state.NextSequence, RunID: e.runID, EmittedAt: emittedAt.UTC(), ElapsedMS: elapsedMS,
		Scope: cloneScope(scope), Payload: payload,
	}
	var line bytes.Buffer
	jsonEncoder := json.NewEncoder(&line)
	jsonEncoder.SetEscapeHTML(false)
	if err := jsonEncoder.Encode(envelope); err != nil {
		return Envelope{}, fmt.Errorf("marshal %s envelope: %w", envelope.Type, err)
	}
	maxBytes := e.advertisedMaxEventBytes(event)
	if maxBytes > 0 && uint64(line.Len()) > maxBytes {
		return Envelope{}, fmt.Errorf("%w: sequence %d is %d bytes, limit is %d", ErrEventTooLarge, envelope.Seq, line.Len(), maxBytes)
	}
	if err := e.reducer.Apply(envelope); err != nil {
		return Envelope{}, err
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
	e.lastElapsedMS = elapsedMS
	return envelope, nil
}

// Finalize verifies that run.finished was emitted. Emit already flushed the
// terminal record; flushing a second time here could report failure after a
// consumer had received a valid run.finished and make the actual process exit
// disagree with the exit_code it contains. Finalize does not close the
// underlying writer (which may be os.Stdout).
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
	if e.failed != nil {
		e.finalErr = fmt.Errorf("ingest event encoder previously failed: %w", e.failed)
		return e.finalErr
	}
	if err := e.reducer.Finalize(); err != nil {
		e.finalErr = err
		return e.finalErr
	}
	return nil
}

// Decoder reads one bounded NDJSON record at a time without bufio.Scanner's
// 64 KiB token limit. It begins with the protocol's hard maximum and adopts the
// lower max_event_bytes advertised by a valid-looking hello envelope. Envelope
// and payload fields added by compatible v2 minors remain ignored.
type Decoder struct {
	reader   *bufio.Reader
	maxBytes uint64
	failed   error
}

// NewDecoder prepares a bounded decoder. A nil source is reported by Decode so
// callers can construct and consume a decoder through one uniform error path.
func NewDecoder(source io.Reader) *Decoder {
	decoder := &Decoder{maxBytes: AdvertisedMaxEventBytesLimit}
	if source == nil {
		decoder.failed = errors.New("ingest event source is required")
		return decoder
	}
	decoder.reader = bufio.NewReader(source)
	return decoder
}

// Decode reads one transport envelope. Use Reducer.Apply as well when semantic
// ordering, lifecycle, terminal coverage, and protocol compatibility matter.
func (d *Decoder) Decode() (Envelope, error) {
	if d == nil {
		return Envelope{}, errors.New("nil ingest event decoder")
	}
	if d.failed != nil {
		return Envelope{}, d.failed
	}
	line, err := d.readLine()
	if err != nil {
		return Envelope{}, err
	}
	jsonDecoder := json.NewDecoder(bytes.NewReader(line))
	var envelope Envelope
	if err := jsonDecoder.Decode(&envelope); err != nil {
		d.failed = err
		return Envelope{}, d.failed
	}
	var trailing any
	if err := jsonDecoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values occur on one NDJSON line")
		}
		d.failed = err
		return Envelope{}, d.failed
	}
	if envelope.Protocol == Protocol && envelope.Seq == 0 && envelope.Type == TypeHello {
		var hello Hello
		if err := json.Unmarshal(envelope.Payload, &hello); err == nil &&
			hello.MaxEventBytes >= MinimumMaxEventBytes && hello.MaxEventBytes <= AdvertisedMaxEventBytesLimit {
			d.maxBytes = hello.MaxEventBytes
		}
	}
	return envelope, nil
}

func (d *Decoder) readLine() ([]byte, error) {
	var line []byte
	for {
		fragment, err := d.reader.ReadSlice('\n')
		if uint64(len(line))+uint64(len(fragment)) > d.maxBytes {
			d.failed = fmt.Errorf("%w: NDJSON line exceeds %d bytes", ErrEventTooLarge, d.maxBytes)
			return nil, d.failed
		}
		line = append(line, fragment...)
		switch {
		case err == nil:
			if len(bytes.TrimSpace(line)) == 0 {
				d.failed = errors.New("empty line in ingest event stream")
				return nil, d.failed
			}
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(line) == 0 {
				return nil, io.EOF
			}
			if len(bytes.TrimSpace(line)) == 0 {
				d.failed = errors.New("empty trailing line in ingest event stream")
				return nil, d.failed
			}
			return line, nil
		default:
			d.failed = err
			return nil, d.failed
		}
	}
}

// Reduce decodes and validates an entire stream. EOF without run.finished is
// returned as ErrIncompleteStream.
func Reduce(source io.Reader) (State, error) {
	return ReduceWithOptions(source, ReducerOptions{})
}

// ReduceWithOptions decodes and validates a complete stream using the selected
// Object observation or retention policy. It returns the committed partial
// state alongside any decode, validation, observer, or incomplete-stream error.
func ReduceWithOptions(source io.Reader, options ReducerOptions) (State, error) {
	decoder := NewDecoder(source)
	reducer := NewReducerWithOptions(options)
	for {
		envelope, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			if err := reducer.Finalize(); err != nil {
				return reducer.Snapshot(), err
			}
			return reducer.Snapshot(), nil
		}
		if err != nil {
			return reducer.Snapshot(), fmt.Errorf("decode ingest event sequence %d: %w", reducer.state.NextSequence, err)
		}
		if err := reducer.Apply(envelope); err != nil {
			return reducer.Snapshot(), err
		}
	}
}

// DecodeEvent decodes a known payload. Unknown event types are deliberately
// returned as known=false with no error so compatible v2 consumers can ignore
// event types added in future minor releases.
func DecodeEvent(envelope Envelope) (event Event, known bool, err error) {
	decode := func(target Event) (Event, bool, error) {
		if err := json.Unmarshal(envelope.Payload, target); err != nil {
			return nil, true, fmt.Errorf("decode %s payload: %w", envelope.Type, err)
		}
		return dereferenceEvent(target), true, nil
	}
	switch envelope.Type {
	case TypeHello:
		return decode(&Hello{})
	case TypeRunStarted:
		return decode(&RunStarted{})
	case TypeInputDeclared:
		return decode(&InputDeclared{})
	case TypeManifestFinished:
		return decode(&ManifestFinished{})
	case TypeInputStarted:
		return decode(&InputStarted{})
	case TypeFlowPlanned:
		return decode(&FlowPlanned{})
	case TypeProgressSnapshot:
		return decode(&ProgressSnapshot{})
	case TypeRetryScheduled:
		return decode(&RetryScheduled{})
	case TypeDiagnostic:
		return decode(&Diagnostic{})
	case TypeObjectResult:
		return decode(&ObjectResult{})
	case TypeFlowResult:
		return decode(&FlowResult{})
	case TypeInputFinished:
		return decode(&InputFinished{})
	case TypeRunCancellationRequested:
		return decode(&RunCancellationRequested{})
	case TypeRunFinished:
		return decode(&RunFinished{})
	default:
		return nil, false, nil
	}
}

func dereferenceEvent(event Event) Event {
	switch value := event.(type) {
	case *Hello:
		return *value
	case *RunStarted:
		return *value
	case *InputDeclared:
		return *value
	case *ManifestFinished:
		return *value
	case *InputStarted:
		return *value
	case *FlowPlanned:
		return *value
	case *ProgressSnapshot:
		return *value
	case *RetryScheduled:
		return *value
	case *Diagnostic:
		return *value
	case *ObjectResult:
		return *value
	case *FlowResult:
		return *value
	case *InputFinished:
		return *value
	case *RunCancellationRequested:
		return *value
	case *RunFinished:
		return *value
	default:
		return event
	}
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

func (e *Encoder) advertisedMaxEventBytes(event Event) uint64 {
	switch value := event.(type) {
	case Hello:
		return value.MaxEventBytes
	case *Hello:
		if value != nil {
			return value.MaxEventBytes
		}
	}
	if e.reducer.state.Hello != nil {
		return e.reducer.state.Hello.MaxEventBytes
	}
	return 0
}
