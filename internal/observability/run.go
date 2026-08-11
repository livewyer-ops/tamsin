// Package observability provides the deliberately small, secret-safe
// operational vocabulary shared by input resolution, ingest, and the TAMS
// client.
//
// It never accepts a URL, header, response body, or error message as a log
// attribute. Callers supply typed operations and ordinary errors/status codes;
// this package reduces them to fixed classes before emitting anything.
package observability

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/netio"
)

// Operation is a closed vocabulary for a retryable operation. Keeping this a
// typed value rather than caller-provided prose prevents a locator or provider
// message from accidentally becoming an operation name.
type Operation uint8

const (
	OperationUnknown Operation = iota
	OperationSourceRequest
	OperationSourceTransfer
	OperationS3Request
	OperationTAMSMetadata
	OperationObjectUpload
	OperationObjectVerification
)

func (o Operation) String() string {
	switch o {
	case OperationSourceRequest:
		return "source_request"
	case OperationSourceTransfer:
		return "source_transfer"
	case OperationS3Request:
		return "s3_request"
	case OperationTAMSMetadata:
		return "tams_metadata"
	case OperationObjectUpload:
		return "object_upload"
	case OperationObjectVerification:
		return "object_verification"
	default:
		return "unknown"
	}
}

// Outcome is the terminal verification state of one unique TAMS Media Object.
type Outcome uint8

const (
	OutcomeUnknown Outcome = iota
	OutcomeVerified
	OutcomeRetracted
	OutcomeStranded
)

// Snapshot is a point-in-time copy of run metrics. Byte counters describe
// unique logical payloads completed by this invocation, not wire bytes: failed
// attempts and resumed prefixes are intentionally not counted twice.
type Snapshot struct {
	Elapsed       time.Duration
	BytesStaged   int64
	BytesUploaded int64
	BytesVerified int64
	Retries       int64
	Verified      int64
	Retracted     int64
	Stranded      int64
}

// RetryEvent is the secret-safe semantic form of a scheduled retry. It is
// suitable for machine event streams because every string belongs to a closed
// vocabulary; raw URLs, provider messages, response bodies, and credentials
// never cross this boundary.
type RetryEvent struct {
	Operation   Operation
	Attempt     int
	MaxAttempts int
	StatusClass string
	ErrorClass  string
	Backoff     time.Duration
}

// RetryObserver receives retries after their counters have been committed.
// Implementations must return promptly. A CLI event publisher may use the
// callback to cancel its run when its output stream has been closed.
type RetryObserver func(RetryEvent)

// Run owns the correlation ID, safe retry events, and concurrency-safe metrics
// for one CLI invocation.
type Run struct {
	runID   string
	logger  *slog.Logger
	summary *slog.Logger
	started time.Time
	now     func() time.Time

	mu          sync.Mutex
	summaryOnce sync.Once
	retry       RetryObserver
	metrics     Snapshot
}

// SetRetryObserver replaces the run's retry observer. It is safe to call
// before or during a run; the CLI installs it before creating any clients.
func (r *Run) SetRetryObserver(observer RetryObserver) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.retry = observer
	r.mu.Unlock()
}

// New starts a run with one logger for retry and terminal records.
func New(runID string, logger *slog.Logger) *Run {
	return newRun(runID, logger, logger)
}

// NewWithSummaryLogger lets a terminal progress view mute ordinary info events
// while it redraws, then emit the one terminal info record through summary
// after the progress reporter has closed. Both loggers are decorated with the
// same run_id.
func NewWithSummaryLogger(runID string, logger, summary *slog.Logger) *Run {
	return newRun(runID, logger, summary)
}

func newRun(runID string, logger, summaryLogger *slog.Logger) *Run {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if summaryLogger == nil {
		summaryLogger = logger
	}
	now := time.Now
	return &Run{
		runID: runID, logger: logger.With("run_id", runID), summary: summaryLogger.With("run_id", runID),
		started: now(), now: now,
	}
}

// RunID is the identifier carried by diagnostics and the versioned result and
// journal contracts.
func (r *Run) RunID() string {
	if r == nil {
		return ""
	}
	return r.runID
}

// Logger returns the run-correlated logger.
func (r *Run) Logger() *slog.Logger {
	if r == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return r.logger
}

// Retry records one additional attempt that has actually been scheduled.
// attempt is the one-based number of the next attempt and max is the maximum
// total attempts, including the initial request.
func (r *Run) Retry(operation Operation, attempt, max, statusCode int, cause error, backoff time.Duration) {
	if r == nil {
		return
	}
	event := RetryEvent{
		Operation: operation, Attempt: attempt, MaxAttempts: max,
		StatusClass: statusClass(statusCode), ErrorClass: errorClass(cause), Backoff: backoff,
	}
	r.mu.Lock()
	r.metrics.Retries++
	observer := r.retry
	r.mu.Unlock()
	r.logger.Debug("retry scheduled",
		"operation", event.Operation.String(),
		"attempt", event.Attempt,
		"max_attempts", event.MaxAttempts,
		"status_class", event.StatusClass,
		"error_class", event.ErrorClass,
		"backoff", event.Backoff)
	if observer != nil {
		observer(event)
	}
}

// Failure emits one secret-safe terminal failure record. The ordinary error is
// reduced to the same closed class vocabulary as retries; its message is never
// accepted as a log attribute.
func (r *Run) Failure(err error) {
	if r == nil || err == nil {
		return
	}
	r.logger.Error("ingest run failed", "error_class", errorClass(err))
}

// Staged records one successfully staged resolved input. A Run belongs to one
// invocation, whose lifecycle reports each completion exactly once.
func (r *Run) Staged(bytes int64) {
	if r == nil || bytes < 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.BytesStaged += bytes
}

// Uploaded records one Media Object whose upload completed in this invocation.
// Retries are internal to that operation and call this method only on success;
// resumed Objects do not pass through the upload completion path.
func (r *Run) Uploaded(bytes int64) {
	if r == nil || bytes < 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.BytesUploaded += bytes
}

// Verification records one Object's terminal verification outcome in this
// invocation. Only successful byte-for-byte verification contributes to
// BytesVerified.
func (r *Run) Verification(bytes int64, outcome Outcome) {
	if r == nil || bytes < 0 || outcome == OutcomeUnknown {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch outcome {
	case OutcomeVerified:
		r.metrics.Verified++
		r.metrics.BytesVerified += bytes
	case OutcomeRetracted:
		r.metrics.Retracted++
	case OutcomeStranded:
		r.metrics.Stranded++
	}
}

// Snapshot returns a consistent metrics view.
func (r *Run) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := r.metrics
	result.Elapsed = r.now().Sub(r.started)
	return result
}

// Summary emits the terminal, human/operator-facing metrics record. It is a
// diagnostic only: the versioned JSON result shape is deliberately unchanged.
func (r *Run) Summary() {
	if r == nil {
		return
	}
	r.summaryOnce.Do(func() {
		metrics := r.Snapshot()
		r.summary.Info("ingest run metrics",
			"elapsed", metrics.Elapsed,
			"bytes_staged", metrics.BytesStaged,
			"bytes_uploaded", metrics.BytesUploaded,
			"bytes_verified", metrics.BytesVerified,
			"retries", metrics.Retries,
			"verified", metrics.Verified,
			"retracted", metrics.Retracted,
			"stranded", metrics.Stranded)
	})
}

func statusClass(status int) string {
	switch {
	case status == 0:
		return "none"
	case status == 408 || status == 504:
		return "timeout"
	case status == 429:
		return "throttled"
	case status >= 500 && status <= 599:
		return "server_error"
	case status >= 400 && status <= 499:
		return "client_error"
	default:
		return "other_http"
	}
}

func errorClass(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	var idle *netio.IdleTimeoutError
	if errors.As(err, &idle) {
		return "idle_timeout"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "timeout"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return "truncated_stream"
	}
	return "transport"
}
