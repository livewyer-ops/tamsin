// Package netio contains the network-stream primitives shared by source
// staging and TAMS Media Object transfers.
package netio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// DefaultIdleTimeout is the longest a network media transfer may make no
// byte-level progress without being treated as stalled.
const DefaultIdleTimeout = time.Minute

// IdleTimeoutError reports that a transfer stopped moving bytes. It is
// distinct from an absolute deadline: a transfer may run for hours as long as
// it continues to make progress.
type IdleTimeoutError struct {
	Duration time.Duration
}

func (e *IdleTimeoutError) Error() string {
	return fmt.Sprintf("network transfer made no progress for %s", e.Duration)
}

// Timeout lets callers use the standard timeout classification without
// discarding the more specific idle-timeout cause.
func (*IdleTimeoutError) Timeout() bool { return true }

// Temporary reports that retrying a stalled transfer may succeed.
func (*IdleTimeoutError) Temporary() bool { return true }

// IdleWatch cancels one transfer attempt when its byte stream stops making
// progress. An IdleWatch belongs to exactly one attempt; retries need a fresh
// one so an expired timer cannot poison the next request.
type IdleWatch struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	duration time.Duration

	mu       sync.Mutex
	timer    *time.Timer
	deadline time.Time
	stopped  bool
	paused   bool
}

// NewIdleWatch derives a cancellable request context and starts its no-progress
// clock. A non-positive duration disables the clock while retaining the same
// API, which is useful to lower-level callers; Tamsin's product configuration
// always supplies a positive default.
func NewIdleWatch(parent context.Context, duration time.Duration) *IdleWatch {
	ctx, cancel := context.WithCancelCause(parent)
	watch := &IdleWatch{ctx: ctx, cancel: cancel, duration: duration}
	if duration > 0 {
		watch.deadline = time.Now().Add(duration)
		watch.timer = time.AfterFunc(duration, watch.expire)
	}
	return watch
}

// Context is the context that must govern the network request. Cancelling it
// is what unblocks a Read or Write already waiting inside net/http.
func (w *IdleWatch) Context() context.Context { return w.ctx }

// Progress resets the no-progress clock. It is safe for concurrent use because
// an HTTP transport may read a request body while another goroutine is handling
// cancellation.
func (w *IdleWatch) Progress() {
	if w.duration <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.paused = false
	w.deadline = time.Now().Add(w.duration)
	// Reset schedules the next callback. If the previous callback has already
	// started, it observes this new deadline under the same lock and returns
	// without cancelling the request.
	w.timer.Reset(w.duration)
}

// Reader resets the clock after each successful read. Zero-byte reads are not
// progress and must not keep a dead peer alive indefinitely.
func (w *IdleWatch) Reader(reader io.Reader) io.Reader {
	return idleReader{reader: reader, watch: w}
}

// Body wraps a response body and stops the watchdog when the caller closes it.
// Closing every body is already required by net/http, so this gives the timer
// the same lifetime as the transfer without a second ownership protocol.
func (w *IdleWatch) Body(body io.ReadCloser) io.ReadCloser {
	return &idleBody{reader: idleReader{reader: body, watch: w}, body: body, watch: w}
}

// DemandBody times only reads, not time spent waiting for the consumer. It is
// used by seekable inputs whose consumer can pause while uploading output.
func (w *IdleWatch) DemandBody(body io.ReadCloser) io.ReadCloser {
	w.Pause()
	return &idleBody{reader: idleReader{reader: body, watch: w}, body: body, watch: w, demand: true}
}

// Pause suspends the idle clock without cancelling the request. Progress
// resumes it with a fresh interval.
func (w *IdleWatch) Pause() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = true
	if w.timer != nil {
		w.timer.Stop()
	}
}

// Error replaces the transport's context-cancellation symptom with its actual
// cause. Parent cancellation and absolute deadlines remain distinguishable
// from a no-progress timeout.
func (w *IdleWatch) Error(err error) error {
	if err == nil {
		return nil
	}
	cause := context.Cause(w.ctx)
	if cause == nil {
		return err
	}
	var idle *IdleTimeoutError
	if errors.As(cause, &idle) {
		return idle
	}
	return cause
}

// Stop releases the timer and derived context. It is idempotent.
func (w *IdleWatch) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	if w.timer != nil {
		w.timer.Stop()
	}
	w.mu.Unlock()
	w.cancel(nil)
}

func (w *IdleWatch) expire() {
	w.mu.Lock()
	if w.stopped || w.paused {
		w.mu.Unlock()
		return
	}
	// Reset and callback can race. The deadline, rather than the fact the
	// callback ran, is authoritative: progress just before this lock moves it
	// forward. Progress's Reset has already scheduled the next callback, so
	// this stale one returns rather than scheduling a duplicate.
	if time.Until(w.deadline) > 0 {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	w.mu.Unlock()
	w.cancel(&IdleTimeoutError{Duration: w.duration})
}

type idleReader struct {
	reader io.Reader
	watch  *IdleWatch
}

func (r idleReader) Read(buffer []byte) (int, error) {
	read, err := r.reader.Read(buffer)
	if read > 0 {
		r.watch.Progress()
	}
	return read, r.watch.Error(err)
}

type idleBody struct {
	reader idleReader
	body   io.ReadCloser
	watch  *IdleWatch
	demand bool
}

func (b *idleBody) Read(buffer []byte) (int, error) {
	if b.demand {
		b.watch.Progress()
		defer b.watch.Pause()
	}
	return b.reader.Read(buffer)
}

func (b *idleBody) Close() error {
	// Cancel before delegating so a Close implementation waiting on the
	// request context cannot itself leave teardown blocked forever.
	b.watch.Stop()
	return b.body.Close()
}
