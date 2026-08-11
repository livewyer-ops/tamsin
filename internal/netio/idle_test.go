package netio

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type pacedReader struct {
	chunks int
	delay  time.Duration
}

func (r *pacedReader) Read(buffer []byte) (int, error) {
	if r.chunks == 0 {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	r.chunks--
	return copy(buffer, "media"), nil
}

type contextReader struct{ ctx context.Context }

func (r contextReader) Read([]byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

type contextCloser struct{ ctx context.Context }

func (c contextCloser) Read([]byte) (int, error) { return 0, io.EOF }

func (c contextCloser) Close() error {
	<-c.ctx.Done()
	return c.ctx.Err()
}

func TestIdleWatchAllowsAHealthyLongTransfer(t *testing.T) {
	t.Parallel()
	const idle = 120 * time.Millisecond
	watch := NewIdleWatch(context.Background(), idle)
	body := watch.Body(io.NopCloser(&pacedReader{chunks: 6, delay: 40 * time.Millisecond}))
	started := time.Now()
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		t.Fatalf("healthy transfer failed: %v", err)
	}
	if string(data) != strings.Repeat("media", 6) {
		t.Fatalf("data = %q", data)
	}
	if elapsed := time.Since(started); elapsed <= idle {
		t.Fatalf("test transfer lasted %s, want longer than the idle interval %s", elapsed, idle)
	}
}

func TestIdleWatchCancelsAStalledReadWithItsCause(t *testing.T) {
	t.Parallel()
	const idle = 80 * time.Millisecond
	watch := NewIdleWatch(context.Background(), idle)
	body := watch.Body(io.NopCloser(contextReader{ctx: watch.Context()}))
	_, err := io.ReadAll(body)
	_ = body.Close()
	var timeout *IdleTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("error = %v, want IdleTimeoutError", err)
	}
	if timeout.Duration != idle {
		t.Fatalf("idle duration = %s, want %s", timeout.Duration, idle)
	}
}

func TestIdleWatchPreservesParentCancellation(t *testing.T) {
	t.Parallel()
	parent, cancel := context.WithCancel(context.Background())
	watch := NewIdleWatch(parent, time.Minute)
	cancel()
	body := watch.Body(io.NopCloser(contextReader{ctx: watch.Context()}))
	_, err := io.ReadAll(body)
	_ = body.Close()
	var timeout *IdleTimeoutError
	if errors.As(err, &timeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want parent cancellation and not idle timeout", err)
	}
}

func TestIdleWatchPreservesAParentCancellationCause(t *testing.T) {
	t.Parallel()
	want := errors.New("operator stopped this input")
	parent, cancel := context.WithCancelCause(context.Background())
	watch := NewIdleWatch(parent, time.Minute)
	cancel(want)
	body := watch.Body(io.NopCloser(contextReader{ctx: watch.Context()}))
	_, err := io.ReadAll(body)
	_ = body.Close()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want parent cause %v", err, want)
	}
}

func TestIdleBodyCloseCancelsBeforeClosingTheBody(t *testing.T) {
	t.Parallel()
	watch := NewIdleWatch(context.Background(), time.Minute)
	body := watch.Body(contextCloser{ctx: watch.Context()})
	if err := body.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close error = %v, want the derived context to be cancelled first", err)
	}
}

func TestIdleWatchProgressAndStopAreRaceSafe(t *testing.T) {
	t.Parallel()
	watch := NewIdleWatch(context.Background(), time.Second)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 1_000 {
				watch.Progress()
			}
		})
	}
	group.Go(func() {
		for range 1_000 {
			watch.Stop()
		}
	})
	group.Wait()
}
