package source

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/observability"
)

// Bridge exposes one pinned input to media tools without giving them upstream
// credentials. The unguessable path is valid only for this input's lifetime.
type Bridge struct {
	URL      string
	ctx      context.Context
	cancel   context.CancelFunc
	server   *http.Server
	done     chan struct{}
	mu       sync.Mutex
	err      error
	snapshot *Snapshot
	retries  int
	run      *observability.Run
}

func NewBridge(ctx context.Context, snapshot *Snapshot, retries int, run *observability.Run) (*Bridge, error) {
	if snapshot == nil || snapshot.Size <= 0 || snapshot.OpenAt == nil {
		return nil, errors.New("remote input has no finite snapshot")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for private media input")
	}
	ctx, cancel := context.WithCancel(ctx)
	path := "/" + rand.Text()
	b := &Bridge{
		URL: "http://" + listener.Addr().String() + path, ctx: ctx, cancel: cancel,
		snapshot: snapshot, retries: retries, run: run, done: make(chan struct{}),
	}
	b.server = &http.Server{
		ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0),
		BaseContext: func(net.Listener) context.Context { return ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != path || r.URL.RawQuery != "" {
				http.NotFound(w, r)
				return
			}
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			reader := &snapshotReader{bridge: b, ctx: r.Context()}
			defer reader.closeBody()
			w.Header().Set("Content-Type", "application/octet-stream")
			http.ServeContent(w, r, "input", time.Time{}, reader)
		}),
	}
	go func() {
		defer close(b.done)
		_ = b.server.Serve(listener)
	}()
	return b, nil
}

func (b *Bridge) Context() context.Context { return b.ctx }

// Upstream reports the first failed upstream read, if any.
func (b *Bridge) Upstream() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Redact(b.err)
}

// Redact removes the local capability from tool diagnostics.
func (b *Bridge) Redact(err error) error {
	if err == nil {
		return nil
	}
	return &bridgeError{err: err, url: b.URL}
}

// Reader opens the pinned input at offset zero with the retry and failure
// accounting of a tool read through the listener.
func (b *Bridge) Reader(ctx context.Context) io.ReadSeekCloser {
	return &snapshotReader{bridge: b, ctx: ctx}
}

type bridgeError struct {
	err error
	url string
}

func (e *bridgeError) Error() string { return strings.ReplaceAll(e.err.Error(), e.url, "remote input") }
func (e *bridgeError) Unwrap() error { return e.err }

func (b *Bridge) Close() {
	b.cancel()
	_ = b.server.Close()
	<-b.done
}

// reconnectResetBytes is the sustained progress after a reconnect that proves
// the source is serving again. The retry budget bounds a stalled range, not the
// number of reconnects a long transfer may need behind an idle-closing proxy.
const reconnectResetBytes = 1 << 20

type snapshotReader struct {
	bridge   *Bridge
	ctx      context.Context
	position int64
	body     io.ReadCloser
	attempt  int
	progress int64
}

func (r *snapshotReader) closeBody() {
	if r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
}

func (r *snapshotReader) Close() error {
	r.closeBody()
	return nil
}

func (r *snapshotReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.position
	case io.SeekEnd:
		offset += r.bridge.snapshot.Size
	default:
		return 0, errors.New("invalid remote input seek")
	}
	if offset < 0 || offset > r.bridge.snapshot.Size {
		return 0, errors.New("remote input seek outside representation")
	}
	if offset != r.position {
		r.closeBody()
		r.position = offset
	}
	return offset, nil
}

func (r *snapshotReader) Read(buffer []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.body == nil {
			var err error
			r.body, err = r.bridge.snapshot.OpenAt(r.ctx, r.position)
			if err != nil {
				return 0, r.fail(err)
			}
		}
		n, err := r.body.Read(buffer)
		r.position += int64(n)
		r.progress += int64(n)
		if r.attempt > 0 && r.progress >= reconnectResetBytes {
			r.attempt = 0
		}
		if err == nil || err == io.EOF {
			return n, err
		}
		var network net.Error
		if r.ctx.Err() == nil && r.position < r.bridge.snapshot.Size && r.attempt < r.bridge.retries && (errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network)) {
			r.closeBody()
			r.attempt++
			r.progress = 0
			r.bridge.run.Retry(observability.OperationSourceTransfer, r.attempt+1, r.bridge.retries+1, 0, err, 0)
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, r.fail(err)
	}
}

func (r *snapshotReader) fail(err error) error {
	// FFmpeg routinely closes one range to seek elsewhere. That cancellation
	// must not poison the snapshot or be mistaken for a failed source transfer.
	if r.ctx.Err() == nil {
		r.bridge.mu.Lock()
		if r.bridge.err == nil {
			r.bridge.err = err
		}
		r.bridge.mu.Unlock()
		r.bridge.cancel()
	}
	return err
}
