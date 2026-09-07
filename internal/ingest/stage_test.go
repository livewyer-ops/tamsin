package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/source"
)

// truncatingReader delivers a prefix and then fails, standing in for a
// connection dropped partway through a transfer.
type truncatingReader struct {
	data  []byte
	limit int
	read  int
}

func (t *truncatingReader) Read(buffer []byte) (int, error) {
	if t.read >= t.limit {
		return 0, errors.New("connection reset by peer")
	}
	n := copy(buffer, t.data[t.read:t.limit])
	t.read += n
	return n, nil
}

func (t *truncatingReader) Close() error { return nil }

// A truncated transfer must resume from the staged prefix.
func TestStagingResumesAfterATruncatedTransfer(t *testing.T) {
	t.Parallel()
	content := bytes.Repeat([]byte("time-addressable media "), 4096)
	digest := sha256.Sum256(content)

	var offsets []int64
	item := source.Item{
		URI:  "https://example.test/media.ts",
		Name: "media.ts",
		Size: int64(len(content)),
		Open: func(context.Context) (io.ReadCloser, error) {
			// Dies a third of the way in.
			return &truncatingReader{data: content, limit: len(content) / 3}, nil
		},
		Reopen: func(_ context.Context, offset int64) (io.ReadCloser, bool, error) {
			offsets = append(offsets, offset)
			// Serves the remainder, as a store honouring a Range request would.
			return io.NopCloser(bytes.NewReader(content[offset:])), true, nil
		},
	}

	staged, err := stage(context.Background(), item, t.TempDir(), 3, nil, nil)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	defer staged.cleanup()

	if len(offsets) != 1 {
		t.Fatalf("reopened %d times at %v, want one resume", len(offsets), offsets)
	}
	if offsets[0] != int64(len(content)/3) {
		t.Fatalf("resumed at %d, want %d: a resume that restates the offset wrongly corrupts the file",
			offsets[0], len(content)/3)
	}
	if staged.size != int64(len(content)) {
		t.Fatalf("staged %d bytes, want %d", staged.size, len(content))
	}
	if staged.sha256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("staged digest does not match the whole resource")
	}
	onDisk, err := os.ReadFile(staged.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, content) {
		t.Fatalf("staged file differs from the source: got %d bytes, want %d", len(onDisk), len(content))
	}
}

// TestStagingRestartsWhenTheSourceWillNotResume covers the other answer a source
// can give. A server that ignores the range, or whose resource changed, replies
// from the beginning -- and what was already written is then not a prefix of
// what is arriving. Appending would splice two bodies together and produce a
// file that matches neither, with a digest nobody can reproduce.
func TestStagingRestartsWhenTheSourceWillNotResume(t *testing.T) {
	t.Parallel()
	replacement := bytes.Repeat([]byte("the resource changed underneath us "), 2048)
	digest := sha256.Sum256(replacement)

	original := bytes.Repeat([]byte("original contents "), 2048)
	item := source.Item{
		URI:  "https://example.test/media.ts",
		Name: "media.ts",
		Size: int64(len(replacement)),
		Open: func(context.Context) (io.ReadCloser, error) {
			return &truncatingReader{data: original, limit: len(original) / 2}, nil
		},
		Reopen: func(context.Context, int64) (io.ReadCloser, bool, error) {
			return io.NopCloser(bytes.NewReader(replacement)), false, nil
		},
	}

	staged, err := stage(context.Background(), item, t.TempDir(), 3, nil, nil)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	defer staged.cleanup()

	if staged.size != int64(len(replacement)) {
		t.Fatalf("staged %d bytes, want %d: the discarded prefix was not truncated away",
			staged.size, len(replacement))
	}
	if staged.sha256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("staged digest covers more than the restarted body; the hash was not reset")
	}
	onDisk, err := os.ReadFile(staged.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, replacement) {
		t.Fatalf("staged file is not the restarted body")
	}
}

// TestStagingFailsWhenTheSourceCannotResume keeps the bound honest. Standard
// input cannot be reopened, so a failure there has to stand rather than being
// retried into a partial file that looks complete.
func TestStagingFailsWhenTheSourceCannotResume(t *testing.T) {
	t.Parallel()
	content := bytes.Repeat([]byte("streamed "), 1024)
	item := source.Item{
		URI:  "stdin:",
		Name: "stdin",
		Size: -1,
		Open: func(context.Context) (io.ReadCloser, error) {
			return &truncatingReader{data: content, limit: len(content) / 2}, nil
		},
	}
	if _, err := stage(context.Background(), item, t.TempDir(), 3, nil, nil); err == nil {
		t.Fatal("a truncated unresumable source must fail rather than stage a partial file")
	}
}

// TestStagingResumesTwiceAgainstARealServer is the counterpart to the tests
// that refuse an untrustworthy range: having made resumption strict, this
// proves it still happens. Two interruptions mean the second resume runs
// against state left by the first, which is where an off-by-one in the offset
// or a stale validator would show up.
func TestStagingResumesTwiceAgainstARealServer(t *testing.T) {
	t.Parallel()
	content := bytes.Repeat([]byte("time-addressable media "), 512)
	digest := sha256.Sum256(content)

	var (
		lock     sync.Mutex
		attempt  int
		requests []int64
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var offset int64
		if header := request.Header.Get("Range"); header != "" {
			_, _ = fmt.Sscanf(header, "bytes=%d-", &offset)
		}
		lock.Lock()
		attempt++
		current := attempt
		requests = append(requests, offset)
		lock.Unlock()

		writer.Header().Set("ETag", `"v1"`)
		if offset > 0 {
			writer.Header().Set("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", offset, len(content)-1, len(content)))
			writer.WriteHeader(http.StatusPartialContent)
		}
		remaining := content[offset:]
		if current <= 2 {
			// Deliver half, then drop the connection the way a real transfer
			// dies: no trailer, no clean EOF.
			_, _ = writer.Write(remaining[:len(remaining)/2])
			writer.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		_, _ = writer.Write(remaining)
	}))
	defer server.Close()

	resolver := source.New(source.Config{})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/programme.ts"})
	if err != nil {
		t.Fatal(err)
	}
	staged, err := stage(context.Background(), items[0], t.TempDir(), 3, nil, nil)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	defer staged.cleanup()

	if staged.size != int64(len(content)) {
		t.Fatalf("staged %d bytes, want %d", staged.size, len(content))
	}
	if staged.sha256 != hex.EncodeToString(digest[:]) {
		t.Fatal("staged digest does not match the resource")
	}
	onDisk, err := os.ReadFile(staged.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, content) {
		t.Fatal("staged file differs from what the server served")
	}

	lock.Lock()
	defer lock.Unlock()
	if len(requests) != 3 {
		t.Fatalf("server saw %d requests at %v, want three: the initial read and two resumes", len(requests), requests)
	}
	// Each resume must pick up strictly after the last, or bytes are repeated
	// or skipped without anything downstream noticing.
	for index := 1; index < len(requests); index++ {
		if requests[index] <= requests[index-1] {
			t.Fatalf("resume offsets did not advance: %v", requests)
		}
	}
}

// failingWriter accepts a prefix and then refuses, standing in for a staging
// volume that fills up partway through a transfer.
type failingWriter struct {
	accept int
	err    error
}

func (w *failingWriter) Write(data []byte) (int, error) {
	if w.accept <= 0 {
		return 0, w.err
	}
	if len(data) > w.accept {
		accepted := w.accept
		w.accept = 0
		return accepted, w.err
	}
	w.accept -= len(data)
	return len(data), nil
}

// TestCopyDistinguishesWritesFromReads covers which side of a copy failed. The
// two need different answers: a source that dropped is worth reopening, and a
// disk that filled is not.
func TestCopyDistinguishesWritesFromReads(t *testing.T) {
	t.Parallel()
	content := bytes.Repeat([]byte("media"), 4096)

	t.Run("a write failure is attributed to the destination", func(t *testing.T) {
		t.Parallel()
		writer := &failingWriter{accept: 100, err: errors.New("no space left on device")}
		_, err := copyContext(context.Background(), writer, bytes.NewReader(content))
		var destination destinationError
		if !errors.As(err, &destination) {
			t.Fatalf("error = %v, want it attributed to the destination", err)
		}
		if !strings.Contains(err.Error(), "no space left") {
			t.Fatalf("the underlying cause was lost: %v", err)
		}
	})

	t.Run("a read failure is not", func(t *testing.T) {
		t.Parallel()
		reader := &truncatingReader{data: content, limit: 100}
		_, err := copyContext(context.Background(), io.Discard, reader)
		var destination destinationError
		if errors.As(err, &destination) {
			t.Fatalf("a source failure was blamed on the destination: %v", err)
		}
	})
}

// TestWorthResuming pins when a broken transfer is retried. Retrying a full
// disk repeats the whole download to fail in the same place, once per remaining
// attempt, which turns one clear failure into a slow one.
func TestWorthResuming(t *testing.T) {
	t.Parallel()
	dropped := errors.New("connection reset by peer")
	full := destinationError{err: errors.New("no space left on device")}
	for _, testCase := range []struct {
		name      string
		err       error
		canReopen bool
		attempt   int
		retries   int
		want      bool
	}{
		{name: "a dropped source with attempts left", err: dropped, canReopen: true, attempt: 0, retries: 3, want: true},
		{name: "a dropped source with none left", err: dropped, canReopen: true, attempt: 3, retries: 3},
		{name: "a source that cannot be reopened", err: dropped, canReopen: false, attempt: 0, retries: 3},
		{name: "a full disk", err: full, canReopen: true, attempt: 0, retries: 3},
		{name: "no failure at all", err: nil, canReopen: true, attempt: 0, retries: 3},
		{name: "retries switched off", err: dropped, canReopen: true, attempt: 0, retries: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := worthResuming(testCase.err, testCase.canReopen, testCase.attempt, testCase.retries); got != testCase.want {
				t.Fatalf("worthResuming = %v, want %v", got, testCase.want)
			}
		})
	}
}

// Staging must respect the configured retry budget.
func TestStagingHonoursTheConfiguredRetryBudget(t *testing.T) {
	t.Parallel()
	content := bytes.Repeat([]byte("media "), 2048)
	for _, retries := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			t.Parallel()
			reopens := 0
			item := source.Item{
				URI: "https://example.test/media.ts", Name: "media.ts", Size: int64(len(content)),
				Open: func(context.Context) (io.ReadCloser, error) {
					return &truncatingReader{data: content, limit: 10}, nil
				},
				Reopen: func(_ context.Context, offset int64) (io.ReadCloser, bool, error) {
					reopens++
					// Always dies again, so the allowance is what stops it.
					return &truncatingReader{data: content, limit: int(offset) + 10}, true, nil
				},
			}
			run := observability.New("930b40c0-0834-4798-a117-e0a311f53bf8", nil)
			_, err := stage(context.Background(), item, t.TempDir(), retries, nil, run)
			if err == nil {
				t.Fatal("a source that keeps dying must eventually fail")
			}
			wantAttempts := fmt.Sprintf("stage input https://example.test/media.ts failed after %d attempt(s)", retries+1)
			if !strings.Contains(err.Error(), wantAttempts) {
				t.Fatalf("error = %q, want source phase and exact attempt count %q", err, wantAttempts)
			}
			if reopens != retries {
				t.Fatalf("reopened %d times with a budget of %d", reopens, retries)
			}
			if got := run.Snapshot().Retries; got != int64(retries) {
				t.Fatalf("observed retries = %d, want %d", got, retries)
			}
		})
	}
}

func TestStageUsesAFixedFilenameForRemoteInputs(t *testing.T) {
	t.Parallel()
	item := source.Item{
		URI: "https://media.example.test/evil.m3u8", Name: "evil.m3u8", Size: 5,
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("#EXTM")), nil },
	}
	staged, err := stage(context.Background(), item, t.TempDir(), 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.cleanup()
	if filepath.Base(staged.path) != "input.m3u8" {
		t.Fatalf("remote basename reached the staged file: %s", staged.path)
	}
	for name, want := range map[string]string{"": "input.bin", "clip": "input.bin", "../x/clip.APTX": "input.aptx",
		"a.b.verylongextension": "input.bin", "clip.m4v?sig=1": "input.bin", "stdin.bin": "input.bin"} {
		if got := stagedFilename(name); got != want {
			t.Errorf("stagedFilename(%q) = %q, want %q", name, got, want)
		}
	}
}
