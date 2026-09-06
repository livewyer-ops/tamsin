package media

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type disappearingSegmentEntry struct{}

func (disappearingSegmentEntry) Name() string               { return "gone.ts" }
func (disappearingSegmentEntry) IsDir() bool                { return false }
func (disappearingSegmentEntry) Type() fs.FileMode          { return 0 }
func (disappearingSegmentEntry) Info() (fs.FileInfo, error) { return nil, os.ErrNotExist }

func TestSegmentDirectorySamplingToleratesRemovedChildrenOnly(t *testing.T) {
	t.Parallel()
	total := int64(0)
	if err := addSegmentDirectoryEntry("/segments", "/segments/gone.ts", disappearingSegmentEntry{}, nil, &total); err != nil {
		t.Fatalf("vanished entry failed sampling: %v", err)
	}
	if err := addSegmentDirectoryEntry("/segments", "/segments/gone.ts", nil, os.ErrNotExist, &total); err != nil {
		t.Fatalf("vanished walk child failed sampling: %v", err)
	}
	err := addSegmentDirectoryEntry("/segments", "/segments", nil, os.ErrNotExist, &total)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing staging root error = %v, want os.ErrNotExist", err)
	}
}

type fakeStagingProcess struct {
	mu      sync.Mutex
	stops   int
	resumes int
}

func (p *fakeStagingProcess) stop() error {
	p.mu.Lock()
	p.stops++
	p.mu.Unlock()
	return nil
}

func (p *fakeStagingProcess) resume() error {
	p.mu.Lock()
	p.resumes++
	p.mu.Unlock()
	return nil
}

func (p *fakeStagingProcess) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stops, p.resumes
}

func TestSegmentBackpressureFlushesClosedOutputBeforeResumingActiveOutput(t *testing.T) {
	directory := t.TempDir()
	process := &fakeStagingProcess{}
	closed := filepath.Join(directory, "closed.ts")
	active := filepath.Join(directory, "active.ts")
	if err := os.WriteFile(closed, make([]byte, 5), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(active, make([]byte, 5), 0o600); err != nil {
		t.Fatal(err)
	}
	backpressure := &segmentBackpressure{
		process: process, directory: directory,
		window: SegmentStagingWindow{HighBytes: 10, LowBytes: 4},
	}
	flush, err := backpressure.beforeSink()
	if err != nil {
		t.Fatal(err)
	}
	if !flush {
		t.Fatal("high watermark did not request a completed-output flush")
	}
	if err := os.Remove(closed); err != nil {
		t.Fatal(err)
	}
	// The active file is still above the low watermark. With no closed output
	// left to reclaim, FFmpeg must nevertheless resume so it can close that file
	// and produce the next sink callback.
	if err := backpressure.afterSink(); err != nil {
		t.Fatal(err)
	}
	stops, resumes := process.counts()
	if stops != 1 || resumes != 1 {
		t.Fatalf("process controls = stop %d resume %d, want 1/1", stops, resumes)
	}
}

func TestSegmentBackpressureWaitsForEveryClosedOutput(t *testing.T) {
	directory := t.TempDir()
	process := &fakeStagingProcess{}
	first := filepath.Join(directory, "first.ts")
	second := filepath.Join(directory, "second.ts")
	for _, path := range []string{first, second} {
		if err := os.WriteFile(path, make([]byte, 5), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	backpressure := &segmentBackpressure{
		process: process, directory: directory,
		window: SegmentStagingWindow{HighBytes: 10, LowBytes: 4},
	}
	for range 2 {
		flush, err := backpressure.beforeSink()
		if err != nil || !flush {
			t.Fatalf("beforeSink() = flush %v, error %v", flush, err)
		}
	}
	if err := os.Remove(first); err != nil {
		t.Fatal(err)
	}
	if err := backpressure.afterSink(); err != nil {
		t.Fatal(err)
	}
	if _, resumes := process.counts(); resumes != 0 {
		t.Fatalf("resumed with another completed output pending: resumes = %d", resumes)
	}
	if err := os.Remove(second); err != nil {
		t.Fatal(err)
	}
	if err := backpressure.afterSink(); err != nil {
		t.Fatal(err)
	}
	stops, resumes := process.counts()
	if stops != 1 || resumes != 1 {
		t.Fatalf("process controls = stop %d resume %d, want 1/1", stops, resumes)
	}
}
