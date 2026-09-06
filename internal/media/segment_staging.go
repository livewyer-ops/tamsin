package media

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type stagingProcess interface {
	stop() error
	resume() error
}

type osStagingProcess struct{ process *os.Process }

func (p osStagingProcess) stop() error   { return stopSegmentProcess(p.process) }
func (p osStagingProcess) resume() error { return continueSegmentProcess(p.process) }

// segmentBackpressure only stops FFmpeg after a live manifest announces a
// closed output. Pausing while the only large file is still being written can
// deadlock forever: the sink has nothing reclaimable and FFmpeg cannot close
// the file which would make it reclaimable. Checking at closure boundaries may
// exceed the high watermark by the active output for each stream, which is
// unavoidable for an external process; the Pipeline capacity ledger remains
// responsible for cancelling an output that cannot fit at all.
type segmentBackpressure struct {
	mu        sync.Mutex
	process   stagingProcess
	directory string
	window    SegmentStagingWindow
	paused    bool
	pending   int
}

func (b *segmentBackpressure) beforeSink() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending++
	bytes, err := segmentDirectoryBytes(b.directory)
	if err != nil {
		b.pending--
		return false, fmt.Errorf("measure rolling segment staging: %w", err)
	}
	if !b.paused && bytes >= b.window.HighBytes {
		if err := b.process.stop(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			b.pending--
			return false, fmt.Errorf("pause FFmpeg at staging high watermark: %w", err)
		}
		b.paused = true
	}
	return b.paused, nil
}

func (b *segmentBackpressure) afterSink() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending--
	if !b.paused {
		return nil
	}
	bytes, err := segmentDirectoryBytes(b.directory)
	if err != nil {
		_ = b.resume()
		return fmt.Errorf("measure rolling segment staging: %w", err)
	}
	// Resume at the low watermark in the normal case. If no closed outputs are
	// waiting for their sink, the remaining bytes belong to active outputs;
	// they must be allowed to close even when they exceed the low watermark.
	if bytes <= b.window.LowBytes || b.pending == 0 {
		if err := b.resume(); err != nil {
			return fmt.Errorf("resume FFmpeg after staging pressure: %w", err)
		}
	}
	return nil
}

func (b *segmentBackpressure) finish() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.resume(); err != nil {
		return fmt.Errorf("resume FFmpeg during segmenter shutdown: %w", err)
	}
	return nil
}

func (b *segmentBackpressure) resume() error {
	if !b.paused {
		return nil
	}
	if err := b.process.resume(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	b.paused = false
	return nil
}

func segmentDirectoryBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		return addSegmentDirectoryEntry(root, path, entry, walkErr, &total)
	})
	return total, err
}

func addSegmentDirectoryEntry(root, path string, entry os.DirEntry, walkErr error, total *int64) error {
	if walkErr != nil {
		// The sink removes a committed Segment while the backpressure sampler
		// walks the same directory. A vanished child no longer consumes staging;
		// a missing root still means the staging contract itself was lost.
		if errors.Is(walkErr, os.ErrNotExist) && path != root {
			return nil
		}
		return walkErr
	}
	if !entry.Type().IsRegular() {
		return nil
	}
	info, err := entry.Info()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Size() > 0 && *total > int64(^uint64(0)>>1)-info.Size() {
		return errors.New("segment staging size exceeds the supported byte range")
	}
	*total += info.Size()
	return nil
}
