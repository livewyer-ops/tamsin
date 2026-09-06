package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/source"
)

type stagedFile struct {
	path   string
	size   int64
	sha256 string
	// owned reports whether the file at path was created by this run. A staged
	// copy of a remote input is ours alone and cannot change underneath us; a
	// local input belongs to whoever is running Tamsin and may be rewritten at
	// any point, so the two cannot be treated alike.
	owned   bool
	cleanup func()
	lease   *stagingLease
}

func stage(ctx context.Context, item source.Item, tempRoot string, retries int, lease *stagingLease,
	run *observability.Run) (stagedFile, error) {
	if item.Open == nil {
		return stagedFile{}, errors.New("source item has no opener")
	}
	input, err := item.Open(ctx)
	if err != nil {
		return stagedFile{}, err
	}

	if item.LocalPath != "" {
		hash := sha256.New()
		size, err := copyContext(ctx, hash, input)
		_ = input.Close()
		if err != nil {
			return stagedFile{}, fmt.Errorf("hash input %q: %w", item.LocalPath, err)
		}
		if item.Size >= 0 && size != item.Size {
			return stagedFile{}, fmt.Errorf("input %q changed size while being read: expected %d, got %d", item.LocalPath, item.Size, size)
		}
		return stagedFile{path: item.LocalPath, size: size, sha256: hex.EncodeToString(hash.Sum(nil)), cleanup: func() {}, lease: lease}, nil
	}

	directory, err := os.MkdirTemp(tempRoot, "tamsin-input-")
	if err != nil {
		_ = input.Close()
		return stagedFile{}, fmt.Errorf("create input staging directory: %w", err)
	}
	var stagedBytes int64
	cleanup := func() {
		_ = os.RemoveAll(directory)
		lease.subtract(stagedBytes)
		stagedBytes = 0
	}
	filename := filepath.Join(directory, safeFilename(item.Name))
	output, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = input.Close()
		cleanup()
		return stagedFile{}, fmt.Errorf("create staged input: %w", err)
	}
	// A transfer that dies partway is resumed rather than restarted. Staging a
	// large input is often the longest part of an ingest, and losing an hour of
	// it to a dropped connection at the end is the difference between a retry
	// that costs seconds and one that costs the whole transfer again.
	hash := sha256.New()
	var (
		size     int64
		copyErr  error
		attempts int
	)
	for attempt := 0; ; attempt++ {
		attempts = attempt + 1
		var written int64
		written, copyErr = copyContext(ctx, io.MultiWriter(stagingWriter{lease: lease, destination: output}, hash), input)
		size += written
		stagedBytes = size
		_ = input.Close()
		if copyErr == nil {
			break
		}
		// A source that cannot be reopened, a cancelled run, and an exhausted
		// allowance all mean the failure stands.
		if ctx.Err() != nil || !worthResuming(copyErr, item.Reopen != nil, attempt, retries) {
			break
		}
		run.Retry(observability.OperationSourceTransfer, attempts+1, retries+1, 0, copyErr, 0)
		resumed, continuing, reopenErr := item.Reopen(ctx, size)
		if reopenErr != nil {
			copyErr = errors.Join(copyErr, reopenErr)
			break
		}
		if !continuing {
			// The source is answering from the beginning, so what has been
			// written is not a prefix of what is about to arrive and every
			// record of it has to go: the file, the digest, and the count.
			if _, err := output.Seek(0, io.SeekStart); err != nil {
				_ = resumed.Close()
				copyErr = errors.Join(copyErr, err)
				break
			}
			if err := output.Truncate(0); err != nil {
				_ = resumed.Close()
				copyErr = errors.Join(copyErr, err)
				break
			}
			hash.Reset()
			lease.subtract(size)
			size = 0
			stagedBytes = 0
		}
		input = resumed
	}
	closeErr := output.Close()
	if copyErr != nil {
		cleanup()
		return stagedFile{}, fmt.Errorf("stage input %s failed after %d attempt(s): %w", safeURI(item.URI), attempts, copyErr)
	}
	if closeErr != nil {
		cleanup()
		return stagedFile{}, fmt.Errorf("close staged input: %w", closeErr)
	}
	if item.Size >= 0 && size != item.Size {
		cleanup()
		return stagedFile{}, fmt.Errorf("input %s size mismatch: expected %d, got %d", safeURI(item.URI), item.Size, size)
	}
	return stagedFile{
		path: filename, size: size, sha256: hex.EncodeToString(hash.Sum(nil)),
		owned: true, cleanup: cleanup, lease: lease,
	}, nil
}
