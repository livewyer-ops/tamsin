//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package ingest

import (
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

func filesystemAvailable(path string) (int64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, err
	}
	blocks := uint64(stats.Bavail)
	blockSize := uint64(stats.Bsize)
	if blockSize != 0 && blocks > math.MaxInt64/blockSize {
		return 0, fmt.Errorf("available byte count exceeds int64")
	}
	return int64(blocks * blockSize), nil
}
