//go:build windows

package ingest

import (
	"fmt"
	"math"

	"golang.org/x/sys/windows"
)

func filesystemAvailable(path string) (int64, error) {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var available uint64
	if err := windows.GetDiskFreeSpaceEx(pointer, &available, nil, nil); err != nil {
		return 0, err
	}
	if available > math.MaxInt64 {
		return 0, fmt.Errorf("available byte count exceeds int64")
	}
	return int64(available), nil
}
