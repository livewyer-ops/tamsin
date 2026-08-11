//go:build !linux && !darwin

package media

import (
	"errors"
	"os"
)

var errSegmentProcessBackpressureUnsupported = errors.New("FFmpeg staging backpressure is unsupported on this platform")

func stopSegmentProcess(*os.Process) error {
	return errSegmentProcessBackpressureUnsupported
}

func continueSegmentProcess(*os.Process) error {
	return nil
}
