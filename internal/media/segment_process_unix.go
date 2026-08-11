//go:build linux || darwin

package media

import (
	"os"
	"syscall"
)

func stopSegmentProcess(process *os.Process) error {
	return process.Signal(syscall.SIGSTOP)
}

func continueSegmentProcess(process *os.Process) error {
	return process.Signal(syscall.SIGCONT)
}
