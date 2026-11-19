//go:build unix

package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// A consumer that exits before tamsin, such as a pager or a crashed
// supervisor reading the event stream, closes the pipe. The process must see
// a write error it can handle, not die of SIGPIPE before any cleanup.
func TestClosedOutputPipeDoesNotKillTheProcess(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "tamsin")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	command := exec.Command(binary, "--help")
	command.Stdout = writer
	err = command.Run()
	_ = writer.Close()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			t.Fatalf("tamsin was killed by %v writing to a closed pipe", status.Signal())
		}
	}
}
