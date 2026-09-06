//go:build linux

package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestJSONTTYPublishesPermanentMetricsInTheEventStream(t *testing.T) {
	master, terminal := openPseudoTerminal(t)
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	code := Execute(context.Background(), []string{
		"--profile", "preserve", "--ffprobe", fakeMediaTool(t, directory), "--format", "json", "--dry-run=fast", "-d", "0", input,
	}, strings.NewReader(""), &stdout, terminal)
	if code != ExitOK {
		t.Fatalf("exit = %d; stdout = %s", code, stdout.String())
	}
	if err := terminal.Close(); err != nil {
		t.Fatal(err)
	}
	stderr := readPseudoTerminal(t, master)

	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	if stream.state.Finished == nil || stream.state.Finished.BytesStaged != 7 ||
		stream.state.Finished.BytesUploaded != 0 || stream.state.Finished.BytesVerified != 0 ||
		stream.state.Finished.Retries != 0 {
		t.Fatalf("terminal transfer metrics = %#v", stream.state.Finished)
	}
	if strings.Contains(stderr, `msg="ingest run metrics"`) || strings.Contains(stderr, "bytes_staged=7") {
		t.Fatalf("obsolete INFO metrics footer was still emitted beside structured output: %q", stderr)
	}
}

func openPseudoTerminal(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("open pseudo-terminal: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatalf("unlock pseudo-terminal: %v", err)
	}
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatalf("read pseudo-terminal number: %v", err)
	}
	terminal, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatalf("open pseudo-terminal slave: %v", err)
	}
	t.Cleanup(func() { _ = terminal.Close() })
	return master, terminal
}

func readPseudoTerminal(t *testing.T, master *os.File) string {
	t.Helper()
	var output bytes.Buffer
	buffer := make([]byte, 4096)
	for {
		count, err := master.Read(buffer)
		output.Write(buffer[:count])
		if err == nil {
			continue
		}
		// Linux PTYs report EIO rather than EOF after the final slave closes.
		if errors.Is(err, io.EOF) || errors.Is(err, unix.EIO) {
			return output.String()
		}
		t.Fatalf("read pseudo-terminal: %v", err)
	}
}
