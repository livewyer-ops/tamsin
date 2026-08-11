package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/ingestevent"
	"github.com/livewyer-ops/tamsin/internal/cli"
	"github.com/rogpeppe/go-internal/testscript"
)

// TestMain registers the CLI as an in-process command for testscript. Running
// it this way keeps coverage attribution and avoids depending on a built
// binary, while each script still gets its own process-like environment.
func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"tamsin": func() {
			os.Exit(cli.Execute(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
		},
	})
}

// TestScript exercises the CLI the way an operator meets it: real arguments,
// real exit codes, and stdout kept separate from diagnostic stderr. The scripts
// live in testdata/script as txtar archives, so a case is a readable transcript
// rather than a wall of assertions.
//
// Run `go test ./cmd/tamsin -update` to refresh golden output after an
// intentional change.
func TestScript(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir:                 filepath.Join("testdata", "script"),
		UpdateScripts:       *update,
		RequireExplicitExec: true,
		Setup: func(env *testscript.Env) error {
			// Scripts must not pick up an operator's real configuration or
			// credentials, so the environment starts empty of both.
			env.Setenv("HOME", env.WorkDir)
			env.Setenv("XDG_CONFIG_HOME", filepath.Join(env.WorkDir, "config"))
			for _, name := range []string{
				"TAMSIN_ENDPOINT", "TAMSIN_AUTH_MODE", "TAMSIN_AUTH_TOKEN",
				"TAMSIN_AUTH_USERNAME", "TAMSIN_AUTH_PASSWORD", "TAMSIN_AUTH_ALLOW_INSECURE_LOOPBACK",
				"TAMSIN_CONFIG",
			} {
				env.Setenv(name, "")
			}
			return nil
		},
		Cmds: map[string]func(ts *testscript.TestScript, neg bool, args []string){
			// checkevents validates the complete ingest process protocol rather
			// than treating newline-delimited JSON as a final batch document.
			// Usage: checkevents FILE OUTCOME TOTAL EXIT_CODE [INPUT_STATUS,...]
			"checkevents": checkEvents,
			// ffprobe/ffmpeg are external tools; a script that needs them says so
			// and is skipped where they are unavailable rather than failing.
			"requiremedia": func(ts *testscript.TestScript, neg bool, args []string) {
				if neg {
					ts.Fatalf("requiremedia does not support negation")
				}
				for _, tool := range []string{"ffprobe", "ffmpeg"} {
					if _, err := exec.LookPath(tool); err != nil {
						ts.Fatalf("skip: %s is not installed", tool)
					}
				}
			},
		},
		Condition: func(cond string) (bool, error) {
			switch cond {
			case "media":
				for _, tool := range []string{"ffprobe", "ffmpeg"} {
					if _, err := exec.LookPath(tool); err != nil {
						return false, nil
					}
				}
				return true, nil
			case "unix":
				return runtime.GOOS != "windows", nil
			default:
				return false, fmt.Errorf("unknown condition %q", cond)
			}
		},
	})
}

func checkEvents(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("checkevents does not support negation")
	}
	if len(args) < 4 || len(args) > 5 {
		ts.Fatalf("usage: checkevents FILE OUTCOME TOTAL EXIT_CODE [INPUT_STATUS,...]")
	}

	data, err := os.ReadFile(ts.MkAbs(args[0]))
	if err != nil {
		ts.Fatalf("read event stream: %v", err)
	}
	if bytes.ContainsAny(data, "\r\x1b") {
		ts.Fatalf("event stream contains terminal control bytes")
	}
	state, err := ingestevent.Reduce(bytes.NewReader(data))
	if err != nil {
		ts.Fatalf("reduce event stream: %v", err)
	}
	if state.Finished == nil {
		ts.Fatalf("event stream has no run.finished record")
	}
	if got, want := string(state.Finished.Outcome), args[1]; got != want {
		ts.Fatalf("run outcome is %q, want %q", got, want)
	}
	wantTotal, err := strconv.Atoi(args[2])
	if err != nil || wantTotal < 0 {
		ts.Fatalf("invalid total %q", args[2])
	}
	wantExit, err := strconv.Atoi(args[3])
	if err != nil {
		ts.Fatalf("invalid exit code %q", args[3])
	}
	if state.Manifest == nil || state.Manifest.TotalInputs != uint64(wantTotal) {
		ts.Fatalf("manifest total is %v, want %d", state.Manifest, wantTotal)
	}
	if state.Finished.Total != uint64(wantTotal) {
		ts.Fatalf("run.finished total is %d, want %d", state.Finished.Total, wantTotal)
	}
	if state.Finished.ExitCode != wantExit {
		ts.Fatalf("run.finished exit code is %d, want %d", state.Finished.ExitCode, wantExit)
	}
	if len(state.Inputs) != wantTotal {
		ts.Fatalf("reduced input count is %d, want %d", len(state.Inputs), wantTotal)
	}

	var statuses []string
	if len(args) == 5 {
		statuses = strings.Split(args[4], ",")
		if len(statuses) != wantTotal {
			ts.Fatalf("input status count is %d, want %d", len(statuses), wantTotal)
		}
	}
	for index := 0; index < wantTotal; index++ {
		input := state.Inputs[index]
		if input == nil || input.Declared == nil || input.Finished == nil {
			ts.Fatalf("input %d is missing declared or terminal state", index)
		}
		if len(statuses) > 0 && string(input.Finished.Status) != statuses[index] {
			ts.Fatalf("input %d status is %q, want %q", index, input.Finished.Status, statuses[index])
		}
		if input.Finished.Status != ingestevent.InputFailed {
			if input.Started == nil {
				ts.Fatalf("successful input %d has no input.started lifecycle event", index)
			}
			if len(input.PlannedFlows) != len(input.FlowResults) {
				ts.Fatalf("input %d planned %d of %d terminal Flows", index, len(input.PlannedFlows), len(input.FlowResults))
			}
		}
	}
}
