package cli

import (
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/presentation"
	"golang.org/x/term"
)

const fallbackOutputWidth = 80

// humanOptions resolves terminal capability once, immediately before writing
// the permanent receipt. The receipt itself remains deterministic and usable
// by library callers which provide explicit options.
func (a *application) humanOptions() presentation.HumanOptions {
	return presentation.HumanOptions{
		Width:   outputWidth(a.stdout),
		Verbose: a.v.GetBool("verbose"),
		Quiet:   a.v.GetBool("quiet"),
		Color:   a.humanColorEnabled(),
	}
}

func outputWidth(writer io.Writer) int {
	if file, ok := writer.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		if width, _, err := term.GetSize(int(file.Fd())); err == nil && width > 0 {
			return width
		}
	}
	// COLUMNS is useful for deterministic wrappers and terminals whose size
	// cannot be queried through the supplied writer. Reject implausible values
	// rather than letting one environment variable create pathological output.
	if width, err := strconv.Atoi(strings.TrimSpace(os.Getenv("COLUMNS"))); err == nil && width >= 20 && width <= 1000 {
		return width
	}
	return fallbackOutputWidth
}

func (a *application) humanColorEnabled() bool {
	switch strings.ToLower(a.v.GetString("color")) {
	case "always":
		return true
	case "never":
		return false
	}
	if _, disabled := os.LookupEnv("NO_COLOR"); disabled {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	file, ok := a.stdout.(*os.File)
	return ok && term.IsTerminal(int(file.Fd()))
}
