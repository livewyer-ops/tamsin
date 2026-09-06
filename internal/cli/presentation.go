package cli

import (
	"os"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/presentation"
	"golang.org/x/term"
)

// humanOptions resolves colour policy immediately before writing the receipt.
func (a *application) humanOptions() presentation.HumanOptions {
	return presentation.HumanOptions{
		Verbose: a.v.GetBool("verbose"),
		Quiet:   a.v.GetBool("quiet"),
		Color:   a.humanColorEnabled(),
	}
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
