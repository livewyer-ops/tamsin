package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/spf13/cobra"
)

// ErrSignal is installed as the cancellation cause by the executable when an
// operator sends SIGINT or SIGTERM. Library callers ordinarily cancel their
// own context and are reported as parent cancellation instead.
var ErrSignal = errors.New("termination signal received")

const (
	ExitOK          = 0
	ExitGeneral     = 1
	ExitUsage       = 2
	ExitAuth        = 3
	ExitPartial     = 4
	ExitSource      = 5
	ExitMedia       = 6
	ExitRemote      = 7
	ExitInterrupted = 8
)

const (
	helpGroupAnnotation         = "tamsin.help_group"
	configIndependentAnnotation = "tamsin.config_independent"
)

func usageArgs(validate cobra.PositionalArgs) cobra.PositionalArgs {
	return func(command *cobra.Command, args []string) error {
		return withExit(ExitUsage, validate(command, args))
	}
}

// helpGroupCommand makes a namespace command validate stray arguments while
// preserving the familiar successful help output when it is invoked alone.
// The annotation keeps that help-only path independent of runtime config.
func helpGroupCommand(use, short string) *cobra.Command {
	command := &cobra.Command{
		Use:         use,
		Short:       short,
		Args:        usageArgs(cobra.NoArgs),
		Annotations: map[string]string{helpGroupAnnotation: "true"},
	}
	command.RunE = func(command *cobra.Command, _ []string) error { return command.Help() }
	return command
}

type ExitError struct {
	Code int
	Err  error
}

type processFailureError struct {
	code           string
	message        string
	actionRequired bool
	err            error
}

func (e *processFailureError) Error() string         { return e.err.Error() }
func (e *processFailureError) Unwrap() error         { return e.err }
func (e *processFailureError) PublicMessage() string { return e.message }

func safeProcessFailure(code, message string, actionRequired bool, err error) error {
	if err == nil {
		return nil
	}
	return &processFailureError{code: code, message: message, actionRequired: actionRequired, err: err}
}

func (e *ExitError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("exit status %d", e.Code)
	}
	return e.Err.Error()
}

func (e *ExitError) Unwrap() error {
	return e.Err
}

func withExit(code int, err error) error {
	if err == nil {
		return nil
	}
	return &ExitError{Code: code, Err: err}
}

func exitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	var exitError *ExitError
	if errors.As(err, &exitError) {
		return exitError.Code
	}
	var requestTimeout *tams.RequestTimeoutError
	if errors.As(err, &requestTimeout) {
		return ExitRemote
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ExitInterrupted
	}
	return ExitGeneral
}
