package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/livewyer-ops/tamsin/internal/cli"
)

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel(cli.ErrSignal)
		// A second signal is the explicit escape hatch from bounded recovery.
		// The first signal still gets the normal terminal-event and retraction
		// path; a forced exit cannot promise either.
		<-signals
		os.Exit(130)
	}()
	code := cli.Execute(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	signal.Stop(signals)
	cancel(nil)
	os.Exit(code)
}
