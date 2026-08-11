// Package ingestevent defines and validates the public, newline-delimited
// process protocol emitted by `tamsin ingest --format json`.
//
// Decoder reads bounded NDJSON envelopes without bufio.Scanner's token limit.
// Reducer validates protocol version, sequence, lifecycle ordering, terminal
// coverage, and exit semantics while reconstructing a renderer-friendly State.
// Consumers should treat EOF without run.finished as ErrIncompleteStream and
// independently compare the terminal exit_code with the child process status.
// The package is public so Go wrappers can use the same bounds and compatibility
// rules as TAMSin itself instead of parsing terminal text or stderr.
package ingestevent
