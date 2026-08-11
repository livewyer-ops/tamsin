# 0001: Implement TAMSin in Go

- Status: accepted
- Date: 2026-08-06

## Context

TAMSin is a non-interactive media ingest process intended for workstations, OCI containers, Kubernetes jobs, event handlers, and UI wrappers. It needs bounded concurrency, streaming HTTP and S3 I/O, OAuth, predictable cross-compilation, and a small operational footprint. The repository started empty, so compatibility with existing code does not constrain the choice.

## Decision

Implement TAMSin in Go, initially targeting Go 1.24 or newer.

Use the standard library for HTTP primitives, process control, structured logging, cryptography, and concurrency. Use established libraries for substantial protocol or user-interface surfaces: Cobra for CLI parsing and completion, Viper for flag/environment/config precedence, HashiCorp `go-retryablehttp` for replay-safe HTTP input retries, AWS SDK for Go v2 for S3, `golang.org/x/oauth2` for OAuth, and `google/uuid` for identifiers.

## Evidence

- The official AWS SDK for Go v2 provides maintained S3, credential-chain, endpoint, and retry behaviour without an external `aws` process.
- Cobra and Viper are the established Go pairing for composable command trees, shell completion, configuration files, environment variables, and flags.
- Go produces a single CGO-free TAMSin binary when FFmpeg is kept behind a process boundary. Its standard HTTP and concurrency models fit simultaneous streaming uploads without a runtime service.
- TAMOSS is exercised over its language-neutral HTTP API, so sharing its Python implementation language would not provide code reuse.
- Rust would offer comparable runtime properties, but would add an unavailable local toolchain and a smaller pool of directly maintained AWS/CLI integrations without removing the FFmpeg ABI problem.

## Consequences

The native TAMSin executable has no C ABI dependency. FFmpeg remains an explicit runtime dependency and is included in the OCI image. Go dependencies are kept narrow and pinned by `go.mod` and `go.sum`.

## Sources

- [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)
- [Cobra](https://github.com/spf13/cobra)
- [Viper](https://github.com/spf13/viper)
- [HashiCorp retryable HTTP client](https://github.com/hashicorp/go-retryablehttp)
- [Go OAuth2](https://pkg.go.dev/golang.org/x/oauth2)
- [TAMS API](https://github.com/bbc/tams)
- [TAMOSS](https://github.com/livewyer-ops/tamoss)
