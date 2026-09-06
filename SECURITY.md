# Security

## Supported versions

The latest stable minor release receives security fixes. The `main` branch and
prereleases are development snapshots, not supported deployment targets.

## Reporting a vulnerability

Report suspected vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/livewyer-ops/tamsin/security/advisories/new).
Do not open a public issue or include production credentials or customer media.

Include the affected TAMSin version, reproduction, impact, and any known
mitigation. Maintainers will acknowledge a complete report, coordinate a fix
and disclosure, and credit reporters who want acknowledgement.

## Credential handling

TAMSin accepts secrets through standard environment, configuration, and flag
mechanisms but recommends workload identity, Kubernetes Secret environment
injection, or a mode-`0600` local configuration file. Command-line secret flags
can be visible to other processes.

The process never logs configured credential values. Diagnostic and provenance
URL rendering removes userinfo and redacts every query value; configured
secrets are also redacted from diagnostics. The CLI omits TAMS response error
bodies rather than displaying untrusted server text that could contain secrets.

TAMS, OAuth, and presigned-transfer clients do not follow redirects.
Cross-origin HTTP input redirects strip configured input headers. TAMS
credentials are sent only to the configured origin, including service-provided
media URLs outside the API path. Pagination must stay within the API path.
Cross-origin storage URLs receive only headers supplied by TAMS.

Credential-bearing TAMS requests and OAuth token endpoints
require HTTPS. `--allow-insecure-auth-loopback` is a narrow, explicit
development exception for the exact `localhost` name or a literal loopback
address. It cannot enable plaintext authentication to a private-network or
remote host. OAuth authorisation codes must be obtained separately; TAMSin does
not run a callback listener.

`--insecure-skip-verify` is a development option. It disables certificate
verification for TAMS, OAuth, HTTP input, S3 input, and presigned Object
transfers and must not be used against production endpoints. It does not permit
authenticated HTTP.

## Media processing

Media is untrusted input. FFprobe and FFmpeg 5.1 or newer execute as child
processes without a shell. Arguments are passed as an array, input files are
staged with owner-only permissions, tool output is bounded, and cancellation
terminates children. Child processes receive an explicit operational
environment allow-list covering paths, locale, temporary directories,
certificate stores, dynamic-linker paths, font/media drivers, GPU selection,
OpenMP, display/runtime directories, and essential Windows paths. TAMSin,
cloud-credential, proxy, and `FFREPORT` variables are not inherited. Keep the
independently installed FFmpeg package or TAMSin OCI image patched.

The OCI image runs as UID/GID 65532. Mount inputs read-only and use a writable
temporary volume when ingesting remote sources or segmenting large media.

## Verification

`make verify` runs reachable vulnerability analysis, static analysis,
race-enabled tests and module checksum verification. GitHub secret scanning and
push protection cover the public repository. `make e2e` uses ephemeral Kind and
RustFS resources and tears them down after the run.
