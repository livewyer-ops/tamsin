# Security

## Supported versions

The latest stable minor release receives security fixes. Until the first stable
release, only the latest release candidate is supported. The `main` branch and
older prereleases are development snapshots, not supported deployment targets.

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
secrets are also removed from TAMS error bodies. Under OAuth, TAMS error bodies
are omitted because refreshed bearer values cannot be safely distinguished
from arbitrary server text.

TAMS, OAuth, and presigned-transfer clients do not follow redirects.
Cross-origin HTTP input redirects strip configured input headers. TAMS
credentials are sent only to the configured API origin and path. Cross-origin
presigned storage URLs receive only headers supplied by TAMS.

Credential-bearing TAMS requests and OAuth token or authorization endpoints
require HTTPS. `--allow-insecure-auth-loopback` is a narrow, explicit
development exception for the exact `localhost` name or a literal loopback
address. It cannot enable plaintext authentication to a private-network or
remote host. The inbound OAuth authorization-code callback remains loopback
HTTP because credentials are received there rather than sent to it.

`--insecure-skip-verify` is a development option. It disables certificate
verification for TAMS, OAuth, HTTP input, S3 input, and presigned Object
transfers and must not be used against production endpoints. It does not permit
authenticated HTTP.

## Media processing

Media is untrusted input. FFprobe and FFmpeg execute as child processes without
a shell. Arguments are passed as an array, input files are staged with
owner-only permissions, tool output is bounded, and cancellation terminates
children. Keep the independently installed FFmpeg package or TAMSin OCI image
patched.

The OCI image runs as UID/GID 65532. Mount inputs read-only and use a writable
temporary volume when ingesting remote sources or segmenting large media.

## Verification

`make verify` runs reachable vulnerability analysis, static analysis,
race-enabled tests, module checksum verification, and current-tree Gitleaks.
The release-readiness process additionally scans complete Git history.
`make e2e` uses ephemeral Kind and RustFS resources and tears them down after
the run.
