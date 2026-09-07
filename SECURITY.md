# Security

## Supported versions

The latest `-inN` release for the newest supported TAMS API version receives
security fixes. `-rcM` tags and the `main` branch are development snapshots,
not supported deployment targets.

## Reporting a vulnerability

Report suspected vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/livewyer-ops/tamsin/security/advisories/new).
Do not open a public issue or include production credentials or customer media.

Include the affected TAMSin version, reproduction, impact, and any known
mitigation. Maintainers will acknowledge a complete report, coordinate a fix
and disclosure, and credit reporters who want acknowledgement.

## Credential handling

Use workload secret injection or a mode-`0600` configuration file. Command-line
secrets can appear in process listings. See
[authentication](docs/configuration.md#authentication) for setup.

Diagnostics redact configured credentials and URL query values and remove URL
userinfo. The CLI omits untrusted TAMS response error bodies.

TAMS, OAuth, and presigned-transfer clients do not follow redirects.
Cross-origin HTTP input redirects strip configured input headers. TAMS
credentials are sent only to the configured origin, including service-provided
media URLs outside the API path. Pagination must stay within the API path.
Cross-origin storage URLs receive only headers supplied by TAMS.

Credentials require HTTPS. The loopback HTTP exception and
`--insecure-skip-verify` are development options; do not use them in production.
Disabling certificate verification does not permit authenticated HTTP.

## Media processing

Media is untrusted input. FFprobe and FFmpeg 5.1 or newer execute as child
processes without a shell. Arguments are passed as an array, input files are
staged with owner-only permissions, tool output is bounded, and cancellation
terminates children. Child processes receive an operational environment
allow-list that excludes TAMSin, cloud-credential, proxy and `FFREPORT`
variables. Keep the installed FFmpeg package or TAMSin OCI image patched.

Every input runs with an FFmpeg protocol allowlist (`file` for local and
staged input, `http,tcp` for the loopback bridge) and a format allowlist of
self-contained demuxers. Playlist, manifest, concatenation and pattern
demuxers such as HLS, DASH, concat and image sequences are refused, so a
crafted input cannot make the media tools open other files or network hosts.
Staged remote and stdin inputs are written as `input` plus a short
alphanumeric extension; the remote basename never reaches the tools.

Stream mode serves remote bytes to the media tools through a private loopback
URL passed as a child-process argument. That URL is readable from the process
list by anything sharing the PID namespace, and the bridge does not check its
peer. Use stream mode only where the container or process does not share a
PID namespace with untrusted processes.

The FFmpeg runtime image installs from a dated Debian snapshot archive and
therefore disables APT `Check-Valid-Until` in `Dockerfile.ffmpeg`; the
snapshot date and package versions are pinned there and revisioned with the
runtime tag.

See [containers](docs/operations.md#containers) for input permissions and
temporary-volume requirements.

## Verification

`make verify` runs reachable vulnerability analysis, static analysis,
race-enabled tests and module checksum verification. GitHub secret scanning and
push protection cover the public repository. `make e2e` uses ephemeral Kind and
RustFS resources and tears them down after the run.
