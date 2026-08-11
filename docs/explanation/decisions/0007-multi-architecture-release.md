# 0007: Retain one multi-platform OCI index through the release gate

- Status: accepted
- Date: 2026-08-09
- Last reviewed: 2026-08-11

## Context

The release gate must test what it publishes. Saving a Docker daemon image met
that requirement for one runner-native image, but it could not represent an
amd64+arm64 index. Building arm64 later, or assembling a manifest after TAMOSS,
would make publication contain bytes that the gate never accepted.

A registry staging tag would retain a multi-platform index, but it would also
make a tag-triggered build mutate an external registry before E2E succeeds.
Actions artefacts already provide an immutable service-side ID and digest and
can retain an OCI image layout without making it consumable.

## Decision

The release build exports exactly `linux/amd64` and `linux/arm64` together as a
local OCI image layout. Buildx, BuildKit, Docker, QEMU and their setup actions
are pinned. Each platform also carries minimal BuildKit provenance and an SPDX
SBOM as an in-toto attestation manifest. The layout validator streams every
referenced content-addressed blob, requires one runnable image and one bound
attestation manifest for each supported platform, and records one external
image identity:

- the versioned image reference;
- the OCI index digest.

The member-manifest and image-configuration digests remain recorded inside the
bundle as derived metadata for Docker selection checks. They are not separate
trust roots: the SHA-256 index digest already commits to both member manifests,
and each member manifest commits to its configuration and layers. Passing all
four descendants between jobs duplicated state without adding integrity.

The build loads the retained index into Docker's containerd image store and
executes CLI and diagnostic smoke checks for both explicit platforms. The arm64
checks therefore execute through QEMU on the amd64 runner rather than selecting
amd64 twice.

Callable TAMOSS E2E accepts the image reference and index digest plus the Actions
artefact ID and digest only as one all-or-none set. It loads the same layout,
derives and checks both members, addresses the image by the immutable index
digest, and explicitly selects its recorded amd64 member.
Scheduled and manual E2E calls omit the identity set and may still build an
ordinary development image.

Only after E2E succeeds does pinned ORAS copy the local OCI index to the version
tag using its documented OCI-layout source mode. A retry accepts an existing
version tag only when it already resolves to the same index; it never replaces
a different release image. The registry's raw index bytes are checked against
the tested digest before the GitHub release is created. Stable promotion copies
that same remote index by digest to `latest`, checks it again, and retains the
serialised newest-stable-tag guard.

Release tags are parsed as strict SemVer, including the identifier rules that
reject empty components and leading zeroes in numeric prerelease identifiers.
Build metadata is rejected because it has no SemVer precedence and would permit
multiple release tags for one product version. The release tag must be an
annotated object signed by the SSH key pinned in
`.github/release-allowed-signers`, and its target must already be on `main`.
Tag-protection rules additionally restrict who may create it. A signer rotation
therefore requires a reviewed change on `main` before the new key can authorize
a release.

The commit timestamp is used for the binary version date, OCI creation time and
`SOURCE_DATE_EPOCH`. The runtime packages come from the dated Debian snapshot
recorded by the pinned base image, timestamped package-manager logs are removed,
and the OCI exporter rewrites filesystem timestamps. Rebuilding one commit with
the pinned toolchain therefore produces the same layout bytes instead of
changing the version digest with wall-clock time or dependency drift.

## Consequences

Both version tags and `latest` are genuine multi-platform indexes, and neither
E2E nor publication has a Dockerfile build path. An artefact with a missing,
substituted or wrongly labelled member fails locally before external mutation.
The release bundle is larger because it retains both images, and local
distribution builds need Buildx plus binfmt support. Registry implementations
that rewrite OCI index JSON are deliberately unsupported by the release gate:
digest continuity is stronger than accepting a semantically similar manifest.

The full Debian FFmpeg installation is intentionally retained for broad codec
and container support. At the reviewed versions it is approximately 209 MB
compressed and 559 MB expanded per platform; clients pulling the multi-platform
tag receive only their selected platform. Attestations add about 4 MB per
platform to registry and release-bundle storage, but are separate metadata and
do not increase runtime image pulls or container memory.

## Sources

- [OCI Image Layout specification](https://github.com/opencontainers/image-spec/blob/v1.1.1/image-layout.md)
- [Docker multi-platform image documentation](https://docs.docker.com/build/building/multi-platform/)
- [Buildx OCI exporter documentation](https://docs.docker.com/build/exporters/oci-docker/)
- [Docker build attestation documentation](https://docs.docker.com/build/metadata/attestations/)
- [BuildKit reproducible-build documentation](https://github.com/moby/buildkit/blob/master/docs/build-repro.md)
- [Debian snapshot archive](https://snapshot.debian.org/)
- [ORAS copy documentation](https://oras.land/docs/commands/oras_cp/)
