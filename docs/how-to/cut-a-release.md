# Cut a release

This guide is for a repository administrator publishing TAMSin to GitHub
Releases and GitHub Container Registry. A release tag is an external
publication event: never move or reuse one.

## Before you begin

Confirm that:

- all intended pull requests are merged into `main`;
- CI is green on the exact `main` commit;
- `CHANGELOG.md` has a dated section for the intended stable version;
- the TAMS conformance inventory has no release-blocking finding;
- both pinned TAMS 8.2 and 8.1 TAMOSS matrix jobs are green;
- the immutable FFmpeg runtime revision named in `Dockerfile` exists with
  amd64/arm64 members, SBOM, and provenance;
- the active `Protect main` ruleset requires pull requests and green CI;
- the active `Protect release tags` ruleset restricts creation, updates, and
  deletion of `v*` tags to the authorised release account;
- GitHub private vulnerability reporting and repository security scanning are
  enabled; and
- you can create a signed Git tag with the pinned release key.

The workflow derives the binary version and GHCR tag from the Git tag. Do not
edit a source-code version constant. Release tags accept `vMAJOR.MINOR.PATCH`
and optional prerelease identifiers; SemVer `+build` metadata is not accepted.
The workflow rejects lightweight tags, tags not signed by the SSH key pinned in
`.github/release-allowed-signers`, and commits that are not already part of
`main`. Rotate the release key through an ordinary reviewed pull request before
using it to sign a release.

## Prepare the release commit

Create a release pull request that:

1. adds an empty `Unreleased` section to `CHANGELOG.md`;
2. moves accumulated entries into `[VERSION] - YYYY-MM-DD`;
3. describes compatibility and operational limitations prominently; and
4. passes the complete local gate.

```sh
./scripts/check-release-gate.sh
make clean verify
make e2e
```

Merge the pull request, fetch `main`, and record its exact commit:

```sh
git fetch origin main --tags
git rev-parse origin/main
```

## Rehearse with a release candidate

For the first release, exercise the complete tag workflow with a release
candidate. Replace `RELEASE_SHA` with the recorded commit:

```sh
git tag -s v1.0.0-rc.1 RELEASE_SHA
git push origin v1.0.0-rc.1
```

The tag is the publication approval boundary. After verify, build, and
exact-image TAMOSS E2E pass, the workflow publishes a GitHub prerelease and
`ghcr.io/livewyer-ops/tamsin:1.0.0-rc.1`; it does not move `latest`.

Check:

- the GitHub release contains four platform binaries,
  `tamsin-third-party-licenses.tar.gz`, `tamsin-container-metadata.json`,
  `tamsin-supply-chain.tar.gz`, and `SHA256SUMS`;
- `gh attestation verify BINARY --repo livewyer-ops/tamsin` accepts every
  downloaded binary;
- the image has the recorded immutable index digest, with runnable amd64 and
  arm64 members and a provenance/SBOM attestation for each;
- the container metadata records the exact immutable FFmpeg runtime index, and
  the supply-chain archive contains both platforms' SPDX and provenance JSON;
- `tamsin --version` reports `1.0.0-rc.1`;
- `tamsin doctor --format json` returns the documented schema; and
- the first-ingest procedure succeeds against the intended environment.

If the candidate fails, fix the problem on `main` and cut `rc.2`. Do not delete
or replace `rc.1`.

## Publish the stable release

After accepting the candidate, tag the exact reviewed stable commit:

```sh
git tag -s v1.0.0 RELEASE_SHA
git push origin v1.0.0
```

After the same gates pass, the workflow publishes the GitHub release, the
versioned multi-platform image `ghcr.io/livewyer-ops/tamsin:1.0.0`, and then
promotes the byte-identical image index to
`ghcr.io/livewyer-ops/tamsin:latest`.

Verify the checksums, build attestations, image digest, generated version,
doctor output, and one real ingest before announcing the release. A normal pull
downloads only the host's platform image; the other platform and the attached
attestations remain registry metadata.

## Correct a released defect

Never force-push a release tag or overwrite a versioned image. Correct the
source on `main`, add a new changelog section, and release the next SemVer patch
such as `v1.0.1`.
