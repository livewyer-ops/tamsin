# Cut a release

Releases are triggered by a SemVer tag such as `v1.0.0` or a new `v1.0.0-rc.N`.
Before tagging:

1. ensure CI is green on `main`;
2. ensure the version has a dated, non-empty section in `CHANGELOG.md`;
3. run `make verify`, `make image-smoke`, and the live `make e2e` matrix;
4. check `python3 ./scripts/check-release-gate.py` reports no blocking TAMS gap; and
5. confirm the FFmpeg runtime tag and digest referenced by `Dockerfile` resolve.

Create and push the tag according to the repository's protected-tag policy:

```sh
git switch main
git pull --ff-only
git tag -a v1.0.0 -m 'Release v1.0.0'
git push origin v1.0.0
```

The release workflow repeats verification and both TAMOSS compatibility tests,
then:

- builds Linux and macOS binaries for amd64 and arm64;
- bundles dependency licences and required source in
  `tamsin-third-party-licenses.tar.gz`, covered by checksums and attestations;
- publishes `SHA256SUMS` and GitHub build attestations;
- rejects an existing application version tag or an ambiguous registry lookup;
- builds a candidate non-root amd64/arm64 image and smoke-tests both platforms;
- attaches BuildKit SBOM and provenance attestations to the image;
- assigns the version tag after smoke tests, with moving major, minor and
  `latest` tags only for a stable release; and
- creates the GitHub release from the matching changelog section.

After completion, verify one binary and the versioned image from a clean
machine. Do not move or recreate an existing release tag; publish a new patch
or release-candidate tag instead.

The FFmpeg runtime is a separate, immutable build. Change its revision when
changing `Dockerfile.ffmpeg`, publish it from `main`, then update the application
image's runtime tag and index digest together. Existing tags and ambiguous
registry errors stop the runtime workflow. A runtime change triggers it on
`main`; manual runs also require `main`.

The application image includes its licence and dependency notices under
`/usr/share/doc/tamsin`; Debian/FFmpeg package notices remain in the runtime.
SBOMs describe components and do not replace licence/source distribution.
