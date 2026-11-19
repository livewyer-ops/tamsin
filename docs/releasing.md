# Cut a release

Release tags are bare `MAJOR.MINOR.PATCH-inN` versions: the triple is the BBC
TAMS API version the release targets and `N` counts TAMSin releases for it,
for example `8.2.0-in1`. Append `-rcM` for a release candidate, for example
`8.2.0-in2-rc1`. The release workflow rejects any other tag form. A tag
without `-rc` is a stable release and moves the minor, major and `latest`
image tags; a candidate is published as a GitHub prerelease and moves none.

Repository tag protection must cover `*.*.*`, including candidates. A rule
covering only legacy `v*` tags does not protect these releases.

The commands below assume a clone of `https://github.com/livewyer-ops/tamsin`
with that repository as `origin`. Confirm its push URL with
`git remote get-url --push origin` before tagging. Push only the intended tag
using an explicit refspec; do not use `--tags` or `--mirror`.

See [Contributing](../CONTRIBUTING.md) for prerequisites. Select an unused
version and keep it in the same shell for the commands below (replace the
placeholder before running them):

```sh
version=MAJOR.MINOR.PATCH-inN
```

For a candidate, include its `-rcM` suffix. Before tagging:

1. move the intended unreleased notes into a dated section in
   [CHANGELOG.md](../CHANGELOG.md), with the base release version in brackets
   and a non-empty body. A candidate such as `8.2.0-in3-rc1` uses the
   `[8.2.0-in3]` section. Check extraction with
   `./scripts/release-notes.py "$version" CHANGELOG.md` before tagging;
2. for a stable release, update the README downloads, image example and
   documentation version links to the selected version; for a candidate, keep
   the stable install links and identify the candidate documentation explicitly;
3. merge the release documentation changes into `main` and wait for green CI;
4. run `make verify` from a fresh clone of the release commit, then
   `make image-smoke` and the live `make e2e` matrix;
5. confirm the `ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime` package is public
   and that the runtime tag and digest in `Dockerfile` resolve;
6. verify the first-ingest tutorial with that build.
   For a candidate, record the current major, minor and `latest` image digests
   so they can be checked for unintended movement afterwards.

Create and push the tag:

```sh
git fetch origin
git switch main
git merge --ff-only origin/main
git tag -a "$version" -m "Release $version"
git push origin "refs/tags/$version:refs/tags/$version"
```

The release workflow requires the tagged commit to belong to `main`, repeats
verification and both TAMOSS compatibility tests,
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
machine. For a stable release, confirm that the major, minor and `latest`
image tags share the versioned image's digest. For a candidate, confirm that
the moving tags retain their previous digests and the GitHub release is marked
as a prerelease. Do not move or recreate an existing release tag; publish a new
`-inN` or `-rcM` tag
instead. `go install` is not a supported install path: the binaries and the
image are the distribution.

The FFmpeg runtime is a separate, immutable build. Change its revision when
changing `Dockerfile.ffmpeg`, publish it from `main`, then update the application
image's runtime tag and index digest together. Existing tags and ambiguous
registry errors stop the runtime workflow. A runtime change triggers it on
`main`; manual runs also require `main`.

The application image includes its licence and dependency notices under
`/usr/share/doc/tamsin`; Debian/FFmpeg package notices remain in the runtime.
SBOMs describe components and do not replace licence/source distribution.
