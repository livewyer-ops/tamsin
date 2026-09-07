# Cut a release

Release tags are bare `MAJOR.MINOR.PATCH-inN` versions: the triple is the BBC
TAMS API version the release targets and `N` counts TAMSin releases for it,
for example `8.2.0-in1`. Append `-rcM` for a release candidate, for example
`8.2.0-in2-rc1`. The release workflow rejects any other tag form. A tag
without `-rc` is a stable release and moves the minor, major and `latest`
image tags; a candidate is published as a GitHub prerelease and moves none.

Releases are published from the public repository, so the tag is pushed to
the `public` remote by explicit refspec. `origin` is not the release remote.
Never push with `--tags` or `--mirror`; either would publish private history
and archive tags.

See [Contributing](../CONTRIBUTING.md) for prerequisites. Before tagging:

1. ensure CI is green on `main`;
2. add a `## [8.2.0-in1] - YYYY-MM-DD` section to [CHANGELOG.md](../CHANGELOG.md)
   with the full version in brackets and a non-empty body; the workflow takes
   the release notes from it and fails without it;
3. run `make verify` from a fresh clone of the release commit, then
   `make image-smoke` and the live `make e2e` matrix;
4. delete stray local tags so that only the release tag can be pushed;
5. confirm the `ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime` package is public
   and that the runtime tag and digest in `Dockerfile` resolve.

Create and push the tag:

```sh
git fetch public
git switch main
git merge --ff-only public/main
git tag -a 8.2.0-in1 -m 'Release 8.2.0-in1'
git push public 8.2.0-in1
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
machine and confirm that `latest` and the minor tag share its digest. Do not
move or recreate an existing release tag; publish a new `-inN` or `-rcM` tag
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
