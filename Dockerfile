# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG SOURCE_DATE_EPOCH=0
FROM --platform=$BUILDPLATFORM golang:1.26-bookworm@sha256:6c5605ab3a9a9fb3c4eafe5b3d63cdbf3881caf113262b67862547b54a9db599 AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=1970-01-01T00:00:00Z
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags "-s -w -X github.com/livewyer-ops/tamsin/internal/version.Version=${VERSION} -X github.com/livewyer-ops/tamsin/internal/version.Commit=${COMMIT} -X github.com/livewyer-ops/tamsin/internal/version.Date=${BUILD_DATE}" \
      -o /out/tamsin ./cmd/tamsin

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=1970-01-01T00:00:00Z
LABEL org.opencontainers.image.title="Tamsin" \
      org.opencontainers.image.description="TAMS media ingest CLI" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/livewyer-ops/tamsin" \
      org.opencontainers.image.licenses="Apache-2.0"

ARG DEBIAN_SNAPSHOT=20260803T000000Z
ARG FFMPEG_VERSION=7:5.1.9-0+deb12u1
ARG CA_CERTIFICATES_VERSION=20230311+deb12u1
LABEL io.livewyer.tamsin.ffmpeg.version="${FFMPEG_VERSION}" \
      io.livewyer.tamsin.debian.snapshot="${DEBIAN_SNAPSHOT}"

# FFmpeg decides how media is cut. Use the dated archives recorded by the
# official Debian base image so its transitive packages cannot drift when an
# existing release tag is rebuilt. A dependency refresh is an explicit update
# to the base digest, snapshot, and direct package versions together.
RUN sed -i \
      -e "s|^URIs: http://deb.debian.org/debian$|URIs: http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}/|" \
      -e "s|^URIs: http://deb.debian.org/debian-security$|URIs: http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}/|" \
      -e '/^Signed-By:/a Check-Valid-Until: no' \
      /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
      "ca-certificates=${CA_CERTIFICATES_VERSION}" "ffmpeg=${FFMPEG_VERSION}" \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 tamsin \
    && useradd --uid 65532 --gid 65532 --create-home --home-dir /home/tamsin --shell /usr/sbin/nologin tamsin \
    && rm -rf /var/log/*

COPY --from=build /out/tamsin /usr/local/bin/tamsin
USER 65532:65532
WORKDIR /home/tamsin
ENTRYPOINT ["/usr/local/bin/tamsin"]
