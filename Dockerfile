# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG FFMPEG_RUNTIME_IMAGE=ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime:5.1.9-bookworm-r2@sha256:cc5e5965ace04ead6c1db6a5b3d121229d149a1c942bdf8d810c5a94b63fbafc
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b AS source

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

FROM source AS licences
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    mkdir -p /out && bash scripts/build-third-party-licenses.sh /out/licences.tar.gz && \
    tar -xzf /out/licences.tar.gz -C /out

FROM source AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=1970-01-01T00:00:00Z
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags "-s -w -X github.com/livewyer-ops/tamsin/internal/version.Version=${VERSION} -X github.com/livewyer-ops/tamsin/internal/version.Commit=${COMMIT} -X github.com/livewyer-ops/tamsin/internal/version.Date=${BUILD_DATE}" \
      -o /out/tamsin ./cmd/tamsin

FROM ${FFMPEG_RUNTIME_IMAGE}

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=1970-01-01T00:00:00Z
ARG FFMPEG_RUNTIME_IMAGE
LABEL org.opencontainers.image.title="Tamsin" \
      org.opencontainers.image.description="TAMS media ingest CLI" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/livewyer-ops/tamsin" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.base.name="${FFMPEG_RUNTIME_IMAGE}"

COPY --from=build /out/tamsin /usr/local/bin/tamsin
COPY --from=licences /out/third-party-licenses /usr/share/doc/tamsin/third-party-licenses
COPY LICENSE /usr/share/doc/tamsin/LICENSE
WORKDIR /work
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/tamsin"]
