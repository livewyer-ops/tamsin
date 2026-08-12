# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
ARG SOURCE_DATE_EPOCH=0
ARG FFMPEG_RUNTIME_IMAGE=ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime:5.1.9-bookworm-r1
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
ENTRYPOINT ["/usr/local/bin/tamsin"]
