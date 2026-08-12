SHELL := /bin/bash

BINARY := bin/tamsin
IMAGE ?= tamsin:dev
IMAGE_PLATFORMS ?= linux/amd64,linux/arm64
OCI_LAYOUT ?= .tmp/release-bundle/tamsin-image
FFMPEG_RUNTIME_IMAGE ?= ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime:5.1.9-bookworm-r1
E2E_PLATFORM ?= linux/amd64
GITLEAKS_IMAGE ?= zricethezav/gitleaks@sha256:cdbb7c955abce02001a9f6c9f602fb195b7fadc1e812065883f695d1eeaba854
VERSION ?= dev
# Preserve source provenance for ordinary developer builds as well as releases.
# Release workflows still pass the exact GITHUB_SHA explicitly.
COMMIT ?= $(shell git describe --always --dirty --abbrev=40 2>/dev/null || printf unknown)
BUILD_DATE ?= 1970-01-01T00:00:00Z
SOURCE_DATE_EPOCH ?= 0
LDFLAGS := -s -w -X github.com/livewyer-ops/tamsin/internal/version.Version=$(VERSION) -X github.com/livewyer-ops/tamsin/internal/version.Commit=$(COMMIT) -X github.com/livewyer-ops/tamsin/internal/version.Date=$(BUILD_DATE)

.PHONY: all build clean verify format-check mod-check lint test vuln secret-scan smoke dist image oci-image image-smoke e2e e2e-existing

all: build

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/tamsin

clean:
	rm -rf bin dist coverage.out .tmp .cache
	rm -f *.test

format-check:
	./scripts/check-format.sh
	bash -n scripts/*.sh
	python3 ./scripts/check-python-heredocs.py scripts/*.sh
	python3 -c 'import pathlib, sys; [compile(pathlib.Path(name).read_text(), name, "exec") for name in sys.argv[1:]]' scripts/*.py

mod-check:
	go mod tidy -diff
	go mod verify

lint:
	go vet ./...
	go tool staticcheck ./...

# Race detection is part of the default gate because batch ingestion is concurrent.
#
# -coverpkg attributes coverage to the package a statement lives in rather than
# to the package whose test ran it. Without it the contract and CLI suites,
# which drive internal/ from outside, report nothing. Read the aggregate with
# `make coverage`: the per-package lines below become each test binary's share
# of the whole internal tree, which is easy to misread as a regression.
test:
	go test -race -coverpkg=./internal/... -coverprofile=coverage.out ./...

coverage: test
	@go tool cover -func=coverage.out | tail -1

vuln:
	go tool govulncheck ./...

secret-scan:
	docker run --rm -v "$(CURDIR):/repo:ro" $(GITLEAKS_IMAGE) detect --source /repo --config /repo/.gitleaks.toml --no-git --redact --exit-code 1

smoke: build
	$(BINARY) --help >/dev/null
	$(BINARY) --version >/dev/null
	$(BINARY) completion bash >/dev/null

verify: format-check mod-check lint test vuln secret-scan smoke

image:
	docker build --build-arg VERSION='$(VERSION)' --build-arg COMMIT='$(COMMIT)' --build-arg BUILD_DATE='$(BUILD_DATE)' --build-arg SOURCE_DATE_EPOCH='$(SOURCE_DATE_EPOCH)' --build-arg FFMPEG_RUNTIME_IMAGE='$(FFMPEG_RUNTIME_IMAGE)' -t $(IMAGE) .

# Export a registry-ready OCI image layout instead of placing a host-only image
# in the Docker daemon. Release automation retains this directory verbatim and
# never invokes this target after the exact-artifact E2E gate.
oci-image:
	@test ! -e '$(OCI_LAYOUT)' \
		|| { echo "OCI layout already exists at $(OCI_LAYOUT); run make clean first" >&2; exit 1; }
	@mkdir -p '$(dir $(OCI_LAYOUT))'
	docker buildx build \
		--pull \
		--platform '$(IMAGE_PLATFORMS)' \
		--provenance=mode=min \
		--sbom=true \
		--build-arg VERSION='$(VERSION)' \
		--build-arg COMMIT='$(COMMIT)' \
		--build-arg BUILD_DATE='$(BUILD_DATE)' \
		--build-arg SOURCE_DATE_EPOCH='$(SOURCE_DATE_EPOCH)' \
		--build-arg FFMPEG_RUNTIME_IMAGE='$(FFMPEG_RUNTIME_IMAGE)' \
		--tag '$(IMAGE)' \
		--output 'type=oci,dest=$(OCI_LAYOUT),tar=false,rewrite-timestamp=true' \
		.

image-smoke: image
	docker run --rm $(IMAGE) --help >/dev/null
	docker run --rm $(IMAGE) doctor --format json >/dev/null
	@test "$$(docker image inspect $(IMAGE) --format '{{.Config.User}}')" != "" \
		|| { echo "OCI image does not configure a non-root user" >&2; exit 1; }

# Produce reproducible CGO-free client binaries; FFmpeg remains a runtime dependency.
dist: clean
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-linux-amd64 ./cmd/tamsin
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-linux-arm64 ./cmd/tamsin
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-darwin-amd64 ./cmd/tamsin
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-darwin-arm64 ./cmd/tamsin
	$(MAKE) oci-image

# Uses the pinned TAMOSS local-kind profile and tears it down on exit.
e2e:
	$(MAKE) image
	$(MAKE) e2e-existing

# Exercise an image that has already been built or loaded. Release automation
# uses this target so the TAMOSS gate cannot quietly rebuild something different
# from the immutable image bundle that will be published.
e2e-existing:
	docker image inspect --platform '$(E2E_PLATFORM)' '$(IMAGE)' >/dev/null
	DOCKER_DEFAULT_PLATFORM='$(E2E_PLATFORM)' IMAGE='$(IMAGE)' ./scripts/e2e-kind.sh
