SHELL := /bin/bash

BINARY := bin/tamsin
IMAGE ?= tamsin:dev
FFMPEG_RUNTIME_IMAGE ?= ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime:5.1.9-bookworm-r2@sha256:cc5e5965ace04ead6c1db6a5b3d121229d149a1c942bdf8d810c5a94b63fbafc
E2E_PLATFORM ?= linux/amd64
TAMSIN_E2E_CONTRACT ?= tams-v8.1.json tams-v8.2.json
VERSION ?= dev
COMMIT ?= $(shell git describe --always --dirty --abbrev=40 2>/dev/null || printf unknown)
BUILD_DATE ?= 1970-01-01T00:00:00Z
LDFLAGS := -s -w -X github.com/livewyer-ops/tamsin/internal/version.Version=$(VERSION) -X github.com/livewyer-ops/tamsin/internal/version.Commit=$(COMMIT) -X github.com/livewyer-ops/tamsin/internal/version.Date=$(BUILD_DATE)

.PHONY: all build clean verify format-check mod-check lint test coverage vuln smoke dist image image-smoke e2e e2e-existing

all: build

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/tamsin

clean:
	rm -rf bin dist coverage.out .tmp .cache
	rm -f *.test

format-check:
	./scripts/check-format.sh
	@for script in scripts/*.sh; do bash -n "$$script" || exit; done
	python3 -c 'import pathlib, sys; [compile(pathlib.Path(name).read_text(), name, "exec") for name in sys.argv[1:]]' scripts/*.py

mod-check:
	go mod tidy -diff
	go mod verify

lint:
	go vet ./...
	go tool staticcheck ./...

test:
	go test -race ./...

coverage:
	go test -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

vuln:
	go tool govulncheck ./...

smoke: build
	$(BINARY) --help >/dev/null
	$(BINARY) --version >/dev/null
	$(BINARY) completion bash >/dev/null

verify: format-check mod-check lint test vuln smoke

dist:
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-linux-amd64 ./cmd/tamsin
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-linux-arm64 ./cmd/tamsin
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-darwin-amd64 ./cmd/tamsin
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/tamsin-darwin-arm64 ./cmd/tamsin
	./scripts/build-third-party-licenses.sh
	(cd dist && sha256sum tamsin-linux-amd64 tamsin-linux-arm64 tamsin-darwin-amd64 tamsin-darwin-arm64 tamsin-third-party-licenses.tar.gz > SHA256SUMS)

image:
	docker build --pull --build-arg VERSION='$(VERSION)' --build-arg COMMIT='$(COMMIT)' --build-arg BUILD_DATE='$(BUILD_DATE)' --build-arg FFMPEG_RUNTIME_IMAGE='$(FFMPEG_RUNTIME_IMAGE)' -t $(IMAGE) .

image-smoke: image
	docker run --rm $(IMAGE) --help >/dev/null
	docker run --rm $(IMAGE) doctor --format json >/dev/null
	@user="$$(docker image inspect $(IMAGE) --format '{{.Config.User}}')"; \
	case "$$user" in ''|0|0:*|root|root:*) \
		echo "OCI image configures an unsafe user: $${user:-<empty>}" >&2; exit 1 ;; \
	esac

e2e: image
	$(MAKE) e2e-existing

e2e-existing:
	docker image inspect --platform '$(E2E_PLATFORM)' '$(IMAGE)' >/dev/null
	@for contract in $(TAMSIN_E2E_CONTRACT); do \
		DOCKER_DEFAULT_PLATFORM='$(E2E_PLATFORM)' IMAGE='$(IMAGE)' TAMSIN_E2E_CONTRACT="$$contract" ./scripts/e2e-kind.sh || exit; \
	done
