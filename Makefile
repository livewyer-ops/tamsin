SHELL := /bin/bash

BINARY := bin/tamsin
IMAGE ?= tamsin:dev
E2E_PLATFORM ?= linux/amd64
TAMSIN_E2E_VERSION ?= 8.1 8.2
VERSION ?= dev
COMMIT ?= $(shell commit=$$(git rev-parse HEAD 2>/dev/null) || commit=unknown; test -z "$$(git status --porcelain --untracked-files=no 2>/dev/null)" || commit=$$commit-dirty; printf '%s' "$$commit")
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
	@unformatted="$$(find . \( -path ./.git -o -path ./.cache -o -path ./.tmp -o -path ./bin -o -path ./dist \) -prune -o -name '*.go' -print0 | xargs -0 -r gofmt -l)" && \
		{ test -z "$$unformatted" || { printf '%s\n' "$$unformatted"; exit 1; }; }
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
	docker build --pull --build-arg VERSION='$(VERSION)' --build-arg COMMIT='$(COMMIT)' --build-arg BUILD_DATE='$(BUILD_DATE)' $(if $(FFMPEG_RUNTIME_IMAGE),--build-arg FFMPEG_RUNTIME_IMAGE='$(FFMPEG_RUNTIME_IMAGE)') -t $(IMAGE) .

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
	docker image inspect '$(IMAGE)' >/dev/null
	@for version in $(TAMSIN_E2E_VERSION); do \
		DOCKER_DEFAULT_PLATFORM='$(E2E_PLATFORM)' IMAGE='$(IMAGE)' TAMSIN_E2E_VERSION="$$version" ./scripts/e2e-kind.sh || exit; \
	done
