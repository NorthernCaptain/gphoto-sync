GO ?= go
export GOTOOLCHAIN ?= auto
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test check vet lint image clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/gphoto-sync ./cmd/gphoto-sync

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

check: vet test

image:
	docker build --build-arg VERSION=$(VERSION) -t gphoto-sync:local .

clean:
	rm -rf bin
