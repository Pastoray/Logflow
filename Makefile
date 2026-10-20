.PHONY: build docker-build compose-up compose-down clean

GOFLAGS_OFFLINE := -mod=vendor
LDFLAGS := -s -w

# Compiles agent/loadgen/healthcheck as static Linux binaries into dist/,
# using only the vendored module cache - no network access needed. This is
# the step `docker build` depends on (see Dockerfile): Docker just packages
# these into a scratch image, it doesn't compile them.
build:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux go build $(GOFLAGS_OFFLINE) -trimpath -ldflags="$(LDFLAGS)" -o dist/agent ./cmd/agent
	CGO_ENABLED=0 GOOS=linux go build $(GOFLAGS_OFFLINE) -trimpath -ldflags="$(LDFLAGS)" -o dist/loadgen ./cmd/loadgen
	CGO_ENABLED=0 GOOS=linux go build $(GOFLAGS_OFFLINE) -trimpath -ldflags="$(LDFLAGS)" -o dist/healthcheck ./cmd/healthcheck
	cp /etc/ssl/certs/ca-certificates.crt dist/ca-certificates.crt

docker-build: build
	docker build -t logflow:latest .

compose-up: docker-build
	docker compose up -d

compose-down:
	docker compose down -v

clean:
	rm -rf dist bin
