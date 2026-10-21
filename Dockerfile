# logflow runtime image.
#
# This Dockerfile intentionally does NOT use a `FROM golang:... AS builder`
# multi-stage pull. It's built FROM scratch and just packages statically
# linked binaries that are compiled first, on the host, via `make build`
# (see Makefile) - a plain `CGO_ENABLED=0 go build ...`. Modules are
# fetched by `make build` on the host (`go mod download`, verified against
# go.sum), not inside `docker build`.
#
# Why: this keeps the whole pipeline - `make build` then `docker build` -
# usable in locked-down environments with no container-registry egress at
# all (this one included: registry-1.docker.io, ghcr.io, gcr.io, quay.io
# and friends are all unreachable here), which is also just a smaller,
# more auditable image in general - no base-image CVEs, nothing in the
# image that isn't one of our own three binaries plus CA roots.
#
# If you're building this somewhere with normal registry access and would
# rather have Docker do the compilation too, replace this file's single
# stage with a standard `FROM golang:1.24-alpine AS builder` stage running
# the same `go build` commands from the Makefile, then copy
# /out/* into this same scratch stage below - nothing else here changes.

FROM scratch

# Go's crypto/x509 on Linux checks /etc/ssl/certs/ca-certificates.crt
# (among a couple of other well-known paths) when no other cert pool is
# configured; scratch has no OS cert store of its own, so this is required
# for the gRPC TLS/mTLS sink and source paths to verify real-world
# certificates. (configs/secure-*.yaml supply their own CA files directly
# and don't depend on this, but anything using the system pool does.)
COPY dist/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY dist/agent /app/agent
COPY dist/loadgen /app/loadgen
COPY dist/healthcheck /app/healthcheck
COPY configs /app/configs

WORKDIR /app
# Runs as root (uid 0): scratch has no /etc/passwd, and a named volume
# Docker creates fresh at /app/data (collector-1-data, collector-2-data in
# docker-compose.yml) is root-owned by default, so a non-root numeric UID
# would hit permission-denied on the file channel's MkdirAll into it. A
# scratch image has no shell, package manager, or setuid binary to escalate
# from in the first place, so running as root here doesn't carry the usual
# risk it would on a full OS image.
ENTRYPOINT ["/app/agent"]
CMD ["-config", "/app/configs/standalone.yaml"]
