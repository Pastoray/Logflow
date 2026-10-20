# logflow

A distributed log aggregator in Go, modeled on Apache Flume's
source → channel → sink architecture, built on gRPC + Protocol Buffers.

```
 producers            edge agent                      collector agent
┌──────────┐   gRPC   ┌───────────────────────┐  gRPC  ┌───────────────────────┐
│  app /   │ ───────▶ │ FileTailSource         │──────▶ │ GRPCSource             │
│ loadgen  │          │      │                 │        │      │                 │
└──────────┘          │      ▼                 │        │      ▼                 │
                       │ MemoryChannel (sharded)│        │ FileChannel (WAL)      │
                       │      │                 │        │      │                 │
                       │      ▼                 │        │      ▼                 │
                       │ GRPCSink (pipelined)   │        │ FileSink (rolling)     │
                       └───────────────────────┘        └───────────────────────┘
```

Any number of these agents can be chained: a sink in one agent is a gRPC
client of the next agent's source, exactly like Flume's Avro source/sink
pairing. Each agent is independently configured via YAML and can mix and
match source/channel/sink types.

## Components

**Sources** (`internal/source`)
- `GRPCSource` — implements the `Collector` gRPC service. Accepts a
  bidirectional streaming RPC (`StreamEvents`) so a well-behaved client can
  pipeline many batches without a round trip per batch, plus a unary
  `SendBatch` for simple/low-volume producers. Used both for external
  ingestion and for agent-to-agent forwarding.
- `FileTailSource` — follows a growing/rotating file like `tail -F`.

Every source runs its events through an **interceptor chain** and then a
**channel selector** before they're considered received — see below.

**Interceptors** (`internal/interceptor`)

A source can declare an ordered list of interceptors that transform or
filter every event before it's routed to a channel. Built-ins:

| type              | does |
|-------------------|------|
| `timestamp`       | stamps a header with ingest time (ms since epoch); `preserve_existing` skips events that already have it |
| `host`            | tags the event with the agent's hostname (or IP with `use_ip: true`) |
| `static`          | adds a fixed `key`/`value` header |
| `regex_filter`    | drops events whose body matches (or, with `exclude: true`, doesn't match) a pattern |
| `regex_extractor` | promotes regex capture groups from the body into headers, by name (`(?P<name>...)`) or 1-based index |

New interceptor types are added by implementing one `Intercept(*Event) *Event`
method (return `nil` to drop) — the config schema (`type` + a string-keyed
`params` map) never has to change.

**Channel Selectors** (`internal/router`)

A source can target more than one channel via `channels: [...]` plus a
`selector`:
- `replicating` (default when more than one channel is listed) — every
  event goes to every listed channel.
- `multiplexing` — routes by the value of one header: `mapping` maps header
  values to a list of destination channels, `default` catches everything
  else. This is how you'd split one firehose into per-tenant, per-priority,
  or per-destination channels.

**Channels** (`internal/channel`)
- `MemoryChannel` — bounded, **sharded** in-memory queue. Producers hash
  across N independent Go channels instead of funneling through one shared
  queue, reducing lock contention under many concurrent goroutines; a fair
  fan-in read uses `reflect.Select` across all shards. Fast, but not
  durable — a crash loses whatever's buffered.
- `FileChannel` — a real **write-ahead log**: events are appended to disk
  (flushed immediately, fsynced on a timer) and only exposed to consumers
  once durably written. Segments roll at a configurable size and are
  deleted once fully drained. On restart it replays whatever segment files
  remain on disk. Every entry carries a CRC32 checksum; recovery stops
  replaying a segment at the first entry that fails verification rather
  than risking a torn write (or genuine bit rot) cascading into garbage.
  Delivery is **at-least-once**: a crash between "consumed" and "segment
  fully drained" can replay already-delivered events — this is the same
  trade-off Flume's file channel makes, and downstream consumers should be
  idempotent or dedupe if that matters.

  Segment cleanup happens two ways. The common path is reactive: a segment
  is deleted the moment its last entry is taken, as a normal side effect
  of consumption. The backstop is an **active retention sweep** enforcing
  an optional `RetentionPolicy` (`retention_max_age` / `retention_max_bytes`,
  swept every `compaction_interval`) - independent of whether consumers
  have caught up at all. A segment past its retention limit is purged even
  if it still holds unconsumed entries, the same trade-off Kafka's
  `retention.ms`/`retention.bytes` make: a retention limit describes how
  long you're willing to keep data, period, separate from whether it's
  been delivered. Purged-but-still-unconsumed events are counted via
  `PurgedUnconsumed()` (surfaced in `/stats` as `purged_unconsumed`) so
  it's visible when this is actually discarding data versus just
  reclaiming already-delivered space.
- `SpillableMemoryChannel` (`type: spillable`) — Flume's other durability
  pattern: a fast `MemoryChannel` primary with automatic overflow to a
  durable `FileChannel` once the primary fills, rather than blocking or
  dropping. `Take()` drains the overflow with priority, since anything
  sitting there means the system was already falling behind. Reports a
  `Spilled()` counter (surfaced in `/stats`) as a signal that downstream
  consumers aren't keeping up. The same `RetentionPolicy` can be applied
  to its overflow.
- Any of the above can be wrapped in a `cluster:` block to replicate it
  across multiple agent processes via Raft instead of running
  single-node, or a `sharded_cluster:` block to partition it across
  several *independent* Raft groups for write throughput that scales past
  one group's single-leader ceiling — see "Clustered collector (Raft)"
  below.

**Sinks** (`internal/sink`)
- `GRPCSink` — drains a channel, batches events (by count or a linger
  timeout, whichever comes first), and forwards them over a streaming RPC.
  Runs several worker goroutines (`parallelism`) each holding its own
  stream, and each worker keeps several batches in flight
  (`in_flight_batches`) rather than waiting for an ack before sending the
  next one. Supports gzip compression, automatic reconnect with backoff,
  and requeues events back onto the channel on send failure.
- `FileSink` — appends batches as newline-delimited text to files that roll
  by size or time.
- `StdoutSink` / `NullSink` — for local testing.

**Sink Processors / groups** (`internal/sink`, configured under
`sink_groups` instead of `sinks`)

A group of backends draining one channel, coordinated by a policy. A target
is normally a gRPC `host:port`, or `file:<dir>` for a rolling-file backend
(tuned by `file_roll_max_bytes` / `file_roll_interval`, defaults 128MiB / 1m)
- e.g. `targets: ["collector:7000", "file:/var/spool/logflow"]` makes a
local disk spool the failover when the collector is unreachable. A file
target counts as healthy when a probe file can be created in its directory.
A write error (e.g. disk full) hands the batch back to the channel, so a
partly-written batch may duplicate its first events (at-least-once):
- `load_balance` — every backend is active simultaneously; `parallelism`
  workers each bind to one target (`round_robin`, wrapping if there are
  more workers than targets, or `random`), all pulling concurrently from
  the same channel. A target being down only removes that fraction of
  capacity.
- `failover` — exactly one backend is active at a time, chosen by priority
  (`targets[0]` first). On failure it's marked down and traffic moves to
  the next available target; a background prober `Ping`s every
  higher-priority target every `recovery_probe_interval` and promotes one
  back the moment it responds, by cancelling the active stream so the
  drain loop reconnects immediately rather than waiting for the current
  target to fail on its own.

**Agent** (`internal/agent`) wires up sources/interceptors/selectors/
channels/sinks/sink-groups from a `config.Config` and exposes `/metrics`
(Prometheus text format), `/healthz`, and `/stats` over HTTP.

## Encryption and security

Every gRPC endpoint (source or sink) is unencrypted, unauthenticated
plaintext by default — the same "secure is opt-in" stance as the rest of
this project. Turn on either or both of two independent layers per
endpoint via `internal/security`:

**TLS / mutual TLS** — set `tls.enabled: true` under a source, sink, or
sink group:

```yaml
tls:
  enabled: true
  cert_file: certs/server-cert.pem   # this endpoint's own identity
  key_file: certs/server-key.pem
  client_auth: true                  # server only: require+verify a client cert (mTLS)
  ca_file: certs/ca-cert.pem         # server: CA that signs client certs; client: CA that signs the server cert
```

On a sink/sink-group (client role), also set `cert_file`/`key_file` if the
server requires a client certificate (mTLS), and optionally `server_name`
(override the hostname used for certificate verification, e.g. when
`target_addr` is a bare IP) or `insecure_skip_verify: true` (skip server
certificate verification — local testing only, never on an untrusted
network).

**Bearer token auth** — set `auth_token` on a source (the exact string
every caller must present) and the matching `auth_token` on whatever
sink/sink-group talks to it. This is checked independently of TLS via gRPC
metadata, using a constant-time comparison, so it composes with or without
encryption — though without TLS the token itself travels in clear text, so
`grpc-go` refuses to send it over a plaintext connection unless the client
config's TLS is enabled (see `RequireTLS` in `internal/security`).

**Parallel WALs** — a `file` channel (or a `spillable` channel's overflow)
funnels every `Put` through one active write segment. Set `wal_shards: N`
(N >= 2) to split it into N independent logs under `dir/wal-0 ... wal-N-1`,
each with its own segment, lock and fsync loop; events are assigned
round-robin and `Take` fans in across all of them, so ordering holds within
a WAL, not across them (same trade as the memory channel's `shards`).
`capacity` is split evenly. Changing the count on a non-empty `dir` is
refused at startup (switching to/from the single-WAL layout, or shrinking N)
because it would strand unread events - drain first or use a new `dir`;
growing N is allowed.

**Per-caller tokens** — instead of one shared `auth_token`, a source can set
`auth_tokens_file: /etc/logflow/tokens.yaml` (mutually exclusive with
`auth_token`). Each entry is one caller with its own identity, optional expiry
and a revoked flag:

```yaml
tokens:
  - id: edge-eu-1
    token_sha256: 9f86d0...        # sha256 hex of the secret (preferred)
    expires_at: 2027-01-01T00:00:00Z
  - id: batch-job
    token: plain-secret            # convenience; hashed on load, never kept
    revoked: true
```

`go run ./cmd/tokengen -id edge-eu-1 -expires-in 720h` mints a secret (shown
once) and the matching entry. The file is re-read when it changes (checked at
most once a second), so adding, expiring or revoking a caller needs no
restart; an unparseable edit keeps the last good set, while a bad file at
startup is a hard error. Open streams re-validate about once a second, so
revoking or expiring a caller also cuts off a stream it already had open
(`Unauthenticated`), not just new connections. Per-caller metrics:
`logflow_auth_accepted_total{caller}`, `logflow_auth_rejected_total{caller,reason}`
(`invalid`/`revoked`/`expired`; `caller="unknown"` when the token matched
nothing) and `logflow_auth_tokens_active`. Clients keep using the same
`auth_token` field with their own secret.

Generate a throwaway CA + server + client certificate for local testing:

```sh
./scripts/gen-certs.sh   # writes certs/{ca,server,client}-{cert,key}.pem
```

`configs/secure-standalone.yaml` (mTLS + token on a single agent's
ingestion source) and `configs/secure-collector-agent.yaml` /
`configs/secure-edge-agent.yaml` (mTLS + token on an agent-to-agent hop)
are ready-to-run examples — see "Verified behavior" below for exactly what
was exercised against them.

`cmd/loadgen` has matching flags (`-tls`, `-cacert`, `-cert`, `-key`,
`-server-name`, `-insecure-skip-verify`, `-token`) so it can drive a
TLS/mTLS/token-protected agent directly.

## Graceful shutdown and controlled draining

Shutdown is a staged drain, not an instant kill, on the first SIGINT or
SIGTERM:

1. **Stop ingestion.** Sources are cancelled immediately - gRPC sources
   `GracefulStop()` (refuse new streams, let any RPC already in flight
   finish naturally rather than severing it), `filetail` sources stop
   polling. No new events are admitted from this point on.
2. **Drain.** Sinks keep running against whatever's already buffered in
   channels, polling every 100ms, until every channel reports empty or
   the agent's `drain_timeout` (default 30s) elapses - whichever comes
   first. The HTTP server (and `/stats`) stays up through this stage so
   you can watch progress.
3. **Stop sinks and close.** Sinks are cancelled, the HTTP server is shut
   down, and every channel is closed.

Each stage logs what it's doing and how it concluded (fully drained vs.
timed out with N events left, etc). A **second** SIGINT/SIGTERM at any
point during this sequence forces an immediate stop instead of waiting
out the current stage's timeout - the conventional "I understand this may
lose buffered data, stop now" escape hatch, for a source stuck waiting on
a slow producer or a drain that's taking longer than you want to wait for.

`drain_timeout` is set per agent (top-level config, alongside `agent_id`).

## High-performance features

- Bidirectional gRPC streaming with per-worker pipelining (multiple batches
  in flight, not send-wait-send-wait)
- Sharded memory channel to cut lock contention across many goroutines
- Configurable batching (size + linger) at both source and sink boundaries
- Optional gzip compression on the wire
- Bounded channels provide real backpressure (`Put` blocks / respects
  context) instead of unbounded queues that can OOM an agent
- Parallel sink workers per channel, and parallel/priority-ordered
  backends via sink groups (load balance / failover)
- Async batch acking (ack for batch N doesn't block batch N+1 from sending)
- Durable file channel with immediate flush + timed fsync, so durability
  doesn't require fsync-per-event, and per-segment (not per-channel)
  locking so a slow fsync doesn't stall unrelated writers
- Spillable memory channel: fast in-memory path for the common case,
  automatic durable overflow under burst instead of blocking or dropping
- CRC32-verified WAL entries with bounded-allocation recovery, so a torn
  write or on-disk corruption is caught and safely truncated rather than
  crashing the process or replaying garbage
- Automatic reconnect + exponential-ish backoff on downstream failures,
  with best-effort requeue instead of silent data loss
- In-flight interceptor chain (filter/enrich/tag) and channel selectors
  (replicating/multiplexing fan-out) for routing without extra hops
- Zero-copy-oriented I/O throughout the pipeline: an event's body bytes
  are shared by reference (never duplicated) from gRPC ingestion through
  channel routing, replication fan-out, and sink forwarding; the file
  channel additionally pools its write-side encode buffer (`sync.Pool`)
  and aliases the read-side decode buffer directly into the returned
  `Event` instead of copying it - see "Verified behavior" for measured
  before/after allocation numbers
- Graceful, staged shutdown: on SIGINT/SIGTERM, sources stop admitting new
  events first, then sinks keep draining whatever's already buffered until
  every channel is empty or `drain_timeout` elapses, and only then are
  sinks stopped and channels closed - a second signal during that drain
  forces an immediate quit instead of waiting it out

## Building

```sh
go build ./...
```

The repo vendors its full dependency tree under `vendor/` (built with
`go mod vendor`). Go 1.14+ auto-detects a top-level `vendor/` directory and
defaults to `-mod=vendor`, so the command above needs no network access at
all - no `GOPROXY`, no module-proxy reachability required. This is also
what the Docker image's build relies on (see Deployment below). To build
against the module proxy instead (e.g. after bumping a dependency version),
pass `-mod=mod` explicitly, then re-run `go mod vendor` to resync.

If you regenerate `proto/logflow.proto`, you'll need `protoc`,
`protoc-gen-go`, and `protoc-gen-go-grpc` on your `PATH`:

```sh
protoc --go_out=gen/logflowpb --go_opt=paths=source_relative \
       --go-grpc_out=gen/logflowpb --go-grpc_opt=paths=source_relative \
       -I proto proto/logflow.proto
```

## Running

Quick single-agent smoke test:

```sh
go run ./cmd/agent -config configs/standalone.yaml &
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 20000 -batch 500 -duration 10s
curl localhost:9100/stats
```

Two-tier topology (edge agent tailing a file, forwarding to a collector
agent that persists to disk):

```sh
go run ./cmd/agent -config configs/collector-agent.yaml &
go run ./cmd/agent -config configs/edge-agent.yaml &
echo "hello world" >> /var/log/app/app.log
curl localhost:9101/stats   # collector
curl localhost:9100/stats   # edge
```

`configs/*.yaml` document every tunable (batch size/linger, parallelism,
in-flight batches, retry/backoff, channel capacity/shards, WAL segment
size, roll policy, etc).

Interceptors + multiplexing selector + a load-balance sink group, all in
one agent, with two receiving backends:

```sh
go run ./cmd/agent -config configs/backend-a.yaml &
go run ./cmd/agent -config configs/backend-b.yaml &
go run ./cmd/agent -config configs/multiplex-demo.yaml &
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 5000 -batch 100 -duration 5s
curl localhost:9100/stats   # events split between the `prod` and `default` channels
curl localhost:9200/stats   # backend-a's share of the load-balanced prod traffic
curl localhost:9201/stats   # backend-b's share
```

Failover sink group — kill and restart `backend-a` while traffic is
flowing and watch `configs/failover-demo.yaml`'s log switch to
`backend-b`, then promote `backend-a` back the moment it's reachable
again:

```sh
go run ./cmd/agent -config configs/backend-a.yaml &
go run ./cmd/agent -config configs/backend-b.yaml &
go run ./cmd/agent -config configs/failover-demo.yaml &
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 1000 -batch 25 -duration 30s
# in another shell: kill -9 <backend-a pid>, watch it fail over, then
# restart backend-a and watch it get promoted back within recovery_probe_interval
```

TLS + mutual TLS + bearer token, both for direct ingestion and for an
agent-to-agent hop:

```sh
./scripts/gen-certs.sh
go run ./cmd/agent -config configs/secure-standalone.yaml &
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 5000 -batch 100 -duration 5s \
  -tls -cacert certs/ca-cert.pem -cert certs/client-cert.pem -key certs/client-key.pem \
  -token s3cret-demo-token
curl localhost:9100/stats   # events landed
# omit -cert/-key to see the mTLS rejection, or pass -token wrong-token to
# see the auth rejection - both fail closed with 0 events landing
```

Spillable memory channel — a tiny in-memory primary with a slow sink
forces most of the burst to overflow to disk; watch the `spilled` counter
climb in `/stats` while `dropped` stays at 0:

```sh
go run ./cmd/agent -config configs/spillable-demo.yaml &
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 20000 -batch 500 -duration 5s
curl localhost:9100/stats   # e.g. put=104475 take=104475 dropped=0 spilled=98392
ls data/spillable-demo/overflow/   # the durable WAL holding the spilled backlog
```

Graceful shutdown — start a long-running producer, send SIGTERM once to
watch the staged drain, or twice in quick succession to force an
immediate quit instead:

```sh
go run ./cmd/agent -config configs/shutdown-test.yaml &
AGENT_PID=$!
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 20000 -batch 500 -duration 30s &
sleep 1
kill -TERM $AGENT_PID        # first signal: graceful drain begins
# watch the log; send a second signal to force an immediate stop instead:
# kill -TERM $AGENT_PID
```

Active WAL retention/purging — tiny segments and a 3-second retention
window with no sink attached, so nothing is ever consumed and every purge
visibly discards unconsumed data:

```sh
go run ./cmd/agent -config configs/retention-demo.yaml &
go run ./cmd/loadgen -addr 127.0.0.1:6000 -rate 2000 -batch 100 -duration 2s
ls data/retention-demo/wal/ | wc -l   # many segments right after the burst
sleep 4
ls data/retention-demo/wal/ | wc -l   # down to just the active segment
curl localhost:9100/stats             # purged_unconsumed=... in the output
```

## Deployment (Docker)

`Dockerfile` + `docker-compose.yml` package logflow as a container image and
stand up a realistic multi-agent topology with one command.

**Image design.** The image is built `FROM scratch` from statically linked
(`CGO_ENABLED=0`) binaries that are compiled first, on the host, by
`make build` - *not* by a `golang:...` build stage inside `docker build`.
`make build` runs `go build -mod=vendor`, which needs no network access at
all since every dependency is vendored. This means the whole pipeline
(`make build` then `docker build`) works with zero container-registry
access and zero Go-module-proxy access, which also happens to make for a
smaller, more auditable image: nothing in it but logflow's own three
binaries (`agent`, `loadgen`, and a tiny `healthcheck` probe - see below)
and a CA bundle, no base-image CVEs to track. If you're building somewhere
with normal registry access and would rather have Docker compile too, the
Dockerfile has a comment showing the one-stage swap back to a conventional
`FROM golang:1.24-alpine AS builder` pattern.

**Why a `healthcheck` binary.** A `FROM scratch` image has no shell, so the
conventional `HEALTHCHECK CMD wget -qO- http://...` doesn't work. `cmd/healthcheck`
is a ~10-line Go program that GETs a URL and exits 0/1 on 2xx/otherwise;
`docker-compose.yml`'s healthchecks (and the Dockerfile's own `HEALTHCHECK`,
if you add one for standalone `docker run` use) call it directly as the
container's only "binary available."

**Topology.** `docker-compose.yml` stands up two independent edge agents
(each fed by its own `loadgen` sidecar) load-balancing forwarded batches
round-robin across two collector agents, which persist to a durable WAL and
roll output files onto a named Docker volume per collector:

```
  loadgen-1 --> edge-1 --\          /--> collector-1 (WAL + disk, volume collector1-data)
                           >--------<
  loadgen-2 --> edge-2 --/          \--> collector-2 (WAL + disk, volume collector2-data)
```

This is the same edge/collector tiering as `configs/edge-agent.yaml` +
`configs/collector-agent.yaml`, scaled to two agents per tier (over
`configs/docker/*.yaml`, which use Docker service names instead of
`127.0.0.1`) specifically so the load-balance sink group and the WAL's
crash-recovery path both get exercised by the demo, not just described.

```sh
make compose-up                        # build + docker compose up -d
curl -s http://localhost:19100/stats   # edge-1
curl -s http://localhost:19101/stats   # collector-1
```

Published host ports: `19100`/`19103` (edge-1/edge-2 HTTP), `16000`/`16001`
(edge-1/edge-2 gRPC ingest, if you want to point your own `loadgen` at one
from the host), `19101`/`19102` (collector-1/collector-2 HTTP). Since the
image has no shell, `docker compose exec` won't give you a prompt to poke
around in - use the published ports, or `docker compose logs`, instead.

Kill a collector to watch the edges' load-balance groups route around it,
then bring it back and watch the WAL survive the restart:

```sh
docker compose stop collector-1
curl -s http://localhost:19102/stats   # collector-2 now absorbing all the traffic
docker compose start collector-1       # replays its WAL, rejoins the rotation
```

`make compose-down` tears everything down including the named volumes.

## Deployment (Kubernetes)

`k8s/` has manifests for the same topology as `docker-compose.yml` - 2
edges (each with its own `loadgen`) load-balancing across 2 collectors -
as plain Kubernetes objects (`Namespace`, `ConfigMap`, `PersistentVolumeClaim`,
`Deployment`, `Service`), applied in one shot via Kustomize:

```sh
make docker-build                       # build the image first (no registry push - see below)
kind load docker-image logflow:latest   # or: docker save logflow:latest | k3s ctr images import -
kubectl apply -k k8s/
kubectl -n logflow get pods -w
kubectl -n logflow port-forward svc/edge-1 19100:9100 &
curl -s http://localhost:19100/stats
```

Each collector and each edge is a named, single-replica `Deployment` +
`Service` (`collector-1`, `collector-2`, `edge-1`, `edge-2`) rather than a
2-replica `StatefulSet`, so the `ConfigMap` can carry `configs/docker/*.yaml`
completely unchanged - those configs address peers by the Docker Compose
service names they were written for, and this gives every agent that same
name as a Kubernetes `Service` too. `k8s/README.md` has the full writeup,
including the deliberate tradeoff this makes against horizontal scaling
and what a scaled version would need instead.

No image is pushed to a registry here - see "Docker deployment" under
Verified behavior below for why (this environment blocks every container
registry, not just one), and `k8s/README.md` for how to load a locally
built image into `kind`/`k3s`/`minikube` without one.

## Clustered collector (Raft)

Every collector in the topologies above is a single point of failure for
its own data: if `collector-1` dies, whatever was buffered in its WAL is
unavailable until it comes back, and if its disk is lost, that data is
gone. `internal/cluster` adds an alternative: wrap a channel in a `cluster:`
block and N agent processes replicate it via [Raft](https://raft.github.io/)
(leader election + log replication) instead of each holding an independent
copy - every replica ends up with a full durable copy of every committed
event, one node is automatically elected leader, and the group survives
losing a minority of its nodes (2 of 3, 3 of 5, ...) without losing data or
needing a human to intervene.

**Why a hand-rolled Raft instead of a library.** This project already
hand-builds its own channels, retry/backoff, load-balancing and failover
rather than pulling in a message-queue client; the same reasoning applies
here. `hashicorp/raft`, the obvious off-the-shelf choice, turned out to
need Go 1.25 (newer than this project targets) and pulls in a chain of
indirect dependencies (`go-hclog`, `go-metrics`, `go-immutable-radix`,
`golang-lru`, `fatih/color`, and what *those* need in turn) just to get a
terminal-color-aware logger none of this needs. The core Raft algorithm
(Ongaro & Ousterhout, *In Search of an Understandable Consensus
Algorithm*) is well-specified and modest in size, so `internal/cluster`
implements it directly - zero new third-party dependencies, transported
over the stdlib's `net/rpc` (a separate internal control-plane protocol
from the gRPC data path the rest of logflow uses).

**What's in scope, and what's deliberately not.** Implemented: leader
election with randomized timeouts, log replication with the standard
consistency check and backtracking, the majority-commit rule (including
the term-matching safety condition from Raft §5.4.2 that keeps an old
leader's uncommitted entries from being silently committed later),
persistence of term/vote/log across a process restart, live cluster
membership changes via joint consensus (see "Live membership changes"
below), and sharding across multiple independent Raft groups to scale
write throughput past one group's single-leader ceiling (see "Sharding"
below), and opt-in Raft log compaction (see "Log compaction" below).
Deliberately out of scope: automatic shard rebalancing
(shard count and each shard's peer list are fixed at config time - see
"Sharding"). `Put()` calls *are* batched - see "Batching" below - the same
`batch_size`/`batch_linger` trade-off `GRPCSink` already makes, applied
one layer up to the Raft log itself.

### Log compaction

Off by default (`snapshot_threshold: 0` - the log is kept forever, the
original behaviour). Set `snapshot_threshold: N` under a channel's
`cluster:` (per shard for sharded channels) and, once N entries have been
applied since the last snapshot, each node discards the applied prefix of
its Raft log, keeping `snapshot_trailing` recent entries (default N/4) so a
follower that is only slightly behind still catches up from the log. What
survives is the boundary - the last included index, its term, and the
cluster configuration in effect at it - persisted with the rest of the Raft
state, so restarts resume from it instead of replaying history, and a
membership change whose config entries have been compacted away is still
remembered (a node restarted with a stale bootstrap `peers` list picks up the
real membership from the snapshot).

A follower or brand-new member whose next needed entry has been compacted
away is sent an `InstallSnapshot` RPC and then resumes normal replication
after the boundary. **The snapshot is the boundary only, not event data**:
this state machine is "append to the local channel", and a channel's
contents (some already consumed) can't be exported, so a node that catches
up this way has *no copy* of the events in the range it skipped (it logs
`installed snapshot ... log positions A..B were not replayed`). Those events
are still on every node that applied them, so durability is unchanged while
a majority that applied them survives - but a late joiner is not a full
replica of history. Leave compaction off if every replica must hold
everything; the cost is a log (and a new member's catch-up) that grows
without bound. Compaction only advances over entries a node has *applied*,
so a node whose local channel is full (see "Stuck-apply detection") stops
compacting until it drains. Observability: `raft_log_entries` /
`raft_snapshot_index` on `/stats` and
`logflow_cluster_raft_log_entries` / `logflow_cluster_raft_snapshot_index`
on `/metrics`.

### Running the cluster in Docker Compose and Kubernetes

- **Compose:** `docker-compose.cluster.yml` runs three Raft collectors
  (`cluster-1..3`), an edge that load-balances across them, and a loadgen:
  `make build && docker compose -f docker-compose.cluster.yml up --build`.
  `COLLECTOR_CFG=collector-sharded.yaml` switches to two sharded Raft groups
  on the same three nodes. All three collectors run the same config file
  (`configs/docker/cluster/`); the per-node identity comes from `NODE_ID`.
- **Kubernetes:** `k8s/cluster/` (`kubectl apply -k k8s/cluster`) is a
  separate `logflow-cluster` namespace with a 3-replica `StatefulSet`, a
  headless `collector-raft` Service for stable pod DNS, and an edge. All
  pods share one ConfigMap (generated from the compose config by
  `scripts/gen-k8s-cluster-config.py`); `NODE_ID` comes from the pod name.
  It is the single-group layout (the sharded config is compose-only), and
  `replicas: 3` must match the peer list - resizing needs a live membership
  change, not `kubectl scale`.
- Two config features make one shared file work: `${VAR}` / `${VAR:-default}`
  expansion in config files (an unset variable without a default is a
  startup error, a bare `$` is left alone), and `cluster.listen_addr` /
  `client_listen_addr`, which set what a node *binds* separately from the
  peer address others *dial* (a pod can bind `:8301` while advertising
  `collector-0.collector-raft:8301`, a name that may not resolve to it yet).

**How it wires into the existing pipeline.** `cluster.ReplicatedChannel`
implements the same `channel.Channel` interface every other channel type
does (`Put`/`Take`/`Stats`/`Close`), wrapping an underlying channel (a
`FileChannel`, so a replica's copy survives its own restart too, not just
the group's). This is what keeps the integration small:

- `Put()`: if this node is leader, propose the event to the Raft group and
  block until a majority commits it and *this* node applies it locally. If
  not leader, transparently forward the write to whichever node is
  currently believed to be leader (redirecting once more if that belief
  was stale) - so a gRPC source needs zero cluster-awareness; any node in
  the group can accept a client's write.
- `Take()`: only the current leader actually drains the underlying
  channel; a follower's `Take()` blocks (polling leadership, not the
  channel) until it either becomes leader or the caller gives up. Since a
  collector's sinks just call `Take()` in a loop already, this is the
  entire mechanism behind "only the leader's sinks run" - no separate
  gating logic was needed anywhere else in the agent.
- `/stats` and `/metrics` gain `cluster_state`/`cluster_term`/
  `cluster_leader`/`cluster_leader_changes` (and Prometheus
  `logflow_cluster_is_leader`/`logflow_cluster_term`/
  `logflow_cluster_leader_changes_total`) on any clustered channel, via the
  same type-assertion pattern `/stats` already used for `Spilled()`/
  `PurgedUnconsumed()`.

**Batching.** An early version of this feature proposed one Raft log entry
per `Put()` call, which measured at ~113-120 `Apply()`s/sec on a 3-node
localhost cluster (`internal/cluster`'s `TestApplyThroughput` keeps that
number pinned as a regression test) - every single event paying for its
own full replication round trip. `ReplicatedChannel` now batches the same
way `GRPCSink` already batches a network send: concurrent `Put()` callers
join a shared pending buffer, which flushes as one `logflowpb.EventBatch`-
encoded Raft entry once `batch_size` events have joined it or
`batch_linger` has elapsed since the first one, whichever comes first
(`cluster.batch_size`/`cluster.batch_linger` in a channel's config,
defaulting to 200/50ms - see `ClusterConfig` in `internal/config`). Fixing
this took two changes, not one: `internal/cluster/channel.go`'s batching
itself, and a real bottleneck one layer up it exposed -
`internal/source/grpc_source.go`'s `GRPCSource.putBatch` used to `Put()`
every event in an incoming gRPC batch *sequentially*, which meant at most
one event was ever in flight at a time and each one paid out its own full
`batch_linger` wait individually instead of ever joining a shared batch;
`putBatch` now fires all of a batch's `Put()` calls concurrently (safe -
`channel.Channel` implementations are already required to support
concurrent `Put()`/`Take()`), which is what actually lets batching
coalesce in practice. See Verified behavior below for the before/after
numbers, both the raw `Apply()` measurement and a live end-to-end run.

**Live membership changes.** A cluster's peer list no longer has to be
static for a node's whole process lifetime. `AddServer`/`RemoveServer`
(on `cluster.Node`, and `ReplicatedChannel`'s thin passthroughs of the
same names) drive Raft's standard two-phase joint-consensus protocol
(Ongaro & Ousterhout §6) to add or remove a member live, with no restart
of any existing node:

1. **Cold,new** - the leader proposes a joint configuration naming both
   the current member set and the target one. From the moment this entry
   is merely *appended* to a node's log (not yet committed), that node
   requires separate majorities of Cold **and** Cnew to agree on anything
   - elections, commits - closing the window where two disjoint
   single-majority views could otherwise each elect their own leader
   mid-change.
2. **Cnew** - once Cold,new commits, the leader proposes the new
   configuration alone. Once *that* commits, the change is final. A
   leader that removed itself from Cnew steps down once this commits
   (Raft §6.2), rather than going on managing a cluster it's no longer
   part of.

Adding a server runs a catch-up phase first: the leader replicates its
full log directly to the new node - which doesn't count toward any quorum
yet - until it's caught up, *then* proposes Cold,new, so a brand-new,
empty-log member can't stall everyone else's quorum the moment it's
added. A node meant to join this way is started standalone, with
`cluster.joining: true` and `cluster.self` instead of a full
`cluster.peers` list (see `configs/cluster-demo/collector-4.yaml`) - it
answers RPCs and can be caught up and voted for, but deliberately never
*originates* an election itself until it observes a real multi-member
configuration in its own log, so it can't disrupt a live leader before
it's actually a member.

The reverse problem - a node that *was* removed, whose process is still
running - gets the same deliberate treatment, and it's worth calling out
because it's exactly the kind of bug that only shows up under live,
multi-process testing, not an in-process unit test: once `RemoveServer`
commits, the leader's normal replication stops contacting the removed
node at all (it's no longer in the config), so if nothing else were done,
that node's own election timer would eventually fire with nothing to
suppress it - and since a `RequestVote` handler that updates its own term
before checking cluster membership would still adopt that climbing term
and (if it were leader) step down, a decommissioned-but-still-running
process could disrupt the real cluster indefinitely, forcing pointless
re-elections it could never actually win. Two things close this off:
`RequestVote` rejects a candidate that isn't part of the voter's own
current configuration *before* touching its term or stepping down (not
just before granting the vote), and the leader gives a removed node one
best-effort direct courtesy push of the final Cnew entry after commit, so
a still-running removed node actually learns it was removed and stops
calling elections altogether, rather than retrying forever into a void.

Two HTTP endpoints on every agent (not just a cluster node's own Go API)
make this operable without writing code: `POST /cluster/add?id=...&addr=...&client_addr=...`
and `POST /cluster/remove?id=...`, both callable against *any* node in the
group - a non-leader forwards the request (up to 2 redirect hops, the
same bounded pattern `ReplicatedChannel.Put` already uses to forward a
client write) to whichever node it currently believes is leader. `/stats`
gained `cluster_phase` (`stable`/`joint`) and `cluster_members` (the live
member-ID list) alongside the existing `cluster_state`/`cluster_term`/
`cluster_leader` fields.

**What clustering doesn't give you for free.** Replicating the *data*
doesn't by itself give clients a single virtual endpoint - there's no
built-in VIP, DNS failover, or service mesh here. An edge still needs to
be configured with all of a cluster's addresses (a `failover` or
`load_balance` sink group, same as against any non-clustered multi-collector
topology) so that if the specific node it's connected to dies - leader or
not - it reconnects elsewhere. What Raft buys is that wherever it
reconnects to already has every committed event, not that it never needs
to reconnect at all. `configs/cluster-demo/edge.yaml` is wired this way on
purpose, and its header comment spells this out.

`configs/cluster-demo/collector-{1,2,3}.yaml` is a runnable 3-node
bootstrap example (the one exercised in Verified behavior below) -
`cluster.peers` lists all three members' Raft address and
client-forwarding address, and `cluster.node_id` is the only thing that
differs between the three files besides the obvious per-node ports/paths.
`configs/cluster-demo/collector-4.yaml` is a 4th node meant to be started
*after* those three and added live (`cluster.joining: true` - see "Live
membership changes" above). Not yet wired into `docker-compose.yml`/`k8s/`
- see Known limitations.

**Sharding.** A single Raft group's write throughput is capped by its one
leader's batched-commit rate - batching (above) raised that ceiling
roughly 7x, but it's still one ceiling, and adding more nodes to *one*
group doesn't raise it further (more nodes just means more copies of the
same log, replicated by the same one leader). `cluster.ShardedChannel`
gets past that by partitioning one logical channel's events across N
*independent* Raft groups ("shards") instead of just one, each a full
`ReplicatedChannel` - its own leader election, log, replication, and
batching - completely unaware of any other shard. With S shards, S
different physical nodes can each be leading a different shard at the
same time, so aggregate write throughput scales roughly with S instead of
being capped by any single leader.

Routing a `Put()` to a shard is the whole mechanism: `ShardedChannel.Put`
picks a shard first (round-robin by default - maximum throughput spread,
no ordering preference; or by a hash of a configured event header, via
`sharded_cluster.shard_key_header`, to keep everything with the same key
sticky to one shard's single ordered log), then that shard's own
`ReplicatedChannel.Put` does its usual "I'm leader, apply it" or "forward
to whoever is" dance, completely unaware it's one of several shards. A
gRPC source calling `Put()` never needs to know sharding is involved at
all - the same client transparency a plain (non-sharded) `ReplicatedChannel`
already gives any node in its group.

**What sharding does NOT give you, and why that's a deliberate, documented
trade-off rather than an oversight:**

- **No ordering across shards.** Each shard is its own totally-ordered
  Raft log; two events that land on different shards have no defined
  relative order, ever, even if one was `Put()` before the other returned
  (this project's channels never promised an ordering guarantee across
  concurrent `Put()` callers in the first place, sharded or not).
- **No cross-shard atomicity.** A single `Put()` call always goes to
  exactly one shard - there's no multi-shard transaction.
- **No automatic rebalancing.** The shard count and each shard's peer list
  are fixed at config time; splitting or merging shards, or moving a key
  range from one shard to another, isn't implemented. Each shard's own
  membership can still change live (`AddServer`/`RemoveServer` - see "Live
  membership changes" above) independently of every other shard, but that
  changes who replicates *that* shard, not how many shards there are.

A sharded channel's HTTP surface mirrors the non-sharded one, with one
addition: `/stats` reports `shards=N` plus a `shard[i]_state`/
`shard[i]_term`/`shard[i]_leader`/`shard[i]_phase`/`shard[i]_members` block
per shard (the sharded analogue of `cluster_state`/`cluster_term`/etc.),
and `/cluster/add`/`/cluster/remove` both require a `&shard=N` query
parameter identifying which shard's Raft group the request targets, since
each shard has its own independent membership (a request with no `shard=`
against a sharded channel returns `400`, same as an out-of-range or
non-integer shard index does).

`configs/sharded-demo/collector-{1,2,3}.yaml` is a runnable 3-node x
2-shard example (the one exercised in Verified behavior below): each
physical node participates in both shard-0's and shard-1's Raft group as
one of their 3 members (so `sharded_cluster.shards` lists two full
`cluster:`-style blocks, one per shard, with distinct ports), configured
with a channel the gRPC source writes to exactly as if it weren't sharded
at all.

## Observability

Every agent exposes real Prometheus metrics at `/metrics` via
[`client_golang`](https://github.com/prometheus/client_golang)
(`internal/metrics`) - the standard counter/gauge/histogram types and
`promhttp` exposition format, plus the usual process/Go-runtime
collectors (goroutines, GC pauses, RSS, open FDs) every
Prometheus-instrumented service gets for free. What's actually
instrumented:

- `logflow_events_received_total`, `logflow_events_forwarded_total`,
  `logflow_events_forward_failed_total`, `logflow_events_dropped_total` -
  counters at the source/sink boundary.
- `logflow_channel_size`, `logflow_channel_capacity`,
  `logflow_channel_put_total`, `logflow_channel_take_total`,
  `logflow_channel_dropped_total`, `logflow_channel_spilled_total`,
  `logflow_channel_purged_unconsumed_total` - one gauge/counter set per
  channel, labeled by channel name, collected on demand from each
  channel's existing `Stats()` (a custom `prometheus.Collector` in
  `internal/agent/channel_collector.go`, not a name-string metric per
  channel) so there's no separate bookkeeping to keep in sync with the
  channel's own counters.
- `logflow_batch_send_duration_seconds` - a histogram around every gRPC
  sink batch send (`internal/sink/grpc_sink.go`), bucketed from 1ms to
  10s to cover everything from a loopback send to a loaded, retrying
  network sink, for real p50/p95/p99 batch latency instead of just an
  average.

`observability/` has a ready-to-go Prometheus scrape config and a
provisioned Grafana dashboard (6 panels: events received, forwarded vs.
failed, channel fill, dropped events, batch latency percentiles, and
cumulative spilled/purged-unconsumed) - wired into both
`docker-compose.yml` (`prometheus`/`grafana` services) and `k8s/`
(`41-prometheus.yaml`/`42-grafana.yaml`) so either one comes up with
metrics and a dashboard already in place, no manual setup. See the
Deployment sections above for exact URLs.

### Alerting

`observability/alerts.yml` is a Prometheus rule file (loaded via
`prometheus.yml`'s `rule_files:`) covering the failure modes the metrics
above actually expose:

| Alert | Fires on | Severity |
|---|---|---|
| `LogflowTargetDown` | a collector/edge stops responding to scrapes for 1m | critical |
| `LogflowEventsDropped` | the source/sink boundary counter increases at all | warning |
| `LogflowChannelDropped` | a channel's drop counter increases at all | warning |
| `LogflowChannelNearFull` | a channel stays over 80% full for 2m (leading indicator before it starts dropping) | warning |
| `LogflowForwardFailuresHigh` | nonzero forward-failure rate to a sink target | warning |
| `LogflowPurgedUnconsumed` | a collector purges retention-expired, never-forwarded WAL data on restart - real, already-lost data | critical |
| `LogflowBatchLatencyHigh` | p95 gRPC batch send latency stays over 1s for 2m | warning |

Prometheus ships firing alerts to `observability/alertmanager.yml`
(Alertmanager, `alerting: alertmanagers:` pointed at the `alertmanager`
service/Service by name) - a minimal routing tree (group by
`alertname`+`job`, `severity=critical` gets a shorter repeat interval) with
one webhook receiver pointed at a placeholder URL, plus an inhibit rule so
a target that's already down doesn't also page for every derived
symptom (drops, latency, forward failures) on the same `job`. Swap the
receiver's `url:` for a real webhook (Slack/PagerDuty/OpsGenie) to get
actual notifications; everything else (routing, grouping, inhibition)
works as-is. Wired into both `docker-compose.yml` (`alertmanager` service,
port `19093`) and `k8s/` (`43-alertmanager.yaml`), using the exact same
config files as the rest of the observability stack.

## CI/CD

`.github/workflows/ci.yml` runs on every push/PR to `main`, as four
independent jobs:

| Job | What it checks |
|---|---|
| `test` | `go mod verify`, `gofmt -l` (fails on any unformatted file), `go vet ./...`, `go build ./...`, `go test -race ./...` |
| `docker` | `make build` (host binaries) + `docker build` (the real `FROM scratch` Dockerfile) + `docker compose config -q` (Compose file syntax) |
| `manifests` | `kustomize build k8s/` - renders every object in `k8s/`, catching bad `kustomization.yaml` entries, malformed YAML, or broken ConfigMap/volume references |
| `observability` | `promtool check rules`/`check config` and `amtool check-config` against the real `observability/*.yml` files, using the same Prometheus/Alertmanager release binaries the Verified behavior section downloads below |

No step needs `go mod download`, `go mod tidy`, or anything else that
would try to resolve the `replace` directives in `go.mod` for real - the
vendored `vendor/` tree is checked into the repo, and Go 1.14+ auto-detects
it and defaults to `-mod=vendor` with zero network calls, the same way
every build in this README does.

Caching (`actions/cache@v4`): the Go *build* cache (`~/.cache/go-build`,
including `-race`-instrumented packages) is keyed on Go version + `go.mod` +
`vendor/modules.txt` + a hash of the sources, falling back to the previous
cache on a source-only change; the downloaded `kustomize`, `promtool` and
`amtool` binaries are cached by version. There is no module-download cache
because dependencies are vendored. The `manifests` job also renders
`k8s/cluster/` and fails if `k8s/cluster/01-configmap.yaml` is out of sync
with `configs/docker/cluster/`; the `docker` job also checks
`docker-compose.cluster.yml`. Not verified: an actual runner has never
executed any of this (the workflow was only YAML-parsed here).

## Verified behavior

This was built and exercised end-to-end across two sandboxed environments
with no `proxy.golang.org` access (dependencies were fetched directly from
GitHub source and vendored under `vendor/` for this reason - see Building
above; a normal machine just needs `go build ./...`), using Go 1.22 for
the original feature work and Go 1.24 for the Docker deployment work:

- Standalone agent sustained **~19,900 events/sec** end-to-end
  (loadgen → gRPC source → sharded memory channel → sink) with zero drops
  over a 5s / ~100k event run.
- Two-tier edge→collector forwarding over gRPC verified with zero data
  loss under normal operation.
- Crash recovery of the durable file channel verified: killing the
  collector agent mid-flight (`SIGKILL`) and restarting it replayed the
  unconsumed backlog with no data loss (and, as documented above, replayed
  the tail of the already-consumed-but-undeleted active segment, matching
  the documented at-least-once guarantee).
- Multiplexing channel selector verified: events tagged `env=prod` were
  routed exclusively to the `prod` channel (0 landed in `default`), and
  untagged events landed exclusively in `default` (0 in `prod`).
- Load-balance sink group verified: 3,000 events split across two
  independent backend agents (802 / 2,198 — uneven but confirming both
  were genuinely receiving traffic concurrently).
- Failover sink group verified through a full cycle: traffic flowed
  through the priority-0 target only; killing that process mid-stream
  (`SIGKILL`) triggered failover to the priority-1 target with zero events
  lost (all subsequent traffic landed on the backup); restarting the
  priority-0 target triggered automatic promotion back to it within one
  `recovery_probe_interval`, confirmed by post-recovery traffic landing on
  the primary again.
- TLS/mTLS/token auth verified through four scenarios against a source
  configured with `client_auth: true` + `auth_token` set: a plaintext
  client is refused at the transport level (can't even complete a TLS
  handshake); a TLS client with no certificate is refused with `tls:
  certificate required`; a client with a valid certificate but the wrong
  token is refused with `Unauthenticated: invalid authorization token` and
  0 events land; a client with the correct certificate and token
  successfully delivers events (verified with both a direct client and a
  full edge→collector agent-to-agent hop, including confirming the edge
  agent's sink keeps retrying safely — no data loss, `dropped` count stays
  at 0 — when given a deliberately wrong token, rather than silently
  discarding events).
- Spillable memory channel verified under real load: a 500-event memory
  primary against a ~20K events/sec, ~100K-event burst spilled 98,392
  events to the durable file overflow with **zero drops** and a full
  clean drain (`put=104475 take=104475 dropped=0`). This surfaced and
  fixed two real bugs along the way (see below) — worth knowing about
  since they'd otherwise silently undermine the durability guarantee.
- File channel CRC32 corruption detection verified two ways: flipping a
  byte inside an entry's length prefix (corrupting "how many bytes to
  read" into a huge number) previously crashed the process with an
  out-of-memory error — fixed by bounding the trusted length against a
  sane maximum before allocating; flipping a byte inside an entry's
  payload/checksum region is caught by the CRC32 mismatch. In both cases,
  after the fix, recovery now logs the exact byte offset and reason, stops
  replay of that segment at the corrupted entry, and every entry it does
  return is confirmed to be genuine original data — never garbage from a
  misaligned read.

**Bugs found and fixed while building this feature** (documented here
rather than quietly folded in, since they're the kind of thing that
matters for a durability feature specifically):
1. The spillable channel's Put briefly retried the primary channel with a
   `time.After(1ms)` sleep between attempts before spilling. Since `Take()`
   drains the overflow with priority, the primary stays full once spilling
   begins, so *every* event was paying that retry cost — enough that under
   real load the server fell far behind the client, making a normal test
   look like a hang. Fixed by making the primary check a single
   non-blocking attempt with immediate fallback to overflow.
2. That same retry path was (before the fix above) also inflating the
   primary channel's drop counter for every spilled event, since
   `MemoryChannel.Put` counts any context-timeout as a drop — even one
   from an intentionally short probe. Fixed by adding a `TryPut` that
   never touches the drop counter on failure.
3. A corrupted or torn length prefix was used directly to size a byte
   slice allocation (`make([]byte, n)`) with no upper bound, so a few
   flipped bits could crash the whole process with an out-of-memory error
   instead of being caught as the corruption it is. Fixed with a sane
   maximum entry size checked before allocating.
- Zero-copy I/O verified with a before/after benchmark
  (`go test -bench=. -benchmem ./internal/channel`) on a full file-channel
  Put+Take round trip: pooling the write-side encode buffer and aliasing
  the read-side decode buffer (instead of copying it) took allocation cost
  from **776 B/op, 14 allocs/op to 592 B/op, 13 allocs/op** for the same
  operation, confirmed by reverting the optimization and re-running the
  identical benchmark to get the "before" numbers.
- Graceful shutdown verified through both paths: (1) a producer streaming
  continuously was allowed to finish naturally after SIGTERM -
  `GracefulStop()` correctly waited for the in-flight RPC rather than
  severing it, and the full sequence (stop ingestion → nothing to drain →
  stop sinks → close) completed and logged cleanly; (2) with a
  30-second producer still streaming, a **second** SIGTERM sent partway
  through the wait forced an immediate stop - logged abandoning both the
  source-stop wait and the drain wait - exiting in about 7 seconds instead
  of waiting out the remaining ~27.
- Active WAL retention/purging verified live: a burst of 3,900 events with
  a 4KiB max segment size (forcing 80 rolled segments) and no sink
  attached, against a 3-second `retention_max_age`. Before the retention
  window elapsed: 80 segments on disk, 3,900 buffered, 0 purged. After one
  sweep past the window: 79 of 80 segments purged (only the still-active
  segment remains), `purged_unconsumed=3871` correctly surfaced in
  `/stats`, with a clear per-segment log line for each
  (`removed segment N (older than retention_max_age (3s)), discarding 49
  unconsumed event(s)`).

**A serious pre-existing bug found and fixed while building this feature**
(not introduced by it - present since the file channel's locking was
refactored earlier in this project - but only exposed once a test forced
multiple segment rolls with partial consumption, which no earlier test
had done): `openNewWriteSegment` created **two separate struct instances**
for the same logical segment - one for the write path, one for the read
path - so the read-side `written` counter was never incremented. That
made `maybeGC`'s "has every entry been consumed" check
(`consumed >= written`) evaluate to true after just the **first** `Take()`
from any rolled-but-not-yet-fully-consumed segment, silently deleting it
- discarding every other still-unconsumed entry in it, with no error, no
dropped-count increment, nothing. This would have affected any real
workload where segments actually roll (small `max_segment_bytes` relative
to sustained volume) and a consumer only partially drains a segment
before the next one rolls; it went unnoticed earlier because every prior
test used a segment size large enough that everything fit in one
never-rolled segment, where a separate early-return guard
(`seg.id == fc.writeSeg.id`) happens to skip the buggy check entirely.
Fixed by using one shared `*segment` struct (holding both a write handle
and a read handle) for `fc.writeSeg` and `fc.readSegs[id]` while a segment
is active, so both bookkeeping counters are always consistent regardless
of which path touches them. Verified via a dedicated regression test
(`TestFileChannelDoesNotDropUnconsumedOnPartialTake`) that fails cleanly
against the unfixed code and passes against the fix.

**Docker deployment**, verified live with the real `docker compose up`
topology (2 edges, 2 collectors, 2 loadgens), in an environment where
*every* container registry (`registry-1.docker.io`, `ghcr.io`, `gcr.io`,
`quay.io`, `mcr.microsoft.com`, `public.ecr.aws`, `registry.k8s.io`) was
unreachable (org egress policy, confirmed via the proxy's own status
endpoint - not a transient failure). That's what drove the image design
above (`FROM scratch` + host-compiled binaries instead of a
`golang:...` build stage, plus the custom `healthcheck` binary in place of
`wget`): it's the only design that builds and runs with zero registry
access, and it turned out to also be a legitimately smaller/more
auditable image, not just a workaround.

- `make build` compiled `agent`/`loadgen`/`healthcheck` as static Linux
  binaries using only `vendor/` - confirmed zero network calls needed
  (`go build -mod=vendor` with no `GOPROXY`/`GOFLAGS` overrides).
- `docker build` produced a **40.6MB** image (11.7MB uncompressed content)
  with no `FROM` pull at all - confirmed via the build log showing every
  step as a local `COPY`, nothing fetched.
- `docker compose up -d` brought up all 6 containers (2 edges, 2
  collectors, 2 loadgens) in correct dependency order via
  `depends_on: condition: service_healthy` - collectors healthy first,
  then edges (which depend on both), then loadgens (which depend on their
  edge) - confirmed via the compose output's `Healthy`/`Started`
  sequencing and `docker compose ps` showing all four long-running
  services `(healthy)`.
- End-to-end traffic flow confirmed via `/stats` on all four agents
  simultaneously: loadgen → edge (gRPC ingest) → load-balance sink group
  → collector (gRPC source → durable file channel → file sink) with
  `put == take` and `dropped=0` at every hop, and the collectors'
  on-disk output files (pulled out with `docker cp` for inspection, since
  the scratch image has no shell to `exec` into) containing the actual
  forwarded log lines.
- File backends in sink groups: with a gRPC primary down, a `failover`
  group spooled 2000/2000 events to its `file:` backup; once the collector
  started, the prober promoted it and the next 2000 reached it (0 lost).
  A genuine `ENOSPC` (`/dev/full`) requeues the batch instead of discarding
  it (regression test; fails if the requeue is removed).
- Per-caller tokens, live: with `edge-a` (no expiry) and `edge-b` (expires
  9s out) in a token file, `edge-a` streamed 500/500; after `revoked: true`
  was edited into the file (no restart) its next connection was refused;
  `edge-b`'s long-running stream was cut at expiry with `sent=4000
  acked=4000` (nothing in flight lost) and the metrics showed
  `rejected{edge-a,revoked}` and `rejected{edge-b,expired}`. Unit tests cover
  identity, expiry, revocation, hot reload, bad-reload fallback, file
  validation and cutting off an open stream (mutation-checked: disabling the
  re-check fails the test).
- Parallel WALs (`wal_shards: 4` on a file channel): 15000/15000 events
  acked across `wal-0..3`, then a `kill -9` and restart recovered all 15000
  from the four logs; restarting with `wal_shards: 2` on that directory was
  refused with an error rather than stranding data. Measured on this 2-core
  sandbox (`go test -bench ParallelPut ./internal/channel`, 16 producers):
  ~2.4µs/Put with one WAL vs ~1.2-1.5µs with 2-8 WALs (~1.6x) - the gain is
  bounded by the cores/disk here, not by the design.
- Raft log compaction, live (`configs/cluster-demo`, `snapshot_threshold: 50`):
  16000 events through a 3-node group left every node with ~36 retained
  log entries and `raft_snapshot_index=190` (vs. the whole log without it).
  With `collector-3` `kill -9`'d, 24000 more events were committed by the
  other two; on restart it logged `restored persisted state ... log_len=16`,
  then `installed snapshot through index 190 ... positions 77..190 were not
  replayed`, converged on the same index, and took part in the next 2000
  events - holding 24400 of the 42000 events in its local channel, i.e. the
  documented gap. Unit tests cover bounded log growth, restart from the
  boundary, a far-behind follower and a brand-new member catching up by
  snapshot, membership surviving compaction + a stale-peers restart, and
  never regressing to an older snapshot (the last was a real bug the live run
  found: a follower with boundary 228 accepted the leader's 190 and dropped
  its retained log); mutation-checked by disabling the snapshot send, the
  membership fallback and the stale-snapshot guard.
- Cluster deployment configs: `configs/docker/cluster/{collector,collector-sharded,edge}.yaml`
  were run as real processes - three collectors with the identical file,
  differing only in env (`NODE_ID`, a per-node loopback `BIND_IP`, data dir) -
  plus an edge load-balancing to them by hostname: leader elected,
  12000/12000 events replicated to all three nodes and written by the
  leader's sink; the sharded variant elected different leaders for the two
  shards (`cluster-2`, `cluster-3`) and delivered 12000/12000. The k8s
  ConfigMap's collector config was loaded with `NODE_ID=collector-0` and
  started, binding `:8301` while advertising an unresolvable DNS name.
  **Not verified here:** no container runtime or Kubernetes cluster is
  available, so `docker-compose.cluster.yml` (checked with `docker compose
  config`) and `k8s/cluster/` (YAML parse only - no `kubectl`/`kubeconform`)
  have never actually been run as containers/pods. One gotcha seen while
  testing: gRPC honours `HTTP(S)_PROXY` for non-loopback hostnames, so in an
  environment with a proxy set, add the collector names to `NO_PROXY`.
- Raft fault injection (`Config.LinkFilter` drops outbound RPCs between
  chosen nodes, test-only): an isolated leader cannot commit, the majority
  elects a new one and commits, and after healing the isolated leader's
  uncommitted entry is discarded everywhere. A randomized 5-node chaos test
  (random isolations/2-3 splits/heals for 8s under 3 concurrent writers; ~650
  acknowledged writes over ~7 terms) checks that no term has two leaders,
  every node applies the identical sequence, and every acknowledged write
  appears exactly once. Mutation-checked: weakening the quorum rule to "any
  one node" makes both fail. The chaos test also flaked once under CPU load,
  which exposed a real liveness gap: a new leader never committed anything of
  its own until the next client write, so entries inherited from earlier
  terms could sit uncommitted on a quiet cluster. Fixed with the standard
  Raft section 8 leader no-op (`EntryNoop`, never applied to the state
  machine), with a test that fails without it. This is in-process partition
  simulation, not Jepsen-style testing (no clock skew, disk faults or real
  network).
- Load-balance failover under a real container kill: `docker stop
  collector-1` while traffic was flowing - both edges immediately started
  logging reconnect attempts to `collector-1:7000` (DNS removed along with
  the stopped container: `no such host`, correctly treated as a
  connection failure and retried) while **continuing to forward
  everything to collector-2 with zero drops** (`edge-1` `put`/`take` kept
  climbing in lockstep, `dropped=0` throughout). `collector-2` alone
  absorbed the full combined rate of both edges.
- WAL durability across a real container restart: `docker start
  collector-1` after the stop above replayed its WAL
  (`seg-...0000.log`, 3.5MB) and rolled a fresh write segment
  (`seg-...0001.log`), logs showing a clean graceful-shutdown-then-restart
  cycle, and resumed accepting traffic - confirmed both via `/stats`
  (`put`/`take` climbing again, still `dropped=0` on both edges and the
  collector) and via `docker cp`-inspecting the volume directly: WAL and
  `out/` both show files from both before and after the restart.
- `docker compose stop` (graceful, not `kill`) drained all 6 containers
  cleanly - confirmed via each agent's shutdown log sequence (ingestion
  stop → drain → sinks stop → channel close) rather than an abrupt kill.

**Known rough edge, not a correctness issue**: this image intentionally
runs as root (uid 0) inside the container. A `FROM scratch` image has no
`/etc/passwd`, and Docker initializes a freshly created named volume
(`collector1-data`/`collector2-data` here) as root-owned; a non-root
numeric `USER` would hit permission-denied on the file channel's
`MkdirAll` into that volume. Since scratch has no shell, package manager,
or setuid binary to escalate *from* in the first place, this doesn't carry
the risk it would on a full OS base image, but it's worth knowing if
you're adapting this for an environment with stricter container
policies (pre-creating the volume's ownership via an init container, or
switching to a non-root distroless base with its own minimal
`/etc/passwd`, are the usual fixes).

**Kubernetes deployment.** `k8s/`'s manifests were applied against a real,
running single-node cluster (k3s, built from its own GitHub release
binaries + airgap image bundle - `kind`'s node image is itself a container
image, and this environment blocks every container registry, so `kind`
specifically wasn't an option here either). What that confirmed live:

- `kubectl apply -k k8s/` succeeds end to end against a real API server:
  the `Namespace`, `ConfigMap`, both `PersistentVolumeClaim`s, all 6
  `Deployment`s and all 6 `Service`s were all actually created and
  schema-valid, confirmed via `kubectl get` showing every object present
  with the right selectors, ports, and labels.
- `Deployment`s correctly produced `ReplicaSet`s, and every `Pod` reached
  `Scheduled` on the node - the manifests and image references in them
  aren't the problem.

What wasn't confirmed live: a pod actually reaching `Running`. This
sandbox's container runtime can create containers directly just fine
(`docker run`, and `ctr run` against the same containerd k3s itself uses,
both work) but fails specifically inside kubelet's CRI pod-sandbox
creation path, for every pod regardless of image - including the
cluster's own stock `coredns`, with nothing of logflow's involved. That
rules out the manifests or the image as the cause; it's a property of
this one sandboxed host's kubelet/runc interaction, not something the
k8s/ directory did wrong. Full details and the exact error are in
`k8s/README.md`'s "What was verified, and what wasn't" section, including
what to do if this looks familiar on a *normal* cluster (short version:
it isn't this issue - look at cgroup driver or runc/containerd version
skew on that host instead).

**Clustered collector (Raft).** Verified at two levels: an in-process Go
test suite exercising the Raft algorithm directly (fast, deterministic,
runs in CI), and a live multi-process demo exercising the full pipeline
(real OS processes, real TCP, a real forced leader kill mid-traffic).

In-process (`go test ./internal/cluster/... -race`, 5 correctness tests +
1 throughput measurement, all passing, `-count=25` under `-race` with zero
failures or data races):
- Exactly one leader is elected and every node agrees who it is
  (`TestElectsExactlyOneLeader`).
- 20 sequential `Apply()`s on the leader are replicated, in order, to
  every node's state machine (`TestApplyReplicatesToAllNodes`).
- A follower's own `Apply()` is correctly rejected with `ErrNotLeader`
  (`TestFollowerRejectsApply`).
- Killing the leader elects a new one (with a strictly higher term) from
  the remaining nodes, which continues committing new entries - every
  surviving node ends up with the post-failover entry
  (`TestLeaderFailoverContinuesCommitting`).
- A follower stopped mid-cluster, restarted after the leader committed 5
  more entries while it was down, catches up to all 5 via normal Raft log
  replication (`TestRejoinedNodeCatchesUp`).

This test suite is also how a real implementation bug got caught and
fixed before it ever reached a live run: the first version of `Close()`
stopped a node's listener from accepting *new* connections but didn't
close its already-open ones, so a "stopped" node's `net/rpc` server kept
right on being served by a per-connection goroutine over its existing TCP
connection to the leader - a zombie node, logically removed from the test
harness's bookkeeping but still functionally live and applying entries,
indistinguishable from a real node until that stale connection happened to
break on its own. `TestRejoinedNodeCatchesUp` failed with the rejoining
node reporting entries it couldn't legitimately have yet, which is what
surfaced it. Fixed by tracking every accepted connection and force-closing
them in `Close()` (see `serveTracked` in `internal/cluster/raft.go`).

Live, end-to-end (`configs/cluster-demo/`: 3 clustered collectors +
1 edge + loadgen, all real OS processes on localhost, built from
`make build`'s real binaries):
- All 3 collectors came up, held a real leader election over real TCP
  (`net/rpc`, not mocked), and agreed on the result - confirmed via each
  node's own `/stats`: `cluster_state=leader` on exactly one
  (`collector-1`), `cluster_state=follower` and `cluster_leader=collector-1`
  on the other two.
- Sent real traffic through a real edge agent. All 3 collectors' `put`
  counters advanced identically (`508` each) - genuine replication, not
  one node forwarding a summary - while only the leader's `take` counter
  advanced and only the leader's output file had real bytes in it (the
  followers' sink-rolled files existed but were 0 bytes), confirming the
  leader-gated-`Take()` design actually prevents triplicated output rather
  than just being intended to.
- `kill -9`'d the leader process outright mid-traffic. A new leader
  (`collector-2`) was elected within ~2 seconds (one real election, no
  split-vote retries needed this run) and immediately resumed draining -
  confirmed via `/stats` on the two survivors agreeing on the new leader
  and term. The edge's own failover sink group independently detected the
  dead connection (`rpc error: ... EOF`) and redirected to the new leader
  about 150ms later, fully independently of the Raft layer underneath it
  noticing the same failure - exactly the "two separate failure-recovery
  mechanisms, client-facing and data-layer" split this section's doc
  comment describes.
- **Batching, before and after.** The run described above (one Raft log
  entry per `Put()`, no batching) is what first surfaced the throughput
  ceiling: under 500 events/sec sustained load, the post-failover leader's
  accept rate couldn't keep up, the edge's channel backed up accordingly
  and dropped exactly 1 event at capacity, then drained cleanly with zero
  further drops - correct backpressure behavior, but driven by a real
  bottleneck, pinned in isolation by `TestApplyThroughput`: ~113-120
  sequential `Apply()` calls/sec on a 3-node cluster over localhost
  loopback. Batching was added to fix this (see the "Batching" writeup
  above), and re-measured at both levels:
  - In-process, concurrent-caller throughput (`TestBatchedThroughput`,
    `internal/cluster/channel_test.go`): 50 concurrent `Put()` callers
    driving 5,000 events through a 3-node cluster with the package's
    default `batch_size`/`batch_linger` (200/50ms) measured **~794
    commits/sec** - roughly **7x** `TestApplyThroughput`'s unbatched
    number, from the same algorithm, same localhost loopback, same
    majority-of-3 commit rule - batching is the only variable that
    changed. 4 further correctness tests (`TestBatchSizeTriggersFlush`,
    `TestBatchLingerFlushesSubThresholdBatch`,
    `TestBatchCoalescesIntoOneEntry`, `TestEveryPutGetsResult`,
    `TestCloseFlushesPendingBatch`) all pass under `-race -count=15` with
    zero flakes or data races, confirming the size threshold, the linger
    timeout, correct per-caller results, and a clean `Close()` with
    in-flight `Put()`s all actually work, not just the happy-path
    throughput number.
  - Live, end-to-end, re-running the exact same demo (3 clustered
    collectors + 1 edge + loadgen, 500 events/sec, `kill -9` the leader
    mid-traffic): this also caught a second, real bottleneck that
    `ReplicatedChannel`'s own batching couldn't fix by itself -
    `GRPCSource.putBatch` was `Put()`ing every event in an incoming gRPC
    batch sequentially, so at most one event was ever in flight and each
    one paid out its own full `batch_linger` wait alone instead of
    joining a shared batch (measured: ~20/sec, *worse* than unbatched,
    before this was found and fixed - see `internal/source/grpc_source.go`).
    With both fixes in place (Raft-level batching + concurrent `Put()`
    per incoming batch), the same 12,450-event / 500-per-sec run,
    including the same mid-traffic `kill -9` of the leader and failover to
    a new one, completed with **zero backlog** (`edge`'s channel stayed at
    `size=0` throughout - `put` and `take` advanced together) and **zero
    drops**: `edge /stats` showed `put=12588 take=12588 dropped=0`,
    the new leader's `/stats` showed `put=12450 take=12450` (matching
    loadgen's `sent 12450 events`), and the leader's output file held
    exactly `12450` lines (`wc -l`) - every event generated, replicated,
    and written to disk, through a real leader failure, with no
    perceptible backpressure at this load level.

**Live membership changes.** Verified the same two ways as the rest of
clustering: an in-process test suite exercising the joint-consensus
protocol directly, and a live multi-process demo.

In-process (`go test ./internal/cluster/... -race`, 6 tests, all passing,
`-count=15` under `-race` with zero failures or data races):
- Adding a node to a live 3-node cluster (`TestAddServerJoinsLiveCluster`):
  the new node is started standalone (`Config.Joining: true`), added via
  `AddServer`, and every node - including the new one - converges on a
  4-member `cluster_phase=stable` view; a fresh `Apply()` after joining
  replicates to the new node too, confirming it's a real participant, not
  just a name on a list.
- Removing a node from a live cluster and continuing to commit
  (`TestRemoveServerLeavesLiveCluster`).
- A lone node started with `Joining: true` and no reachable peers never
  elects itself, even after many uncontested election timeouts
  (`TestJoiningNodeNeverSelfElects`) - the safety property `Joining`
  exists for.
- A second membership change is rejected with
  `ErrMembershipChangeInProgress` while one is already in flight
  (`TestChangeMembershipRejectsConcurrentChange`) - the standard Raft
  restriction, actually enforced, not just documented.
- A leader that removes itself steps down once Cnew commits, and the
  remaining 2 nodes elect a new leader and keep committing
  (`TestLeaderStepsDownAfterRemovingSelf`) - Raft §6.2.
- **`TestRemovedNodeDoesNotDisruptCluster`** - a regression test for a
  real bug this feature's own live testing caught (see below): removes an
  unrelated node from a 3-node cluster *without* killing its process, then
  confirms the original leader's identity and term are unchanged 500ms
  later (anything else means the removed node disrupted it), the cluster
  still commits, and the removed node itself converges to believing it's
  no longer a member.

Live, end-to-end (`configs/cluster-demo/`: the same 3-collector bootstrap
cluster, plus `collector-4.yaml` started separately and added live):
- Started collector-4 standalone (`cluster.joining: true`) alongside the
  already-running 3-node cluster, confirmed via its own `/stats`
  (`cluster_state=follower cluster_term=0 cluster_members=collector-4`)
  that it sat idle - never calling an election - for several seconds past
  its own election-timeout window.
- `curl -X POST .../cluster/add` against `collector-2` (a **follower**,
  not the leader) added collector-4 live. The request forwarded to the
  real leader exactly as documented, and all 4 agents' `/stats` converged
  on `cluster_phase=stable cluster_members=collector-1,collector-2,collector-3,collector-4`
  - zero restarts of any of the original 3 processes. Sent real traffic
  (`loadgen` against the leader) and confirmed collector-4's own `put`
  counter advanced identically to the leader's (`980`/`980`) - genuine
  replication to the newly-joined node, not just list membership.
- **Caught a real bug here that no unit test had found**: calling
  `curl -X POST .../cluster/remove?id=collector-2` *without* killing
  collector-2's own process (the realistic "decommission, forgot to also
  `kill` it" scenario) sent the leader's term climbing from 14 to 46+
  within seconds, with repeated `stepping down to follower` /
  `starting election` log lines - the leader was being disrupted by the
  very node that was just removed, exactly the failure mode described
  above. Root-caused to `RequestVote` adopting a candidate's term (and
  stepping down if it was leader) *before* checking whether that
  candidate was still a cluster member. Fixed by reordering that check
  first, plus the leader's courtesy push of the final Cnew entry so a
  still-running removed node actually learns to stop calling elections
  (both described in "Live membership changes" above, and now covered by
  `TestRemovedNodeDoesNotDisruptCluster`). Re-ran the identical scenario
  after the fix: removed collector-2 (process still running), waited 5
  seconds, confirmed collector-1's term and leadership were **completely
  unchanged** (`cluster_term=13` before and after, `cluster_leader_changes=1`
  throughout), collector-2's own log shows zero `starting election` lines
  (it received the courtesy push and quieted down before its own timer
  ever fired), and a follow-up `loadgen` run replicated cleanly to both
  remaining members with the leader untouched.

**Sharding.** Verified the same two ways as the rest of clustering: an
in-process test suite, and a live multi-process, multi-Raft-group demo.

In-process (`go test ./internal/cluster/... -race -run TestSharded`, 9
tests, all passing; the 2 tests that run real multi-node Raft groups
additionally checked with `-count=5 -race` with zero failures):
- Round-robin distribution with no `ShardKeyFunc` spreads evenly across
  every shard (`TestShardedChannelRoundRobinDistribution`): 100 `Put()`s
  across 4 shards landed exactly 25 on each.
- Key-based routing is sticky (`TestShardedChannelKeyRouting`): the same
  key always resolves to the same shard across 10 rounds, while different
  keys are free to land on different shards; an empty key correctly falls
  back to round-robin instead of pinning every such event to shard 0
  (`TestShardedChannelKeyRoutingEmptyKeyFallsBackToRoundRobin`).
- `Take()` fan-in aggregates every shard with zero loss and zero
  duplication (`TestShardedChannelTakeAggregatesAllShards`): 60 events put
  across 3 shards, all 60 delivered exactly once each, regardless of which
  shard's `fanIn` goroutine happened to pull them.
- `Stats()` sums every shard's counters rather than reporting only one
  (`TestShardedChannelStatsAggregates`) - `PutCount`/`TakeCount` both
  matched the real totals, `Capacity` matched the sum of each shard's own
  capacity.
- `Close()` returns promptly (no goroutine hang) and a `Take()` after it
  fails cleanly instead of blocking forever
  (`TestShardedChannelCloseIsPromptAndIdempotentPerShard`).
- An out-of-range shard index is rejected with a clean error from both
  `AddServer` and `RemoveServer`, not a panic
  (`TestShardedChannelShardIndexOutOfRange`) - the same check
  `handleClusterMembership`'s `&shard=N` HTTP parameter relies on.
- **`TestShardedChannelIndependentRaftGroups`** - the real integration
  test: 2 shards, each a genuine 3-node Raft group (6 real `Node`
  instances), wired into one `ShardedChannel`. Confirmed each shard elects
  its own leader independently (not necessarily the same-numbered node in
  both groups), both groups show real non-trivial Raft state
  (`ClusterTerm() >= 1`, 3-member `ClusterMembers()`), and 40 events
  round-robined across the `ShardedChannel` were all delivered exactly
  once end-to-end through two completely independent logs.
- Caught and fixed a real bug while writing this suite, not a product bug
  but a test-harness goroutine leak worth recording because of how it
  presented: an early version registered both groups' cleanup in a single
  `defer` placed *after* the loop that creates them; since `t.Fatalf`
  unwinds via `runtime.Goexit` and only runs defers already registered by
  that point, a timeout on the *second* group's election would skip
  cleanup for the *first* group entirely, leaking 3 live Raft goroutines
  that kept calling uncontested elections into the test binary's
  background - surfacing not as a failure in that test, but as a
  completely unrelated-looking later test's leader term having
  inexplicably climbed into the thousands. Fixed by registering each
  group's `defer groups[i].closeAll()` immediately as it's created, inside
  the loop.

Live, end-to-end (`configs/sharded-demo/`: 3 physical agent processes,
each participating in both of 2 independent shards - 6 real Raft `Node`
instances total, same topology as the integration test above but as
actual separate OS processes talking over real sockets):
- Started all three `collector-{1,2,3}` processes and confirmed via
  `/stats` that the two shards elected **different leaders on different
  physical nodes** at the same time - `shard[0]_leader=collector-2
  shard[1]_leader=collector-3` - the concrete scaling property sharding
  exists for (two different nodes genuinely leading concurrently, not one
  leader doing all the work).
- Sent 20,000 events via a small precise verification client (built
  against the real `SendBatch` unary RPC, which blocks for each batch's
  ack before the next - chosen over `loadgen`'s streaming mode specifically
  *because* that mode fires sends asynchronously and tears down the
  connection as soon as its wall-clock duration elapses without waiting
  for in-flight acks, which this verification caught producing a
  misleading ~17% apparent "loss" that was actually just unacked in-flight
  sends at shutdown, not real data loss - a pre-existing imprecision in
  `loadgen` itself, unrelated to sharding, not investigated further since
  the precise client settles the question directly). Result:
  `sent=20000 acked=20000 nacked=0`, and every one of the 20,000 unique
  event bodies was found exactly once across the leader nodes' file sinks
  (`grep -o ... | sort -u | wc -l` = 20000) - zero loss, zero duplication,
  confirmed by actually reading the output files, not just trusting the
  ack count.
- Verified per-shard membership independence end-to-end over HTTP, not
  just via the in-process test: `POST /cluster/remove?id=collector-1&shard=0`
  removed collector-1 from shard-0's membership only
  (`shard[0]_members=collector-2,collector-3` afterward) while shard-1's
  membership was completely untouched
  (`shard[1]_members=collector-1,collector-2,collector-3`) - confirming
  the `&shard=N` parameter actually scopes the change to one independent
  Raft group, not the whole sharded channel.
- Verified the new HTTP validation paths directly: a `/cluster/remove`
  against a sharded channel with no `shard=` parameter returns `400`
  ("channel is sharded - shard query parameter is required"), an
  out-of-range `shard=5` (only 2 shards configured) returns `400` ("shard 5
  out of range (have 2 shard(s))"), and a non-integer `shard=abc` returns
  `400` ("shard query parameter must be an integer") - none of the three
  panics or silently no-ops.

**Stuck-apply observability, apply ordering, and `loadgen` accuracy.**
Three small fixes closing former Known limitations:
- *Stuck follower apply.* A node whose local channel stays full used to
  block its Raft applier with no external signal. `ReplicatedChannel` now
  runs a watchdog around each local `Put`: past `applyStallWarnInterval`
  (2s, `SetApplyStallWarnInterval`) it is reported via `/stats`
  (`apply_stuck=true apply_stuck_seconds=N apply_stall_warnings_total=N`),
  Prometheus (`logflow_cluster_apply_stuck`, `..._stuck_seconds`,
  `..._stall_warnings_total`), a rate-limited log line, and a new
  `LogflowClusterApplyStuck` alert (`promtool check rules` passes). The
  `Put` itself deliberately stays unbounded: every channel's own `Put`
  counts a cancelled attempt as a dropped event, so "retry with a short
  timeout" would have corrupted `dropped_total` with false drops for
  entries never actually abandoned. Live: a 1-node cluster on a
  capacity-50 channel with no sink went `apply_stuck=true` within seconds
  with `dropped=0`.
- *Apply ordering race (found while doing this).* `applyLoop` bumped
  `lastApplied` before calling `ApplyFn`, so a client `Put` could be told
  "applied" before the entry reached the local channel. Now bumped after.
  Regression tests: `TestApplyStuckDetectionAndRecovery`,
  `TestApplyOrderingWaitsForApplyFn`. Throughput unchanged (827/sec
  batched vs ~794 baseline; 124/sec unbatched).
- *`loadgen`* now counts on ack, waits up to `-ack-grace` (10s) for
  trailing acks, and prints `sent= acked= nacked=`; live run: 10000/10000,
  matching the agent's `put` counter.

**Per-channel `drain_timeout`.** `drain_timeout` on a `channels:` entry
overrides the agent-wide value for that channel only (zero inherits it);
each channel is waited on until empty or past *its own* budget, and one
that blows its budget stops holding up the others. Per channel, not per
sink, because "drained" means a channel emptying - sinks sharing a channel
drain the same events, so a per-sink budget wouldn't be well-defined.
Live: agent default 30s, two undrained channels with `1s`/`4s` overrides -
SIGTERM to exit took 4.0s, and the log named the channels that timed out.

**Certificate rotation/reload.** TLS certificates are no longer read once
at startup. Every `tls:` block's cert/key pair (server identity, or the
client certificate presented for mTLS) and CA bundle (client CAs on a
server with `client_auth`, the server-verifying CA on a client) is
re-read when its file's mtime/size changes (checked lazily at handshake
time, at most once a second - no new dependency, no fsnotify), via
`tls.Config` callbacks (`GetCertificate`, `GetClientCertificate`,
`GetConfigForClient`, and `VerifyConnection` for the client-side CA, since
`RootCAs` is fixed at construction). New connections use the new
material; an already-established gRPC stream keeps the certificate it was
negotiated with until it reconnects (normal TLS behavior). A reload that
fails - a half-written file, a cert/key mismatch mid-rotation - keeps
serving the last good certificate and retries on the next check instead
of breaking every handshake. Verified: 4 tests with real generated CAs and
real TLS handshakes (server cert rotation, torn rotation falling back then
recovering, mTLS client-cert + client-CA rotation, client CA rotation with
hostname/untrusted-CA rejection still enforced), and a mutation check
(disabling reload makes all 4 fail). Live: a running `secure-standalone`
agent (mTLS + token), server cert replaced on disk, the same process
(never restarted) went from serial `300461AA...` to `378B4E6D...` for new
connections, and `loadgen` kept delivering 500/500 events over mTLS
before and after.

**Observability (Prometheus + Grafana).** The container-registry block
covers Grafana too - unlike most projects, Grafana doesn't publish its
binary releases as GitHub release assets, only via its own CDN
(`dl.grafana.com`), which this sandbox's egress policy denies the same
way it denies every container registry. So the actual `docker-compose.yml`/
`k8s/` services (standard `prom/prometheus` and `grafana/grafana-oss`
images) couldn't be run here. What *could* be, and was, verified live
instead, entirely outside Docker/Kubernetes:

- Built `logflow`'s own agent/loadgen binaries and ran a real two-agent
  edge→collector pair as plain host processes, instrumented with the
  real `client_golang` metrics described above - confirmed real counters
  and a real, non-empty latency histogram at `/metrics` on both agents
  while traffic was flowing (e.g. `logflow_channel_put_total{channel="main"} 14570`,
  `logflow_batch_send_duration_seconds_bucket{le="0.001"} 50` and climbing
  per bucket).
- Downloaded Prometheus's own release binary directly from its GitHub
  releases (not a container image - same technique the Kubernetes work
  used for k3s) and ran it against `observability/prometheus.yml`'s exact
  scrape config, pointed at those two agents: both targets came up
  `"health": "up"` in Prometheus's own `/api/v1/targets`, actually
  scraping on the configured interval.
- Ran every one of the dashboard's 6 panels' *exact* PromQL expressions
  (copied straight out of `observability/grafana/dashboards/logflow.json`,
  not approximations) against that live Prometheus via its HTTP API and
  got back real, sensible numbers - e.g. `rate(logflow_events_received_total[1m])`
  returning ~650 events/sec on both agents, and the p99 batch-latency
  `histogram_quantile` query returning `0.0064` (6.4ms), matching what a
  loopback gRPC send should look like. Since a Grafana panel is just that
  query rendered as a chart against that same datasource, this confirms
  every panel will show real data the moment Grafana itself can run
  somewhere with registry/CDN access - which is everywhere except this
  one sandbox.

**Alerting (Prometheus + Alertmanager).** Unlike Grafana, Alertmanager
publishes real binary releases on GitHub, so this one *was* run exactly as
it runs in `docker-compose.yml`/`k8s/` - not approximated. Built on the
same real agent/loadgen/Prometheus setup as above, plus a real Alertmanager
(`v0.27.0`) downloaded from its GitHub releases and run against the exact
`observability/alertmanager.yml`, with `observability/alerts.yml` loaded
into Prometheus via `rule_files:` (only `for:` durations were shortened,
from 1m/2m to 10s, purely so the run didn't need to sit idle for minutes -
the expressions themselves are unmodified):

- Pointed one scrape target at an address nothing was listening on: within
  one `for: 10s` window, `LogflowTargetDown` moved to `firing` in
  Prometheus's real `/api/v1/rules`, and the alert showed up `"state":
  "active"` in Alertmanager's real `/api/v2/alerts` - the whole
  Prometheus-evaluates → Alertmanager-receives pipeline, not just the rule
  expression in isolation.
- `SIGKILL`ed the collector process outright (not just a closed port) while
  the edge kept taking traffic: its `collector-1` and `collector-2` scrape
  jobs both went `down`, firing a second and third independent
  `LogflowTargetDown` alert; the edge's memory channel filled solid
  (500/500) with nowhere to drain to, firing both `LogflowChannelNearFull`
  and `LogflowChannelDropped` from real `logflow_channel_size`/
  `logflow_channel_dropped_total` values - at one point Alertmanager's API
  showed 5 simultaneous real, distinct alerts.
- Confirmed the inhibit rule's scope is correct, not just present: `edge-1`
  (itself up, just backpressured) kept its `LogflowChannelNearFull`/
  `LogflowChannelDropped` alerts visible throughout, while the inhibit rule
  only suppresses derived-symptom alerts on a `job` that's *also* the one
  firing `LogflowTargetDown` - verified by `equal: ["job"]` not
  cross-suppressing a healthy agent's alerts just because an unrelated one
  is down.
- Restarted the collector process and confirmed the resolution path, not
  just the firing path: once its `/metrics` endpoint came back, its
  `LogflowTargetDown` and `LogflowChannelNearFull` alerts both dropped out
  of Alertmanager's active-alerts list on their own (`LogflowChannelDropped`
  correctly stayed active a little longer - its `increase(...[5m]) > 0`
  condition is still true until the drop event ages out of the 5m window).
- Not forced live: `LogflowEventsDropped` (same shape as
  `LogflowChannelDropped`, one condition and one metric, already proven to
  evaluate and fire correctly - just not the specific metric that happened
  to move during this run), `LogflowForwardFailuresHigh` (this setup's
  connection-refused failures happen before a batch is ever attempted, so
  `logflow_events_forward_failed_total` - which counts failed *send*
  attempts, not failed *connections* - never got a nonzero value to alert
  on; forcing it needs a target that accepts the TCP connection but then
  rejects/times out the RPC, not one that's simply down), and
  `LogflowPurgedUnconsumed`/`LogflowBatchLatencyHigh` (both require
  conditions - WAL retention actually expiring, or sustained >1s p95
  latency - that take materially longer to construct than a demo run
  justifies). All four rely on the same `rule_files:`/expression
  evaluation path already proven live by the three that were forced, so
  this is a real gap in forced conditions, not in mechanism coverage.

**CI/CD (GitHub Actions).** `.github/workflows/ci.yml` can't actually be
run by GitHub Actions from inside this sandbox (no runner to dispatch to),
so instead every one of its steps was run here exactly as written, outside
the workflow file, to confirm each one does what the workflow claims:

- `go mod verify`, `gofmt -l` (after actually fixing the handful of files
  that weren't formatted - see below), `go vet ./...`, `go build ./...`,
  and `go test -v -race ./...` all pass clean against the current tree.
- `make build` + `docker build -t logflow:ci .` + `docker compose config
  -q` all succeed, reusing the same real Docker daemon the Docker
  deployment work verified against earlier.
- Installed `kustomize` from its real GitHub release binary (not a
  container image - `ghcr.io`/`dl.k8s.io` are both blocked here the same
  way every registry and Grafana's CDN were) and ran `kustomize build
  k8s/` - rendered all 25 objects with no error.
- Installed `promtool`/`amtool` the same way (already downloaded for the
  Alerting verification above) and ran the workflow's three config-check
  steps verbatim - this actually caught a real bug before it ever reached
  GitHub: `promtool check config` resolves a `rule_files:` entry relative
  to the config file's own directory, not the caller's cwd, so the
  original one-line `sed` substitution pointed at a path that didn't
  exist. Fixed to substitute in an absolute path instead, then re-ran it
  to confirm the fix.
- Ran the whole file through [`actionlint`](https://github.com/rhysd/actionlint)
  (downloaded the same way, from its GitHub release binary) with zero
  findings, confirming the YAML is schema-valid GitHub Actions syntax, not
  just valid YAML.
- Also fixed, as a prerequisite: `gofmt -l` found 7 files that weren't
  gofmt-clean (`internal/agent/agent.go`, `internal/channel/{channel,file}.go`,
  `internal/config/config.go`, `internal/sink/{failover_group,file_sink,grpc_sink}.go`)
  - all whitespace/alignment-only diffs (confirmed with `gofmt -d` before
    applying), `go build`/`go vet`/`go test` all still pass after.
- Not verified: an actual GitHub Actions run. The commands behind every
  step were proven to work on this machine; whether they behave
  identically on a `ubuntu-latest` runner (different base image, `sudo`
  availability, pre-installed tool versions) is unconfirmed until this
  is pushed and a real workflow run is watched.

## Known limitations / next steps

- **A single Raft group is still single-leader-bound** (~794 commits/sec
  measured per group, ~7x the unbatched baseline via batching - see the
  "Batching" writeup and Verified behavior above). Every write within one
  group still funnels through that group's one leader's `batchLoop`
  goroutine, which flushes strictly in order - larger `batch_size`/longer
  `batch_linger` trade latency for more throughput, but only within that
  one group's ceiling. **Getting past that ceiling is now implemented**
  (see "Sharding" above): `ShardedChannel` partitions a channel across
  multiple independent Raft groups so several leaders can be committing
  concurrently, on different physical nodes. What sharding itself doesn't
  give you, by design, not oversight: no ordering or atomicity across
  shards, and no automatic rebalancing - shard count and each shard's peer
  list are fixed at config time; splitting/merging shards or moving a key
  range between them isn't implemented. See "Sharding" above for the full
  trade-off list.
- **Membership changes are live now** (joint consensus - see "Live
  membership changes" above). With `snapshot_threshold` set, a new member
  catches up from a snapshot boundary plus the retained tail; without it
  (the default) it still replays the entire Raft log. Also: only one
  membership change may be in flight at a time (`ErrMembershipChangeInProgress`,
  **per shard** for a sharded channel - different shards' memberships can
  still change concurrently with each other, independently), and if the
  node driving a change crashes between committing Cold,new and Cnew, the
  cluster (or that one shard) is left in the joint phase until the same
  AddServer/RemoveServer call is retried against whichever node is leader
  now (safe - joint quorum rules are already protecting it - just
  incomplete until retried).
- **Raft snapshots carry no event data.** Compaction is opt-in
  (`snapshot_threshold`; default 0 keeps the whole log, as before). A node
  brought up to date by `InstallSnapshot` (a new member, or a follower down
  longer than the retained tail) gets the log position and cluster
  membership but **not** the events in the skipped range, so its local
  channel lacks them; the nodes that were up hold them. That is the price
  of a log that doesn't grow forever, because the state machine here is
  "append to a local channel" whose contents can't be exported - see "Log
  compaction" below for when it matters.
- **Deliberate design decision (not a gap): zero-copy reads.** Aliasing the file channel's read buffer directly into the returned
  `Event` (rather than copying it) means that buffer's memory can't be
  recycled until the consumer is done with the event - the standard
  zero-copy tradeoff of extending an allocation's lifetime in exchange for
  skipping a copy. The write-side scratch buffer has no such issue since
  it's only ever used transiently within a single `Put()` call.
- **Deliberate design decision (not a gap): whole-segment retention.** Retention purging deletes whole segments, not individual entries -
  there's no byte-level compaction that rewrites a partially-consumed
  segment to reclaim just the consumed prefix while keeping the rest.
  This is a deliberate simplicity/safety trade-off: rewriting a segment
  while tokens still in the ready queue reference byte offsets into it
  would require rewriting those offsets too, and getting that wrong risks
  silent corruption. Real systems facing the same trade-off (Kafka's
  retention, notably) also delete whole segments rather than
  byte-rewriting for time/size-based retention.
- `observability/alertmanager.yml`'s one receiver points at a placeholder
  `url:` - alerts fire and route correctly (see Verified behavior above)
  but nothing real gets paged until that's swapped for an actual
  Slack/PagerDuty/OpsGenie webhook.
- Alert thresholds in `observability/alerts.yml` (80% channel fill, >1s p95
  batch latency, 1m/2m `for:` windows) are sized for the demo topology's
  traffic, not tuned against any real production load profile.
- **Not verifiable from the authoring sandbox:** `.github/workflows/ci.yml` validates manifests by rendering them
  (`kustomize build`) and validates configs by checking their syntax
  (`promtool`/`amtool`), but doesn't deploy anything or run the live,
  forced-failure scenarios documented above - those still need a human (or
  a separate, heavier integration-test job) running them against a real
  cluster/compose stack.
- The `manifests` job deliberately stops at `kustomize build` - it doesn't
  run a Kubernetes schema validator (`kubeconform`, `kubectl
  --dry-run=client`) because this sandbox's egress policy blocks both
  `ghcr.io` and `dl.k8s.io`, so neither tool could be fetched and test-run
  here before being committed (see CI/CD section above). Worth adding on
  a runner that can reach them.
