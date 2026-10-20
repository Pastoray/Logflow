# logflow on Kubernetes

Manifests for the same multi-agent topology as `docker-compose.yml` (2
edges, each with its own `loadgen`, load-balancing across 2 collectors),
as plain Kubernetes objects instead of Compose services:

```
  loadgen-1 --> edge-1 --\          /--> collector-1 (PVC collector-1-data)
                           >--------<
  loadgen-2 --> edge-2 --/          \--> collector-2 (PVC collector-2-data)
```

## Files

| File | What it is |
|---|---|
| `00-namespace.yaml` | the `logflow` namespace everything else lives in |
| `01-configmap.yaml` | `configs/docker/*.yaml` verbatim, as a ConfigMap (regenerate, don't hand-edit - see the file's own header) |
| `10-collector-1.yaml`, `11-collector-2.yaml` | PVC + Deployment + Service per collector |
| `20-edge-1.yaml`, `21-edge-2.yaml` | Deployment + Service per edge |
| `30-loadgen.yaml` | the two traffic-generator Deployments |
| `40-observability-config.yaml` | `observability/*.yml` + the dashboard JSON verbatim, as ConfigMaps |
| `41-prometheus.yaml`, `42-grafana.yaml`, `43-alertmanager.yaml` | Deployment + Service per observability component |
| `kustomization.yaml` | applies all of the above in one shot |

## Design choice: named Deployments, not a scaled StatefulSet

Each collector and each edge is its own single-replica `Deployment` with
its own `Service` name (`collector-1`, `collector-2`, `edge-1`, `edge-2`),
not a 2-replica `StatefulSet`. This is deliberate: `configs/docker/edge-1.yaml`'s
load-balance sink group targets `collector-1:7000` and `collector-2:7000`
by name - it has no notion of StatefulSet ordinals - so this mirrors
docker-compose's topology exactly and lets the ConfigMap reuse those exact
files unchanged. The collectors' PVCs give them the same "comes back with
its WAL intact" durability a StatefulSet's `volumeClaimTemplates` would,
just declared individually instead of templated.

The tradeoff: scaling collectors from 2 to N means hand-adding
`1N-collector-N.yaml` + a `collector-N.yaml` config entry, not changing a
`replicas:` field. Real horizontal autoscaling would need the agent to
take its identity/targets from the environment (Downward API for a stable
pod name, or a headless Service + ordinal-based peer discovery) rather
than a baked-in YAML config - a bigger change to `internal/config`, not a
k8s-manifests-only one. Worth doing if this ever needs to scale past a
handful of fixed agents; out of scope here.

Note this is about *horizontal* scaling (more independent collectors,
each owning a disjoint slice of data). The main README's "Clustered
collector (Raft)" section adds a different, complementary kind of
scaling: *replicating* one collector's data across a fixed-size group (3,
5, ...) for availability, not sharding more data across more of them. A
Raft group's static peer list is actually a very natural fit for a
`StatefulSet`'s stable per-replica ordinals - but that wiring hasn't been
done here yet (see that section's Known limitations); today's
`configs/cluster-demo/` is host-process only.

The edges are stateless (memory channel only) and *could* already be
scaled via `replicas:` with no further change - nothing but the listen
port is per-replica state for them.

## Building and loading the image

There's no public registry push here (see the main README's Deployment
section for why - this environment's egress policy blocks every
container registry, not just Docker Hub). Build the image locally first:

```sh
make docker-build        # from the repo root; produces logflow:latest
```

Then get it into your cluster's container runtime without a registry:

```sh
# kind:
kind load docker-image logflow:latest

# k3s (single node, image built with `docker build`):
docker save logflow:latest | k3s ctr images import -

# minikube:
minikube image load logflow:latest
```

All the manifests set `imagePullPolicy: IfNotPresent`, so once the image
is loaded this way, nothing tries to pull it from anywhere.

## Applying

```sh
kubectl apply -k k8s/
kubectl -n logflow get pods -w
curl -s $(kubectl -n logflow get svc edge-1 -o jsonpath='{.spec.clusterIP}'):9100/stats   # from inside the cluster, or port-forward:
kubectl -n logflow port-forward svc/edge-1 19100:9100 &
curl -s http://localhost:19100/stats
```

Kill a collector pod to watch the edges' load-balance groups route around
it exactly as in the Docker Compose demo, then watch it come back with its
WAL intact once the Deployment recreates it:

```sh
kubectl -n logflow delete pod -l app=collector-1
kubectl -n logflow logs -f deploy/edge-1   # reconnect attempts, zero drops in /stats meanwhile
```

`kubectl delete -k k8s/` tears everything down, including the PVCs (so
the WAL data goes with it - drop `--cascade` tuning or delete the PVCs
separately if you want to keep it around).

## Observability

`prometheus`, `grafana`, and `alertmanager` Deployments come up alongside
the agents, provisioned from `40-observability-config.yaml` - the exact
same `observability/prometheus.yml`, `observability/alerts.yml`,
`observability/alertmanager.yml`, Grafana provisioning, and dashboard JSON
that `docker-compose.yml` uses, just delivered as ConfigMaps instead of
bind mounts. The scrape config's `collector-1:9101`-style targets, and
Prometheus's `alertmanager:9093` alerting target, resolve the same way
under in-namespace Kubernetes Service DNS as they do under Docker Compose
service DNS, so nothing about it needed changing between the two.

```sh
kubectl -n logflow port-forward svc/prometheus 19090:9090 &
kubectl -n logflow port-forward svc/grafana 13000:3000 &
kubectl -n logflow port-forward svc/alertmanager 19093:9093 &
open http://localhost:19090        # Prometheus
open http://localhost:19090/alerts # Alert rule states
open http://localhost:19093        # Alertmanager
open http://localhost:13000        # Grafana - the "logflow" dashboard is already there (anonymous admin login enabled for this demo)
```

## What was verified, and what wasn't, here

Every manifest here was applied against a real, running single-node k3s
cluster (built from source releases, since this environment's registry
block covers `kind`'s node image and every other container image too -
see the main README) in the same sandboxed environment the Docker work
was verified in. That's as far as live verification went, for a specific,
well-isolated reason documented in the main README's "Verified behavior"
section: this particular sandbox's container runtime can create containers
directly (`docker run`, `ctr run` both work) but fails to create a pod
*sandbox* specifically through kubelet's CRI path, for every pod
regardless of image - including the cluster's own stock `coredns`, not
just logflow's. So what's actually confirmed live:

- `kubectl apply -k k8s/` succeeds against a real API server: every
  object (`Namespace`, `ConfigMap`, both `PersistentVolumeClaim`s, all 6
  `Deployment`s, all 6 `Service`s) is schema-valid and was actually
  created, confirmed via `kubectl get` showing them all present with the
  right selectors/ports/labels.
- `Deployment`s correctly produced `ReplicaSet`s and `Pod`s got
  `Scheduled` to the node - the manifests themselves are not the problem.
- The one thing not confirmed live here is a pod actually reaching
  `Running` - not a logflow-specific gap, since the exact same
  `CreatePodSandboxError` / `can't get final child's PID from pipe`
  failure hits `coredns` too, in a vanilla k3s install with nothing of
  ours involved.

If you hit something that looks like this same error on a *normal*
Kubernetes install (cloud-managed, kind on a real machine, a bare-metal
cluster), it is not this issue - go looking at cgroup driver mismatches or
runc/containerd version skew on that host instead. On a normal cluster,
these manifests should just work; if something doesn't, it's worth a
bug report with that cluster's actual pod events, since that's the one
layer this write-up couldn't exercise.

## Clustered collector (Raft) variant

`k8s/cluster/` is a separate kustomization (namespace `logflow-cluster`): a
3-replica `StatefulSet` of Raft-replicated collectors plus an edge. See the
main README, "Running the cluster in Docker Compose and Kubernetes". Apply
with `kubectl apply -k k8s/cluster`. Written and parse-checked only; not
applied to a real cluster.
