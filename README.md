# local-pvc-cleaner

Clean up local `PersistentVolume`s, `PersistentVolumeClaim`s, and referencing `Pod`s when the node holding their local storage is deleted. This is desirable when running with preemptible nodes and operators or workloads that do not handle local-storage-backed persistent volumes on removable nodes.

## How It Works

`local-pvc-cleaner` runs as a Kubernetes controller that watches `Node`, `Pod`, `PersistentVolumeClaim`, and `PersistentVolume` resources using informer caches with custom indexers (and object transforms that strip heavy unused fields like `managedFields`, node image lists, and pod container `env` slices to keep memory usage low).

When a node disappears and will never come back:

1. **Detection (events + periodic sweep):**
   - Watches `Node` deletion events and immediately enqueues the deleted node.
   - Watches `PVC`, `PV`, and `Pod` updates to catch volumes referencing a node already absent from cache.
   - Runs an initial sweep after informer caches sync and a periodic sweep every `--resync-period` (default `10m`) so nodes deleted during controller downtime or leader failover are still cleaned up.
2. **Grace period & direct API verification:**
   - Waits for `--node-missing-grace-period` (default `1m`) after first observing the node as missing. If the node reappears in the cluster before the grace period expires, the timer is cleared and no resources are touched.
   - Performs a direct `GET` against the Kubernetes API server for the `Node` and proceeds **only** on an explicit `404 NotFound` response. If the node still exists on the API server, cleanup is aborted; transient API errors are retried with workqueue backoff.
3. **Volume matching:**
   - **Dynamic local provisioners:** Matches PVCs annotated with `volume.kubernetes.io/storage-provisioner` (or legacy `volume.beta.kubernetes.io/storage-provisioner`) and PVs annotated with `pv.kubernetes.io/provisioned-by` matching `--provisioners` (default: `rancher.io/local-path`).
   - **Native `local` PVs:** When `--include-local-pvs=true` (default), also manages Kubernetes native local PVs (`pv.Spec.Local != nil`) and automatically discovers their bound PVCs via `spec.claimRef`, even if the PVC lacks provisioner or `selected-node` annotations.
   - **Orphaned PVs via `NodeAffinity`:** Matches PVs pinned to a single node via `spec.nodeAffinity.required.nodeSelectorTerms` using `--hostname-topology-keys` (`kubernetes.io/hostname`, `topology.kubernetes.io/hostname`). A PV is only matched when every term constrains it unambiguously to the exact same single node.
   - **Generic ephemeral volumes:** Tracks Pods referencing both standard `persistentVolumeClaim` volumes and generic `ephemeral` volumes (`<pod-name>-<volume-name>`).
4. **Safeguards & cleanup order (`Pod` → `PVC` → `PV`):**
   - Before deleting anything for a PVC, the controller checks the opt-out annotation (`local-pvc-cleaner.kmr.pw/ignore: "true"`) on the PVC, its bound PV, and every referencing Pod, as well as `--skip-unmanaged-pods`. If any safeguard triggers, the entire PVC cleanup is skipped so the PVC is never left stuck in `Terminating` behind a skipped Pod.
   - Resources are deleted in order: referencing **Pods** first, then the **PVC**, then the bound **PV** (and any remaining orphaned PVs pinned to the missing node).
   - **`Retain` reclaim policy:** PVs with `persistentVolumeReclaimPolicy: Retain` are skipped by default (emitting a `SkippedRetainedPV` event). Set `--delete-retained-pvs=true` to delete them as well.

## Container Images

Multi-architecture (`linux/amd64`, `linux/arm64`) distroless non-root images are published to both Quay.io and GitHub Container Registry:

- Primary: [`quay.io/drangon/local-pvc-cleaner`](https://quay.io/repository/drangon/local-pvc-cleaner)
- Mirror: [`ghcr.io/clusterfucked/local-pvc-cleaner`](https://github.com/clusterfucked/local-pvc-cleaner/pkgs/container/local-pvc-cleaner)

```bash
podman pull quay.io/drangon/local-pvc-cleaner:latest
# or from the mirror:
podman pull ghcr.io/clusterfucked/local-pvc-cleaner:latest
```

Pin a specific release tag (or digest) in production deployments instead of `:latest`.

## Deployment

Kustomize-ready, least-privilege manifests are provided in [`deploy/`](deploy/):

- Dedicated `local-pvc-cleaner` namespace with `restricted` Pod Security Standard labels
- Least-privilege `ServiceAccount`, `Role`/`RoleBinding` (leader election `Lease`), and `ClusterRole`/`ClusterRoleBinding`
- Hardened 2-replica HA `Deployment` (non-root UID/GID `65532`, read-only root filesystem, dropped capabilities, `RuntimeDefault` seccomp profile, liveness/readiness probes, and soft pod anti-affinity)

Deploy directly with Kustomize:

```bash
kubectl apply -k deploy/
```

### Recommended Rollout with `--dry-run`

Before enabling live deletions on a cluster, deploy with `--dry-run=true` using a Kustomize overlay to verify which resources would be cleaned up:

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - https://github.com/clusterfucked/local-pvc-cleaner//deploy?ref=master

images:
  - name: quay.io/drangon/local-pvc-cleaner
    newTag: v1.0.0 # pin to a release tag

patches:
  - target:
      kind: Deployment
      name: local-pvc-cleaner
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --dry-run=true
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --exclude-namespaces=kube-system
```

Inspect controller logs (`kubectl -n local-pvc-cleaner logs -l app.kubernetes.io/name=local-pvc-cleaner -f`) and the `local_pvc_cleaner_deletions_total{dry_run="true"}` metric, then remove `--dry-run=true` when ready.

## Configuration

### CLI Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--kubeconfig` | `""` | Path to a kubeconfig file. Precedence: `--kubeconfig`, `KUBECONFIG` env var, in-cluster config, then `~/.kube/config`. |
| `--pod-name` | `""` (`$POD_NAME`) | Pod identity for leader election. Required when `--leader-election=true`. |
| `--pod-namespace` | `""` (`$POD_NAMESPACE`) | Namespace for the leader election `Lease`. Required when `--leader-election=true`. |
| `--leader-election` | `true` | Enable leader election for high-availability deployments. |
| `--lease-name` | `local-pvc-cleaner` | Name of the `coordination.k8s.io/v1` `Lease` resource used for leader election. |
| `--lease-duration` | `15s` | Duration that non-leader candidates wait before acquiring leadership. |
| `--renew-deadline` | `10s` | Interval that the acting leader retries refreshing leadership before giving up. |
| `--retry-period` | `2s` | Duration between leader election retry attempts. |
| `--workers` | `2` | Number of concurrent reconciliation workers. |
| `--resync-period` | `10m` | Periodic informer resync and missing-node sweep interval (`0` disables periodic sweep). |
| `--api-timeout` | `30s` | Per-request timeout for Kubernetes API calls. |
| `--node-missing-grace-period` | `1m` | Minimum time a node must remain missing before deleting its volumes (`0` disables wait). |
| `--dry-run` | `false` | Log planned deletions without modifying Kubernetes resources or emitting events. |
| `--provisioners` | `rancher.io/local-path` | Comma-separated list of storage provisioners to manage (empty disables provisioner matching). |
| `--include-local-pvs` | `true` | Also manage native Kubernetes local `PersistentVolume`s (`pv.Spec.Local != nil`). |
| `--storage-classes` | `""` | Comma-separated allowlist of `StorageClass` names to manage (empty allows all). |
| `--hostname-topology-keys` | `kubernetes.io/hostname,topology.kubernetes.io/hostname` | Comma-separated `NodeAffinity` topology keys used to determine a PV's pinned node. |
| `--delete-retained-pvs` | `false` | Also delete `PersistentVolume`s with reclaim policy `Retain`. |
| `--skip-unmanaged-pods` | `false` | Skip cleaning a PVC if any referencing Pod has no `OwnerReferences`. |
| `--include-namespaces` | `""` | Comma-separated allowlist of namespaces to manage (empty allows all). |
| `--exclude-namespaces` | `""` | Comma-separated denylist of namespaces to ignore. |
| `--ignore-annotation` | `local-pvc-cleaner.kmr.pw/ignore` | Annotation key that opts a PVC, PV, or Pod out of cleanup when set to `"true"` (empty disables). |
| `--kube-qps` | `20` | Maximum QPS to the Kubernetes API server. |
| `--kube-burst` | `40` | Maximum burst for throttling requests to the Kubernetes API server. |
| `--log-level` | `info` | Log verbosity level: `debug`, `info`, `warn`, `error`. |
| `--log-format` | `text` | Log output format: `text`, `json`. |
| `--metrics-bind-address` | `:8080` | Address for `/healthz`, `/readyz`, and `/metrics` HTTP server (empty disables). |

### Environment Variables

| Variable | Description |
| --- | --- |
| `POD_NAME` | Default value for `--pod-name` (populated via Downward API `metadata.name` in `deploy/deployment.yaml`). |
| `POD_NAMESPACE` | Default value for `--pod-namespace` (populated via Downward API `metadata.namespace` in `deploy/deployment.yaml`). |
| `KUBECONFIG` | Path to a kubeconfig file when `--kubeconfig` is not set (takes precedence over in-cluster config). |

### Opting Out Individual Resources

Add the annotation `local-pvc-cleaner.kmr.pw/ignore: "true"` (or the custom key set via `--ignore-annotation`) to any `PersistentVolumeClaim`, `PersistentVolume`, or referencing `Pod` to opt it out of cleanup:

```yaml
metadata:
  annotations:
    local-pvc-cleaner.kmr.pw/ignore: "true"
```

If set on a bound `PV` or referencing `Pod`, cleanup of the associated `PVC` is also skipped so the workload is not left partially deleted.

## RBAC Permissions

The manifests in [`deploy/rbac.yaml`](deploy/rbac.yaml) grant the minimum permissions required:

| Scope | API Group | Resource | Verbs | Purpose |
| --- | --- | --- | --- | --- |
| Cluster (`ClusterRole`) | `""` (core) | `nodes` | `get`, `list`, `watch` | Watch node deletions and verify `404 NotFound` against the API server. |
| Cluster (`ClusterRole`) | `""` (core) | `persistentvolumes`, `persistentvolumeclaims`, `pods` | `get`, `list`, `watch`, `delete` | Watch and delete local PVs, PVCs, and referencing Pods on deleted nodes. |
| Cluster (`ClusterRole`) | `""` (core) | `events` | `create`, `patch` | Emit core `v1` Kubernetes Events for deletions and skipped cleanups. |
| Namespace (`Role`) | `coordination.k8s.io` | `leases` | `create` | Initial creation of the leader election `Lease` (Kubernetes RBAC does not evaluate `resourceNames` on `create`). |
| Namespace (`Role`) | `coordination.k8s.io` | `leases` (`local-pvc-cleaner`) | `get`, `list`, `watch`, `update`, `patch` | Acquire and renew leadership on the named `Lease`. |

## Observability

### HTTP Probes & Prometheus Metrics

When `--metrics-bind-address` is non-empty (default `:8080`), the controller serves:

- `GET /healthz`: Liveness endpoint. With leader election enabled, delegates to `client-go`'s `LeaderHealthzAdaptor` (20s watchdog timeout) so a stalled leader fails liveness while standby replicas remain healthy.
- `GET /readyz`: Readiness endpoint. Returns `503` during startup or while the active leader (or single-replica instance) is syncing informer caches; returns `200` once initialized (standby) or cache-synced (leader/single).
- `GET /metrics`: Prometheus metrics in text exposition format:
  - `local_pvc_cleaner_deletions_total{resource,dry_run}` (`counter`): Deletions or planned dry-run deletions (`resource`: `pod`, `pvc`, `pv`; `dry_run`: `true`, `false`).
  - `local_pvc_cleaner_errors_total{operation}` (`counter`): Reconciliation errors (`operation`: `get_node`, `delete_pod`, `delete_pvc`, `delete_pv`, `index_lookup`).
  - `local_pvc_cleaner_skipped_total{resource,reason}` (`counter`): Resources skipped due to safeguards (`reason`: `ignored`, `unmanaged_pod`, `retain_policy`).
  - `local_pvc_cleaner_is_leader` (`gauge`): `1` if this replica holds leadership, `0` if standby.
  - `local_pvc_cleaner_queue_depth` (`gauge`): Current number of node items queued for reconciliation.

### Kubernetes Events & Structured Logging

- **Events** (component `local-pvc-cleaner`, suppressed in `--dry-run`):
  - `Normal DeletedPodOnMissingNode` (on `Pod`)
  - `Normal DeletedPVCOnMissingNode` (on `PVC`)
  - `Normal DeletedPVOnMissingNode` (on `PV`)
  - `Normal SkippedCleanup` (on `PVC` or `PV` skipped due to ignore annotation)
  - `Warning SkippedUnmanagedPod` (on `PVC` skipped due to `--skip-unmanaged-pods`)
  - `Normal SkippedRetainedPV` (on `PV` skipped due to `Retain` reclaim policy)
- **Logs:** Structured logs via Go's `log/slog`, configurable with `--log-level` and `--log-format=text|json` (`deploy/deployment.yaml` defaults to `json`).

## Development

This repository uses [devenv](https://devenv.sh/) and [ko](https://ko.build/) (configured in [`devenv.nix`](devenv.nix) and [`.ko.yaml`](.ko.yaml)).

### Run Tests

```bash
devenv test
```

(`devenv test` runs `go vet ./...` and `go test -race -count=1 ./...`.)

### Run Locally

```bash
devenv shell -- go run . --leader-election=false --dry-run=true --log-level=debug
```

### Build & Release Container Images

Build a multi-arch (`linux/amd64`, `linux/arm64`) image locally without pushing:

```bash
devenv tasks run app:build-image
```

Build and publish release images to both `quay.io/drangon/local-pvc-cleaner` and `ghcr.io/clusterfucked/local-pvc-cleaner` (requires `RELEASE_TAG` or `HEAD` on an exact git tag, plus registry authentication):

```bash
RELEASE_TAG=v1.0.0 devenv tasks run app:release
```
