# local-pvc-cleaner

Kubernetes controller that deletes local `PersistentVolume`s, `PersistentVolumeClaim`s, and referencing `Pod`s when the node backing their local storage is deleted. Useful for clusters with ephemeral or preemptible nodes where workloads or operators do not clean up local storage volumes after node removal.

## Container Images

Multi-architecture (`linux/amd64`, `linux/arm64`) images:

- `quay.io/drangon/local-pvc-cleaner`
- `ghcr.io/clusterfucked/local-pvc-cleaner`

## Deployment

Deploy using the Kustomize manifests in [`deploy/`](deploy/):

```bash
kubectl apply -k deploy/
```

To test in dry-run mode first, use a Kustomize overlay:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - https://github.com/clusterfucked/local-pvc-cleaner//deploy?ref=master

images:
  - name: quay.io/drangon/local-pvc-cleaner
    newTag: v1.0.0

patches:
  - target:
      kind: Deployment
      name: local-pvc-cleaner
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: --dry-run=true
```

## Configuration

### CLI Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--kubeconfig` | `""` | Path to a kubeconfig file (falls back to `KUBECONFIG`, in-cluster config, then `~/.kube/config`). |
| `--pod-name` | `""` (`$POD_NAME`) | Pod identity for leader election (required when `--leader-election=true`). |
| `--pod-namespace` | `""` (`$POD_NAMESPACE`) | Namespace for the leader election `Lease` (required when `--leader-election=true`). |
| `--leader-election` | `true` | Enable leader election. |
| `--lease-name` | `local-pvc-cleaner` | Name of the leader election `Lease` resource. |
| `--lease-duration` | `15s` | Duration non-leader candidates wait before acquiring leadership. |
| `--renew-deadline` | `10s` | Interval the active leader retries renewing leadership before giving up. |
| `--retry-period` | `2s` | Duration between leader election retry attempts. |
| `--workers` | `2` | Number of concurrent reconciliation workers. |
| `--resync-period` | `10m` | Periodic informer resync and sweep interval (`0` disables). |
| `--api-timeout` | `30s` | Per-request timeout for Kubernetes API calls. |
| `--node-missing-grace-period` | `1m` | Time a node must remain missing before deleting its volumes (`0` disables wait). |
| `--dry-run` | `false` | Log planned deletions without modifying resources or emitting events. |
| `--provisioners` | `rancher.io/local-path` | Comma-separated storage provisioners to manage (empty disables provisioner matching). |
| `--include-local-pvs` | `true` | Also manage native Kubernetes local PVs (`pv.Spec.Local != nil`). |
| `--storage-classes` | `""` | Comma-separated allowlist of StorageClass names (empty allows all). |
| `--hostname-topology-keys` | `kubernetes.io/hostname,topology.kubernetes.io/hostname` | Comma-separated `NodeAffinity` topology keys identifying a PV's pinned node. |
| `--delete-retained-pvs` | `false` | Also delete PVs with reclaim policy `Retain`. |
| `--skip-unmanaged-pods` | `false` | Skip cleaning a PVC if any referencing Pod has no `OwnerReferences`. |
| `--include-namespaces` | `""` | Comma-separated allowlist of namespaces (empty allows all). |
| `--exclude-namespaces` | `""` | Comma-separated denylist of namespaces to ignore. |
| `--ignore-annotation` | `local-pvc-cleaner.kmr.pw/ignore` | Annotation key that opts a PVC, PV, or Pod out of cleanup when `"true"` (empty disables). |
| `--kube-qps` | `20` | Maximum QPS to the Kubernetes API server. |
| `--kube-burst` | `40` | Maximum burst to the Kubernetes API server. |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error`. |
| `--log-format` | `text` | Log format: `text`, `json`. |
| `--metrics-bind-address` | `:8080` | Bind address for `/healthz`, `/readyz`, and `/metrics` (empty disables). |

### Environment Variables

| Variable | Description |
| --- | --- |
| `POD_NAME` | Default value for `--pod-name`. |
| `POD_NAMESPACE` | Default value for `--pod-namespace`. |
| `KUBECONFIG` | Kubeconfig path when `--kubeconfig` is unset. |

### Ignore Annotation

Add `local-pvc-cleaner.kmr.pw/ignore: "true"` to any `PersistentVolumeClaim`, `PersistentVolume`, or referencing `Pod` to skip cleanup for that resource and its associated volume group:

```yaml
metadata:
  annotations:
    local-pvc-cleaner.kmr.pw/ignore: "true"
```
