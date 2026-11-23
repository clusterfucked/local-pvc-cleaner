package cleaner

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	SelectedNodeAnnotation           = "volume.kubernetes.io/selected-node"
	StorageProvisionerAnnotation     = "volume.kubernetes.io/storage-provisioner"
	BetaStorageProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	PVProvisionedByAnnotation        = "pv.kubernetes.io/provisioned-by"
	BetaStorageClassAnnotation       = "volume.beta.kubernetes.io/storage-class"
)

// Filter encapsulates all resource matching, node extraction, and opt-out rules.
type Filter struct {
	provisioners         map[string]struct{}
	includeLocalPVs      bool
	storageClasses       map[string]struct{}
	hostnameTopologyKeys map[string]struct{}
	includeNamespaces    map[string]struct{}
	excludeNamespaces    map[string]struct{}
	ignoreAnnotation     string
}

// NewFilter creates a Filter from Config.
func NewFilter(cfg Config) *Filter {
	topologyKeys := cfg.HostnameTopologyKeys
	if len(topologyKeys) == 0 {
		topologyKeys = DefaultHostnameTopologyKeys
	}
	return &Filter{
		provisioners:         toSet(cfg.Provisioners),
		includeLocalPVs:      cfg.IncludeLocalPVs,
		storageClasses:       toSet(cfg.StorageClasses),
		hostnameTopologyKeys: toSet(topologyKeys),
		includeNamespaces:    toSet(cfg.IncludeNamespaces),
		excludeNamespaces:    toSet(cfg.ExcludeNamespaces),
		ignoreAnnotation:     strings.TrimSpace(cfg.IgnoreAnnotation),
	}
}

// NamespaceAllowed reports whether a namespace passes the include/exclude filters.
func (f *Filter) NamespaceAllowed(ns string) bool {
	if len(f.includeNamespaces) > 0 {
		if _, ok := f.includeNamespaces[ns]; !ok {
			return false
		}
	}
	if _, excluded := f.excludeNamespaces[ns]; excluded {
		return false
	}
	return true
}

// PVNamespaceAllowed checks namespace filters for a cluster-scoped PV.
// If the PV has a ClaimRef with a namespace, that namespace must be allowed.
// If the PV has no ClaimRef namespace (e.g., Available), it is allowed only when
// --include-namespaces is not restricting the controller to specific namespaces.
func (f *Filter) PVNamespaceAllowed(pv *corev1.PersistentVolume) bool {
	if pv == nil {
		return false
	}
	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace != "" {
		return f.NamespaceAllowed(pv.Spec.ClaimRef.Namespace)
	}
	return len(f.includeNamespaces) == 0
}

// IsIgnored reports whether the given annotations opt a resource out of cleanup.
func (f *Filter) IsIgnored(annotations map[string]string) bool {
	if f.ignoreAnnotation == "" || len(annotations) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(annotations[f.ignoreAnnotation]), "true")
}

// MatchesStorageClass reports whether storageClassName passes the optional StorageClass allowlist.
func (f *Filter) MatchesStorageClass(storageClassName string) bool {
	if len(f.storageClasses) == 0 {
		return true
	}
	_, ok := f.storageClasses[storageClassName]
	return ok
}

// MatchesPVC reports whether a PVC is managed based on its provisioner and StorageClass.
// Note: PVCs bound to native local PVs (pv.Spec.Local != nil) may lack a provisioner
// annotation and are also discovered via their bound PV during node reconciliation.
func (f *Filter) MatchesPVC(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}
	if !f.MatchesStorageClass(PVCStorageClass(pvc)) {
		return false
	}
	prov := PVCProvisioner(pvc)
	if prov == "" {
		return false
	}
	_, ok := f.provisioners[prov]
	return ok
}

// MatchesPV reports whether a PV is managed based on StorageClass, native local PV
// configuration, and provisioner annotation.
func (f *Filter) MatchesPV(pv *corev1.PersistentVolume) bool {
	if pv == nil {
		return false
	}
	if !f.MatchesStorageClass(pv.Spec.StorageClassName) {
		return false
	}
	if f.includeLocalPVs && pv.Spec.Local != nil {
		return true
	}
	if pv.Annotations != nil {
		if prov := strings.TrimSpace(pv.Annotations[PVProvisionedByAnnotation]); prov != "" {
			if _, ok := f.provisioners[prov]; ok {
				return true
			}
		}
	}
	return false
}

// ExtractPVCNode returns the non-empty selected-node name from a PVC, or "" if absent.
func (f *Filter) ExtractPVCNode(pvc *corev1.PersistentVolumeClaim) string {
	if pvc == nil || pvc.Annotations == nil {
		return ""
	}
	return strings.TrimSpace(pvc.Annotations[SelectedNodeAnnotation])
}

// ExtractPVNode extracts the single pinned node name from a PV's required NodeAffinity.
// It returns "" unless every NodeSelectorTerm constrains the PV to the exact same single node.
func (f *Filter) ExtractPVNode(pv *corev1.PersistentVolume) string {
	if pv == nil || pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return ""
	}
	terms := pv.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) == 0 {
		return ""
	}

	var resolvedNode string
	for _, term := range terms {
		termNode := ""
		for _, expr := range term.MatchExpressions {
			if _, ok := f.hostnameTopologyKeys[expr.Key]; !ok {
				continue
			}
			if expr.Operator != corev1.NodeSelectorOpIn || len(expr.Values) != 1 {
				return ""
			}
			val := strings.TrimSpace(expr.Values[0])
			if val == "" {
				return ""
			}
			if termNode != "" && termNode != val {
				return ""
			}
			termNode = val
		}
		if termNode == "" {
			return ""
		}
		if resolvedNode != "" && resolvedNode != termNode {
			return ""
		}
		resolvedNode = termNode
	}
	return resolvedNode
}

// PVCStorageClass returns the effective StorageClass name of a PVC.
func PVCStorageClass(pvc *corev1.PersistentVolumeClaim) string {
	if pvc == nil {
		return ""
	}
	if pvc.Spec.StorageClassName != nil {
		return strings.TrimSpace(*pvc.Spec.StorageClassName)
	}
	if pvc.Annotations != nil {
		return strings.TrimSpace(pvc.Annotations[BetaStorageClassAnnotation])
	}
	return ""
}

// PVCProvisioner returns the storage provisioner annotation value on a PVC,
// checking the GA annotation first and falling back to the legacy beta annotation.
func PVCProvisioner(pvc *corev1.PersistentVolumeClaim) string {
	if pvc == nil || pvc.Annotations == nil {
		return ""
	}
	if v := strings.TrimSpace(pvc.Annotations[StorageProvisionerAnnotation]); v != "" {
		return v
	}
	return strings.TrimSpace(pvc.Annotations[BetaStorageProvisionerAnnotation])
}

// IsUnmanagedPod reports whether the pod has no OwnerReferences (standalone pod).
func IsUnmanagedPod(pod *corev1.Pod) bool {
	return pod != nil && len(pod.OwnerReferences) == 0
}

// PVCKey returns the canonical namespaced key "<namespace>/<name>" for a PVC.
func PVCKey(namespace, name string) string {
	return namespace + "/" + name
}

// PodPVCKeys returns all deduplicated "<namespace>/<claimName>" keys referenced by a Pod,
// covering both PersistentVolumeClaim volumes and generic Ephemeral volumes.
func PodPVCKeys(pod *corev1.Pod) []string {
	if pod == nil || len(pod.Spec.Volumes) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(pod.Spec.Volumes))
	keys := make([]string, 0, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		var claimName string
		switch {
		case volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName != "":
			claimName = volume.PersistentVolumeClaim.ClaimName
		case volume.Ephemeral != nil && volume.Name != "":
			claimName = pod.Name + "-" + volume.Name
		default:
			continue
		}
		key := PVCKey(pod.Namespace, claimName)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

// StripCachedObject is an informer TransformFunc that removes heavy unused fields
// (ManagedFields, Node.Status.Images, and Pod container Env slices) to reduce memory usage.
func StripCachedObject(obj any) (any, error) {
	if metaObj, ok := obj.(metav1.Object); ok {
		metaObj.SetManagedFields(nil)
	}
	switch v := obj.(type) {
	case *corev1.Node:
		v.Status.Images = nil
	case *corev1.Pod:
		for i := range v.Spec.Containers {
			v.Spec.Containers[i].Env = nil
		}
		for i := range v.Spec.InitContainers {
			v.Spec.InitContainers[i].Env = nil
		}
		for i := range v.Spec.EphemeralContainers {
			v.Spec.EphemeralContainers[i].Env = nil
		}
	}
	return obj, nil
}

// UnwrapDeletedObject safely extracts a typed object from an informer DeleteFunc argument,
// handling both direct objects and cache.DeletedFinalStateUnknown tombstones.
func UnwrapDeletedObject[T any](obj any) (T, bool) {
	var zero T
	if typed, ok := obj.(T); ok {
		return typed, true
	}
	switch t := obj.(type) {
	case cache.DeletedFinalStateUnknown:
		typed, ok := t.Obj.(T)
		return typed, ok
	case *cache.DeletedFinalStateUnknown:
		if t == nil {
			return zero, false
		}
		typed, ok := t.Obj.(T)
		return typed, ok
	default:
		return zero, false
	}
}
