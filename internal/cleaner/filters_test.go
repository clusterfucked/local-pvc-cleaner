package cleaner

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestExtractPVNode(t *testing.T) {
	f := NewFilter(DefaultConfig())

	tests := []struct {
		name string
		pv   *corev1.PersistentVolume
		want string
	}{
		{
			name: "standard kubernetes.io/hostname",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-1"}},
									},
								},
							},
						},
					},
				},
			},
			want: "node-1",
		},
		{
			name: "topology.kubernetes.io/hostname",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "topology.kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-2"}},
									},
								},
							},
						},
					},
				},
			},
			want: "node-2",
		},
		{
			name: "multiple hostnames in single expression is not pinned to one node",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-1", "node-2"}},
									},
								},
							},
						},
					},
				},
			},
			want: "",
		},
		{
			name: "multiple OR terms with different nodes is not pinned to one node",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-1"}},
									},
								},
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-2"}},
									},
								},
							},
						},
					},
				},
			},
			want: "",
		},
		{
			name: "zone affinity only returns empty",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"us-east-1a"}},
									},
								},
							},
						},
					},
				},
			},
			want: "",
		},
		{
			name: "no affinity returns empty",
			pv:   &corev1.PersistentVolume{},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := f.ExtractPVNode(tc.pv); got != tc.want {
				t.Errorf("ExtractPVNode() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMatchesPVCAndPV(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StorageClasses = []string{"local-path", "fast-local"}
	f := NewFilter(cfg)

	scLocal := "local-path"
	scOther := "gp3"

	pvcBeta := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				BetaStorageProvisionerAnnotation: DefaultProvisioner,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &scLocal},
	}
	if !f.MatchesPVC(pvcBeta) {
		t.Error("expected beta storage-provisioner annotation to match")
	}

	pvcWrongSC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				StorageProvisionerAnnotation: DefaultProvisioner,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &scOther},
	}
	if f.MatchesPVC(pvcWrongSC) {
		t.Error("expected PVC with non-allowed StorageClass not to match")
	}

	pvNativeLocal := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "fast-local",
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/mnt/disks/ssd1"},
			},
		},
	}
	if !f.MatchesPV(pvNativeLocal) {
		t.Error("expected native local PV in allowed StorageClass to match")
	}

	pvWrongSC := pvNativeLocal.DeepCopy()
	pvWrongSC.Spec.StorageClassName = "other"
	if f.MatchesPV(pvWrongSC) {
		t.Error("expected native local PV in non-allowed StorageClass not to match")
	}
}

func TestPodPVCKeysIncludesEphemeralAndDeduplicates(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-0",
			Namespace: "prod",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pvc"},
					},
				},
				{
					Name: "data-dup",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-pvc"},
					},
				},
				{
					Name: "scratch",
					VolumeSource: corev1.VolumeSource{
						Ephemeral: &corev1.EphemeralVolumeSource{},
					},
				},
				{
					Name: "config",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}

	keys := PodPVCKeys(pod)
	want := []string{"prod/data-pvc", "prod/web-0-scratch"}
	if len(keys) != len(want) {
		t.Fatalf("PodPVCKeys() = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("keys[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}

func TestUnwrapDeletedObject(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}

	if got, ok := UnwrapDeletedObject[*corev1.Node](node); !ok || got.Name != "node-1" {
		t.Errorf("direct unwrap failed: got=%v ok=%v", got, ok)
	}
	if got, ok := UnwrapDeletedObject[*corev1.Node](cache.DeletedFinalStateUnknown{Key: "node-1", Obj: node}); !ok || got.Name != "node-1" {
		t.Errorf("value tombstone unwrap failed: got=%v ok=%v", got, ok)
	}
	if got, ok := UnwrapDeletedObject[*corev1.Node](&cache.DeletedFinalStateUnknown{Key: "node-1", Obj: node}); !ok || got.Name != "node-1" {
		t.Errorf("pointer tombstone unwrap failed: got=%v ok=%v", got, ok)
	}
	if _, ok := UnwrapDeletedObject[*corev1.Node](cache.DeletedFinalStateUnknown{Key: "bad", Obj: "not-a-node"}); ok {
		t.Error("expected wrong type inside tombstone to return ok=false")
	}
	if _, ok := UnwrapDeletedObject[*corev1.Node]("not-a-node"); ok {
		t.Error("expected unexpected type to return ok=false")
	}
}
