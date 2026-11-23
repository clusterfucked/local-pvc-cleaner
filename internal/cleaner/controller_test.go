package cleaner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func testBaseConfig() Config {
	cfg := DefaultConfig()
	cfg.LeaderElection = false
	cfg.Workers = 2
	cfg.ResyncPeriod = 0
	cfg.NodeMissingGracePeriod = 0
	cfg.APITimeout = 5 * time.Second
	return cfg
}

func makeNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

func makePVC(ns, name, node, pvName string, extraAnnotations map[string]string) *corev1.PersistentVolumeClaim {
	ann := map[string]string{
		StorageProvisionerAnnotation: DefaultProvisioner,
	}
	if node != "" {
		ann[SelectedNodeAnnotation] = node
	}
	for k, v := range extraAnnotations {
		ann[k] = v
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   ns,
			Name:        name,
			UID:         types.UID(ns + "-" + name + "-uid"),
			Annotations: ann,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}
}

func makePV(name, node string, reclaimPolicy corev1.PersistentVolumeReclaimPolicy, phase corev1.PersistentVolumePhase, claimRef *corev1.ObjectReference) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				PVProvisionedByAnnotation: DefaultProvisioner,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: reclaimPolicy,
			ClaimRef:                      claimRef,
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: phase,
		},
	}
	if node != "" {
		pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{
			Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{node},
							},
						},
					},
				},
			},
		}
	}
	return pv
}

func makePod(ns, name, claimName string, managed bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				},
			},
		},
	}
	if managed {
		isCtrl := true
		pod.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "app",
				Controller: &isCtrl,
			},
		}
	}
	return pod
}

func startTestController(t *testing.T, client *fake.Clientset, cfg Config, nowFn func() time.Time) (*Controller, *Metrics, *record.FakeRecorder, context.CancelFunc) {
	t.Helper()
	metrics := NewMetrics()
	recorder := record.NewFakeRecorder(100)
	ctrl, err := NewController(Options{
		Client:   client,
		Config:   cfg,
		Logger:   testLogger(),
		Metrics:  metrics,
		Recorder: recorder,
		Now:      nowFn,
	})
	if err != nil {
		t.Fatalf("NewController() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ctrl.Run(ctx)
	}()

	waitUntil(t, 5*time.Second, "controller caches synced", func() bool {
		return ctrl.synced.Load()
	})

	cleanup := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ctrl.Run() returned unexpected error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timed out waiting for controller shutdown")
		}
	}
	t.Cleanup(cleanup)
	return ctrl, metrics, recorder, cancel
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, desc)
}

func objectExists(err error) bool {
	return err == nil
}

func TestCrossNamespacePVCNameCollision(t *testing.T) {
	pvcDead := makePVC("ns-dead", "shared-data", "node-dead", "pv-dead", nil)
	pvDead := makePV("pv-dead", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
		Namespace: "ns-dead", Name: "shared-data", UID: pvcDead.UID,
	})
	podDead := makePod("ns-dead", "app-0", "shared-data", true)

	pvcHealthy := makePVC("ns-healthy", "shared-data", "node-healthy", "pv-healthy", nil)
	pvHealthy := makePV("pv-healthy", "node-healthy", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
		Namespace: "ns-healthy", Name: "shared-data", UID: pvcHealthy.UID,
	})
	podHealthy := makePod("ns-healthy", "app-0", "shared-data", true)

	client := fake.NewSimpleClientset(
		makeNode("node-healthy"),
		pvcDead, pvDead, podDead,
		pvcHealthy, pvHealthy, podHealthy,
	)

	startTestController(t, client, testBaseConfig(), nil)

	waitUntil(t, 5*time.Second, "dead namespace resources deleted", func() bool {
		_, errPod := client.CoreV1().Pods("ns-dead").Get(context.Background(), "app-0", metav1.GetOptions{})
		_, errPVC := client.CoreV1().PersistentVolumeClaims("ns-dead").Get(context.Background(), "shared-data", metav1.GetOptions{})
		_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-dead", metav1.GetOptions{})
		return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
	})

	// Ensure healthy namespace's pod, PVC, and PV with the exact same PVC name are untouched!
	if _, err := client.CoreV1().Pods("ns-healthy").Get(context.Background(), "app-0", metav1.GetOptions{}); !objectExists(err) {
		t.Fatalf("healthy namespace pod was unexpectedly deleted: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("ns-healthy").Get(context.Background(), "shared-data", metav1.GetOptions{}); !objectExists(err) {
		t.Fatalf("healthy namespace pvc was unexpectedly deleted: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-healthy", metav1.GetOptions{}); !objectExists(err) {
		t.Fatalf("healthy namespace pv was unexpectedly deleted: %v", err)
	}
}

func TestEmptySelectedNodePVCNotDeleted(t *testing.T) {
	pvcEmpty := makePVC("default", "pending-empty", "", "", nil)
	pvcBlank := makePVC("default", "pending-blank", "   ", "", nil)
	pod := makePod("default", "pending-pod", "pending-empty", true)

	client := fake.NewSimpleClientset(makeNode("node-1"), pvcEmpty, pvcBlank, pod)
	ctrl, _, _, _ := startTestController(t, client, testBaseConfig(), nil)

	// Explicitly trigger reconcile for empty string too.
	if _, err := ctrl.reconcileNode(context.Background(), ""); err != nil {
		t.Fatalf("reconcileNode(\"\") unexpected error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pending-empty", metav1.GetOptions{}); err != nil {
		t.Fatalf("empty selected-node PVC should not be deleted: %v", err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pending-blank", metav1.GetOptions{}); err != nil {
		t.Fatalf("whitespace selected-node PVC should not be deleted: %v", err)
	}
	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "pending-pod", metav1.GetOptions{}); err != nil {
		t.Fatalf("pod referencing pending PVC should not be deleted: %v", err)
	}
}

func TestTombstoneHandlingNoPanic(t *testing.T) {
	node := makeNode("node-tomb")
	pvc := makePVC("default", "pvc-tomb", "node-tomb", "pv-tomb", nil)
	pv := makePV("pv-tomb", "node-tomb", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
		Namespace: "default", Name: "pvc-tomb", UID: pvc.UID,
	})
	pod := makePod("default", "pod-tomb", "pvc-tomb", true)

	client := fake.NewSimpleClientset(node, pvc, pv, pod)
	ctrl, _, _, _ := startTestController(t, client, testBaseConfig(), nil)

	// Delete node from fake API server so cache + API reflect deletion.
	if err := client.CoreV1().Nodes().Delete(context.Background(), "node-tomb", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete node error: %v", err)
	}

	// Simulate cache.DeletedFinalStateUnknown tombstones directly on informer handlers (must not panic).
	var nodeHandler cache.ResourceEventHandlerFuncs
	nodeHandler = cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj any) {
			if n, ok := UnwrapDeletedObject[*corev1.Node](obj); ok && n.Name != "" {
				ctrl.markNodeMissingAt(n.Name, ctrl.now())
				ctrl.queue.Add(n.Name)
			}
		},
	}
	nodeHandler.OnDelete(cache.DeletedFinalStateUnknown{Key: "node-tomb", Obj: node})
	nodeHandler.OnDelete(&cache.DeletedFinalStateUnknown{Key: "node-tomb", Obj: node})
	nodeHandler.OnDelete(cache.DeletedFinalStateUnknown{Key: "bad", Obj: "garbage"})
	nodeHandler.OnDelete("unexpected-string")

	// Also ensure pod tombstone helper never panics.
	if got, ok := UnwrapDeletedObject[*corev1.Pod](cache.DeletedFinalStateUnknown{Key: "default/pod-tomb", Obj: pod}); !ok || got.Name != "pod-tomb" {
		t.Fatalf("expected pod tombstone unwrap to succeed")
	}

	waitUntil(t, 5*time.Second, "tombstone node resources deleted", func() bool {
		_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-tomb", metav1.GetOptions{})
		_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-tomb", metav1.GetOptions{})
		_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-tomb", metav1.GetOptions{})
		return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
	})
}

func TestDeletionOrderPartialFailureAndNotFoundTolerance(t *testing.T) {
	t.Run("pods_first_then_pvc_then_pv_with_transient_failure_retry", func(t *testing.T) {
		pvc := makePVC("default", "pvc-order", "node-dead", "pv-order", nil)
		pv := makePV("pv-order", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-order", UID: pvc.UID,
		})
		pod := makePod("default", "pod-order", "pvc-order", true)

		client := fake.NewSimpleClientset(pvc, pv, pod)

		var (
			mu           sync.Mutex
			deleteOrder  []string
			podDeleteErr atomic.Bool
		)
		podDeleteErr.Store(true)

		client.PrependReactor("delete", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
			del := action.(k8stesting.DeleteAction)
			res := del.GetResource().Resource
			if res == "pods" && podDeleteErr.Swap(false) {
				return true, nil, apierrors.NewInternalError(errors.New("transient apiserver hiccup"))
			}
			// Only record deletions of objects that still exist in the fake tracker;
			// level-triggered re-reconciles before informer watch propagation may issue
			// a second Delete that returns NotFound and is gracefully ignored.
			if _, err := client.Tracker().Get(del.GetResource(), del.GetNamespace(), del.GetName()); err == nil {
				mu.Lock()
				deleteOrder = append(deleteOrder, res)
				mu.Unlock()
			}
			return false, nil, nil
		})

		_, metrics, _, _ := startTestController(t, client, testBaseConfig(), nil)

		waitUntil(t, 5*time.Second, "all resources deleted after retry", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-order", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-order", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-order", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})

		mu.Lock()
		gotOrder := append([]string(nil), deleteOrder...)
		mu.Unlock()
		wantOrder := []string{"pods", "persistentvolumeclaims", "persistentvolumes"}
		if !reflect.DeepEqual(gotOrder, wantOrder) {
			t.Errorf("deletion order = %v, want %v", gotOrder, wantOrder)
		}
		prom := metrics.RenderPrometheus()
		for _, wantLine := range []string{
			`local_pvc_cleaner_errors_total{operation="delete_pod"} 1`,
			`local_pvc_cleaner_deletions_total{resource="pod",dry_run="false"} 1`,
			`local_pvc_cleaner_deletions_total{resource="pvc",dry_run="false"} 1`,
			`local_pvc_cleaner_deletions_total{resource="pv",dry_run="false"} 1`,
		} {
			if !strings.Contains(prom, wantLine) {
				t.Errorf("expected metric %q in output, got:\n%s", wantLine, prom)
			}
		}
	})

	t.Run("pods_and_pvc_deleted_even_when_pv_already_gone", func(t *testing.T) {
		pvc := makePVC("default", "pvc-no-pv", "node-dead", "pv-already-gone", nil)
		pod := makePod("default", "pod-no-pv", "pvc-no-pv", true)
		client := fake.NewSimpleClientset(pvc, pod)

		startTestController(t, client, testBaseConfig(), nil)

		waitUntil(t, 5*time.Second, "pod and pvc deleted when pv is missing", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-no-pv", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-no-pv", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC)
		})
	})

	t.Run("terminating_pvc_still_deletes_pods_and_pv_without_redeleting_pvc", func(t *testing.T) {
		now := metav1.Now()
		pvc := makePVC("default", "pvc-term", "node-dead", "pv-term", nil)
		pvc.DeletionTimestamp = &now
		pv := makePV("pv-term", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-term", UID: pvc.UID,
		})
		pod := makePod("default", "pod-term", "pvc-term", true)

		client := fake.NewSimpleClientset(pvc, pv, pod)
		var pvcDeletes atomic.Int32
		client.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
			pvcDeletes.Add(1)
			return false, nil, nil
		})

		startTestController(t, client, testBaseConfig(), nil)

		waitUntil(t, 5*time.Second, "pod and pv deleted for terminating pvc", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-term", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-term", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPV)
		})

		if pvcDeletes.Load() != 0 {
			t.Errorf("expected 0 Delete calls on already-terminating PVC, got %d", pvcDeletes.Load())
		}
	})
}

func TestGenericEphemeralVolumeIndexingAndDeletion(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ephem-ns",
			Name:      "job-worker-0",
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "scratch",
					VolumeSource: corev1.VolumeSource{
						Ephemeral: &corev1.EphemeralVolumeSource{},
					},
				},
			},
		},
	}
	pvc := makePVC("ephem-ns", "job-worker-0-scratch", "node-dead", "pv-ephem", nil)
	pv := makePV("pv-ephem", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
		Namespace: "ephem-ns", Name: "job-worker-0-scratch", UID: pvc.UID,
	})

	client := fake.NewSimpleClientset(pod, pvc, pv)
	startTestController(t, client, testBaseConfig(), nil)

	waitUntil(t, 5*time.Second, "ephemeral pod, pvc, and pv deleted", func() bool {
		_, errPod := client.CoreV1().Pods("ephem-ns").Get(context.Background(), "job-worker-0", metav1.GetOptions{})
		_, errPVC := client.CoreV1().PersistentVolumeClaims("ephem-ns").Get(context.Background(), "job-worker-0-scratch", metav1.GetOptions{})
		_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-ephem", metav1.GetOptions{})
		return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
	})
}

func TestOrphanedAndAvailablePVCleanupViaNodeAffinity(t *testing.T) {
	pvReleased := makePV("pv-released", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeReleased, &corev1.ObjectReference{
		Namespace: "default", Name: "deleted-pvc",
	})
	pvAvailable := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-available"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				Local: &corev1.LocalVolumeSource{Path: "/mnt/disk1"},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{Key: "topology.kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-dead"}},
							},
						},
					},
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeAvailable},
	}
	pvMultiNode := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pv-multinode",
			Annotations: map[string]string{PVProvisionedByAnnotation: DefaultProvisioner},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-dead", "node-alive"}},
							},
						},
					},
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeAvailable},
	}

	client := fake.NewSimpleClientset(makeNode("node-alive"), pvReleased, pvAvailable, pvMultiNode)
	startTestController(t, client, testBaseConfig(), nil)

	waitUntil(t, 5*time.Second, "orphaned and available PVs deleted", func() bool {
		_, errRel := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-released", metav1.GetOptions{})
		_, errAvail := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-available", metav1.GetOptions{})
		return apierrors.IsNotFound(errRel) && apierrors.IsNotFound(errAvail)
	})

	if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-multinode", metav1.GetOptions{}); err != nil {
		t.Fatalf("multi-node PV must not be deleted: %v", err)
	}
}

func TestRetainReclaimPolicy(t *testing.T) {
	t.Run("default_preserves_retained_pvs_while_cleaning_pods_and_pvc", func(t *testing.T) {
		pvc := makePVC("default", "pvc-retain", "node-dead", "pv-retain-bound", nil)
		pvBound := makePV("pv-retain-bound", "node-dead", corev1.PersistentVolumeReclaimRetain, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-retain", UID: pvc.UID,
		})
		pvOrphan := makePV("pv-retain-orphan", "node-dead", corev1.PersistentVolumeReclaimRetain, corev1.VolumeReleased, &corev1.ObjectReference{
			Namespace: "default", Name: "old-pvc",
		})
		pod := makePod("default", "pod-retain", "pvc-retain", true)

		client := fake.NewSimpleClientset(pvc, pvBound, pvOrphan, pod)
		cfg := testBaseConfig()
		cfg.DeleteRetainedPVs = false
		startTestController(t, client, cfg, nil)

		waitUntil(t, 5*time.Second, "pod and pvc deleted", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-retain", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-retain", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC)
		})

		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-retain-bound", metav1.GetOptions{}); err != nil {
			t.Fatalf("retained bound PV should NOT be deleted by default: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-retain-orphan", metav1.GetOptions{}); err != nil {
			t.Fatalf("retained orphan PV should NOT be deleted by default: %v", err)
		}
	})

	t.Run("delete_retained_pvs_true_deletes_retained_pvs", func(t *testing.T) {
		pvc := makePVC("default", "pvc-retain", "node-dead", "pv-retain-bound", nil)
		pvBound := makePV("pv-retain-bound", "node-dead", corev1.PersistentVolumeReclaimRetain, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-retain", UID: pvc.UID,
		})
		pvOrphan := makePV("pv-retain-orphan", "node-dead", corev1.PersistentVolumeReclaimRetain, corev1.VolumeReleased, &corev1.ObjectReference{
			Namespace: "default", Name: "old-pvc",
		})
		pod := makePod("default", "pod-retain", "pvc-retain", true)

		client := fake.NewSimpleClientset(pvc, pvBound, pvOrphan, pod)
		cfg := testBaseConfig()
		cfg.DeleteRetainedPVs = true
		startTestController(t, client, cfg, nil)

		waitUntil(t, 5*time.Second, "pod, pvc, and retained PVs deleted", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-retain", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-retain", metav1.GetOptions{})
			_, errBound := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-retain-bound", metav1.GetOptions{})
			_, errOrphan := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-retain-orphan", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errBound) && apierrors.IsNotFound(errOrphan)
		})
	})
}

func TestNativeLocalPVSupport(t *testing.T) {
	makeLocalSetup := func() (*corev1.PersistentVolume, *corev1.PersistentVolumeClaim, *corev1.Pod) {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "db",
				Name:      "local-disk-0",
				UID:       "local-disk-0-uid",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: "pv-native-local",
			},
		}
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-native-local"},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					Local: &corev1.LocalVolumeSource{Path: "/mnt/nvme0"},
				},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "db", Name: "local-disk-0", UID: pvc.UID,
				},
				NodeAffinity: &corev1.VolumeNodeAffinity{
					Required: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-dead"}},
								},
							},
						},
					},
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		}
		pod := makePod("db", "postgres-0", "local-disk-0", true)
		return pv, pvc, pod
	}

	t.Run("include_local_pvs_true", func(t *testing.T) {
		pv, pvc, pod := makeLocalSetup()
		client := fake.NewSimpleClientset(pv, pvc, pod)
		cfg := testBaseConfig()
		cfg.IncludeLocalPVs = true
		startTestController(t, client, cfg, nil)

		waitUntil(t, 5*time.Second, "native local pv, bound pvc, and pod deleted", func() bool {
			_, errPod := client.CoreV1().Pods("db").Get(context.Background(), "postgres-0", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("db").Get(context.Background(), "local-disk-0", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-native-local", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})
	})

	t.Run("include_local_pvs_false", func(t *testing.T) {
		pv, pvc, pod := makeLocalSetup()
		client := fake.NewSimpleClientset(pv, pvc, pod)
		cfg := testBaseConfig()
		cfg.IncludeLocalPVs = false
		startTestController(t, client, cfg, nil)

		time.Sleep(100 * time.Millisecond)

		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-native-local", metav1.GetOptions{}); err != nil {
			t.Fatalf("native local PV should be untouched when --include-local-pvs=false: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumeClaims("db").Get(context.Background(), "local-disk-0", metav1.GetOptions{}); err != nil {
			t.Fatalf("PVC bound to native local PV should be untouched: %v", err)
		}
	})
}

func TestNodeMissingGracePeriodAndReappearance(t *testing.T) {
	t.Run("deterministic_clock_grace_period_and_reset_on_return", func(t *testing.T) {
		var clockMu sync.Mutex
		now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
		nowFn := func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return now
		}
		advance := func(d time.Duration) {
			clockMu.Lock()
			now = now.Add(d)
			clockMu.Unlock()
		}

		pvc := makePVC("default", "pvc-grace", "node-grace", "pv-grace", nil)
		pv := makePV("pv-grace", "node-grace", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-grace", UID: pvc.UID,
		})
		pod := makePod("default", "pod-grace", "pvc-grace", true)

		client := fake.NewSimpleClientset(pvc, pv, pod)
		cfg := testBaseConfig()
		cfg.NodeMissingGracePeriod = 1 * time.Minute

		ctrl, _, _, _ := startTestController(t, client, cfg, nowFn)

		// Wait for the initial background sweep at t=0 to record node-grace before advancing the fake clock.
		waitUntil(t, 5*time.Second, "initial grace period recorded at t=0", func() bool {
			ctrl.missingMu.Lock()
			defer ctrl.missingMu.Unlock()
			_, ok := ctrl.missingSince["node-grace"]
			return ok
		})

		// At t=0, grace period has not elapsed: reconcileNode should return remaining ~1m and delete nothing.
		requeueAfter, err := ctrl.reconcileNode(context.Background(), "node-grace")
		if err != nil {
			t.Fatalf("reconcileNode error: %v", err)
		}
		if requeueAfter != 1*time.Minute {
			t.Fatalf("requeueAfter = %s, want 1m", requeueAfter)
		}
		if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-grace", metav1.GetOptions{}); err != nil {
			t.Fatalf("PVC deleted prematurely before grace period: %v", err)
		}

		// At t=30s, still within grace period.
		advance(30 * time.Second)
		requeueAfter, err = ctrl.reconcileNode(context.Background(), "node-grace")
		if err != nil {
			t.Fatalf("reconcileNode error at 30s: %v", err)
		}
		if requeueAfter != 30*time.Second {
			t.Fatalf("requeueAfter = %s, want 30s", requeueAfter)
		}

		// Simulate node reappearing: clearNodeMissing resets the timer.
		ctrl.clearNodeMissing("node-grace")

		// Node disappears again at t=45s; at t=75s only 30s have passed since re-disappearance!
		advance(15 * time.Second) // t=45s
		requeueAfter, err = ctrl.reconcileNode(context.Background(), "node-grace")
		if err != nil || requeueAfter != 1*time.Minute {
			t.Fatalf("expected fresh 1m grace period after node reappearance, got requeueAfter=%s err=%v", requeueAfter, err)
		}

		advance(30 * time.Second) // t=75s
		requeueAfter, err = ctrl.reconcileNode(context.Background(), "node-grace")
		if err != nil || requeueAfter != 30*time.Second {
			t.Fatalf("expected 30s remaining at t=75s, got requeueAfter=%s err=%v", requeueAfter, err)
		}
		if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-grace", metav1.GetOptions{}); err != nil {
			t.Fatalf("PVC should still exist before reset grace period expires: %v", err)
		}

		// Advance past grace period (t=106s -> 61s elapsed since t=45s).
		advance(31 * time.Second)
		requeueAfter, err = ctrl.reconcileNode(context.Background(), "node-grace")
		if err != nil || requeueAfter != 0 {
			t.Fatalf("expected cleanup after grace period, got requeueAfter=%s err=%v", requeueAfter, err)
		}

		if _, err := client.CoreV1().Pods("default").Get(context.Background(), "pod-grace", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected pod deleted after grace period, got: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-grace", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected pvc deleted after grace period, got: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-grace", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("expected pv deleted after grace period, got: %v", err)
		}
	})

	t.Run("end_to_end_delayed_requeue_cancels_when_node_returns", func(t *testing.T) {
		node := makeNode("node-flaky")
		pvc := makePVC("default", "pvc-flaky", "node-flaky", "pv-flaky", nil)
		pv := makePV("pv-flaky", "node-flaky", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-flaky", UID: pvc.UID,
		})
		client := fake.NewSimpleClientset(node, pvc, pv)

		cfg := testBaseConfig()
		cfg.NodeMissingGracePeriod = 200 * time.Millisecond
		startTestController(t, client, cfg, nil)

		// Delete node, then re-create it before 200ms grace period elapses.
		if err := client.CoreV1().Nodes().Delete(context.Background(), "node-flaky", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete node: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
		if _, err := client.CoreV1().Nodes().Create(context.Background(), makeNode("node-flaky"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("recreate node: %v", err)
		}

		// Wait past the original grace period; nothing should be deleted!
		time.Sleep(300 * time.Millisecond)
		if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-flaky", metav1.GetOptions{}); err != nil {
			t.Fatalf("PVC should NOT be deleted when node returned during grace period: %v", err)
		}

		// Now delete node permanently and wait for grace period to pass.
		if err := client.CoreV1().Nodes().Delete(context.Background(), "node-flaky", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete node permanently: %v", err)
		}
		waitUntil(t, 5*time.Second, "pvc and pv deleted after permanent node deletion + grace period", func() bool {
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-flaky", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-flaky", metav1.GetOptions{})
			return apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})
	})
}

func TestDirectAPIGetSafetyCheck(t *testing.T) {
	t.Run("node_exists_on_api_server_aborts_cleanup", func(t *testing.T) {
		pvc := makePVC("default", "pvc-ghost", "node-ghost", "pv-ghost", nil)
		pv := makePV("pv-ghost", "node-ghost", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-ghost", UID: pvc.UID,
		})
		pod := makePod("default", "pod-ghost", "pvc-ghost", true)

		// Node is NOT present in initial list/cache, simulating a stale/split-brain cache,
		// but direct API Nodes().Get returns the live node!
		client := fake.NewSimpleClientset(pvc, pv, pod)
		var getNodeCalls atomic.Int32
		client.PrependReactor("get", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
			get := action.(k8stesting.GetAction)
			if get.GetName() == "node-ghost" {
				getNodeCalls.Add(1)
				return true, makeNode("node-ghost"), nil
			}
			return false, nil, nil
		})

		startTestController(t, client, testBaseConfig(), nil)

		waitUntil(t, 5*time.Second, "direct API Nodes().Get invoked", func() bool {
			return getNodeCalls.Load() >= 1
		})
		time.Sleep(100 * time.Millisecond)

		if _, err := client.CoreV1().Pods("default").Get(context.Background(), "pod-ghost", metav1.GetOptions{}); err != nil {
			t.Fatalf("pod must NOT be deleted when direct API Get finds node: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-ghost", metav1.GetOptions{}); err != nil {
			t.Fatalf("pvc must NOT be deleted when direct API Get finds node: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-ghost", metav1.GetOptions{}); err != nil {
			t.Fatalf("pv must NOT be deleted when direct API Get finds node: %v", err)
		}
	})

	t.Run("transient_get_node_error_retries_and_succeeds", func(t *testing.T) {
		pvc := makePVC("default", "pvc-err", "node-dead", "pv-err", nil)
		pv := makePV("pv-err", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-err", UID: pvc.UID,
		})
		client := fake.NewSimpleClientset(pvc, pv)

		var failOnce atomic.Bool
		failOnce.Store(true)
		client.PrependReactor("get", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if failOnce.Swap(false) {
				return true, nil, apierrors.NewServiceUnavailable("apiserver temporarily unavailable")
			}
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "node-dead")
		})

		_, metrics, _, _ := startTestController(t, client, testBaseConfig(), nil)

		waitUntil(t, 5*time.Second, "cleanup succeeds after transient get_node error", func() bool {
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-err", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-err", metav1.GetOptions{})
			return apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})

		if !strings.Contains(metrics.RenderPrometheus(), `local_pvc_cleaner_errors_total{operation="get_node"} 1`) {
			t.Errorf("expected get_node error count 1, got:\n%s", metrics.RenderPrometheus())
		}
	})
}

func TestDryRunPerformsZeroMutations(t *testing.T) {
	pvc := makePVC("default", "pvc-dry", "node-dead", "pv-dry", nil)
	pv := makePV("pv-dry", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
		Namespace: "default", Name: "pvc-dry", UID: pvc.UID,
	})
	pvOrphan := makePV("pv-dry-orphan", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeReleased, nil)
	pod := makePod("default", "pod-dry", "pvc-dry", true)

	client := fake.NewSimpleClientset(pvc, pv, pvOrphan, pod)
	cfg := testBaseConfig()
	cfg.DryRun = true
	_, metrics, recorder, _ := startTestController(t, client, cfg, nil)

	waitUntil(t, 5*time.Second, "dry-run metrics incremented", func() bool {
		out := metrics.RenderPrometheus()
		return strings.Contains(out, `local_pvc_cleaner_deletions_total{resource="pod",dry_run="true"} 1`) &&
			strings.Contains(out, `local_pvc_cleaner_deletions_total{resource="pvc",dry_run="true"} 1`) &&
			strings.Contains(out, `local_pvc_cleaner_deletions_total{resource="pv",dry_run="true"} 2`)
	})

	for _, action := range client.Actions() {
		switch action.GetVerb() {
		case "list", "watch", "get":
		default:
			t.Errorf("dry-run performed mutating API action: verb=%s resource=%s", action.GetVerb(), action.GetResource().Resource)
		}
	}

	select {
	case ev := <-recorder.Events:
		t.Errorf("dry-run should not emit Kubernetes events, got: %s", ev)
	default:
	}
}

func TestNamespaceFiltersIgnoreAnnotationAndSkipUnmanagedPods(t *testing.T) {
	t.Run("namespace_include_and_exclude", func(t *testing.T) {
		pvcAllowed := makePVC("allowed-a", "pvc-1", "node-dead", "pv-1", nil)
		pvAllowed := makePV("pv-1", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "allowed-a", Name: "pvc-1", UID: pvcAllowed.UID,
		})
		pvcExcluded := makePVC("allowed-b", "pvc-2", "node-dead", "pv-2", nil)
		pvExcluded := makePV("pv-2", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "allowed-b", Name: "pvc-2", UID: pvcExcluded.UID,
		})
		pvcOther := makePVC("other-ns", "pvc-3", "node-dead", "pv-3", nil)
		pvOther := makePV("pv-3", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "other-ns", Name: "pvc-3", UID: pvcOther.UID,
		})
		pvUnbound := makePV("pv-unbound", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeAvailable, nil)

		client := fake.NewSimpleClientset(pvcAllowed, pvAllowed, pvcExcluded, pvExcluded, pvcOther, pvOther, pvUnbound)
		cfg := testBaseConfig()
		cfg.IncludeNamespaces = []string{"allowed-a", "allowed-b"}
		cfg.ExcludeNamespaces = []string{"allowed-b"}

		startTestController(t, client, cfg, nil)

		waitUntil(t, 5*time.Second, "allowed-a resources deleted", func() bool {
			_, errPVC := client.CoreV1().PersistentVolumeClaims("allowed-a").Get(context.Background(), "pvc-1", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-1", metav1.GetOptions{})
			return apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})

		// Excluded, non-included, and unbound PVs (when include-namespaces is set) must remain untouched.
		for _, tc := range []struct{ ns, pvc, pv string }{
			{"allowed-b", "pvc-2", "pv-2"},
			{"other-ns", "pvc-3", "pv-3"},
		} {
			if _, err := client.CoreV1().PersistentVolumeClaims(tc.ns).Get(context.Background(), tc.pvc, metav1.GetOptions{}); err != nil {
				t.Errorf("filtered PVC %s/%s should not be deleted: %v", tc.ns, tc.pvc, err)
			}
			if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), tc.pv, metav1.GetOptions{}); err != nil {
				t.Errorf("filtered PV %s should not be deleted: %v", tc.pv, err)
			}
		}
		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-unbound", metav1.GetOptions{}); err != nil {
			t.Errorf("unbound PV should not be deleted when --include-namespaces is restricted: %v", err)
		}
	})

	t.Run("ignore_annotation_on_pvc_pv_and_pod", func(t *testing.T) {
		ignoreAnn := map[string]string{DefaultIgnoreAnnotation: "true"}

		// 1. Ignored PVC
		pvcIgnored := makePVC("default", "pvc-ign", "node-dead", "pv-ign-pvc", ignoreAnn)
		pvForIgnPVC := makePV("pv-ign-pvc", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-ign", UID: pvcIgnored.UID,
		})
		podForIgnPVC := makePod("default", "pod-ign-pvc", "pvc-ign", true)

		// 2. Ignored bound PV
		pvcForIgnPV := makePVC("default", "pvc-for-ign-pv", "node-dead", "pv-ign", nil)
		pvIgnored := makePV("pv-ign", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-for-ign-pv", UID: pvcForIgnPV.UID,
		})
		pvIgnored.Annotations[DefaultIgnoreAnnotation] = "true"
		podForIgnPV := makePod("default", "pod-for-ign-pv", "pvc-for-ign-pv", true)

		// 3. Ignored referencing Pod
		pvcForIgnPod := makePVC("default", "pvc-for-ign-pod", "node-dead", "pv-for-ign-pod", nil)
		pvForIgnPod := makePV("pv-for-ign-pod", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-for-ign-pod", UID: pvcForIgnPod.UID,
		})
		podIgnored := makePod("default", "pod-ign", "pvc-for-ign-pod", true)
		podIgnored.Annotations = ignoreAnn

		// 4. Control group: non-ignored PVC on same dead node SHOULD be deleted.
		pvcClean := makePVC("default", "pvc-clean", "node-dead", "pv-clean", nil)
		pvClean := makePV("pv-clean", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-clean", UID: pvcClean.UID,
		})
		podClean := makePod("default", "pod-clean", "pvc-clean", true)

		client := fake.NewSimpleClientset(
			pvcIgnored, pvForIgnPVC, podForIgnPVC,
			pvcForIgnPV, pvIgnored, podForIgnPV,
			pvcForIgnPod, pvForIgnPod, podIgnored,
			pvcClean, pvClean, podClean,
		)
		startTestController(t, client, testBaseConfig(), nil)

		waitUntil(t, 5*time.Second, "non-ignored resources deleted", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "pod-clean", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-clean", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-clean", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})

		for _, podName := range []string{"pod-ign-pvc", "pod-for-ign-pv", "pod-ign"} {
			if _, err := client.CoreV1().Pods("default").Get(context.Background(), podName, metav1.GetOptions{}); err != nil {
				t.Errorf("ignored group pod %s was unexpectedly deleted: %v", podName, err)
			}
		}
		for _, pvcName := range []string{"pvc-ign", "pvc-for-ign-pv", "pvc-for-ign-pod"} {
			if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), pvcName, metav1.GetOptions{}); err != nil {
				t.Errorf("ignored group pvc %s was unexpectedly deleted: %v", pvcName, err)
			}
		}
		for _, pvName := range []string{"pv-ign-pvc", "pv-ign", "pv-for-ign-pod"} {
			if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{}); err != nil {
				t.Errorf("ignored group pv %s was unexpectedly deleted: %v", pvName, err)
			}
		}
	})

	t.Run("skip_unmanaged_pods_blocks_until_unmanaged_pod_removed", func(t *testing.T) {
		pvcUnmanaged := makePVC("default", "pvc-unmanaged", "node-dead", "pv-unmanaged", nil)
		pvUnmanaged := makePV("pv-unmanaged", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-unmanaged", UID: pvcUnmanaged.UID,
		})
		podUnmanaged := makePod("default", "standalone-pod", "pvc-unmanaged", false)

		pvcManaged := makePVC("default", "pvc-managed", "node-dead", "pv-managed", nil)
		pvManaged := makePV("pv-managed", "node-dead", corev1.PersistentVolumeReclaimDelete, corev1.VolumeBound, &corev1.ObjectReference{
			Namespace: "default", Name: "pvc-managed", UID: pvcManaged.UID,
		})
		podManaged := makePod("default", "sts-pod-0", "pvc-managed", true)

		client := fake.NewSimpleClientset(
			pvcUnmanaged, pvUnmanaged, podUnmanaged,
			pvcManaged, pvManaged, podManaged,
		)
		cfg := testBaseConfig()
		cfg.SkipUnmanagedPods = true
		startTestController(t, client, cfg, nil)

		waitUntil(t, 5*time.Second, "managed pod/pvc/pv deleted", func() bool {
			_, errPod := client.CoreV1().Pods("default").Get(context.Background(), "sts-pod-0", metav1.GetOptions{})
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-managed", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-managed", metav1.GetOptions{})
			return apierrors.IsNotFound(errPod) && apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})

		// Unmanaged pod's PVC and PV must stay untouched.
		if _, err := client.CoreV1().Pods("default").Get(context.Background(), "standalone-pod", metav1.GetOptions{}); err != nil {
			t.Fatalf("unmanaged pod should not be deleted: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-unmanaged", metav1.GetOptions{}); err != nil {
			t.Fatalf("pvc referenced by unmanaged pod should not be deleted: %v", err)
		}
		if _, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-unmanaged", metav1.GetOptions{}); err != nil {
			t.Fatalf("pv bound to pvc referenced by unmanaged pod should not be deleted: %v", err)
		}

		// Once an operator deletes the unmanaged pod, Pod DeleteFunc re-enqueues node-dead and unblocks cleanup!
		if err := client.CoreV1().Pods("default").Delete(context.Background(), "standalone-pod", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("failed deleting standalone pod: %v", err)
		}
		waitUntil(t, 5*time.Second, "pvc-unmanaged and pv-unmanaged deleted after standalone pod removed", func() bool {
			_, errPVC := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "pvc-unmanaged", metav1.GetOptions{})
			_, errPV := client.CoreV1().PersistentVolumes().Get(context.Background(), "pv-unmanaged", metav1.GetOptions{})
			return apierrors.IsNotFound(errPVC) && apierrors.IsNotFound(errPV)
		})
	})
}
