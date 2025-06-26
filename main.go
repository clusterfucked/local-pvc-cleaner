package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	selectedNodeAnnotation   = "volume.kubernetes.io/selected-node"
	provisionerAnnotation    = "volume.kubernetes.io/storage-provisioner"
	expectedProvisionerValue = "rancher.io/local-path"
	pvcByNodeIndex           = "pvcByNode"
	podByPvcIndex            = "podByPvc"
	lockName                 = "local-pvc-cleaner"
)

var (
	id        = os.Getenv("POD_NAME")
	namespace = os.Getenv("POD_NAMESPACE")
)

func deleteVolumes(ctx context.Context, clientset *kubernetes.Clientset, factory informers.SharedInformerFactory, pvc *corev1.PersistentVolumeClaim) {
	err := clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
	if err != nil {
		fmt.Printf("failed to delete pvc(%s): %v\n", pvc.Name, err)
		return
	}
	fmt.Printf("deleted pvc(%s)\n", pvc.Name)

	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		fmt.Printf("pvc(%s) is not bound to a volume\n", pvc.Name)
		return
	}

	err = clientset.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{})
	if err != nil {
		fmt.Printf("failed to delete pv(%s): %v\n", pvName, err)
		return
	}

	fmt.Printf("deleted pv(%s)\n", pvName)

	pods, err := factory.Core().V1().Pods().Informer().GetIndexer().ByIndex(podByPvcIndex, pvc.Name)
	if err != nil {
		fmt.Printf("error getting pods from index: %v\n", err)
		return
	}

	for _, podAny := range pods {
		pod := podAny.(*corev1.Pod)
		err = clientset.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil {
			fmt.Printf("failed to delete pod(%s): %v\n", pod.Name, err)
			continue
		}

		fmt.Printf("deleted pod(%s)\n", pod.Name)
	}
}

func cleanupVolumesByNode(ctx context.Context, clientset *kubernetes.Clientset, nodeName string, factory informers.SharedInformerFactory) {
	persistentVolumeClaims, err := factory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer().ByIndex(pvcByNodeIndex, nodeName)
	if err != nil {
		fmt.Printf("error getting pvc from index: %v\n", err)
		return
	}
	for _, pvcAny := range persistentVolumeClaims {
		pvc := pvcAny.(*corev1.PersistentVolumeClaim)
		deleteVolumes(ctx, clientset, factory, pvc)
	}
}

func main() {
	// kubeconfig or in-cluster
	var config *rest.Config
	var err error

	if id == "" || namespace == "" {
		panic(fmt.Errorf("required to set id(%s) and namespace(%s)", id, namespace))
	}

	kubeConfig := os.Getenv("KUBECONFIG")
	if kubeConfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	factory := informers.NewSharedInformerFactory(clientset, 0)

	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddIndexers(cache.Indexers{
		podByPvcIndex: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			pvcs := make([]string, 0, len(pod.Spec.Volumes))
			for _, volume := range pod.Spec.Volumes {
				if volume.PersistentVolumeClaim == nil {
					continue
				}

				claimName := volume.PersistentVolumeClaim.ClaimName
				if claimName == "" {
					continue
				}
				pvcs = append(pvcs, claimName)
			}

			return pvcs, nil
		},
	})

	pvcInformer := factory.Core().V1().PersistentVolumeClaims().Informer()
	pvcInformer.AddIndexers(cache.Indexers{
		pvcByNodeIndex: func(obj any) ([]string, error) {
			pvc := obj.(*corev1.PersistentVolumeClaim)
			if pvc.Annotations[provisionerAnnotation] != expectedProvisionerValue {
				return nil, nil
			}

			return []string{pvc.Annotations[selectedNodeAnnotation]}, nil
		},
	})

	nodeInformer := factory.Core().V1().Nodes().Informer()
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj any) {
			node := obj.(*corev1.Node)
			fmt.Printf("node deleted: %s\n", node.Name)
			cleanupVolumesByNode(context.TODO(), clientset, node.Name, factory)
		},
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "local-pvc-cleaner",
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: time.Second * 15,
		RenewDeadline: time.Second * 10,
		RetryPeriod:   time.Second * 2,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				fmt.Printf("me(%s) started leading...\n", id)
				factory.Start(ctx.Done())
				factory.WaitForCacheSync(ctx.Done())

				pvcs, err := factory.Core().V1().PersistentVolumeClaims().Lister().List(labels.Everything())
				if err != nil {
					fmt.Printf("failed to list pvcs: %v\n", err)
					cancel()
					return
				}

				for _, pvc := range pvcs {
					if pvc.Annotations[provisionerAnnotation] != expectedProvisionerValue {
						continue
					}

					nodeName := pvc.Annotations[selectedNodeAnnotation]
					_, exists, err := factory.Core().V1().Nodes().Informer().GetStore().GetByKey(nodeName)
					if err != nil {
						fmt.Printf("failed to get node(%s) from pvc(%s): %v\n", nodeName, pvc.Name, err)
						cancel()
						return
					}

					if exists {
						fmt.Printf("node(%s) does exist in store from pvc(%s)\n", nodeName, pvc.Name)
						continue
					}

					fmt.Printf("node(%s) does not exist in store from pvc(%s)\n", nodeName, pvc.Name)
					deleteVolumes(ctx, clientset, factory, pvc)
				}
			},
			OnStoppedLeading: func() {
				fmt.Printf("me(%s) stopped leading...\n", id)
				cancel()
			},
			OnNewLeader: func(identity string) {
				fmt.Printf("new leader(%s)\n", identity)
			},
		},
	})
}
