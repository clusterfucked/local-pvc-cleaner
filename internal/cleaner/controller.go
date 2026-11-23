package cleaner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const (
	PVCByNodeIndex = "pvcByNode"
	PVByNodeIndex  = "pvByNode"
	PodByPVCIndex  = "podByPVC"
)

// Options configures a Controller instance.
type Options struct {
	Client   kubernetes.Interface
	Config   Config
	Logger   *slog.Logger
	Metrics  *Metrics
	Recorder record.EventRecorder
	Now      func() time.Time
}

// Controller watches Nodes, Pods, PVCs, and PVs and cleans up local volumes
// allocated to nodes that no longer exist.
type Controller struct {
	client   kubernetes.Interface
	cfg      Config
	filter   *Filter
	logger   *slog.Logger
	metrics  *Metrics
	recorder record.EventRecorder
	now      func() time.Time

	factory      informers.SharedInformerFactory
	nodeInformer cache.SharedIndexInformer
	podInformer  cache.SharedIndexInformer
	pvcInformer  cache.SharedIndexInformer
	pvInformer   cache.SharedIndexInformer

	nodeLister corelisters.NodeLister
	pvcLister  corelisters.PersistentVolumeClaimLister
	pvLister   corelisters.PersistentVolumeLister

	queue  workqueue.TypedRateLimitingInterface[string]
	synced atomic.Bool

	missingMu    sync.Mutex
	missingSince map[string]time.Time
}

// NewController constructs a Controller, registers all informer indexers and event
// handlers, and returns an error if any registration fails.
func NewController(opts Options) (*Controller, error) {
	if opts.Client == nil {
		return nil, errors.New("kubernetes client must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = NewMetrics()
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		opts.Client,
		opts.Config.ResyncPeriod,
		informers.WithTransform(StripCachedObject),
	)

	nodeInf := factory.Core().V1().Nodes()
	podInf := factory.Core().V1().Pods()
	pvcInf := factory.Core().V1().PersistentVolumeClaims()
	pvInf := factory.Core().V1().PersistentVolumes()

	c := &Controller{
		client:       opts.Client,
		cfg:          opts.Config,
		filter:       NewFilter(opts.Config),
		logger:       logger,
		metrics:      metrics,
		recorder:     opts.Recorder,
		now:          nowFn,
		factory:      factory,
		nodeInformer: nodeInf.Informer(),
		podInformer:  podInf.Informer(),
		pvcInformer:  pvcInf.Informer(),
		pvInformer:   pvInf.Informer(),
		nodeLister:   nodeInf.Lister(),
		pvcLister:    pvcInf.Lister(),
		pvLister:     pvInf.Lister(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "local-pvc-cleaner",
			},
		),
		missingSince: make(map[string]time.Time),
	}

	if err := c.registerIndexers(); err != nil {
		return nil, err
	}
	if err := c.registerEventHandlers(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Controller) registerIndexers() error {
	if err := c.podInformer.AddIndexers(cache.Indexers{
		PodByPVCIndex: func(obj any) ([]string, error) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, nil
			}
			return PodPVCKeys(pod), nil
		},
	}); err != nil {
		return fmt.Errorf("register %s indexer: %w", PodByPVCIndex, err)
	}

	if err := c.pvcInformer.AddIndexers(cache.Indexers{
		PVCByNodeIndex: func(obj any) ([]string, error) {
			pvc, ok := obj.(*corev1.PersistentVolumeClaim)
			if !ok || !c.filter.MatchesPVC(pvc) {
				return nil, nil
			}
			nodeName := c.filter.ExtractPVCNode(pvc)
			if nodeName == "" {
				return nil, nil
			}
			return []string{nodeName}, nil
		},
	}); err != nil {
		return fmt.Errorf("register %s indexer: %w", PVCByNodeIndex, err)
	}

	if err := c.pvInformer.AddIndexers(cache.Indexers{
		PVByNodeIndex: func(obj any) ([]string, error) {
			pv, ok := obj.(*corev1.PersistentVolume)
			if !ok || !c.filter.MatchesPV(pv) {
				return nil, nil
			}
			nodeName := c.filter.ExtractPVNode(pv)
			if nodeName == "" {
				return nil, nil
			}
			return []string{nodeName}, nil
		},
	}); err != nil {
		return fmt.Errorf("register %s indexer: %w", PVByNodeIndex, err)
	}

	return nil
}

func (c *Controller) registerEventHandlers() error {
	if _, err := c.nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if node, ok := obj.(*corev1.Node); ok && node.Name != "" {
				c.clearNodeMissing(node.Name)
			}
		},
		DeleteFunc: func(obj any) {
			node, ok := UnwrapDeletedObject[*corev1.Node](obj)
			if !ok {
				c.logger.Warn("unexpected object type in node DeleteFunc", "type", fmt.Sprintf("%T", obj))
				return
			}
			if node.Name == "" {
				return
			}
			c.logger.Info("node deleted", "node", node.Name)
			c.markNodeMissingAt(node.Name, c.now())
			c.queue.Add(node.Name)
		},
	}); err != nil {
		return fmt.Errorf("register node event handler: %w", err)
	}

	if _, err := c.pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			c.onPVCChange(obj)
		},
		UpdateFunc: func(_, newObj any) {
			c.onPVCChange(newObj)
		},
	}); err != nil {
		return fmt.Errorf("register pvc event handler: %w", err)
	}

	if _, err := c.pvInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			c.onPVChange(obj)
		},
		UpdateFunc: func(_, newObj any) {
			c.onPVChange(newObj)
		},
	}); err != nil {
		return fmt.Errorf("register pv event handler: %w", err)
	}

	if _, err := c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				c.onPodChange(pod)
			}
		},
		DeleteFunc: func(obj any) {
			pod, ok := UnwrapDeletedObject[*corev1.Pod](obj)
			if !ok {
				c.logger.Warn("unexpected object type in pod DeleteFunc", "type", fmt.Sprintf("%T", obj))
				return
			}
			c.onPodChange(pod)
		},
	}); err != nil {
		return fmt.Errorf("register pod event handler: %w", err)
	}

	return nil
}

func (c *Controller) onPVCChange(obj any) {
	if !c.synced.Load() {
		return
	}
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok || !c.filter.MatchesPVC(pvc) {
		return
	}
	nodeName := c.filter.ExtractPVCNode(pvc)
	if nodeName == "" {
		return
	}
	if !c.nodeExistsInCache(nodeName) {
		c.queue.Add(nodeName)
	}
}

func (c *Controller) onPVChange(obj any) {
	if !c.synced.Load() {
		return
	}
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok || !c.filter.MatchesPV(pv) {
		return
	}
	nodeName := c.filter.ExtractPVNode(pv)
	if nodeName == "" {
		return
	}
	if !c.nodeExistsInCache(nodeName) {
		c.queue.Add(nodeName)
	}
}

func (c *Controller) onPodChange(pod *corev1.Pod) {
	if !c.synced.Load() || pod == nil {
		return
	}
	for _, key := range PodPVCKeys(pod) {
		ns, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			continue
		}
		pvc, err := c.pvcLister.PersistentVolumeClaims(ns).Get(name)
		if err != nil {
			continue
		}
		nodeName := c.filter.ExtractPVCNode(pvc)
		if nodeName == "" && pvc.Spec.VolumeName != "" {
			if pv, pvErr := c.pvLister.Get(pvc.Spec.VolumeName); pvErr == nil && c.filter.MatchesPV(pv) {
				nodeName = c.filter.ExtractPVNode(pv)
			}
		}
		if nodeName != "" && !c.nodeExistsInCache(nodeName) {
			c.queue.Add(nodeName)
		}
	}
}

// Run starts the informers, waits for all caches to sync, runs the initial sweep
// and worker goroutines, and blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	var workerWG sync.WaitGroup
	defer func() {
		c.synced.Store(false)
		c.metrics.SetSynced(false)
		c.metrics.SetQueueLenFunc(nil)
		c.queue.ShutDown()
		workerWG.Wait()
		c.factory.Shutdown()
	}()

	c.logger.Info("starting informers",
		"workers", c.cfg.Workers,
		"resync_period", c.cfg.ResyncPeriod.String(),
		"grace_period", c.cfg.NodeMissingGracePeriod.String(),
		"dry_run", c.cfg.DryRun,
	)

	c.factory.Start(ctx.Done())

	if !cache.WaitForNamedCacheSyncWithContext(ctx,
		c.nodeInformer.HasSynced,
		c.podInformer.HasSynced,
		c.pvcInformer.HasSynced,
		c.pvInformer.HasSynced,
	) {
		if ctx.Err() != nil {
			return fmt.Errorf("informer cache sync aborted: %w", ctx.Err())
		}
		return errors.New("failed to sync informer caches")
	}

	c.synced.Store(true)
	c.metrics.SetSynced(true)
	c.metrics.SetQueueLenFunc(c.queue.Len)
	c.logger.Info("all informer caches synced; starting workers")

	for i := 0; i < c.cfg.Workers; i++ {
		workerWG.Add(1)
		go func(workerID int) {
			defer workerWG.Done()
			c.runWorker(ctx, workerID)
		}(i)
	}

	c.enqueueMissingNodeCandidates()

	var tickerCh <-chan time.Time
	if c.cfg.ResyncPeriod > 0 {
		ticker := time.NewTicker(c.cfg.ResyncPeriod)
		defer ticker.Stop()
		tickerCh = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("shutting down controller")
			return nil
		case <-tickerCh:
			c.logger.Debug("running periodic missing-node sweep")
			c.enqueueMissingNodeCandidates()
		}
	}
}

func (c *Controller) enqueueMissingNodeCandidates() {
	candidates := make(map[string]struct{})
	for _, nodeName := range c.pvcInformer.GetIndexer().ListIndexFuncValues(PVCByNodeIndex) {
		if nodeName != "" {
			candidates[nodeName] = struct{}{}
		}
	}
	for _, nodeName := range c.pvInformer.GetIndexer().ListIndexFuncValues(PVByNodeIndex) {
		if nodeName != "" {
			candidates[nodeName] = struct{}{}
		}
	}
	for nodeName := range candidates {
		if !c.nodeExistsInCache(nodeName) {
			c.queue.Add(nodeName)
		}
	}
}

func (c *Controller) runWorker(ctx context.Context, workerID int) {
	c.logger.Debug("worker started", "worker", workerID)
	defer c.logger.Debug("worker stopped", "worker", workerID)
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	nodeName, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(nodeName)

	requeueAfter, err := c.reconcileNode(ctx, nodeName)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		c.logger.Error("failed reconciling node; requeuing with backoff", "node", nodeName, "error", err)
		c.queue.AddRateLimited(nodeName)
		return true
	}

	c.queue.Forget(nodeName)
	if requeueAfter > 0 {
		c.queue.AddAfter(nodeName, requeueAfter)
	}
	return true
}

func (c *Controller) reconcileNode(ctx context.Context, nodeName string) (time.Duration, error) {
	if nodeName == "" {
		return 0, nil
	}

	if c.nodeExistsInCache(nodeName) {
		c.clearNodeMissing(nodeName)
		return 0, nil
	}

	pvcObjs, err := c.pvcInformer.GetIndexer().ByIndex(PVCByNodeIndex, nodeName)
	if err != nil {
		c.metrics.IncError("index_lookup")
		return 0, fmt.Errorf("lookup pvcs for node %q: %w", nodeName, err)
	}

	pvObjs, err := c.pvInformer.GetIndexer().ByIndex(PVByNodeIndex, nodeName)
	if err != nil {
		c.metrics.IncError("index_lookup")
		return 0, fmt.Errorf("lookup pvs for node %q: %w", nodeName, err)
	}

	pvcByKey := make(map[string]*corev1.PersistentVolumeClaim, len(pvcObjs))
	for _, obj := range pvcObjs {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		if !c.filter.NamespaceAllowed(pvc.Namespace) {
			continue
		}
		pvcByKey[PVCKey(pvc.Namespace, pvc.Name)] = pvc
	}

	pvs := make([]*corev1.PersistentVolume, 0, len(pvObjs))
	hasAllowedPV := false
	for _, obj := range pvObjs {
		pv, ok := obj.(*corev1.PersistentVolume)
		if !ok {
			continue
		}
		pvs = append(pvs, pv)
		if c.filter.PVNamespaceAllowed(pv) {
			hasAllowedPV = true
		}

		// Also discover PVCs bound to managed native local PVs even when the PVC
		// itself lacks selected-node or storage-provisioner annotations.
		if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace != "" && pv.Spec.ClaimRef.Name != "" {
			if c.filter.NamespaceAllowed(pv.Spec.ClaimRef.Namespace) {
				pvc, getErr := c.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
				if getErr == nil && pvc.Spec.VolumeName == pv.Name && (pv.Spec.ClaimRef.UID == "" || pvc.UID == pv.Spec.ClaimRef.UID) {
					key := PVCKey(pvc.Namespace, pvc.Name)
					if _, exists := pvcByKey[key]; !exists {
						pvcByKey[key] = pvc
					}
				}
			}
		}
	}

	if len(pvcByKey) == 0 && !hasAllowedPV {
		c.clearNodeMissing(nodeName)
		return 0, nil
	}

	// Enforce node-missing grace period before contacting the API server.
	if c.cfg.NodeMissingGracePeriod > 0 {
		now := c.now()
		firstSeen := c.markNodeMissingAt(nodeName, now)
		elapsed := now.Sub(firstSeen)
		if elapsed < c.cfg.NodeMissingGracePeriod {
			remaining := c.cfg.NodeMissingGracePeriod - elapsed
			c.logger.Debug("node missing within grace period; delaying cleanup",
				"node", nodeName,
				"elapsed", elapsed.String(),
				"remaining", remaining.String(),
				"grace_period", c.cfg.NodeMissingGracePeriod.String(),
			)
			return remaining, nil
		}
	}

	// Direct API server verification: proceed ONLY on explicit NotFound.
	apiCtx, cancel := c.apiContext(ctx)
	_, err = c.client.CoreV1().Nodes().Get(apiCtx, nodeName, metav1.GetOptions{})
	cancel()
	if err == nil {
		c.clearNodeMissing(nodeName)
		c.logger.Warn("node missing from cache but still exists on API server; aborting cleanup", "node", nodeName)
		return 0, nil
	}
	if !apierrors.IsNotFound(err) {
		c.metrics.IncError("get_node")
		return 0, fmt.Errorf("verify missing node %q via API server: %w", nodeName, err)
	}

	pvcKeys := make([]string, 0, len(pvcByKey))
	for k := range pvcByKey {
		pvcKeys = append(pvcKeys, k)
	}
	sort.Strings(pvcKeys)

	sort.Slice(pvs, func(i, j int) bool {
		return pvs[i].Name < pvs[j].Name
	})

	handledPVs := make(map[string]struct{}, len(pvcKeys))
	var errs []error

	for _, key := range pvcKeys {
		pvc := pvcByKey[key]
		if pvc.Spec.VolumeName != "" {
			handledPVs[pvc.Spec.VolumeName] = struct{}{}
		}
		if err := c.cleanupPVC(ctx, nodeName, pvc); err != nil {
			errs = append(errs, err)
		}
	}

	for _, pv := range pvs {
		if _, handled := handledPVs[pv.Name]; handled {
			continue
		}
		if !c.filter.PVNamespaceAllowed(pv) {
			continue
		}
		if err := c.cleanupPV(ctx, nodeName, pv); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return 0, errors.Join(errs...)
	}
	return 0, nil
}

func (c *Controller) cleanupPVC(ctx context.Context, nodeName string, pvc *corev1.PersistentVolumeClaim) error {
	log := c.logger.With(
		"namespace", pvc.Namespace,
		"pvc", pvc.Name,
		"pv", pvc.Spec.VolumeName,
		"node", nodeName,
	)

	if c.filter.IsIgnored(pvc.Annotations) {
		log.Info("skipping pvc due to ignore annotation", "annotation", c.cfg.IgnoreAnnotation)
		c.metrics.IncSkipped("pvc", "ignored")
		c.recordEvent(pvc, corev1.EventTypeNormal, "SkippedCleanup",
			"Skipping cleanup for PVC on missing node %s because %s=true", nodeName, c.cfg.IgnoreAnnotation)
		return nil
	}

	var boundPV *corev1.PersistentVolume
	if pvc.Spec.VolumeName != "" {
		pv, err := c.pvLister.Get(pvc.Spec.VolumeName)
		if err != nil && !apierrors.IsNotFound(err) {
			c.metrics.IncError("index_lookup")
			return fmt.Errorf("get bound pv %q for pvc %s/%s: %w", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
		}
		if err == nil {
			boundPV = pv
		}
	}

	if boundPV != nil && c.filter.IsIgnored(boundPV.Annotations) {
		log.Info("skipping pvc cleanup because bound pv has ignore annotation", "annotation", c.cfg.IgnoreAnnotation)
		c.metrics.IncSkipped("pv", "ignored")
		c.recordEvent(boundPV, corev1.EventTypeNormal, "SkippedCleanup",
			"Skipping cleanup for PV and bound PVC %s/%s on missing node %s because %s=true",
			pvc.Namespace, pvc.Name, nodeName, c.cfg.IgnoreAnnotation)
		return nil
	}

	podObjs, err := c.podInformer.GetIndexer().ByIndex(PodByPVCIndex, PVCKey(pvc.Namespace, pvc.Name))
	if err != nil {
		c.metrics.IncError("index_lookup")
		return fmt.Errorf("lookup pods for pvc %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	pods := make([]*corev1.Pod, 0, len(podObjs))
	for _, obj := range podObjs {
		if pod, ok := obj.(*corev1.Pod); ok {
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Name < pods[j].Name
	})

	// Check all referencing pods before mutating anything so we never leave a PVC
	// deadlocked in Terminating behind an ignored or unmanaged pod.
	for _, pod := range pods {
		if c.filter.IsIgnored(pod.Annotations) {
			log.Info("skipping pvc cleanup because referencing pod has ignore annotation",
				"pod", pod.Name,
				"annotation", c.cfg.IgnoreAnnotation,
			)
			c.metrics.IncSkipped("pod", "ignored")
			c.recordEvent(pvc, corev1.EventTypeNormal, "SkippedCleanup",
				"Skipping cleanup for PVC on missing node %s: referencing pod %s has %s=true",
				nodeName, pod.Name, c.cfg.IgnoreAnnotation)
			return nil
		}
		if c.cfg.SkipUnmanagedPods && IsUnmanagedPod(pod) {
			log.Info("skipping pvc cleanup because referencing pod is unmanaged", "pod", pod.Name)
			c.metrics.IncSkipped("pod", "unmanaged_pod")
			c.recordEvent(pvc, corev1.EventTypeWarning, "SkippedUnmanagedPod",
				"Skipping cleanup for PVC on missing node %s: referencing pod %s has no OwnerReferences",
				nodeName, pod.Name)
			return nil
		}
	}

	// Step 1: Delete referencing Pods first.
	var podErrs []error
	for _, pod := range pods {
		podLog := log.With("pod", pod.Name)
		if pod.DeletionTimestamp != nil {
			podLog.Debug("referencing pod already terminating")
			continue
		}
		if c.cfg.DryRun {
			podLog.Info("dry-run: would delete referencing pod", "dry_run", true)
			c.metrics.IncDeletion("pod", true)
			continue
		}
		apiCtx, cancel := c.apiContext(ctx)
		err := c.client.CoreV1().Pods(pod.Namespace).Delete(apiCtx, pod.Name, metav1.DeleteOptions{})
		cancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				podLog.Debug("referencing pod already deleted")
				continue
			}
			c.metrics.IncError("delete_pod")
			podLog.Error("failed to delete referencing pod", "error", err)
			podErrs = append(podErrs, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err))
			continue
		}
		podLog.Info("deleted referencing pod")
		c.metrics.IncDeletion("pod", false)
		c.recordEvent(pod, corev1.EventTypeNormal, "DeletedPodOnMissingNode",
			"Deleted pod referencing PVC %s/%s on missing node %s", pvc.Namespace, pvc.Name, nodeName)
	}
	if len(podErrs) > 0 {
		return errors.Join(podErrs...)
	}

	// Step 2: Delete the PVC (unless already terminating).
	if pvc.DeletionTimestamp != nil {
		log.Debug("pvc already terminating")
	} else if c.cfg.DryRun {
		log.Info("dry-run: would delete pvc", "dry_run", true)
		c.metrics.IncDeletion("pvc", true)
	} else {
		apiCtx, cancel := c.apiContext(ctx)
		err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(apiCtx, pvc.Name, metav1.DeleteOptions{})
		cancel()
		if err != nil {
			if !apierrors.IsNotFound(err) {
				c.metrics.IncError("delete_pvc")
				log.Error("failed to delete pvc", "error", err)
				return fmt.Errorf("delete pvc %s/%s: %w", pvc.Namespace, pvc.Name, err)
			}
			log.Debug("pvc already deleted")
		} else {
			log.Info("deleted pvc")
			c.metrics.IncDeletion("pvc", false)
			c.recordEvent(pvc, corev1.EventTypeNormal, "DeletedPVCOnMissingNode",
				"Deleted PVC allocated to missing node %s", nodeName)
		}
	}

	// Step 3: Delete the bound PV (if present and eligible).
	if boundPV == nil {
		return nil
	}
	return c.cleanupPV(ctx, nodeName, boundPV)
}

func (c *Controller) cleanupPV(ctx context.Context, nodeName string, pv *corev1.PersistentVolume) error {
	if pv == nil {
		return nil
	}
	log := c.logger.With(
		"pv", pv.Name,
		"node", nodeName,
		"phase", string(pv.Status.Phase),
	)
	if pv.Spec.ClaimRef != nil {
		log = log.With("namespace", pv.Spec.ClaimRef.Namespace, "pvc", pv.Spec.ClaimRef.Name)
	}

	if c.filter.IsIgnored(pv.Annotations) {
		log.Info("skipping pv due to ignore annotation", "annotation", c.cfg.IgnoreAnnotation)
		c.metrics.IncSkipped("pv", "ignored")
		c.recordEvent(pv, corev1.EventTypeNormal, "SkippedCleanup",
			"Skipping cleanup for PV on missing node %s because %s=true", nodeName, c.cfg.IgnoreAnnotation)
		return nil
	}

	if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain && !c.cfg.DeleteRetainedPVs {
		log.Info("skipping pv with Retain reclaim policy", "reclaim_policy", string(pv.Spec.PersistentVolumeReclaimPolicy))
		c.metrics.IncSkipped("pv", "retain_policy")
		c.recordEvent(pv, corev1.EventTypeNormal, "SkippedRetainedPV",
			"Skipping PV on missing node %s due to Retain reclaim policy (set --delete-retained-pvs=true to delete)", nodeName)
		return nil
	}

	if pv.DeletionTimestamp != nil {
		log.Debug("pv already terminating")
		return nil
	}

	if c.cfg.DryRun {
		log.Info("dry-run: would delete pv", "dry_run", true)
		c.metrics.IncDeletion("pv", true)
		return nil
	}

	apiCtx, cancel := c.apiContext(ctx)
	err := c.client.CoreV1().PersistentVolumes().Delete(apiCtx, pv.Name, metav1.DeleteOptions{})
	cancel()
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("pv already deleted")
			return nil
		}
		c.metrics.IncError("delete_pv")
		log.Error("failed to delete pv", "error", err)
		return fmt.Errorf("delete pv %s: %w", pv.Name, err)
	}

	log.Info("deleted pv")
	c.metrics.IncDeletion("pv", false)
	c.recordEvent(pv, corev1.EventTypeNormal, "DeletedPVOnMissingNode",
		"Deleted PV pinned to missing node %s", nodeName)
	return nil
}

func (c *Controller) nodeExistsInCache(nodeName string) bool {
	_, err := c.nodeLister.Get(nodeName)
	return err == nil
}

func (c *Controller) markNodeMissingAt(nodeName string, ts time.Time) time.Time {
	c.missingMu.Lock()
	defer c.missingMu.Unlock()
	if existing, ok := c.missingSince[nodeName]; ok {
		return existing
	}
	c.missingSince[nodeName] = ts
	return ts
}

func (c *Controller) clearNodeMissing(nodeName string) {
	c.missingMu.Lock()
	defer c.missingMu.Unlock()
	delete(c.missingSince, nodeName)
}

func (c *Controller) apiContext(parent context.Context) (context.Context, context.CancelFunc) {
	if c.cfg.APITimeout > 0 {
		return context.WithTimeout(parent, c.cfg.APITimeout)
	}
	return context.WithCancel(parent)
}

func (c *Controller) recordEvent(obj runtime.Object, eventType, reason, messageFmt string, args ...any) {
	if c.cfg.DryRun || c.recorder == nil || obj == nil {
		return
	}
	c.recorder.Eventf(obj, eventType, reason, messageFmt, args...)
}
