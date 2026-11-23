package cleaner

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type deletionKey struct {
	resource string
	dryRun   bool
}

type skipKey struct {
	resource string
	reason   string
}

// Metrics tracks controller health, readiness, and Prometheus counters/gauges
// using only the Go standard library.
type Metrics struct {
	initialized atomic.Bool
	isLeader    atomic.Bool
	synced      atomic.Bool

	mu         sync.RWMutex
	deletions  map[deletionKey]*atomic.Int64
	errors     map[string]*atomic.Int64
	skipped    map[skipKey]*atomic.Int64
	queueLenFn func() int
}

// NewMetrics creates a Metrics instance with baseline counters pre-registered.
func NewMetrics() *Metrics {
	m := &Metrics{
		deletions: make(map[deletionKey]*atomic.Int64),
		errors:    make(map[string]*atomic.Int64),
		skipped:   make(map[skipKey]*atomic.Int64),
	}
	for _, res := range []string{"pod", "pvc", "pv"} {
		for _, dryRun := range []bool{false, true} {
			m.deletions[deletionKey{resource: res, dryRun: dryRun}] = &atomic.Int64{}
		}
	}
	for _, op := range []string{"get_node", "delete_pod", "delete_pvc", "delete_pv", "index_lookup"} {
		m.errors[op] = &atomic.Int64{}
	}
	return m
}

// SetInitialized marks the process as initialized and ready to serve standby traffic.
func (m *Metrics) SetInitialized(v bool) {
	m.initialized.Store(v)
}

// SetLeader updates whether this instance currently holds leadership.
func (m *Metrics) SetLeader(v bool) {
	m.isLeader.Store(v)
}

// SetSynced updates whether the active controller's informer caches have synced.
func (m *Metrics) SetSynced(v bool) {
	m.synced.Store(v)
}

// SetQueueLenFunc registers a callback returning the current workqueue depth.
func (m *Metrics) SetQueueLenFunc(fn func() int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueLenFn = fn
}

// IncDeletion increments local_pvc_cleaner_deletions_total{resource,dry_run}.
func (m *Metrics) IncDeletion(resource string, dryRun bool) {
	k := deletionKey{resource: resource, dryRun: dryRun}
	m.mu.RLock()
	c, ok := m.deletions[k]
	m.mu.RUnlock()
	if ok {
		c.Add(1)
		return
	}
	m.mu.Lock()
	if c, ok = m.deletions[k]; !ok {
		c = &atomic.Int64{}
		m.deletions[k] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

// IncError increments local_pvc_cleaner_errors_total{operation}.
func (m *Metrics) IncError(operation string) {
	m.mu.RLock()
	c, ok := m.errors[operation]
	m.mu.RUnlock()
	if ok {
		c.Add(1)
		return
	}
	m.mu.Lock()
	if c, ok = m.errors[operation]; !ok {
		c = &atomic.Int64{}
		m.errors[operation] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

// IncSkipped increments local_pvc_cleaner_skipped_total{resource,reason}.
func (m *Metrics) IncSkipped(resource, reason string) {
	k := skipKey{resource: resource, reason: reason}
	m.mu.RLock()
	c, ok := m.skipped[k]
	m.mu.RUnlock()
	if ok {
		c.Add(1)
		return
	}
	m.mu.Lock()
	if c, ok = m.skipped[k]; !ok {
		c = &atomic.Int64{}
		m.skipped[k] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

// RenderPrometheus renders all metrics in Prometheus text exposition format.
func (m *Metrics) RenderPrometheus() string {
	var b strings.Builder

	m.mu.RLock()
	delKeys := make([]deletionKey, 0, len(m.deletions))
	for k := range m.deletions {
		delKeys = append(delKeys, k)
	}
	errKeys := make([]string, 0, len(m.errors))
	for k := range m.errors {
		errKeys = append(errKeys, k)
	}
	skipKeys := make([]skipKey, 0, len(m.skipped))
	for k := range m.skipped {
		skipKeys = append(skipKeys, k)
	}
	qFn := m.queueLenFn
	m.mu.RUnlock()

	sort.Slice(delKeys, func(i, j int) bool {
		if delKeys[i].resource != delKeys[j].resource {
			return delKeys[i].resource < delKeys[j].resource
		}
		return !delKeys[i].dryRun && delKeys[j].dryRun
	})
	sort.Strings(errKeys)
	sort.Slice(skipKeys, func(i, j int) bool {
		if skipKeys[i].resource != skipKeys[j].resource {
			return skipKeys[i].resource < skipKeys[j].resource
		}
		return skipKeys[i].reason < skipKeys[j].reason
	})

	b.WriteString("# HELP local_pvc_cleaner_deletions_total Total number of resource deletions (or planned dry-run deletions).\n")
	b.WriteString("# TYPE local_pvc_cleaner_deletions_total counter\n")
	m.mu.RLock()
	for _, k := range delKeys {
		val := m.deletions[k].Load()
		fmt.Fprintf(&b, "local_pvc_cleaner_deletions_total{resource=%q,dry_run=%q} %d\n", k.resource, strconv.FormatBool(k.dryRun), val)
	}
	m.mu.RUnlock()

	b.WriteString("# HELP local_pvc_cleaner_errors_total Total number of errors encountered during reconciliation.\n")
	b.WriteString("# TYPE local_pvc_cleaner_errors_total counter\n")
	m.mu.RLock()
	for _, op := range errKeys {
		val := m.errors[op].Load()
		fmt.Fprintf(&b, "local_pvc_cleaner_errors_total{operation=%q} %d\n", op, val)
	}
	m.mu.RUnlock()

	if len(skipKeys) > 0 {
		b.WriteString("# HELP local_pvc_cleaner_skipped_total Total number of resources skipped due to safeguards or filters.\n")
		b.WriteString("# TYPE local_pvc_cleaner_skipped_total counter\n")
		m.mu.RLock()
		for _, k := range skipKeys {
			val := m.skipped[k].Load()
			fmt.Fprintf(&b, "local_pvc_cleaner_skipped_total{resource=%q,reason=%q} %d\n", k.resource, k.reason, val)
		}
		m.mu.RUnlock()
	}

	leaderVal := 0
	if m.isLeader.Load() {
		leaderVal = 1
	}
	b.WriteString("# HELP local_pvc_cleaner_is_leader Whether this replica is the active leader (1) or standby (0).\n")
	b.WriteString("# TYPE local_pvc_cleaner_is_leader gauge\n")
	fmt.Fprintf(&b, "local_pvc_cleaner_is_leader %d\n", leaderVal)

	queueDepth := 0
	if qFn != nil {
		queueDepth = qFn()
	}
	b.WriteString("# HELP local_pvc_cleaner_queue_depth Current number of node reconciliation items in the workqueue.\n")
	b.WriteString("# TYPE local_pvc_cleaner_queue_depth gauge\n")
	fmt.Fprintf(&b, "local_pvc_cleaner_queue_depth %d\n", queueDepth)

	return b.String()
}

// NewHTTPHandler constructs an http.Handler serving /healthz, /readyz, and /metrics.
// When healthzCheck is non-nil (e.g. leaderelection.HealthzAdaptor.Check), /healthz
// delegates to it so an unhealthy leader fails liveness while standby replicas stay healthy.
func (m *Metrics) NewHTTPHandler(leaderElectionEnabled bool, healthzCheck func(*http.Request) error) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if healthzCheck != nil {
			if err := healthzCheck(r); err != nil {
				http.Error(w, fmt.Sprintf("unhealthy: %v", err), http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !m.initialized.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("starting\n"))
			return
		}
		if leaderElectionEnabled {
			if !m.isLeader.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok (role=standby)\n"))
				return
			}
			if !m.synced.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("syncing caches (role=leader)\n"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok (role=leader)\n"))
			return
		}
		if !m.synced.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("syncing caches (role=single)\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok (role=single)\n"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(m.RenderPrometheus()))
	})

	return mux
}
