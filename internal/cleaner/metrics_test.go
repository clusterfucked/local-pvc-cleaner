package cleaner

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsAndHealthEndpoints(t *testing.T) {
	m := NewMetrics()
	m.IncDeletion("pod", false)
	m.IncDeletion("pvc", false)
	m.IncDeletion("pv", true)
	m.IncError("get_node")
	m.IncSkipped("pv", "retain_policy")
	m.SetQueueLenFunc(func() int { return 3 })

	var healthErr error
	handler := m.NewHTTPHandler(true, func(_ *http.Request) error {
		return healthErr
	})

	// Before initialization, /readyz returns 503.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before init = %d, want 503", rec.Code)
	}

	// Standby replica (initialized, not leader) passes /readyz with 200.
	m.SetInitialized(true)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "standby") {
		t.Fatalf("/readyz standby = %d %q, want 200 standby", rec.Code, rec.Body.String())
	}

	// Leader before cache sync returns 503.
	m.SetLeader(true)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz leader unsynced = %d, want 503", rec.Code)
	}

	// Leader after cache sync returns 200.
	m.SetSynced(true)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "leader") {
		t.Fatalf("/readyz leader synced = %d %q, want 200 leader", rec.Code, rec.Body.String())
	}

	// /healthz passes when watchdog returns nil, fails with 500 when watchdog fails.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz healthy = %d, want 200", rec.Code)
	}

	healthErr = errors.New("lease renewal expired")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("/healthz unhealthy = %d, want 500", rec.Code)
	}

	// /metrics returns valid Prometheus exposition format with expected values.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, line := range []string{
		`local_pvc_cleaner_deletions_total{resource="pod",dry_run="false"} 1`,
		`local_pvc_cleaner_deletions_total{resource="pvc",dry_run="false"} 1`,
		`local_pvc_cleaner_deletions_total{resource="pv",dry_run="true"} 1`,
		`local_pvc_cleaner_errors_total{operation="get_node"} 1`,
		`local_pvc_cleaner_skipped_total{resource="pv",reason="retain_policy"} 1`,
		`local_pvc_cleaner_is_leader 1`,
		`local_pvc_cleaner_queue_depth 3`,
	} {
		if !strings.Contains(body, line) {
			t.Errorf("/metrics output missing line %q\nFull output:\n%s", line, body)
		}
	}
}

func TestReadyzSingleReplicaMode(t *testing.T) {
	m := NewMetrics()
	m.SetInitialized(true)
	handler := m.NewHTTPHandler(false, nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("single replica unsynced /readyz = %d, want 503", rec.Code)
	}

	m.SetSynced(true)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "single") {
		t.Fatalf("single replica synced /readyz = %d %q, want 200 single", rec.Code, rec.Body.String())
	}
}
