package cleaner

import (
	"flag"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigEnvFallbackAndValidation(t *testing.T) {
	t.Setenv("POD_NAME", "")
	t.Setenv("POD_NAMESPACE", "")

	cfg := DefaultConfig()
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error when POD_NAME and POD_NAMESPACE are empty with leader election enabled")
	}
	if !strings.Contains(err.Error(), "POD_NAME") {
		t.Errorf("expected error to mention POD_NAME, got: %v", err)
	}
	if !strings.Contains(err.Error(), "POD_NAMESPACE") {
		t.Errorf("expected error to mention POD_NAMESPACE, got: %v", err)
	}

	// Disabling leader election should allow empty POD_NAME / POD_NAMESPACE.
	cfg.LeaderElection = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with --leader-election=false, got: %v", err)
	}

	// Setting POD_NAME and POD_NAMESPACE via env should populate DefaultConfig.
	t.Setenv("POD_NAME", "cleaner-0")
	t.Setenv("POD_NAMESPACE", "kube-system")
	cfgEnv := DefaultConfig()
	if cfgEnv.PodName != "cleaner-0" || cfgEnv.PodNamespace != "kube-system" {
		t.Fatalf("unexpected env fallback values: pod=%q ns=%q", cfgEnv.PodName, cfgEnv.PodNamespace)
	}
	if err := cfgEnv.Validate(); err != nil {
		t.Fatalf("expected valid config with env vars set, got: %v", err)
	}
}

func TestConfigRegisterFlags(t *testing.T) {
	t.Setenv("POD_NAME", "env-pod")
	t.Setenv("POD_NAMESPACE", "env-ns")

	cfg := DefaultConfig()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlags(fs)

	args := []string{
		"--leader-election=false",
		"--lease-name=custom-lease",
		"--workers=4",
		"--resync-period=5m",
		"--node-missing-grace-period=30s",
		"--dry-run=true",
		"--provisioners=rancher.io/local-path, custom.io/local ",
		"--include-local-pvs=false",
		"--storage-classes=local-path,fast-local",
		"--hostname-topology-keys=kubernetes.io/hostname",
		"--delete-retained-pvs=true",
		"--skip-unmanaged-pods=true",
		"--include-namespaces=app-a,app-b",
		"--exclude-namespaces=kube-system",
		"--ignore-annotation=example.com/keep",
		"--kube-qps=50",
		"--kube-burst=100",
		"--log-level=debug",
		"--log-format=json",
		"--metrics-bind-address=:9090",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if cfg.LeaderElection {
		t.Error("expected LeaderElection=false")
	}
	if cfg.LeaseName != "custom-lease" {
		t.Errorf("LeaseName = %q, want custom-lease", cfg.LeaseName)
	}
	if cfg.Workers != 4 {
		t.Errorf("Workers = %d, want 4", cfg.Workers)
	}
	if cfg.ResyncPeriod != 5*time.Minute {
		t.Errorf("ResyncPeriod = %s, want 5m", cfg.ResyncPeriod)
	}
	if cfg.NodeMissingGracePeriod != 30*time.Second {
		t.Errorf("NodeMissingGracePeriod = %s, want 30s", cfg.NodeMissingGracePeriod)
	}
	if !cfg.DryRun || !cfg.DeleteRetainedPVs || !cfg.SkipUnmanagedPods || cfg.IncludeLocalPVs {
		t.Errorf("unexpected boolean flags: dryRun=%v deleteRetained=%v skipUnmanaged=%v includeLocal=%v",
			cfg.DryRun, cfg.DeleteRetainedPVs, cfg.SkipUnmanagedPods, cfg.IncludeLocalPVs)
	}
	if len(cfg.Provisioners) != 2 || cfg.Provisioners[0] != "rancher.io/local-path" || cfg.Provisioners[1] != "custom.io/local" {
		t.Errorf("unexpected Provisioners: %#v", cfg.Provisioners)
	}
	if len(cfg.StorageClasses) != 2 || cfg.StorageClasses[0] != "local-path" || cfg.StorageClasses[1] != "fast-local" {
		t.Errorf("unexpected StorageClasses: %#v", cfg.StorageClasses)
	}
	if cfg.LogLevel != "debug" || cfg.LogFormat != "json" || cfg.MetricsBindAddress != ":9090" {
		t.Errorf("unexpected observability config: level=%q format=%q addr=%q", cfg.LogLevel, cfg.LogFormat, cfg.MetricsBindAddress)
	}
}

func TestConfigValidationBounds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LeaderElection = false
	cfg.Workers = 0
	cfg.KubeQPS = -1
	cfg.KubeBurst = 0
	cfg.LogLevel = "verbose"
	cfg.LogFormat = "yaml"
	cfg.NodeMissingGracePeriod = -time.Second

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation errors")
	}
	for _, want := range []string{"--workers", "--kube-qps", "--kube-burst", "--log-level", "--log-format", "--node-missing-grace-period"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected error to contain %q, got: %v", want, err)
		}
	}
}
