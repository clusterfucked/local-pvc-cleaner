package cleaner

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	DefaultLeaseName        = "local-pvc-cleaner"
	DefaultProvisioner      = "rancher.io/local-path"
	DefaultIgnoreAnnotation = "local-pvc-cleaner.io/ignore"
)

// DefaultHostnameTopologyKeys are the standard Kubernetes node topology label keys
// used to identify a single node in PV NodeAffinity rules.
var DefaultHostnameTopologyKeys = []string{
	"kubernetes.io/hostname",
	"topology.kubernetes.io/hostname",
}

// Config holds all runtime configuration for local-pvc-cleaner.
type Config struct {
	Kubeconfig string

	PodName        string
	PodNamespace   string
	LeaderElection bool
	LeaseName      string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration

	Workers                int
	ResyncPeriod           time.Duration
	APITimeout             time.Duration
	NodeMissingGracePeriod time.Duration
	DryRun                 bool

	Provisioners         []string
	IncludeLocalPVs      bool
	StorageClasses       []string
	HostnameTopologyKeys []string
	DeleteRetainedPVs    bool
	SkipUnmanagedPods    bool

	IncludeNamespaces []string
	ExcludeNamespaces []string
	IgnoreAnnotation  string

	KubeQPS   float64
	KubeBurst int

	LogLevel           string
	LogFormat          string
	MetricsBindAddress string
}

// DefaultConfig returns a Config populated with production defaults and
// environment fallbacks (POD_NAME, POD_NAMESPACE).
func DefaultConfig() Config {
	return Config{
		PodName:                os.Getenv("POD_NAME"),
		PodNamespace:           os.Getenv("POD_NAMESPACE"),
		LeaderElection:         true,
		LeaseName:              DefaultLeaseName,
		LeaseDuration:          15 * time.Second,
		RenewDeadline:          10 * time.Second,
		RetryPeriod:            2 * time.Second,
		Workers:                2,
		ResyncPeriod:           10 * time.Minute,
		APITimeout:             30 * time.Second,
		NodeMissingGracePeriod: 1 * time.Minute,
		DryRun:                 false,
		Provisioners:           []string{DefaultProvisioner},
		IncludeLocalPVs:        true,
		StorageClasses:         nil,
		HostnameTopologyKeys:   append([]string(nil), DefaultHostnameTopologyKeys...),
		DeleteRetainedPVs:      false,
		SkipUnmanagedPods:      false,
		IncludeNamespaces:      nil,
		ExcludeNamespaces:      nil,
		IgnoreAnnotation:       DefaultIgnoreAnnotation,
		KubeQPS:                20,
		KubeBurst:              40,
		LogLevel:               "info",
		LogFormat:              "text",
		MetricsBindAddress:     ":8080",
	}
}

// RegisterFlags binds CLI flags on fs to fields in c.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Kubeconfig, "kubeconfig", c.Kubeconfig, "Path to a kubeconfig file (falls back to in-cluster config, KUBECONFIG env, or ~/.kube/config)")

	fs.StringVar(&c.PodName, "pod-name", c.PodName, "Pod identity for leader election (env: POD_NAME)")
	fs.StringVar(&c.PodNamespace, "pod-namespace", c.PodNamespace, "Namespace for the leader election Lease (env: POD_NAMESPACE)")
	fs.BoolVar(&c.LeaderElection, "leader-election", c.LeaderElection, "Enable leader election for high-availability deployments")
	fs.StringVar(&c.LeaseName, "lease-name", c.LeaseName, "Name of the Lease resource used for leader election")
	fs.DurationVar(&c.LeaseDuration, "lease-duration", c.LeaseDuration, "Duration that non-leader candidates wait before acquiring leadership")
	fs.DurationVar(&c.RenewDeadline, "renew-deadline", c.RenewDeadline, "Interval that the acting leader retries refreshing leadership before giving up")
	fs.DurationVar(&c.RetryPeriod, "retry-period", c.RetryPeriod, "Duration between leader election retry attempts")

	fs.IntVar(&c.Workers, "workers", c.Workers, "Number of concurrent reconciliation workers")
	fs.DurationVar(&c.ResyncPeriod, "resync-period", c.ResyncPeriod, "Periodic informer resync and sweep interval (0 disables periodic resync)")
	fs.DurationVar(&c.APITimeout, "api-timeout", c.APITimeout, "Per-request timeout for Kubernetes API calls")
	fs.DurationVar(&c.NodeMissingGracePeriod, "node-missing-grace-period", c.NodeMissingGracePeriod, "Minimum time a node must remain missing before deleting its volumes (0 disables wait)")
	fs.BoolVar(&c.DryRun, "dry-run", c.DryRun, "Log planned deletions without modifying any Kubernetes resources or emitting events")

	fs.Var(&csvFlag{target: &c.Provisioners}, "provisioners", "Comma-separated list of storage provisioners to manage (empty disables provisioner matching)")
	fs.BoolVar(&c.IncludeLocalPVs, "include-local-pvs", c.IncludeLocalPVs, "Also manage native Kubernetes local PersistentVolumes (pv.Spec.Local != nil)")
	fs.Var(&csvFlag{target: &c.StorageClasses}, "storage-classes", "Comma-separated allowlist of StorageClass names to manage (empty allows all)")
	fs.Var(&csvFlag{target: &c.HostnameTopologyKeys}, "hostname-topology-keys", "Comma-separated NodeAffinity topology keys used to determine a PV's pinned node")
	fs.BoolVar(&c.DeleteRetainedPVs, "delete-retained-pvs", c.DeleteRetainedPVs, "Also delete PersistentVolumes with reclaim policy Retain")
	fs.BoolVar(&c.SkipUnmanagedPods, "skip-unmanaged-pods", c.SkipUnmanagedPods, "Skip cleaning a PVC if any referencing Pod has no OwnerReferences")

	fs.Var(&csvFlag{target: &c.IncludeNamespaces}, "include-namespaces", "Comma-separated allowlist of namespaces to manage (empty allows all)")
	fs.Var(&csvFlag{target: &c.ExcludeNamespaces}, "exclude-namespaces", "Comma-separated denylist of namespaces to ignore")
	fs.StringVar(&c.IgnoreAnnotation, "ignore-annotation", c.IgnoreAnnotation, "Annotation key that opts a PVC, PV, or Pod out of cleanup when set to \"true\" (empty disables)")

	fs.Float64Var(&c.KubeQPS, "kube-qps", c.KubeQPS, "Maximum QPS to the Kubernetes API server")
	fs.IntVar(&c.KubeBurst, "kube-burst", c.KubeBurst, "Maximum burst for throttle to the Kubernetes API server")

	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "Log verbosity level: debug, info, warn, error")
	fs.StringVar(&c.LogFormat, "log-format", c.LogFormat, "Log output format: text, json")
	fs.StringVar(&c.MetricsBindAddress, "metrics-bind-address", c.MetricsBindAddress, "Address for /healthz, /readyz, and /metrics HTTP server (empty disables)")
}

// Validate checks that the configuration is internally consistent and complete.
func (c Config) Validate() error {
	var errs []error

	if c.LeaderElection {
		if strings.TrimSpace(c.PodName) == "" {
			errs = append(errs, errors.New("POD_NAME env var or --pod-name flag is required when --leader-election is enabled (set --leader-election=false for single-replica/local runs)"))
		}
		if strings.TrimSpace(c.PodNamespace) == "" {
			errs = append(errs, errors.New("POD_NAMESPACE env var or --pod-namespace flag is required when --leader-election is enabled (set --leader-election=false for single-replica/local runs)"))
		}
		if strings.TrimSpace(c.LeaseName) == "" {
			errs = append(errs, errors.New("--lease-name must not be empty when --leader-election is enabled"))
		}
		if c.LeaseDuration <= 0 {
			errs = append(errs, errors.New("--lease-duration must be positive"))
		}
		if c.RenewDeadline <= 0 {
			errs = append(errs, errors.New("--renew-deadline must be positive"))
		}
		if c.RetryPeriod <= 0 {
			errs = append(errs, errors.New("--retry-period must be positive"))
		}
		if c.LeaseDuration > 0 && c.RenewDeadline > 0 && c.LeaseDuration <= c.RenewDeadline {
			errs = append(errs, errors.New("--lease-duration must be greater than --renew-deadline"))
		}
		if c.RenewDeadline > 0 && c.RetryPeriod > 0 && c.RenewDeadline <= c.RetryPeriod {
			errs = append(errs, errors.New("--renew-deadline must be greater than --retry-period"))
		}
	}

	if c.Workers < 1 {
		errs = append(errs, fmt.Errorf("--workers must be >= 1, got %d", c.Workers))
	}
	if c.ResyncPeriod < 0 {
		errs = append(errs, fmt.Errorf("--resync-period must be >= 0, got %s", c.ResyncPeriod))
	}
	if c.APITimeout <= 0 {
		errs = append(errs, fmt.Errorf("--api-timeout must be > 0, got %s", c.APITimeout))
	}
	if c.NodeMissingGracePeriod < 0 {
		errs = append(errs, fmt.Errorf("--node-missing-grace-period must be >= 0, got %s", c.NodeMissingGracePeriod))
	}
	if c.KubeQPS <= 0 {
		errs = append(errs, fmt.Errorf("--kube-qps must be > 0, got %v", c.KubeQPS))
	}
	if c.KubeBurst <= 0 {
		errs = append(errs, fmt.Errorf("--kube-burst must be > 0, got %d", c.KubeBurst))
	}
	if _, err := ParseLogLevel(c.LogLevel); err != nil {
		errs = append(errs, err)
	}
	switch strings.ToLower(strings.TrimSpace(c.LogFormat)) {
	case "text", "json":
	default:
		errs = append(errs, fmt.Errorf("invalid --log-format %q: must be \"text\" or \"json\"", c.LogFormat))
	}

	return errors.Join(errs...)
}

// ParseLogLevel converts a string log level into a slog.Level.
func ParseLogLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid --log-level %q: must be debug, info, warn, or error", level)
	}
}

type csvFlag struct {
	target *[]string
}

func (f *csvFlag) String() string {
	if f == nil || f.target == nil {
		return ""
	}
	return strings.Join(*f.target, ",")
}

func (f *csvFlag) Set(val string) error {
	*f.target = parseCSV(val)
	return nil
}

func parseCSV(val string) []string {
	if strings.TrimSpace(val) == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func toSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	s := make(map[string]struct{}, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			s[trimmed] = struct{}{}
		}
	}
	if len(s) == 0 {
		return nil
	}
	return s
}
