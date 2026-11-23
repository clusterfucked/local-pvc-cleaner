package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	"github.com/clusterfucked/local-pvc-cleaner/internal/cleaner"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg := cleaner.DefaultConfig()
	fs := flag.NewFlagSet("local-pvc-cleaner", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg.RegisterFlags(fs)

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid configuration: %v\n", err)
		return 1
	}

	logger, err := newLogger(cfg.LogLevel, cfg.LogFormat, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "failed to configure logger: %v\n", err)
		return 1
	}
	slog.SetDefault(logger)

	restCfg, err := buildRestConfig(cfg)
	if err != nil {
		logger.Error("failed to build kubernetes client config", "error", err)
		return 1
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		logger.Error("failed to create kubernetes clientset", "error", err)
		return 1
	}

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()

	runCtx, cancelRun := context.WithCancel(sigCtx)
	defer cancelRun()

	metrics := cleaner.NewMetrics()

	var healthzAdaptor *leaderelection.HealthzAdaptor
	var healthzCheck func(*http.Request) error
	if cfg.LeaderElection {
		healthzAdaptor = leaderelection.NewLeaderHealthzAdaptor(20 * time.Second)
		healthzCheck = healthzAdaptor.Check
	}

	var (
		fatalMu  sync.Mutex
		fatalErr error
	)
	setFatalErr := func(err error) {
		if err == nil {
			return
		}
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
		}
		fatalMu.Unlock()
		cancelRun()
	}
	getFatalErr := func() error {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		return fatalErr
	}

	var httpServer *http.Server
	var httpWG sync.WaitGroup
	if strings.TrimSpace(cfg.MetricsBindAddress) != "" {
		listener, err := net.Listen("tcp", cfg.MetricsBindAddress)
		if err != nil {
			logger.Error("failed to bind metrics/health HTTP server", "address", cfg.MetricsBindAddress, "error", err)
			return 1
		}
		httpServer = &http.Server{
			Handler:           metrics.NewHTTPHandler(cfg.LeaderElection, healthzCheck),
			ReadHeaderTimeout: 5 * time.Second,
		}
		httpWG.Add(1)
		go func() {
			defer httpWG.Done()
			logger.Info("starting HTTP server", "address", listener.Addr().String())
			if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("HTTP server stopped unexpectedly", "error", err)
				setFatalErr(fmt.Errorf("metrics/health HTTP server failed: %w", err))
			}
		}()
	}

	shutdownHTTP := func() {
		if httpServer == nil {
			return
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("HTTP server graceful shutdown failed", "error", err)
		}
		httpWG.Wait()
	}
	defer shutdownHTTP()

	var recorder record.EventRecorder
	if !cfg.DryRun {
		broadcaster := record.NewBroadcaster()
		broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
			Interface: clientset.CoreV1().Events(""),
		})
		defer broadcaster.Shutdown()
		recorder = broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{
			Component: "local-pvc-cleaner",
		})
	}

	runController := func(ctx context.Context) error {
		ctrl, err := cleaner.NewController(cleaner.Options{
			Client:   clientset,
			Config:   cfg,
			Logger:   logger,
			Metrics:  metrics,
			Recorder: recorder,
		})
		if err != nil {
			return fmt.Errorf("create controller: %w", err)
		}
		return ctrl.Run(ctx)
	}

	metrics.SetInitialized(true)

	if !cfg.LeaderElection {
		logger.Info("starting controller without leader election", "dry_run", cfg.DryRun)
		metrics.SetLeader(true)
		if err := runController(runCtx); err != nil && sigCtx.Err() == nil {
			setFatalErr(err)
		}
		metrics.SetLeader(false)
		if err := getFatalErr(); err != nil {
			logger.Error("controller exited with error", "error", err)
			return 1
		}
		logger.Info("controller shut down cleanly")
		return 0
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: cfg.PodNamespace,
			Name:      cfg.LeaseName,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.PodName,
		},
	}

	var (
		leaderWG       sync.WaitGroup
		lostLeadership atomic.Bool
	)

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		WatchDog:        healthzAdaptor,
		Name:            cfg.LeaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leadCtx context.Context) {
				leaderWG.Add(1)
				defer leaderWG.Done()

				logger.Info("acquired leadership", "identity", cfg.PodName, "lease", cfg.LeaseName, "namespace", cfg.PodNamespace)
				metrics.SetLeader(true)
				if err := runController(leadCtx); err != nil && sigCtx.Err() == nil {
					logger.Error("controller stopped with fatal error", "error", err)
					setFatalErr(err)
				}
			},
			OnStoppedLeading: func() {
				metrics.SetLeader(false)
				metrics.SetSynced(false)
				if sigCtx.Err() != nil {
					logger.Info("released leadership due to shutdown signal", "identity", cfg.PodName)
					return
				}
				if getFatalErr() == nil {
					logger.Error("lost leadership unexpectedly", "identity", cfg.PodName)
					lostLeadership.Store(true)
					cancelRun()
				}
			},
			OnNewLeader: func(identity string) {
				if identity == cfg.PodName {
					return
				}
				logger.Info("observed new leader", "leader", identity, "identity", cfg.PodName)
			},
		},
	})
	if err != nil {
		logger.Error("failed to initialize leader elector", "error", err)
		return 1
	}

	logger.Info("starting leader election",
		"identity", cfg.PodName,
		"lease", cfg.LeaseName,
		"namespace", cfg.PodNamespace,
		"dry_run", cfg.DryRun,
	)

	le.Run(runCtx)
	leaderWG.Wait()

	if err := getFatalErr(); err != nil {
		logger.Error("exiting due to fatal error", "error", err)
		return 1
	}
	if lostLeadership.Load() {
		logger.Error("exiting due to unexpected leadership loss")
		return 1
	}
	logger.Info("controller shut down cleanly")
	return 0
}

func newLogger(levelStr, format string, w io.Writer) (*slog.Logger, error) {
	level, err := cleaner.ParseLogLevel(levelStr)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log format %q", format)
	}
}

func buildRestConfig(cfg cleaner.Config) (*rest.Config, error) {
	var (
		restCfg *rest.Config
		err     error
	)

	if strings.TrimSpace(cfg.Kubeconfig) != "" {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		loadingRules.ExplicitPath = cfg.Kubeconfig
		restCfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
	} else if strings.TrimSpace(os.Getenv(clientcmd.RecommendedConfigPathEnvVar)) != "" {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		restCfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
	} else {
		restCfg, err = rest.InClusterConfig()
		if err != nil {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			kubeCfg, kubeErr := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
				loadingRules,
				&clientcmd.ConfigOverrides{},
			).ClientConfig()
			if kubeErr != nil {
				return nil, fmt.Errorf("in-cluster config unavailable (%v) and kubeconfig fallback failed (%w)", err, kubeErr)
			}
			restCfg = kubeCfg
			err = nil
		}
	}
	if err != nil {
		return nil, err
	}

	restCfg.QPS = float32(cfg.KubeQPS)
	restCfg.Burst = cfg.KubeBurst
	return restCfg, nil
}
