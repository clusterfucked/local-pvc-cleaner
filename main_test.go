package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clusterfucked/local-pvc-cleaner/internal/cleaner"
)

func TestNewLoggerFormats(t *testing.T) {
	var textBuf bytes.Buffer
	l, err := newLogger("info", "text", &textBuf)
	if err != nil {
		t.Fatalf("newLogger(text) error: %v", err)
	}
	l.Info("hello", "node", "n1")
	if !strings.Contains(textBuf.String(), "msg=hello") || !strings.Contains(textBuf.String(), "node=n1") {
		t.Errorf("unexpected text log output: %q", textBuf.String())
	}

	var jsonBuf bytes.Buffer
	l, err = newLogger("debug", "json", &jsonBuf)
	if err != nil {
		t.Fatalf("newLogger(json) error: %v", err)
	}
	l.Debug("dbg", "pvc", "p1")
	if !strings.Contains(jsonBuf.String(), `"msg":"dbg"`) || !strings.Contains(jsonBuf.String(), `"pvc":"p1"`) {
		t.Errorf("unexpected json log output: %q", jsonBuf.String())
	}
}

func TestBuildRestConfigMultiFileKubeconfig(t *testing.T) {
	dir := t.TempDir()
	kc1 := filepath.Join(dir, "config1")
	kc2 := filepath.Join(dir, "config2")

	// First file defines the cluster & context; second file defines the user credentials.
	if err := os.WriteFile(kc1, []byte(`apiVersion: v1
kind: Config
clusters:
- name: test-cluster
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: test-ctx
  context:
    cluster: test-cluster
    user: test-user
current-context: test-ctx
`), 0o600); err != nil {
		t.Fatalf("write kc1: %v", err)
	}
	if err := os.WriteFile(kc2, []byte(`apiVersion: v1
kind: Config
users:
- name: test-user
  user:
    token: dummy-token
`), 0o600); err != nil {
		t.Fatalf("write kc2: %v", err)
	}

	t.Setenv("KUBECONFIG", kc1+string(os.PathListSeparator)+kc2)

	cfg := cleaner.DefaultConfig()
	cfg.KubeQPS = 33
	cfg.KubeBurst = 66

	restCfg, err := buildRestConfig(cfg)
	if err != nil {
		t.Fatalf("buildRestConfig() error: %v", err)
	}
	if restCfg.Host != "https://127.0.0.1:6443" {
		t.Errorf("Host = %q, want https://127.0.0.1:6443", restCfg.Host)
	}
	if restCfg.BearerToken != "dummy-token" {
		t.Errorf("BearerToken = %q, want dummy-token (merged from second KUBECONFIG file)", restCfg.BearerToken)
	}
	if restCfg.QPS != 33 || restCfg.Burst != 66 {
		t.Errorf("QPS/Burst = %v/%d, want 33/66", restCfg.QPS, restCfg.Burst)
	}
}

func TestRunCLIErrorsAndHelp(t *testing.T) {
	t.Setenv("POD_NAME", "")
	t.Setenv("POD_NAMESPACE", "")

	var stdout, stderr bytes.Buffer
	code := run([]string{}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("expected exit code 1 for missing POD_NAME/POD_NAMESPACE, got %d", code)
	}
	if !strings.Contains(stderr.String(), "POD_NAME") || !strings.Contains(stderr.String(), "POD_NAMESPACE") {
		t.Errorf("expected stderr to name POD_NAME and POD_NAMESPACE, got: %s", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected exit code 0 for --help, got %d", code)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--does-not-exist"}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected exit code 2 for unknown flag, got %d", code)
	}
}
