package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigYAMLAndSecretOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
server:
  listen: "127.0.0.1:19091"
  origins: ["http://127.0.0.1:8090"]
mysql:
  dsn: "file-user@tcp(localhost:3306)/from_file"
admin:
  username: "operator"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WATCHDOG_MYSQL_DSN", "env-user@tcp(localhost:3306)/from_env")
	t.Setenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE", "/run/secrets/watchdog-clickhouse")
	t.Setenv("WATCHDOG_ADMIN_PASSWORD", "env-secret")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen != "127.0.0.1:19091" || cfg.Admin.Username != "operator" {
		t.Fatalf("yaml was not loaded: %+v", cfg)
	}
	if cfg.MySQL.DSN != "env-user@tcp(localhost:3306)/from_env" || cfg.Admin.Password != "env-secret" {
		t.Fatal("secret environment overrides were not applied")
	}
	if cfg.ClickHouse.PasswordFile != "/run/secrets/watchdog-clickhouse" {
		t.Fatal("ClickHouse secret-file override was not applied")
	}
}

func TestLoadConfigRejectsRemovedStaticFlowRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte("flow:\n  retention_raw_days: 365\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "publish the global Flow lifecycle policy") {
		t.Fatalf("removed static lifecycle setting error=%v", err)
	}
}

func TestLoadConfigMissingFileUsesDefaults(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "")
	t.Setenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE", "")
	t.Setenv("WATCHDOG_ADMIN_PASSWORD", "")
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen == "" || cfg.MySQL.DSN == "" || cfg.Admin.Username != "admin" {
		t.Fatalf("incomplete defaults: %+v", cfg)
	}
	if cfg.Flow.Reconciliation.Enabled || cfg.Flow.Reconciliation.Interval != 5*time.Minute || cfg.Flow.Reconciliation.MaxBatches != 1000 {
		t.Fatalf("unexpected reconciliation defaults: %+v", cfg.Flow.Reconciliation)
	}
	if cfg.Billing.MaxAccountPorts != 1000 || cfg.Billing.MaxPageSize != 500 || cfg.Billing.MaxPeriodDuration != 400*24*time.Hour ||
		cfg.Billing.WorkerLease != 30*time.Second || cfg.Billing.WorkerMaxAttempts != 5 {
		t.Fatalf("unexpected billing defaults: %+v", cfg.Billing)
	}
}

func TestLoadConfigBillingBudgets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
billing:
  max_account_ports: 250
  max_page_size: 200
  max_period_duration: 2160h
  max_export_rows: 25000
  max_publication_refs: 2000
  max_publication_bytes: 1048576
  worker_poll_interval: 500ms
  worker_lease: 15s
  worker_max_attempts: 3
  worker_retry_base: 5s
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Billing.MaxAccountPorts != 250 || cfg.Billing.MaxPageSize != 200 || cfg.Billing.MaxPeriodDuration != 90*24*time.Hour ||
		cfg.Billing.MaxExportRows != 25000 || cfg.Billing.WorkerPollInterval != 500*time.Millisecond || cfg.Billing.WorkerLease != 15*time.Second ||
		cfg.Billing.WorkerMaxAttempts != 3 || cfg.Billing.WorkerRetryBase != 5*time.Second {
		t.Fatalf("unexpected billing config: %+v", cfg.Billing)
	}
}

func TestLoadConfigRejectsInvalidBillingBudgets(t *testing.T) {
	for name, body := range map[string]string{
		"zero budget":         "billing:\n  max_export_rows: 0\n",
		"reader port ceiling": "billing:\n  max_account_ports: 1001\n",
		"reader time ceiling": "billing:\n  max_period_duration: 9601h\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watchdog.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "validate billing") {
				t.Fatalf("expected billing validation failure, got %v", err)
			}
		})
	}
}

func TestLoadConfigFlowReconciliation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
kafka:
  brokers: ["kafka:9092"]
  topic: "watchdog.flow.raw"
  consumer_group: "watchdog-flow-worker"
flow:
  reconciliation:
    enabled: true
    source_stream_id: "source-1"
    interval: 10m
    bootstrap_offsets:
      0: 42
    max_batches: 200
    max_fact_rows: 50000
    max_read_bytes: 67108864
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Flow.Reconciliation.Enabled || cfg.Flow.Reconciliation.SourceStreamID != "source-1" ||
		cfg.Flow.Reconciliation.BootstrapOffsets[0] != 42 || cfg.Flow.Reconciliation.Interval != 10*time.Minute {
		t.Fatalf("unexpected reconciliation config: %+v", cfg.Flow.Reconciliation)
	}
}

func TestLoadConfigRejectsInvalidEnabledFlowReconciliation(t *testing.T) {
	for name, body := range map[string]string{
		"missing source stream": `flow: {reconciliation: {enabled: true}}`,
		"interval too short":    `flow: {reconciliation: {enabled: true, source_stream_id: source-1, interval: 5s}}`,
		"invalid budget":        `flow: {reconciliation: {enabled: true, source_stream_id: source-1, interval: 5m, max_batches: 0}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watchdog.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "flow reconciliation") {
				t.Fatalf("expected reconciliation validation failure, got %v", err)
			}
		})
	}
}

func TestValidDatabaseName(t *testing.T) {
	for _, name := range []string{"watchdog", "watchdog_test", "watchdog$1"} {
		if !validDatabaseName(name) {
			t.Errorf("expected %q to be valid", name)
		}
	}
	for _, name := range []string{"", "watchdog-test", "bad`name", "bad name"} {
		if validDatabaseName(name) {
			t.Errorf("expected %q to be rejected", name)
		}
	}
}
