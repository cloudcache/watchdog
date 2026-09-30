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
	if err == nil || !strings.Contains(err.Error(), "configure flow.lifecycle") {
		t.Fatalf("removed static lifecycle setting error=%v", err)
	}
}

func TestLoadConfigFlowLifecycleMapsToPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
flow:
  lifecycle:
    enabled: true
    bootstrap_from: "2026-09-29"
    raw_delete: true
    auto_delete: true
    require_backup_before_delete: false
    require_kafka_coverage: false
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := cfg.Flow.Lifecycle.policy()
	if err != nil {
		t.Fatal(err)
	}
	if !policy.BootstrapFrom.Equal(time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)) || policy.RawRetentionSeconds != 86400 ||
		policy.LateArrivalSeconds != 6*3600 || policy.DeleteGraceSeconds != 3600 || policy.MaxPartitionsPerRun != 1 ||
		!policy.RawDeleteEnabled || !policy.AutoDelete || policy.RequireBackupBeforeDelete || !policy.WaiveKafkaCoverage {
		t.Fatalf("flow.lifecycle policy=%+v", policy)
	}
	for name, body := range map[string]string{
		"missing bootstrap":         "flow:\n  lifecycle:\n    enabled: true\n",
		"auto without raw delete":   "flow:\n  lifecycle:\n    enabled: true\n    bootstrap_from: \"2026-09-29\"\n    auto_delete: true\n",
		"raw retention below a day": "flow:\n  lifecycle:\n    enabled: true\n    bootstrap_from: \"2026-09-29\"\n    raw_retention: \"12h\"\n",
		"grace below an hour":       "flow:\n  lifecycle:\n    enabled: true\n    bootstrap_from: \"2026-09-29\"\n    delete_grace: \"30m\"\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "validate flow lifecycle") {
			t.Fatalf("%s: err=%v", name, err)
		}
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
	if cfg.ClickHouse.OperationTimeout != 2*time.Minute || cfg.ClickHouse.BatchOperationTimeout != 15*time.Minute {
		t.Fatalf("unexpected ClickHouse operation timeouts: %+v", cfg.ClickHouse)
	}
	if cfg.ClickHouse.MaxConns != 8 || cfg.ClickHouse.MinConns != 1 || cfg.ClickHouse.BatchMaxConns != 1 {
		t.Fatalf("unexpected ClickHouse pool sizes: %+v", cfg.ClickHouse)
	}
	if cfg.Flow.Reconciliation.Enabled || cfg.Flow.Reconciliation.Interval != 5*time.Minute || cfg.Flow.Reconciliation.MaxBatches != 1000 {
		t.Fatalf("unexpected reconciliation defaults: %+v", cfg.Flow.Reconciliation)
	}
	if cfg.Flow.HotRollup.MaxThreads != 4 || cfg.Flow.HotRollup.Priority != 10 || cfg.Flow.HotRollup.MaxMemoryBytes != 6<<30 {
		t.Fatalf("unexpected hot-rollup resource guards: %+v", cfg.Flow.HotRollup)
	}
	if cfg.Flow.Query.ExecutionTimeout != 2*time.Minute || cfg.Flow.Query.SynchronousTimeout != 25*time.Second ||
		cfg.Flow.Query.SynchronousMaxRange != time.Hour || cfg.Flow.Query.PanelConcurrency != 3 ||
		cfg.Flow.Query.AsyncPollInterval != time.Second || cfg.Flow.Query.AsyncResultDir != "data/flow-query-results" {
		t.Fatalf("unexpected flow query defaults: %+v", cfg.Flow.Query)
	}
	if cfg.Billing.MaxAccountPorts != 1000 || cfg.Billing.MaxPageSize != 500 || cfg.Billing.MaxPeriodDuration != 400*24*time.Hour ||
		cfg.Billing.WorkerLease != 30*time.Second || cfg.Billing.WorkerMaxAttempts != 5 {
		t.Fatalf("unexpected billing defaults: %+v", cfg.Billing)
	}
	if cfg.SNMP.QueryMaxIntermediateRows != 250000 || cfg.SNMP.QueryMaxExecutionTime != 15*time.Second ||
		cfg.SNMP.QueryMaxRowsToRead != 50000000 || cfg.SNMP.QueryMaxBytesToRead != 4<<30 || cfg.SNMP.QueryMaxMemoryBytes != 2<<30 {
		t.Fatalf("unexpected SNMP query defaults: %+v", cfg.SNMP)
	}
}

func TestLoadConfigClickHouseOperationTimeouts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
clickhouse:
  operation_timeout: 90s
  batch_operation_timeout: 12m
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClickHouse.OperationTimeout != 90*time.Second || cfg.ClickHouse.BatchOperationTimeout != 12*time.Minute {
		t.Fatalf("unexpected ClickHouse operation timeouts: %+v", cfg.ClickHouse)
	}
}

func TestLoadConfigRejectsInvalidClickHouseOperationTimeouts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte("clickhouse:\n  batch_operation_timeout: 0s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "validate ClickHouse") {
		t.Fatalf("expected ClickHouse timeout validation failure, got %v", err)
	}
}

func TestLoadConfigFlowQueryRuntimeLimits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
flow:
  query:
    execution_timeout: 3m
    synchronous_timeout: 20s
    synchronous_max_range: 30m
    panel_concurrency: 2
    async_poll_interval: 750ms
    async_worker_concurrency: 4
    async_worker_poll_interval: 250ms
    async_worker_lease: 45s
    async_worker_max_attempts: 4
    async_worker_retry_base: 3s
    async_result_dir: data/custom-flow-query-results
    async_result_retention: 48h
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Flow.Query.ExecutionTimeout != 3*time.Minute || cfg.Flow.Query.SynchronousTimeout != 20*time.Second ||
		cfg.Flow.Query.SynchronousMaxRange != 30*time.Minute || cfg.Flow.Query.PanelConcurrency != 2 ||
		cfg.Flow.Query.AsyncWorkerConcurrency != 4 ||
		cfg.Flow.Query.AsyncPollInterval != 750*time.Millisecond || cfg.Flow.Query.AsyncWorkerLease != 45*time.Second ||
		cfg.Flow.Query.AsyncWorkerMaxAttempts != 4 || cfg.Flow.Query.AsyncResultRetention != 48*time.Hour {
		t.Fatalf("unexpected flow query config: %+v", cfg.Flow.Query)
	}
}

func TestLoadConfigRejectsInvalidFlowQueryRuntimeLimits(t *testing.T) {
	for name, body := range map[string]string{
		"zero timeout":       "flow:\n  query:\n    execution_timeout: 0s\n",
		"zero concurrency":   "flow:\n  query:\n    panel_concurrency: 0\n",
		"zero async workers": "flow:\n  query:\n    async_worker_concurrency: 0\n",
		"short worker lease": "flow:\n  query:\n    async_worker_lease: 2s\n",
		"empty result dir":   "flow:\n  query:\n    async_result_dir: '   '\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watchdog.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "validate flow query") {
				t.Fatalf("expected flow query validation failure, got %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsUnsafeHotRollupResourceLimits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte("flow:\n  hot_rollup:\n    enabled: true\n    max_threads: 257\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "validate flow hot rollup") {
		t.Fatalf("expected hot-rollup resource validation failure, got %v", err)
	}
}

func TestLoadConfigSNMPQueryBudgets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
snmp:
  query_max_intermediate_rows: 5000
  query_max_execution_time: 30s
  query_max_rows_to_read: 1000000
  query_max_bytes_to_read: 268435456
  query_max_memory_bytes: 134217728
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SNMP.QueryMaxIntermediateRows != 5000 || cfg.SNMP.QueryMaxExecutionTime != 30*time.Second ||
		cfg.SNMP.QueryMaxRowsToRead != 1000000 || cfg.SNMP.QueryMaxBytesToRead != 268435456 || cfg.SNMP.QueryMaxMemoryBytes != 134217728 {
		t.Fatalf("unexpected SNMP query config: %+v", cfg.SNMP)
	}
}

func TestLoadConfigRejectsUnsafeSNMPQueryBudgets(t *testing.T) {
	for name, body := range map[string]string{
		"zero rows":         "snmp:\n  query_max_intermediate_rows: 0\n",
		"execution ceiling": "snmp:\n  query_max_execution_time: 121s\n",
		"memory ceiling":    "snmp:\n  query_max_memory_bytes: 8589934593\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "watchdog.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "validate SNMP") {
				t.Fatalf("expected SNMP validation failure, got %v", err)
			}
		})
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
