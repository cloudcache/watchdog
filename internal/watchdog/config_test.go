package watchdog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadBackendConfigFromEnvRequiresMySQLDSN(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "")
	_, err := LoadBackendConfigFromEnv()
	if err == nil {
		t.Fatal("expected missing DSN error")
	}
}

func TestLoadWatchdogConfigDoesNotRequireMySQLDSN(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "")
	cfg, err := LoadWatchdogConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.VictoriaMetrics.BaseURL != defaultVictoriaMetricsURL {
		t.Fatalf("VictoriaMetrics URL = %s", cfg.VictoriaMetrics.BaseURL)
	}
}

func TestLoadBackendConfigFromEnvUsesDefaults(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/watchdog")
	cfg, err := LoadBackendConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.MaxOpenConns != defaultMySQLMaxOpenConns {
		t.Fatalf("max open conns = %d", cfg.MySQL.MaxOpenConns)
	}
	if cfg.MySQL.MaxIdleConns != defaultMySQLMaxIdleConns {
		t.Fatalf("max idle conns = %d", cfg.MySQL.MaxIdleConns)
	}
	if cfg.MySQL.ConnMaxLifetime != defaultMySQLConnMaxLifetime {
		t.Fatalf("conn max lifetime = %s", cfg.MySQL.ConnMaxLifetime)
	}
	if cfg.VictoriaMetrics.BaseURL != defaultVictoriaMetricsURL {
		t.Fatalf("VictoriaMetrics URL = %s", cfg.VictoriaMetrics.BaseURL)
	}
	if cfg.Export.Dir != defaultExportDir || cfg.Export.WorkerInterval != defaultExportWorkerInterval || cfg.Export.WorkerBatch != defaultExportWorkerBatch || cfg.Export.Metric != MetricSNMPIfInBps {
		t.Fatalf("export config = %#v", cfg.Export)
	}
	if cfg.SNMPCollector.Interval != defaultSNMPCollectorInterval {
		t.Fatalf("snmp collector config = %#v", cfg.SNMPCollector)
	}
	if cfg.FlowCleanup.Enabled || cfg.FlowCleanup.LeaseDuration != defaultFlowCleanupLease || cfg.FlowCleanup.Kafka.Topic == "" {
		t.Fatalf("flow state-cleanup defaults = %#v", cfg.FlowCleanup)
	}
	if cfg.CollectorPrincipalProvider.Enabled || cfg.CollectorPrincipalProvider.RequestTimeout != defaultPrincipalRequestTimeout || cfg.CollectorPrincipalProvider.FailureThreshold != defaultPrincipalFailureLimit || cfg.CollectorPrincipalProvider.CircuitOpenInterval != defaultPrincipalCircuitOpen || cfg.CollectorPrincipalProvider.MinOperationRetention != defaultPrincipalOperationRetention {
		t.Fatalf("collector principal provider defaults = %#v", cfg.CollectorPrincipalProvider)
	}
}

func TestLoadBackendConfigEnablesRemoteCollectorPrincipalProvider(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/watchdog")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_ENABLED", "true")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_NAME", " enterprise-kafka ")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_BASE_URL", " https://provider.example/ ")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_POLICY_REVISION", " flow-runtime-v7 ")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_REQUEST_TIMEOUT", "7s")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_FAILURE_THRESHOLD", "3")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_CIRCUIT_OPEN_INTERVAL", "45s")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_MIN_OPERATION_RETENTION", "2160h")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_TLS_CERT_FILE", "/etc/watchdog/provider/client.crt")
	t.Setenv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_TLS_KEY_FILE", "/etc/watchdog/provider/client.key")
	cfg, err := LoadBackendConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	provider := cfg.CollectorPrincipalProvider
	if !provider.Enabled || provider.Name != "enterprise-kafka" || provider.BaseURL != "https://provider.example" || provider.PolicyRevision != "flow-runtime-v7" || provider.RequestTimeout != 7*time.Second || provider.FailureThreshold != 3 || provider.CircuitOpenInterval != 45*time.Second || provider.MinOperationRetention != 90*24*time.Hour {
		t.Fatalf("collector principal provider config = %#v", provider)
	}
}

func TestCollectorPrincipalProviderConfigRejectsUnsafeTransportAndSharedIdentity(t *testing.T) {
	cfg := defaultBackendConfig()
	cfg.CollectorPrincipalProvider = RemoteCollectorPrincipalProviderConfig{
		Enabled: true, Name: "enterprise-kafka", BaseURL: "http://provider.example",
		PolicyRevision: "flow-runtime-v7", RequestTimeout: time.Second,
		FailureThreshold: 2, CircuitOpenInterval: time.Minute,
		MinOperationRetention: 30 * 24 * time.Hour,
		TLSCertFile:           "/etc/watchdog/provider/client.crt", TLSKeyFile: "/etc/watchdog/provider/client.key",
	}
	if err := validateWatchdogConfig(cfg, false); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("plaintext provider error=%v", err)
	}
	cfg.CollectorPrincipalProvider.BaseURL = "https://provider.example"
	cfg.FlowCollect.Kafka.TLSCertFile = cfg.CollectorPrincipalProvider.TLSCertFile
	cfg.FlowCollect.Kafka.TLSKeyFile = "/etc/watchdog/kafka/flow-collect.key"
	if err := validateWatchdogConfig(cfg, false); err == nil || !strings.Contains(err.Error(), "dedicated") {
		t.Fatalf("shared provider identity error=%v", err)
	}
}

func TestLoadBackendConfigEnablesDedicatedFlowStateCleanupWorker(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "user:pass@tcp(127.0.0.1:3306)/watchdog")
	t.Setenv("WATCHDOG_FLOW_STATE_CLEANUP_ENABLED", "true")
	t.Setenv("WATCHDOG_FLOW_STATE_CLEANUP_WORKER_ID", "cleanup-worker-a")
	t.Setenv("WATCHDOG_FLOW_STATE_CLEANUP_KAFKA_BROKERS", "127.0.0.1:9092, 127.0.0.1:9092")
	t.Setenv("WATCHDOG_FLOW_STATE_CLEANUP_MAX_ATTEMPTS", "9")
	cfg, err := LoadBackendConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.FlowCleanup.Enabled || cfg.FlowCleanup.WorkerID != "cleanup-worker-a" || len(cfg.FlowCleanup.Kafka.Brokers) != 1 || cfg.FlowCleanup.MaxAttempts != 9 {
		t.Fatalf("flow state-cleanup config = %#v", cfg.FlowCleanup)
	}
	kafka := cfg.FlowCleanup.Kafka.FlowCollectKafkaConfig()
	if kafka.CollectStateTopic != cfg.FlowCleanup.Kafka.Topic || kafka.Acks != "all" || kafka.Compression != "none" {
		t.Fatalf("flow state-cleanup Kafka adapter = %#v", kafka)
	}
}

func TestFlowStateCleanupConfigRejectsUnsafeKafkaIdentity(t *testing.T) {
	cfg := defaultBackendConfig()
	cfg.FlowCleanup.Enabled = true
	cfg.FlowCleanup.WorkerID = "cleanup-worker-a"
	cfg.FlowCleanup.Kafka.Brokers = []string{"kafka.example:9093"}
	if err := validateWatchdogConfig(cfg, false); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("remote plaintext Kafka error = %v", err)
	}

	cfg.FlowCleanup.Kafka.TLS = true
	if err := validateWatchdogConfig(cfg, false); err == nil || !strings.Contains(err.Error(), "dedicated mTLS") {
		t.Fatalf("missing cleanup mTLS error = %v", err)
	}
	cfg.FlowCleanup.Kafka.TLSCertFile = "/etc/watchdog/shared.crt"
	cfg.FlowCleanup.Kafka.TLSKeyFile = "/etc/watchdog/cleanup.key"
	cfg.FlowCollect.Kafka.TLSCertFile = cfg.FlowCleanup.Kafka.TLSCertFile
	cfg.FlowCollect.Kafka.TLSKeyFile = "/etc/watchdog/flow-collect.key"
	if err := validateWatchdogConfig(cfg, false); err == nil || !strings.Contains(err.Error(), "must not reuse") {
		t.Fatalf("reused data-plane identity error = %v", err)
	}
	cfg.FlowCleanup.Kafka.TLSCertFile = "/etc/watchdog/cleanup.crt"
	cfg.FlowCleanup.Kafka.TLSKeyFile = cfg.FlowCollect.Kafka.TLSKeyFile
	if err := validateWatchdogConfig(cfg, false); err == nil || !strings.Contains(err.Error(), "must not reuse") {
		t.Fatalf("reused data-plane key error = %v", err)
	}
}

func TestLoadBackendConfigFromEnvOverridesValues(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "dsn")
	t.Setenv("WATCHDOG_MYSQL_MAX_OPEN_CONNS", "50")
	t.Setenv("WATCHDOG_MYSQL_MAX_IDLE_CONNS", "10")
	t.Setenv("WATCHDOG_MYSQL_CONN_MAX_LIFETIME", "45m")
	t.Setenv("WATCHDOG_VICTORIAMETRICS_URL", "http://victoria:8428")
	t.Setenv("WATCHDOG_EXPORT_DIR", "/var/lib/watchdog/exports")
	t.Setenv("WATCHDOG_EXPORT_WORKER_INTERVAL", "15s")
	t.Setenv("WATCHDOG_EXPORT_WORKER_BATCH", "25")
	t.Setenv("WATCHDOG_EXPORT_METRIC", MetricSNMPIfOutBps)
	t.Setenv("WATCHDOG_SNMP_COLLECTOR_INTERVAL", "20s")
	t.Setenv("WATCHDOG_SNMP_MIB_DIRS", "/opt/librenms/mibs,/opt/vendor-mibs")
	t.Setenv("WATCHDOG_SNMP_MIBS", "ALL")
	t.Setenv("WATCHDOG_AGENT_HUB_URL", "http://hub:8091")
	t.Setenv("WATCHDOG_AGENT_ID", "agent-a")
	t.Setenv("WATCHDOG_AGENT_TOKEN", "token-a")
	t.Setenv("WATCHDOG_AGENT_INTERVAL", "45s")
	cfg, err := LoadBackendConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.MaxOpenConns != 50 {
		t.Fatalf("max open conns = %d", cfg.MySQL.MaxOpenConns)
	}
	if cfg.MySQL.MaxIdleConns != 10 {
		t.Fatalf("max idle conns = %d", cfg.MySQL.MaxIdleConns)
	}
	if cfg.MySQL.ConnMaxLifetime != 45*time.Minute {
		t.Fatalf("conn max lifetime = %s", cfg.MySQL.ConnMaxLifetime)
	}
	if cfg.VictoriaMetrics.BaseURL != "http://victoria:8428" {
		t.Fatalf("VictoriaMetrics URL = %s", cfg.VictoriaMetrics.BaseURL)
	}
	if cfg.Export.Dir != "/var/lib/watchdog/exports" || cfg.Export.WorkerInterval != 15*time.Second || cfg.Export.WorkerBatch != 25 || cfg.Export.Metric != MetricSNMPIfOutBps {
		t.Fatalf("export config = %#v", cfg.Export)
	}
	if cfg.SNMPCollector.Interval != 20*time.Second {
		t.Fatalf("snmp collector config = %#v", cfg.SNMPCollector)
	}
	if len(cfg.SNMP.MIBDirs) != 2 || cfg.SNMP.MIBLoad != "ALL" {
		t.Fatalf("snmp config = %#v", cfg.SNMP)
	}
	if cfg.Agent.HubURL != "http://hub:8091" || cfg.Agent.AgentID != "agent-a" || cfg.Agent.Token != "token-a" || cfg.Agent.Interval != 45*time.Second {
		t.Fatalf("agent config = %#v", cfg.Agent)
	}
}

func TestLoadBackendConfigReadsYAMLAndAllowsEnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	err := os.WriteFile(path, []byte(`
mysql:
  dsn: "file-dsn"
  max_open_conns: 11
  max_idle_conns: 4
  conn_max_lifetime: 20m
victoriametrics:
  base_url: "http://file-vm:8428"
export:
  dir: "/tmp/watchdog-exports"
  worker_interval: 12s
  worker_batch: 9
  metric: "watchdog_snmp_if_out_bps"
snmp_collector:
  interval: 2m
snmp:
  mib_dirs:
    - "/opt/librenms/mibs"
  mib_load: "ALL"
agent:
  hub_url: "http://file-hub:8091"
  agent_id: "agent-file"
  token: "file-token"
  interval: 2m
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("WATCHDOG_MYSQL_DSN", "env-dsn")
	cfg, err := LoadBackendConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.DSN != "env-dsn" {
		t.Fatalf("dsn = %q", cfg.MySQL.DSN)
	}
	if cfg.MySQL.MaxOpenConns != 11 || cfg.MySQL.MaxIdleConns != 4 || cfg.MySQL.ConnMaxLifetime != 20*time.Minute {
		t.Fatalf("mysql config = %#v", cfg.MySQL)
	}
	if cfg.VictoriaMetrics.BaseURL != "http://file-vm:8428" {
		t.Fatalf("VictoriaMetrics URL = %s", cfg.VictoriaMetrics.BaseURL)
	}
	if cfg.Export.Dir != "/tmp/watchdog-exports" || cfg.Export.WorkerInterval != 12*time.Second || cfg.Export.WorkerBatch != 9 || cfg.Export.Metric != MetricSNMPIfOutBps {
		t.Fatalf("export config = %#v", cfg.Export)
	}
	if cfg.SNMPCollector.Interval != 2*time.Minute {
		t.Fatalf("snmp collector config = %#v", cfg.SNMPCollector)
	}
	if len(cfg.SNMP.MIBDirs) != 1 || cfg.SNMP.MIBDirs[0] != "/opt/librenms/mibs" || cfg.SNMP.MIBLoad != "ALL" {
		t.Fatalf("snmp config = %#v", cfg.SNMP)
	}
	if cfg.Agent.HubURL != "http://file-hub:8091" || cfg.Agent.AgentID != "agent-file" || cfg.Agent.Token != "file-token" || cfg.Agent.Interval != 2*time.Minute {
		t.Fatalf("agent config = %#v", cfg.Agent)
	}
}

func TestLoadWatchdogConfigRejectsUnknownYAMLFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte("victoriametrics:\n  base_urll: http://127.0.0.1:8428\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWatchdogConfig(path)
	if err == nil || !strings.Contains(err.Error(), "base_urll") {
		t.Fatalf("error = %v, want unknown field error", err)
	}
}

func TestWatchdogExampleConfigsIncludeValidFlowCollectSchema(t *testing.T) {
	for _, name := range []string{"watchdog.example.yaml", "watchdog.dev.yaml"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := LoadWatchdogConfig(filepath.Join("..", "..", "config", name))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.FlowCollect.NormalizedBatch.MaxRecords != 1024 || cfg.FlowCollect.WAL.FsyncInterval != 10*time.Millisecond {
				t.Fatalf("flow config not loaded: %+v", cfg.FlowCollect)
			}
		})
	}
}

func TestLoadWatchdogConfigRejectsMultipleYAMLDocuments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte("mysql: {}\n---\nmysql: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWatchdogConfig(path)
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("error = %v, want multiple document error", err)
	}
}

func TestLoadWatchdogConfigPreservesExplicitZeroIdleConnections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte("mysql:\n  max_idle_conns: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadWatchdogConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.MaxIdleConns != 0 {
		t.Fatalf("max idle conns = %d, want 0", cfg.MySQL.MaxIdleConns)
	}
}

func TestLoadWatchdogConfigAllowsEnvironmentToClearMIBDirectories(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte("snmp:\n  mib_dirs: [/opt/mibs]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WATCHDOG_SNMP_MIB_DIRS", "")
	cfg, err := LoadWatchdogConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.SNMP.MIBDirs) != 0 {
		t.Fatalf("mib dirs = %#v, want empty", cfg.SNMP.MIBDirs)
	}
}

func TestLoadWatchdogConfigRejectsInvalidEnvironmentOverride(t *testing.T) {
	t.Setenv("WATCHDOG_EXPORT_WORKER_BATCH", "many")
	_, err := LoadWatchdogConfig("")
	if err == nil || !strings.Contains(err.Error(), "WATCHDOG_EXPORT_WORKER_BATCH") {
		t.Fatalf("error = %v, want named environment error", err)
	}
}

func TestLoadWatchdogConfigRejectsRemovedVictoriaLogsEnvironment(t *testing.T) {
	for _, key := range []string{"WATCHDOG_VICTORIALOGS_URL", "WATCHDOG_SFLOW_VLOGS_URL"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "http://127.0.0.1:9428")
			_, err := LoadWatchdogConfig("")
			if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "removed") {
				t.Fatalf("error = %v, want explicit removed-variable error", err)
			}
		})
	}
}

func TestLoadWatchdogConfigRejectsRemovedVictoriaLogsYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte("victorialogs:\n  base_url: http://127.0.0.1:9428\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWatchdogConfig(path)
	if err == nil || !strings.Contains(err.Error(), "victorialogs") {
		t.Fatalf("error = %v, want removed YAML field error", err)
	}
}

func TestLoadWatchdogConfigNormalizesValuesAndAppliesAllRuntimeIntervals(t *testing.T) {
	t.Setenv("WATCHDOG_SFLOW_AGG_INTERVAL", "15s")
	t.Setenv("WATCHDOG_SFLOW_PREFIX_SYNC_INTERVAL", "45s")
	t.Setenv("WATCHDOG_AGGREGATE_GRAPH_ROLLUP_INTERVAL", "2m")
	t.Setenv("WATCHDOG_SNMP_COLLECTOR_TENANT_ID", " tenant-a ")
	t.Setenv("WATCHDOG_SNMP_COLLECTOR_POLL_LIMIT", "42")
	t.Setenv("WATCHDOG_SNMP_DISCOVERY_INTERVAL", "20s")
	t.Setenv("WATCHDOG_SNMP_DISCOVERY_BATCH", "7")
	t.Setenv("WATCHDOG_AGENT_HUB_URL", " https://watchdog.example.com/ ")
	t.Setenv("WATCHDOG_AGENT_ID", " agent-a ")
	t.Setenv("WATCHDOG_SNMP_MIB_DIRS", " /opt/mibs , /opt/mibs , /opt/vendor/../vendor ")
	cfg, err := LoadWatchdogConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SFlowCollector.AggInterval != 15*time.Second || cfg.SFlowCollector.PrefixSyncInterval != 45*time.Second {
		t.Fatalf("sflow intervals = %#v", cfg.SFlowCollector)
	}
	if cfg.AggregateGraph.RollupInterval != 2*time.Minute {
		t.Fatalf("aggregate graph config = %#v", cfg.AggregateGraph)
	}
	if cfg.SNMPCollector.TenantID != "tenant-a" || cfg.SNMPCollector.PollLimit != 42 || cfg.SNMPCollector.DiscoveryInterval != 20*time.Second || cfg.SNMPCollector.DiscoveryBatch != 7 {
		t.Fatalf("snmp collector config = %#v", cfg.SNMPCollector)
	}
	if cfg.Agent.HubURL != "https://watchdog.example.com" || cfg.Agent.AgentID != "agent-a" {
		t.Fatalf("agent config = %#v", cfg.Agent)
	}
	if len(cfg.SNMP.MIBDirs) != 2 || cfg.SNMP.MIBDirs[0] != "/opt/mibs" || cfg.SNMP.MIBDirs[1] != "/opt/vendor" {
		t.Fatalf("mib dirs = %#v", cfg.SNMP.MIBDirs)
	}
}

func TestLoadWatchdogConfigRejectsInconsistentConnectionPool(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_MAX_OPEN_CONNS", "4")
	t.Setenv("WATCHDOG_MYSQL_MAX_IDLE_CONNS", "5")
	_, err := LoadWatchdogConfig("")
	if err == nil || !strings.Contains(err.Error(), "max_idle_conns") {
		t.Fatalf("error = %v, want pool consistency error", err)
	}
}

func TestNormalizeAndValidateAgentClientConfig(t *testing.T) {
	cfg, err := NormalizeAndValidateAgentClientConfig(AgentClientConfig{
		HubURL:   " https://watchdog.example.com/ ",
		AgentID:  " agent-a ",
		Token:    "secret",
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HubURL != "https://watchdog.example.com" || cfg.AgentID != "agent-a" {
		t.Fatalf("agent config = %#v", cfg)
	}
	_, err = NormalizeAndValidateAgentClientConfig(AgentClientConfig{HubURL: "watchdog.example.com", AgentID: "agent-a", Token: "secret", Interval: time.Minute})
	if err == nil {
		t.Fatal("expected invalid hub URL error")
	}
}

func TestNormalizeAndValidateSNMPTrapAgentConfig(t *testing.T) {
	cfg, err := NormalizeAndValidateSNMPTrapAgentConfig(SNMPTrapAgentConfig{
		APIURL: " http://127.0.0.1:8091/ ",
		Token:  "secret",
		Listen: " :1162 ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIURL != "http://127.0.0.1:8091" || cfg.Listen != ":1162" {
		t.Fatalf("trap agent config = %#v", cfg)
	}
}

func TestWatchdogExampleConfigsMatchSchemaAndValidation(t *testing.T) {
	for _, name := range []string{"watchdog.example.yaml", "watchdog.dev.yaml"} {
		t.Run(name, func(t *testing.T) {
			cfg := defaultBackendConfig()
			path := filepath.Join("..", "..", "config", name)
			if err := loadBackendConfigFile(path, &cfg); err != nil {
				t.Fatal(err)
			}
			normalizeBackendConfig(&cfg)
			if err := validateWatchdogConfig(cfg, true); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSNMPCollectorConfig(cfg.SNMPCollector); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSFlowCollectorConfig(cfg.SFlowCollector); err != nil {
				t.Fatal(err)
			}
			if _, err := NormalizeAndValidateAgentClientConfig(cfg.Agent); err != nil {
				t.Fatal(err)
			}
			if _, err := NormalizeAndValidateSNMPTrapAgentConfig(cfg.SNMPTrapAgent); err != nil {
				t.Fatal(err)
			}
		})
	}
}
