package watchdog

import (
	"os"
	"path/filepath"
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
