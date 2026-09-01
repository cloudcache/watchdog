package watchdog

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultMySQLMaxOpenConns     = 25
	defaultMySQLMaxIdleConns     = 5
	defaultMySQLConnMaxLifetime  = 30 * time.Minute
	defaultVictoriaMetricsURL    = "http://127.0.0.1:8428"
	defaultVictoriaLogsURL       = "http://127.0.0.1:9428"
	defaultExportDir             = "exports"
	defaultExportWorkerInterval  = 30 * time.Second
	defaultExportWorkerBatch     = 10
	defaultSNMPCollectorInterval = time.Minute
	defaultSFlowListen           = ":6343"
	defaultSFlowAggInterval      = time.Minute
	defaultSFlowPrefixSync       = 5 * time.Minute
)

type BackendConfig struct {
	MySQL           MySQLConfig           `yaml:"mysql"`
	VictoriaMetrics VictoriaMetricsConfig `yaml:"victoriametrics"`
	VictoriaLogs    VictoriaLogsConfig    `yaml:"victorialogs"`
	Export          ExportConfig          `yaml:"export"`
	SNMPCollector   SNMPCollectorConfig   `yaml:"snmp_collector"`
	SFlowCollector  SFlowCollectorConfig  `yaml:"sflow_collector"`
	AggregateGraph  AggregateGraphConfig  `yaml:"aggregate_graph"`
	SNMP            SNMPConfig            `yaml:"snmp"`
	Agent           AgentClientConfig     `yaml:"agent"`
}

type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

type VictoriaMetricsConfig struct {
	BaseURL string `yaml:"base_url"`
}

type VictoriaLogsConfig struct {
	BaseURL string `yaml:"base_url"`
}

type SFlowCollectorConfig struct {
	Listen     string        `yaml:"listen"`
	TenantID   ID            `yaml:"tenant_id"`
	VLogsURL   string        `yaml:"vlogs_url"`
	AggInterval time.Duration `yaml:"agg_interval"`
	PrefixSyncInterval time.Duration `yaml:"prefix_sync_interval"`
}

type ExportConfig struct {
	Dir            string        `yaml:"dir"`
	WorkerInterval time.Duration `yaml:"worker_interval"`
	WorkerBatch    int           `yaml:"worker_batch"`
	Metric         string        `yaml:"metric"`
}

type SNMPCollectorConfig struct {
	Interval time.Duration `yaml:"interval"`
}

type AggregateGraphConfig struct {
	RollupInterval time.Duration `yaml:"rollup_interval"`
}

type SNMPConfig struct {
	MIBDirs []string `yaml:"mib_dirs"`
	MIBLoad string   `yaml:"mib_load"`
}

type AgentClientConfig struct {
	HubURL   string        `yaml:"hub_url"`
	AgentID  ID            `yaml:"agent_id"`
	Token    string        `yaml:"token"`
	Interval time.Duration `yaml:"interval"`
}

func LoadWatchdogConfig(path string) (BackendConfig, error) {
	cfg := defaultBackendConfig()
	if path == "" {
		path = getEnv("WATCHDOG_CONFIG", "")
	}
	if path != "" {
		fileCfg, err := loadBackendConfigFile(path)
		if err != nil {
			return cfg, err
		}
		mergeBackendConfig(&cfg, fileCfg)
	}
	applyBackendConfigEnv(&cfg)
	return cfg, nil
}

func LoadBackendConfig(path string) (BackendConfig, error) {
	cfg, err := LoadWatchdogConfig(path)
	if err != nil {
		return cfg, err
	}
	if cfg.MySQL.DSN == "" {
		return cfg, errors.New("mysql dsn is required; set mysql.dsn in config or WATCHDOG_MYSQL_DSN")
	}
	return cfg, nil
}

func LoadBackendConfigFromEnv() (BackendConfig, error) {
	cfg := defaultBackendConfig()
	applyBackendConfigEnv(&cfg)
	if cfg.MySQL.DSN == "" {
		return cfg, errors.New("WATCHDOG_MYSQL_DSN is required")
	}
	return cfg, nil
}

func defaultBackendConfig() BackendConfig {
	return BackendConfig{
		MySQL: MySQLConfig{
			MaxOpenConns:    defaultMySQLMaxOpenConns,
			MaxIdleConns:    defaultMySQLMaxIdleConns,
			ConnMaxLifetime: defaultMySQLConnMaxLifetime,
		},
		VictoriaMetrics: VictoriaMetricsConfig{
			BaseURL: defaultVictoriaMetricsURL,
		},
		VictoriaLogs: VictoriaLogsConfig{
			BaseURL: defaultVictoriaLogsURL,
		},
		Export: ExportConfig{
			Dir:            defaultExportDir,
			WorkerInterval: defaultExportWorkerInterval,
			WorkerBatch:    defaultExportWorkerBatch,
			Metric:         MetricSNMPIfInBps,
		},
		SNMPCollector: SNMPCollectorConfig{
			Interval: defaultSNMPCollectorInterval,
		},
		SFlowCollector: SFlowCollectorConfig{
			Listen:              defaultSFlowListen,
			AggInterval:         defaultSFlowAggInterval,
			PrefixSyncInterval:  defaultSFlowPrefixSync,
		},
		AggregateGraph: AggregateGraphConfig{
			RollupInterval: defaultAggregateGraphRollupInterval,
		},
	}
}

func loadBackendConfigFile(path string) (BackendConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return BackendConfig{}, err
	}
	var cfg BackendConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return BackendConfig{}, err
	}
	return cfg, nil
}

func mergeBackendConfig(cfg *BackendConfig, override BackendConfig) {
	if override.MySQL.DSN != "" {
		cfg.MySQL.DSN = override.MySQL.DSN
	}
	if override.MySQL.MaxOpenConns > 0 {
		cfg.MySQL.MaxOpenConns = override.MySQL.MaxOpenConns
	}
	if override.MySQL.MaxIdleConns > 0 {
		cfg.MySQL.MaxIdleConns = override.MySQL.MaxIdleConns
	}
	if override.MySQL.ConnMaxLifetime > 0 {
		cfg.MySQL.ConnMaxLifetime = override.MySQL.ConnMaxLifetime
	}
	if override.VictoriaMetrics.BaseURL != "" {
		cfg.VictoriaMetrics.BaseURL = override.VictoriaMetrics.BaseURL
	}
	if override.VictoriaLogs.BaseURL != "" {
		cfg.VictoriaLogs.BaseURL = override.VictoriaLogs.BaseURL
	}
	if override.Export.Dir != "" {
		cfg.Export.Dir = override.Export.Dir
	}
	if override.Export.WorkerInterval > 0 {
		cfg.Export.WorkerInterval = override.Export.WorkerInterval
	}
	if override.Export.WorkerBatch > 0 {
		cfg.Export.WorkerBatch = override.Export.WorkerBatch
	}
	if override.Export.Metric != "" {
		cfg.Export.Metric = override.Export.Metric
	}
	if override.SNMPCollector.Interval > 0 {
		cfg.SNMPCollector.Interval = override.SNMPCollector.Interval
	}
	if override.SFlowCollector.Listen != "" {
		cfg.SFlowCollector.Listen = override.SFlowCollector.Listen
	}
	if override.SFlowCollector.TenantID != "" {
		cfg.SFlowCollector.TenantID = override.SFlowCollector.TenantID
	}
	if override.SFlowCollector.VLogsURL != "" {
		cfg.SFlowCollector.VLogsURL = override.SFlowCollector.VLogsURL
	}
	if override.SFlowCollector.AggInterval > 0 {
		cfg.SFlowCollector.AggInterval = override.SFlowCollector.AggInterval
	}
	if override.SFlowCollector.PrefixSyncInterval > 0 {
		cfg.SFlowCollector.PrefixSyncInterval = override.SFlowCollector.PrefixSyncInterval
	}
	if override.AggregateGraph.RollupInterval > 0 {
		cfg.AggregateGraph.RollupInterval = override.AggregateGraph.RollupInterval
	}
	if len(override.SNMP.MIBDirs) > 0 {
		cfg.SNMP.MIBDirs = override.SNMP.MIBDirs
	}
	if override.SNMP.MIBLoad != "" {
		cfg.SNMP.MIBLoad = override.SNMP.MIBLoad
	}
	if override.Agent.HubURL != "" {
		cfg.Agent.HubURL = override.Agent.HubURL
	}
	if override.Agent.AgentID != "" {
		cfg.Agent.AgentID = override.Agent.AgentID
	}
	if override.Agent.Token != "" {
		cfg.Agent.Token = override.Agent.Token
	}
	if override.Agent.Interval > 0 {
		cfg.Agent.Interval = override.Agent.Interval
	}
}

func applyBackendConfigEnv(cfg *BackendConfig) {
	cfg.MySQL.DSN = getEnv("WATCHDOG_MYSQL_DSN", cfg.MySQL.DSN)
	cfg.MySQL.MaxOpenConns = getEnvInt("WATCHDOG_MYSQL_MAX_OPEN_CONNS", cfg.MySQL.MaxOpenConns)
	cfg.MySQL.MaxIdleConns = getEnvInt("WATCHDOG_MYSQL_MAX_IDLE_CONNS", cfg.MySQL.MaxIdleConns)
	cfg.MySQL.ConnMaxLifetime = getEnvDuration("WATCHDOG_MYSQL_CONN_MAX_LIFETIME", cfg.MySQL.ConnMaxLifetime)
	cfg.VictoriaMetrics.BaseURL = getEnv("WATCHDOG_VICTORIAMETRICS_URL", cfg.VictoriaMetrics.BaseURL)
	cfg.VictoriaLogs.BaseURL = getEnv("WATCHDOG_VICTORIALOGS_URL", cfg.VictoriaLogs.BaseURL)
	cfg.SFlowCollector.Listen = getEnv("WATCHDOG_SFLOW_LISTEN", cfg.SFlowCollector.Listen)
	cfg.SFlowCollector.VLogsURL = getEnv("WATCHDOG_SFLOW_VLOGS_URL", cfg.SFlowCollector.VLogsURL)
	if tid := getEnv("WATCHDOG_SFLOW_TENANT_ID", ""); tid != "" {
		cfg.SFlowCollector.TenantID = ID(tid)
	}
	cfg.Export.Dir = getEnv("WATCHDOG_EXPORT_DIR", cfg.Export.Dir)
	cfg.Export.WorkerInterval = getEnvDuration("WATCHDOG_EXPORT_WORKER_INTERVAL", cfg.Export.WorkerInterval)
	cfg.Export.WorkerBatch = getEnvInt("WATCHDOG_EXPORT_WORKER_BATCH", cfg.Export.WorkerBatch)
	cfg.Export.Metric = getEnv("WATCHDOG_EXPORT_METRIC", cfg.Export.Metric)
	cfg.SNMPCollector.Interval = getEnvDuration("WATCHDOG_SNMP_COLLECTOR_INTERVAL", cfg.SNMPCollector.Interval)
	cfg.SNMP.MIBDirs = getEnvStringList("WATCHDOG_SNMP_MIB_DIRS", cfg.SNMP.MIBDirs)
	cfg.SNMP.MIBLoad = getEnv("WATCHDOG_SNMP_MIBS", cfg.SNMP.MIBLoad)
	cfg.Agent.HubURL = getEnv("WATCHDOG_AGENT_HUB_URL", cfg.Agent.HubURL)
	if agentID := getEnv("WATCHDOG_AGENT_ID", ""); agentID != "" {
		cfg.Agent.AgentID = ID(agentID)
	}
	cfg.Agent.Token = getEnv("WATCHDOG_AGENT_TOKEN", cfg.Agent.Token)
	cfg.Agent.Interval = getEnvDuration("WATCHDOG_AGENT_INTERVAL", cfg.Agent.Interval)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err == nil && parsed > 0 {
		return parsed
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func getEnvStringList(key string, fallback []string) []string {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == filepath.ListSeparator
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
