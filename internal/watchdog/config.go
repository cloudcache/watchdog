package watchdog

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultMySQLMaxOpenConns      = 25
	defaultMySQLMaxIdleConns      = 5
	defaultMySQLConnMaxLifetime   = 30 * time.Minute
	defaultVictoriaMetricsURL     = "http://127.0.0.1:8428"
	defaultVictoriaLogsURL        = "http://127.0.0.1:9428"
	defaultExportDir              = "exports"
	defaultExportWorkerInterval   = 30 * time.Second
	defaultExportWorkerBatch      = 10
	defaultSNMPCollectorInterval  = time.Minute
	defaultSNMPCollectorPollLimit = 500
	defaultSNMPDiscoveryInterval  = 30 * time.Second
	defaultSNMPDiscoveryBatch     = 10
	defaultSFlowListen            = ":6343"
	defaultSFlowAggInterval       = time.Minute
	defaultSFlowPrefixSync        = 5 * time.Minute
	defaultSNMPTrapListen         = ":162"
	defaultAgentInterval          = time.Minute
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
	SNMPTrapAgent   SNMPTrapAgentConfig   `yaml:"snmp_trap_agent"`
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
	Listen             string        `yaml:"listen"`
	TenantID           ID            `yaml:"tenant_id"`
	VLogsURL           string        `yaml:"vlogs_url"`
	AggInterval        time.Duration `yaml:"agg_interval"`
	PrefixSyncInterval time.Duration `yaml:"prefix_sync_interval"`
}

type ExportConfig struct {
	Dir            string        `yaml:"dir"`
	WorkerInterval time.Duration `yaml:"worker_interval"`
	WorkerBatch    int           `yaml:"worker_batch"`
	Metric         string        `yaml:"metric"`
}

type SNMPCollectorConfig struct {
	TenantID          ID            `yaml:"tenant_id"`
	Interval          time.Duration `yaml:"interval"`
	PollLimit         int           `yaml:"poll_limit"`
	DiscoveryInterval time.Duration `yaml:"discovery_interval"`
	DiscoveryBatch    int           `yaml:"discovery_batch"`
}

type AggregateGraphConfig struct {
	RollupInterval time.Duration `yaml:"rollup_interval"`
}

type SNMPConfig struct {
	MIBDirs []string `yaml:"mib_dirs"`
	MIBLoad string   `yaml:"mib_load"`
}

type SNMPTrapAgentConfig struct {
	APIURL string `yaml:"api_url"`
	Token  string `yaml:"token"`
	Listen string `yaml:"listen"`
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
		path = strings.TrimSpace(getEnv("WATCHDOG_CONFIG", ""))
	} else {
		path = strings.TrimSpace(path)
	}
	if path != "" {
		if err := loadBackendConfigFile(path, &cfg); err != nil {
			return cfg, err
		}
	}
	if err := applyBackendConfigEnv(&cfg); err != nil {
		return cfg, err
	}
	normalizeBackendConfig(&cfg)
	if err := validateWatchdogConfig(cfg, false); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func LoadBackendConfig(path string) (BackendConfig, error) {
	cfg, err := LoadWatchdogConfig(path)
	if err != nil {
		return cfg, err
	}
	if err := validateWatchdogConfig(cfg, true); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func LoadBackendConfigFromEnv() (BackendConfig, error) {
	cfg := defaultBackendConfig()
	if err := applyBackendConfigEnv(&cfg); err != nil {
		return cfg, err
	}
	normalizeBackendConfig(&cfg)
	if err := validateWatchdogConfig(cfg, true); err != nil {
		return cfg, err
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
			TenantID:          "tenant_dev",
			Interval:          defaultSNMPCollectorInterval,
			PollLimit:         defaultSNMPCollectorPollLimit,
			DiscoveryInterval: defaultSNMPDiscoveryInterval,
			DiscoveryBatch:    defaultSNMPDiscoveryBatch,
		},
		SFlowCollector: SFlowCollectorConfig{
			Listen:             defaultSFlowListen,
			AggInterval:        defaultSFlowAggInterval,
			PrefixSyncInterval: defaultSFlowPrefixSync,
		},
		AggregateGraph: AggregateGraphConfig{
			RollupInterval: defaultAggregateGraphRollupInterval,
		},
		SNMPTrapAgent: SNMPTrapAgentConfig{
			Listen: defaultSNMPTrapListen,
		},
		Agent: AgentClientConfig{
			Interval: defaultAgentInterval,
		},
	}
}

func loadBackendConfigFile(path string, cfg *BackendConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return fmt.Errorf("decode watchdog config %q: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode watchdog config %q: multiple YAML documents are not allowed", path)
		}
		return fmt.Errorf("decode watchdog config %q: %w", path, err)
	}
	return nil
}

func applyBackendConfigEnv(cfg *BackendConfig) error {
	var err error
	cfg.MySQL.DSN = getEnv("WATCHDOG_MYSQL_DSN", cfg.MySQL.DSN)
	if cfg.MySQL.MaxOpenConns, err = getEnvInt("WATCHDOG_MYSQL_MAX_OPEN_CONNS", cfg.MySQL.MaxOpenConns, 1); err != nil {
		return err
	}
	if cfg.MySQL.MaxIdleConns, err = getEnvInt("WATCHDOG_MYSQL_MAX_IDLE_CONNS", cfg.MySQL.MaxIdleConns, 0); err != nil {
		return err
	}
	if cfg.MySQL.ConnMaxLifetime, err = getEnvDuration("WATCHDOG_MYSQL_CONN_MAX_LIFETIME", cfg.MySQL.ConnMaxLifetime); err != nil {
		return err
	}
	cfg.VictoriaMetrics.BaseURL = getEnv("WATCHDOG_VICTORIAMETRICS_URL", cfg.VictoriaMetrics.BaseURL)
	cfg.VictoriaLogs.BaseURL = getEnv("WATCHDOG_VICTORIALOGS_URL", cfg.VictoriaLogs.BaseURL)
	cfg.SFlowCollector.Listen = getEnv("WATCHDOG_SFLOW_LISTEN", cfg.SFlowCollector.Listen)
	cfg.SFlowCollector.VLogsURL = getEnv("WATCHDOG_SFLOW_VLOGS_URL", cfg.SFlowCollector.VLogsURL)
	if tid, ok := os.LookupEnv("WATCHDOG_SFLOW_TENANT_ID"); ok {
		cfg.SFlowCollector.TenantID = ID(tid)
	}
	if cfg.SFlowCollector.AggInterval, err = getEnvDuration("WATCHDOG_SFLOW_AGG_INTERVAL", cfg.SFlowCollector.AggInterval); err != nil {
		return err
	}
	if cfg.SFlowCollector.PrefixSyncInterval, err = getEnvDuration("WATCHDOG_SFLOW_PREFIX_SYNC_INTERVAL", cfg.SFlowCollector.PrefixSyncInterval); err != nil {
		return err
	}
	cfg.Export.Dir = getEnv("WATCHDOG_EXPORT_DIR", cfg.Export.Dir)
	if cfg.Export.WorkerInterval, err = getEnvDuration("WATCHDOG_EXPORT_WORKER_INTERVAL", cfg.Export.WorkerInterval); err != nil {
		return err
	}
	if cfg.Export.WorkerBatch, err = getEnvInt("WATCHDOG_EXPORT_WORKER_BATCH", cfg.Export.WorkerBatch, 1); err != nil {
		return err
	}
	cfg.Export.Metric = getEnv("WATCHDOG_EXPORT_METRIC", cfg.Export.Metric)
	if tid, ok := os.LookupEnv("WATCHDOG_SNMP_COLLECTOR_TENANT_ID"); ok {
		cfg.SNMPCollector.TenantID = ID(tid)
	}
	if cfg.SNMPCollector.Interval, err = getEnvDuration("WATCHDOG_SNMP_COLLECTOR_INTERVAL", cfg.SNMPCollector.Interval); err != nil {
		return err
	}
	if cfg.SNMPCollector.PollLimit, err = getEnvInt("WATCHDOG_SNMP_COLLECTOR_POLL_LIMIT", cfg.SNMPCollector.PollLimit, 1); err != nil {
		return err
	}
	if cfg.SNMPCollector.DiscoveryInterval, err = getEnvDuration("WATCHDOG_SNMP_DISCOVERY_INTERVAL", cfg.SNMPCollector.DiscoveryInterval); err != nil {
		return err
	}
	if cfg.SNMPCollector.DiscoveryBatch, err = getEnvInt("WATCHDOG_SNMP_DISCOVERY_BATCH", cfg.SNMPCollector.DiscoveryBatch, 1); err != nil {
		return err
	}
	cfg.SNMP.MIBDirs = getEnvStringList("WATCHDOG_SNMP_MIB_DIRS", cfg.SNMP.MIBDirs)
	cfg.SNMP.MIBLoad = getEnv("WATCHDOG_SNMP_MIBS", cfg.SNMP.MIBLoad)
	if cfg.AggregateGraph.RollupInterval, err = getEnvDuration("WATCHDOG_AGGREGATE_GRAPH_ROLLUP_INTERVAL", cfg.AggregateGraph.RollupInterval); err != nil {
		return err
	}
	cfg.SNMPTrapAgent.APIURL = getEnv("WATCHDOG_SNMP_TRAP_API_URL", cfg.SNMPTrapAgent.APIURL)
	cfg.SNMPTrapAgent.Token = getEnv("WATCHDOG_SNMP_TRAP_TOKEN", cfg.SNMPTrapAgent.Token)
	cfg.SNMPTrapAgent.Listen = getEnv("WATCHDOG_SNMP_TRAP_LISTEN", cfg.SNMPTrapAgent.Listen)
	cfg.Agent.HubURL = getEnv("WATCHDOG_AGENT_HUB_URL", cfg.Agent.HubURL)
	if agentID, ok := os.LookupEnv("WATCHDOG_AGENT_ID"); ok {
		cfg.Agent.AgentID = ID(agentID)
	}
	cfg.Agent.Token = getEnv("WATCHDOG_AGENT_TOKEN", cfg.Agent.Token)
	if cfg.Agent.Interval, err = getEnvDuration("WATCHDOG_AGENT_INTERVAL", cfg.Agent.Interval); err != nil {
		return err
	}
	return nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback, minimum int) (int, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed < minimum {
		return fallback, fmt.Errorf("%s must be an integer >= %d", key, minimum)
	}
	return parsed, nil
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	value = strings.TrimSpace(value)
	parsed, err := time.ParseDuration(value)
	if err == nil && parsed > 0 {
		return parsed, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return fallback, fmt.Errorf("%s must be a positive duration or number of seconds", key)
	}
	return time.Duration(seconds) * time.Second, nil
}

func getEnvStringList(key string, fallback []string) []string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	if strings.TrimSpace(value) == "" {
		return []string{}
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

func normalizeBackendConfig(cfg *BackendConfig) {
	cfg.VictoriaMetrics.BaseURL = normalizeBaseURL(cfg.VictoriaMetrics.BaseURL)
	cfg.VictoriaLogs.BaseURL = normalizeBaseURL(cfg.VictoriaLogs.BaseURL)
	cfg.Export.Dir = strings.TrimSpace(cfg.Export.Dir)
	cfg.Export.Metric = strings.TrimSpace(cfg.Export.Metric)
	cfg.SNMPCollector.TenantID = ID(strings.TrimSpace(string(cfg.SNMPCollector.TenantID)))
	cfg.SFlowCollector.Listen = strings.TrimSpace(cfg.SFlowCollector.Listen)
	cfg.SFlowCollector.TenantID = ID(strings.TrimSpace(string(cfg.SFlowCollector.TenantID)))
	cfg.SFlowCollector.VLogsURL = normalizeBaseURL(cfg.SFlowCollector.VLogsURL)
	cfg.SNMP.MIBDirs = normalizeStringPaths(cfg.SNMP.MIBDirs)
	cfg.SNMP.MIBLoad = strings.TrimSpace(cfg.SNMP.MIBLoad)
	cfg.SNMPTrapAgent.APIURL = normalizeBaseURL(cfg.SNMPTrapAgent.APIURL)
	cfg.SNMPTrapAgent.Listen = strings.TrimSpace(cfg.SNMPTrapAgent.Listen)
	cfg.Agent.HubURL = normalizeBaseURL(cfg.Agent.HubURL)
	cfg.Agent.AgentID = ID(strings.TrimSpace(string(cfg.Agent.AgentID)))
}

func normalizeBaseURL(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "/")
}

func normalizeStringPaths(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		value = filepath.Clean(value)
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func validateWatchdogConfig(cfg BackendConfig, requireMySQL bool) error {
	if requireMySQL && strings.TrimSpace(cfg.MySQL.DSN) == "" {
		return errors.New("mysql dsn is required; set mysql.dsn in config or WATCHDOG_MYSQL_DSN")
	}
	if cfg.MySQL.MaxOpenConns <= 0 {
		return errors.New("mysql.max_open_conns must be positive")
	}
	if cfg.MySQL.MaxIdleConns < 0 || cfg.MySQL.MaxIdleConns > cfg.MySQL.MaxOpenConns {
		return errors.New("mysql.max_idle_conns must be between 0 and mysql.max_open_conns")
	}
	if cfg.MySQL.ConnMaxLifetime <= 0 {
		return errors.New("mysql.conn_max_lifetime must be positive")
	}
	if err := validateHTTPBaseURL("victoriametrics.base_url", cfg.VictoriaMetrics.BaseURL, true); err != nil {
		return err
	}
	if err := validateHTTPBaseURL("victorialogs.base_url", cfg.VictoriaLogs.BaseURL, true); err != nil {
		return err
	}
	if err := validateHTTPBaseURL("sflow_collector.vlogs_url", cfg.SFlowCollector.VLogsURL, false); err != nil {
		return err
	}
	if err := validateListenAddress("sflow_collector.listen", cfg.SFlowCollector.Listen); err != nil {
		return err
	}
	if cfg.SFlowCollector.AggInterval <= 0 || cfg.SFlowCollector.PrefixSyncInterval <= 0 {
		return errors.New("sflow_collector intervals must be positive")
	}
	if cfg.Export.Dir == "" || cfg.Export.WorkerInterval <= 0 || cfg.Export.WorkerBatch <= 0 {
		return errors.New("export dir, worker_interval, and worker_batch must be configured with positive worker values")
	}
	if cfg.Export.Metric != MetricSNMPIfInBps && cfg.Export.Metric != MetricSNMPIfOutBps {
		return fmt.Errorf("export.metric must be %q or %q", MetricSNMPIfInBps, MetricSNMPIfOutBps)
	}
	if cfg.SNMPCollector.Interval <= 0 || cfg.SNMPCollector.PollLimit <= 0 || cfg.SNMPCollector.DiscoveryInterval <= 0 || cfg.SNMPCollector.DiscoveryBatch <= 0 {
		return errors.New("snmp_collector interval, limit, and batch values must be positive")
	}
	if cfg.AggregateGraph.RollupInterval <= 0 {
		return errors.New("aggregate_graph.rollup_interval must be positive")
	}
	if err := validateHTTPBaseURL("snmp_trap_agent.api_url", cfg.SNMPTrapAgent.APIURL, false); err != nil {
		return err
	}
	if err := validateListenAddress("snmp_trap_agent.listen", cfg.SNMPTrapAgent.Listen); err != nil {
		return err
	}
	if err := validateHTTPBaseURL("agent.hub_url", cfg.Agent.HubURL, false); err != nil {
		return err
	}
	if cfg.Agent.Interval <= 0 {
		return errors.New("agent.interval must be positive")
	}
	return nil
}

func validateHTTPBaseURL(name, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("%s is required", name)
		}
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("%s must be an absolute http(s) URL", name)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s must not contain a query or fragment", name)
	}
	return nil
}

func validateListenAddress(name, value string) error {
	_, portText, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%s must be a host:port listen address: %w", name, err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return fmt.Errorf("%s must contain a port between 1 and 65535", name)
	}
	return nil
}

func NormalizeAndValidateAgentClientConfig(cfg AgentClientConfig) (AgentClientConfig, error) {
	cfg.HubURL = normalizeBaseURL(cfg.HubURL)
	cfg.AgentID = ID(strings.TrimSpace(string(cfg.AgentID)))
	if err := validateHTTPBaseURL("agent.hub_url", cfg.HubURL, true); err != nil {
		return cfg, err
	}
	if cfg.AgentID == "" {
		return cfg, errors.New("agent.agent_id is required")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return cfg, errors.New("agent.token is required")
	}
	if cfg.Interval <= 0 {
		return cfg, errors.New("agent.interval must be positive")
	}
	return cfg, nil
}

func NormalizeAndValidateSNMPTrapAgentConfig(cfg SNMPTrapAgentConfig) (SNMPTrapAgentConfig, error) {
	cfg.APIURL = normalizeBaseURL(cfg.APIURL)
	cfg.Listen = strings.TrimSpace(cfg.Listen)
	if err := validateHTTPBaseURL("snmp_trap_agent.api_url", cfg.APIURL, true); err != nil {
		return cfg, err
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return cfg, errors.New("snmp_trap_agent.token is required")
	}
	if err := validateListenAddress("snmp_trap_agent.listen", cfg.Listen); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func ValidateSFlowCollectorConfig(cfg SFlowCollectorConfig) error {
	if cfg.TenantID == "" {
		return errors.New("sflow_collector.tenant_id is required")
	}
	return nil
}

func ValidateSNMPCollectorConfig(cfg SNMPCollectorConfig) error {
	if cfg.TenantID == "" {
		return errors.New("snmp_collector.tenant_id is required")
	}
	if cfg.Interval <= 0 || cfg.PollLimit <= 0 || cfg.DiscoveryInterval <= 0 || cfg.DiscoveryBatch <= 0 {
		return errors.New("snmp_collector interval, limit, and batch values must be positive")
	}
	return nil
}
