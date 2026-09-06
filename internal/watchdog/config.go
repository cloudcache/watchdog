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
	defaultMySQLMaxOpenConns           = 25
	defaultMySQLMaxIdleConns           = 5
	defaultMySQLConnMaxLifetime        = 30 * time.Minute
	defaultVictoriaMetricsURL          = "http://127.0.0.1:8428"
	defaultExportDir                   = "exports"
	defaultExportWorkerInterval        = 30 * time.Second
	defaultExportWorkerBatch           = 10
	defaultSNMPCollectorInterval       = time.Minute
	defaultSNMPCollectorPollLimit      = 500
	defaultSNMPDiscoveryInterval       = 30 * time.Second
	defaultSNMPDiscoveryBatch          = 10
	defaultSFlowListen                 = ":6343"
	defaultSFlowAggInterval            = time.Minute
	defaultSFlowPrefixSync             = 5 * time.Minute
	defaultSNMPTrapListen              = ":162"
	defaultAgentInterval               = time.Minute
	defaultPrincipalRequestTimeout     = 10 * time.Second
	defaultPrincipalFailureLimit       = uint32(5)
	defaultPrincipalCircuitOpen        = 30 * time.Second
	defaultPrincipalOperationRetention = 30 * 24 * time.Hour
	defaultFlowRollupScanInterval      = 30 * time.Second
	defaultFlowRollupLateWindow        = 5 * time.Minute
	defaultFlowRollupBootstrapLookback = 24 * time.Hour
	defaultFlowRollupTenantScanLimit   = 500
	defaultFlowRollupSeriesScanLimit   = 500
	defaultFlowRollupScanLimit         = 5_000
	defaultFlowRollupWorkerConcurrency = 2
	defaultFlowRollupLease             = 2 * time.Minute
	defaultFlowRollupMaxAttempts       = uint32(5)
	defaultFlowRollupRetryBase         = 30 * time.Second
	defaultAddressLibraryDir           = "address-artifacts"
	defaultAddressLibraryBatchSize     = 1_000
	defaultAddressLibraryWorkers       = 1
)

type BackendConfig struct {
	MySQL           MySQLConfig           `yaml:"mysql"`
	VictoriaMetrics VictoriaMetricsConfig `yaml:"victoriametrics"`
	MetricsScrape   MetricsScrapeConfig   `yaml:"metrics_scrape"`
	Export          ExportConfig          `yaml:"export"`
	AddressLibrary  AddressLibraryConfig  `yaml:"address_library"`
	SNMPCollector   SNMPCollectorConfig   `yaml:"snmp_collector"`
	SFlowCollector  SFlowCollectorConfig  `yaml:"sflow_collector"`
	FlowGeo         FlowGeoConfig         `yaml:"flow_geo"`
	FlowRollup      FlowRollupConfig      `yaml:"flow_rollup"`

	CollectorPrincipalProvider RemoteCollectorPrincipalProviderConfig `yaml:"collector_principal_provider"`

	AggregateGraph AggregateGraphConfig `yaml:"aggregate_graph"`
	SNMP           SNMPConfig           `yaml:"snmp"`
	SNMPTrapAgent  SNMPTrapAgentConfig  `yaml:"snmp_trap_agent"`
	Agent          AgentClientConfig    `yaml:"agent"`
}

// MetricsScrapeConfig exposes the composed hub metrics on the existing HTTP
// listener. TokenFile is used instead of an inline token so production secrets
// do not enter YAML, process arguments, or environment values.
type MetricsScrapeConfig struct {
	Enabled      bool     `yaml:"enabled"`
	TokenFile    string   `yaml:"token_file"`
	AllowedCIDRs []string `yaml:"allowed_cidrs"`
}

type FlowRollupConfig struct {
	Enabled                 bool          `yaml:"enabled"`
	ScanInterval            time.Duration `yaml:"scan_interval"`
	LateArrivalWindow       time.Duration `yaml:"late_arrival_window"`
	BootstrapLookback       time.Duration `yaml:"bootstrap_lookback"`
	MaxTenantsPerScan       int           `yaml:"max_tenants_per_scan"`
	MaxBucketsPerSeriesScan int           `yaml:"max_buckets_per_series_scan"`
	MaxBucketsPerScan       int           `yaml:"max_buckets_per_scan"`
	WorkerConcurrency       int           `yaml:"worker_concurrency"`
	LeaseFor                time.Duration `yaml:"lease_for"`
	MaxAttempts             uint32        `yaml:"max_attempts"`
	RetryBase               time.Duration `yaml:"retry_base"`

	ClickHouseAddress          string        `yaml:"clickhouse_address"`
	ClickHouseDatabase         string        `yaml:"clickhouse_database"`
	ClickHouseUser             string        `yaml:"clickhouse_user"`
	ClickHousePasswordFile     string        `yaml:"clickhouse_password_file"`
	ClickHouseMaxConns         int           `yaml:"clickhouse_max_conns"`
	ClickHouseMinConns         int           `yaml:"clickhouse_min_conns"`
	ClickHouseDialTimeout      time.Duration `yaml:"clickhouse_dial_timeout"`
	ClickHouseReadTimeout      time.Duration `yaml:"clickhouse_read_timeout"`
	ClickHouseOperationTimeout time.Duration `yaml:"clickhouse_operation_timeout"`
	ClickHouseTLS              bool          `yaml:"clickhouse_tls"`
	ClickHouseCAFile           string        `yaml:"clickhouse_tls_ca"`
	ClickHouseCertFile         string        `yaml:"clickhouse_tls_cert"`
	ClickHouseKeyFile          string        `yaml:"clickhouse_tls_key"`
	ClickHouseServerName       string        `yaml:"clickhouse_tls_server_name"`
}

type RemoteCollectorPrincipalProviderConfig struct {
	Enabled               bool          `yaml:"enabled"`
	Name                  string        `yaml:"name"`
	BaseURL               string        `yaml:"base_url"`
	PolicyRevision        string        `yaml:"policy_revision"`
	RequestTimeout        time.Duration `yaml:"request_timeout"`
	FailureThreshold      uint32        `yaml:"failure_threshold"`
	CircuitOpenInterval   time.Duration `yaml:"circuit_open_interval"`
	MinOperationRetention time.Duration `yaml:"min_operation_retention"`
	TLSCAFile             string        `yaml:"tls_ca_file"`
	TLSCertFile           string        `yaml:"tls_cert_file"`
	TLSKeyFile            string        `yaml:"tls_key_file"`
	TLSServerName         string        `yaml:"tls_server_name"`
}

type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	// EncryptionKey is a base64-encoded 32-byte AES-256 key for at-rest secrets
	// (webhook URLs). Empty disables encryption; new secrets are then stored as
	// plaintext until a key is configured.
	EncryptionKey string `yaml:"encryption_key"`
}

type VictoriaMetricsConfig struct {
	BaseURL string `yaml:"base_url"`
}

type SFlowCollectorConfig struct {
	Listen             string        `yaml:"listen"`
	TenantID           ID            `yaml:"tenant_id"`
	AggInterval        time.Duration `yaml:"agg_interval"`
	PrefixSyncInterval time.Duration `yaml:"prefix_sync_interval"`
}

type ExportConfig struct {
	Dir            string        `yaml:"dir"`
	WorkerInterval time.Duration `yaml:"worker_interval"`
	WorkerBatch    int           `yaml:"worker_batch"`
	Metric         string        `yaml:"metric"`
}

type AddressLibraryConfig struct {
	Dir               string `yaml:"dir"`
	MaxUploadBytes    int64  `yaml:"max_upload_bytes"`
	ImportBatchSize   int    `yaml:"import_batch_size"`
	WorkerConcurrency int    `yaml:"worker_concurrency"`
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
		Export: ExportConfig{
			Dir:            defaultExportDir,
			WorkerInterval: defaultExportWorkerInterval,
			WorkerBatch:    defaultExportWorkerBatch,
			Metric:         MetricSNMPIfInBps,
		},
		AddressLibrary: AddressLibraryConfig{
			Dir: defaultAddressLibraryDir, MaxUploadBytes: DefaultAddressArtifactMaxBytes,
			ImportBatchSize: defaultAddressLibraryBatchSize, WorkerConcurrency: defaultAddressLibraryWorkers,
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
		CollectorPrincipalProvider: RemoteCollectorPrincipalProviderConfig{
			RequestTimeout:        defaultPrincipalRequestTimeout,
			FailureThreshold:      defaultPrincipalFailureLimit,
			CircuitOpenInterval:   defaultPrincipalCircuitOpen,
			MinOperationRetention: defaultPrincipalOperationRetention,
		},
		FlowRollup: FlowRollupConfig{
			ScanInterval: defaultFlowRollupScanInterval, LateArrivalWindow: defaultFlowRollupLateWindow,
			BootstrapLookback: defaultFlowRollupBootstrapLookback, MaxTenantsPerScan: defaultFlowRollupTenantScanLimit,
			MaxBucketsPerSeriesScan: defaultFlowRollupSeriesScanLimit, MaxBucketsPerScan: defaultFlowRollupScanLimit,
			WorkerConcurrency: defaultFlowRollupWorkerConcurrency, LeaseFor: defaultFlowRollupLease,
			MaxAttempts: defaultFlowRollupMaxAttempts, RetryBase: defaultFlowRollupRetryBase,
			ClickHouseAddress: "127.0.0.1:9000", ClickHouseDatabase: "watchdog_flow", ClickHouseUser: "default",
			ClickHouseMaxConns: 2, ClickHouseMinConns: 0,
			ClickHouseDialTimeout: 3 * time.Second, ClickHouseReadTimeout: 90 * time.Second,
			ClickHouseOperationTimeout: 5 * time.Minute,
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
	for _, key := range []string{"WATCHDOG_VICTORIALOGS_URL", "WATCHDOG_SFLOW_VLOGS_URL"} {
		if _, ok := os.LookupEnv(key); ok {
			return fmt.Errorf("%s was removed; VictoriaLogs is no longer a supported watchdog storage backend", key)
		}
	}
	var err error
	cfg.MySQL.DSN = getEnv("WATCHDOG_MYSQL_DSN", cfg.MySQL.DSN)
	cfg.MySQL.EncryptionKey = getEnv("WATCHDOG_ENCRYPTION_KEY", cfg.MySQL.EncryptionKey)
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
	if cfg.MetricsScrape.Enabled, err = getEnvBool("WATCHDOG_METRICS_SCRAPE_ENABLED", cfg.MetricsScrape.Enabled); err != nil {
		return err
	}
	cfg.MetricsScrape.TokenFile = getEnv("WATCHDOG_METRICS_SCRAPE_TOKEN_FILE", cfg.MetricsScrape.TokenFile)
	cfg.MetricsScrape.AllowedCIDRs = getEnvCommaList("WATCHDOG_METRICS_SCRAPE_ALLOWED_CIDRS", cfg.MetricsScrape.AllowedCIDRs)
	cfg.FlowGeo.Path = getEnv("WATCHDOG_FLOW_GEO_PATH", cfg.FlowGeo.Path)
	if cfg.FlowRollup.Enabled, err = getEnvBool("WATCHDOG_FLOW_ROLLUP_ENABLED", cfg.FlowRollup.Enabled); err != nil {
		return err
	}
	if cfg.FlowRollup.ScanInterval, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_SCAN_INTERVAL", cfg.FlowRollup.ScanInterval); err != nil {
		return err
	}
	if cfg.FlowRollup.LateArrivalWindow, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_LATE_ARRIVAL_WINDOW", cfg.FlowRollup.LateArrivalWindow); err != nil {
		return err
	}
	if cfg.FlowRollup.BootstrapLookback, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_BOOTSTRAP_LOOKBACK", cfg.FlowRollup.BootstrapLookback); err != nil {
		return err
	}
	if cfg.FlowRollup.MaxTenantsPerScan, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_MAX_TENANTS_PER_SCAN", cfg.FlowRollup.MaxTenantsPerScan, 1); err != nil {
		return err
	}
	if cfg.FlowRollup.MaxBucketsPerSeriesScan, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_MAX_BUCKETS_PER_SERIES_SCAN", cfg.FlowRollup.MaxBucketsPerSeriesScan, 1); err != nil {
		return err
	}
	if cfg.FlowRollup.MaxBucketsPerScan, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_MAX_BUCKETS_PER_SCAN", cfg.FlowRollup.MaxBucketsPerScan, 1); err != nil {
		return err
	}
	if cfg.FlowRollup.WorkerConcurrency, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_WORKER_CONCURRENCY", cfg.FlowRollup.WorkerConcurrency, 1); err != nil {
		return err
	}
	if cfg.FlowRollup.LeaseFor, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_LEASE_FOR", cfg.FlowRollup.LeaseFor); err != nil {
		return err
	}
	if cfg.FlowRollup.MaxAttempts, err = getEnvUint32("WATCHDOG_FLOW_ROLLUP_MAX_ATTEMPTS", cfg.FlowRollup.MaxAttempts); err != nil {
		return err
	}
	if cfg.FlowRollup.RetryBase, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_RETRY_BASE", cfg.FlowRollup.RetryBase); err != nil {
		return err
	}
	cfg.FlowRollup.ClickHouseAddress = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_ADDRESS", cfg.FlowRollup.ClickHouseAddress)
	cfg.FlowRollup.ClickHouseDatabase = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_DATABASE", cfg.FlowRollup.ClickHouseDatabase)
	cfg.FlowRollup.ClickHouseUser = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_USER", cfg.FlowRollup.ClickHouseUser)
	cfg.FlowRollup.ClickHousePasswordFile = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_PASSWORD_FILE", cfg.FlowRollup.ClickHousePasswordFile)
	if cfg.FlowRollup.ClickHouseMaxConns, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_MAX_CONNS", cfg.FlowRollup.ClickHouseMaxConns, 1); err != nil {
		return err
	}
	if cfg.FlowRollup.ClickHouseMinConns, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_MIN_CONNS", cfg.FlowRollup.ClickHouseMinConns, 0); err != nil {
		return err
	}
	if cfg.FlowRollup.ClickHouseDialTimeout, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_DIAL_TIMEOUT", cfg.FlowRollup.ClickHouseDialTimeout); err != nil {
		return err
	}
	if cfg.FlowRollup.ClickHouseReadTimeout, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_READ_TIMEOUT", cfg.FlowRollup.ClickHouseReadTimeout); err != nil {
		return err
	}
	if cfg.FlowRollup.ClickHouseOperationTimeout, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_OPERATION_TIMEOUT", cfg.FlowRollup.ClickHouseOperationTimeout); err != nil {
		return err
	}
	if cfg.FlowRollup.ClickHouseTLS, err = getEnvBool("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_TLS", cfg.FlowRollup.ClickHouseTLS); err != nil {
		return err
	}
	cfg.FlowRollup.ClickHouseCAFile = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_TLS_CA", cfg.FlowRollup.ClickHouseCAFile)
	cfg.FlowRollup.ClickHouseCertFile = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_TLS_CERT", cfg.FlowRollup.ClickHouseCertFile)
	cfg.FlowRollup.ClickHouseKeyFile = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_TLS_KEY", cfg.FlowRollup.ClickHouseKeyFile)
	cfg.FlowRollup.ClickHouseServerName = getEnv("WATCHDOG_FLOW_ROLLUP_CLICKHOUSE_TLS_SERVER_NAME", cfg.FlowRollup.ClickHouseServerName)
	cfg.SFlowCollector.Listen = getEnv("WATCHDOG_SFLOW_LISTEN", cfg.SFlowCollector.Listen)
	if tid, ok := os.LookupEnv("WATCHDOG_SFLOW_TENANT_ID"); ok {
		cfg.SFlowCollector.TenantID = ID(tid)
	}
	if cfg.SFlowCollector.AggInterval, err = getEnvDuration("WATCHDOG_SFLOW_AGG_INTERVAL", cfg.SFlowCollector.AggInterval); err != nil {
		return err
	}
	if cfg.SFlowCollector.PrefixSyncInterval, err = getEnvDuration("WATCHDOG_SFLOW_PREFIX_SYNC_INTERVAL", cfg.SFlowCollector.PrefixSyncInterval); err != nil {
		return err
	}
	if cfg.CollectorPrincipalProvider.Enabled, err = getEnvBool("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_ENABLED", cfg.CollectorPrincipalProvider.Enabled); err != nil {
		return err
	}
	cfg.CollectorPrincipalProvider.Name = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_NAME", cfg.CollectorPrincipalProvider.Name)
	cfg.CollectorPrincipalProvider.BaseURL = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_BASE_URL", cfg.CollectorPrincipalProvider.BaseURL)
	cfg.CollectorPrincipalProvider.PolicyRevision = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_POLICY_REVISION", cfg.CollectorPrincipalProvider.PolicyRevision)
	if cfg.CollectorPrincipalProvider.RequestTimeout, err = getEnvDuration("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_REQUEST_TIMEOUT", cfg.CollectorPrincipalProvider.RequestTimeout); err != nil {
		return err
	}
	if cfg.CollectorPrincipalProvider.FailureThreshold, err = getEnvUint32("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_FAILURE_THRESHOLD", cfg.CollectorPrincipalProvider.FailureThreshold); err != nil {
		return err
	}
	if cfg.CollectorPrincipalProvider.CircuitOpenInterval, err = getEnvDuration("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_CIRCUIT_OPEN_INTERVAL", cfg.CollectorPrincipalProvider.CircuitOpenInterval); err != nil {
		return err
	}
	if cfg.CollectorPrincipalProvider.MinOperationRetention, err = getEnvDuration("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_MIN_OPERATION_RETENTION", cfg.CollectorPrincipalProvider.MinOperationRetention); err != nil {
		return err
	}
	cfg.CollectorPrincipalProvider.TLSCAFile = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_TLS_CA_FILE", cfg.CollectorPrincipalProvider.TLSCAFile)
	cfg.CollectorPrincipalProvider.TLSCertFile = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_TLS_CERT_FILE", cfg.CollectorPrincipalProvider.TLSCertFile)
	cfg.CollectorPrincipalProvider.TLSKeyFile = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_TLS_KEY_FILE", cfg.CollectorPrincipalProvider.TLSKeyFile)
	cfg.CollectorPrincipalProvider.TLSServerName = getEnv("WATCHDOG_COLLECTOR_PRINCIPAL_PROVIDER_TLS_SERVER_NAME", cfg.CollectorPrincipalProvider.TLSServerName)
	cfg.Export.Dir = getEnv("WATCHDOG_EXPORT_DIR", cfg.Export.Dir)
	if cfg.Export.WorkerInterval, err = getEnvDuration("WATCHDOG_EXPORT_WORKER_INTERVAL", cfg.Export.WorkerInterval); err != nil {
		return err
	}
	if cfg.Export.WorkerBatch, err = getEnvInt("WATCHDOG_EXPORT_WORKER_BATCH", cfg.Export.WorkerBatch, 1); err != nil {
		return err
	}
	cfg.Export.Metric = getEnv("WATCHDOG_EXPORT_METRIC", cfg.Export.Metric)
	cfg.AddressLibrary.Dir = getEnv("WATCHDOG_ADDRESS_LIBRARY_DIR", cfg.AddressLibrary.Dir)
	if cfg.AddressLibrary.MaxUploadBytes, err = getEnvInt64("WATCHDOG_ADDRESS_LIBRARY_MAX_UPLOAD_BYTES", cfg.AddressLibrary.MaxUploadBytes, 1); err != nil {
		return err
	}
	if cfg.AddressLibrary.ImportBatchSize, err = getEnvInt("WATCHDOG_ADDRESS_LIBRARY_IMPORT_BATCH_SIZE", cfg.AddressLibrary.ImportBatchSize, 1); err != nil {
		return err
	}
	if cfg.AddressLibrary.WorkerConcurrency, err = getEnvInt("WATCHDOG_ADDRESS_LIBRARY_WORKER_CONCURRENCY", cfg.AddressLibrary.WorkerConcurrency, 1); err != nil {
		return err
	}
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

func getEnvInt64(key string, fallback, minimum int64) (int64, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
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

func getEnvBool(key string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fallback, fmt.Errorf("%s must be a boolean", key)
	}
	return parsed, nil
}

func getEnvUint32(key string, fallback uint32) (uint32, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return fallback, fmt.Errorf("%s must be an integer between 0 and %d", key, uint64(^uint32(0)))
	}
	return uint32(parsed), nil
}

func splitConfigList(value string) []string {
	return strings.FieldsFunc(value, func(char rune) bool { return char == ',' || char == ';' })
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

func getEnvCommaList(key string, fallback []string) []string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	if strings.TrimSpace(value) == "" {
		return []string{}
	}
	return splitConfigList(value)
}

func normalizeBackendConfig(cfg *BackendConfig) {
	cfg.VictoriaMetrics.BaseURL = normalizeBaseURL(cfg.VictoriaMetrics.BaseURL)
	cfg.MetricsScrape.TokenFile = cleanOptionalConfigPath(cfg.MetricsScrape.TokenFile)
	cfg.MetricsScrape.AllowedCIDRs = normalizeStringList(cfg.MetricsScrape.AllowedCIDRs)
	cfg.FlowRollup.ClickHouseAddress = strings.TrimSpace(cfg.FlowRollup.ClickHouseAddress)
	cfg.FlowRollup.ClickHouseDatabase = strings.TrimSpace(cfg.FlowRollup.ClickHouseDatabase)
	cfg.FlowRollup.ClickHouseUser = strings.TrimSpace(cfg.FlowRollup.ClickHouseUser)
	cfg.FlowRollup.ClickHousePasswordFile = cleanOptionalConfigPath(cfg.FlowRollup.ClickHousePasswordFile)
	cfg.FlowRollup.ClickHouseCAFile = cleanOptionalConfigPath(cfg.FlowRollup.ClickHouseCAFile)
	cfg.FlowRollup.ClickHouseCertFile = cleanOptionalConfigPath(cfg.FlowRollup.ClickHouseCertFile)
	cfg.FlowRollup.ClickHouseKeyFile = cleanOptionalConfigPath(cfg.FlowRollup.ClickHouseKeyFile)
	cfg.FlowRollup.ClickHouseServerName = strings.TrimSpace(cfg.FlowRollup.ClickHouseServerName)
	cfg.Export.Dir = strings.TrimSpace(cfg.Export.Dir)
	cfg.Export.Metric = strings.TrimSpace(cfg.Export.Metric)
	cfg.AddressLibrary.Dir = strings.TrimSpace(cfg.AddressLibrary.Dir)
	cfg.SNMPCollector.TenantID = ID(strings.TrimSpace(string(cfg.SNMPCollector.TenantID)))
	cfg.SFlowCollector.Listen = strings.TrimSpace(cfg.SFlowCollector.Listen)
	cfg.SFlowCollector.TenantID = ID(strings.TrimSpace(string(cfg.SFlowCollector.TenantID)))
	cfg.CollectorPrincipalProvider.Name = strings.TrimSpace(cfg.CollectorPrincipalProvider.Name)
	cfg.CollectorPrincipalProvider.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.CollectorPrincipalProvider.BaseURL), "/")
	cfg.CollectorPrincipalProvider.PolicyRevision = strings.TrimSpace(cfg.CollectorPrincipalProvider.PolicyRevision)
	cfg.CollectorPrincipalProvider.TLSCAFile = cleanOptionalConfigPath(cfg.CollectorPrincipalProvider.TLSCAFile)
	cfg.CollectorPrincipalProvider.TLSCertFile = cleanOptionalConfigPath(cfg.CollectorPrincipalProvider.TLSCertFile)
	cfg.CollectorPrincipalProvider.TLSKeyFile = cleanOptionalConfigPath(cfg.CollectorPrincipalProvider.TLSKeyFile)
	cfg.CollectorPrincipalProvider.TLSServerName = strings.TrimSpace(cfg.CollectorPrincipalProvider.TLSServerName)
	cfg.SNMP.MIBDirs = normalizeStringPaths(cfg.SNMP.MIBDirs)
	cfg.SNMP.MIBLoad = strings.TrimSpace(cfg.SNMP.MIBLoad)
	cfg.SNMPTrapAgent.APIURL = normalizeBaseURL(cfg.SNMPTrapAgent.APIURL)
	cfg.SNMPTrapAgent.Listen = strings.TrimSpace(cfg.SNMPTrapAgent.Listen)
	cfg.Agent.HubURL = normalizeBaseURL(cfg.Agent.HubURL)
	cfg.Agent.AgentID = ID(strings.TrimSpace(string(cfg.Agent.AgentID)))
}

func normalizeStringList(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func cleanOptionalConfigPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return filepath.Clean(value)
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
	if _, err := decodeEncryptionKey(cfg.MySQL.EncryptionKey); err != nil {
		return fmt.Errorf("mysql.encryption_key: %w", err)
	}
	if err := validateHTTPBaseURL("victoriametrics.base_url", cfg.VictoriaMetrics.BaseURL, true); err != nil {
		return err
	}
	if err := validateMetricsScrapeConfig(cfg.MetricsScrape); err != nil {
		return err
	}
	if err := validateListenAddress("sflow_collector.listen", cfg.SFlowCollector.Listen); err != nil {
		return err
	}
	if err := validateFlowRollupConfig(cfg.FlowRollup); err != nil {
		return err
	}
	if cfg.SFlowCollector.AggInterval <= 0 || cfg.SFlowCollector.PrefixSyncInterval <= 0 {
		return errors.New("sflow_collector intervals must be positive")
	}
	if cfg.CollectorPrincipalProvider.Enabled {
		if err := validateRemoteCollectorPrincipalProviderConfig(cfg.CollectorPrincipalProvider); err != nil {
			return err
		}
	}
	if cfg.Export.Dir == "" || cfg.Export.WorkerInterval <= 0 || cfg.Export.WorkerBatch <= 0 {
		return errors.New("export dir, worker_interval, and worker_batch must be configured with positive worker values")
	}
	if cfg.Export.Metric != MetricSNMPIfInBps && cfg.Export.Metric != MetricSNMPIfOutBps {
		return fmt.Errorf("export.metric must be %q or %q", MetricSNMPIfInBps, MetricSNMPIfOutBps)
	}
	if cfg.AddressLibrary.Dir == "" || cfg.AddressLibrary.MaxUploadBytes <= 0 || cfg.AddressLibrary.MaxUploadBytes > 16<<30 ||
		cfg.AddressLibrary.ImportBatchSize <= 0 || cfg.AddressLibrary.ImportBatchSize > maxAddressImportBatch ||
		cfg.AddressLibrary.WorkerConcurrency <= 0 || cfg.AddressLibrary.WorkerConcurrency > 32 {
		return errors.New("address_library dir, upload limit, batch size, and worker concurrency are invalid")
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

func validateMetricsScrapeConfig(cfg MetricsScrapeConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.TokenFile == "" {
		return errors.New("enabled metrics_scrape requires token_file")
	}
	if len(cfg.AllowedCIDRs) == 0 {
		return errors.New("enabled metrics_scrape requires at least one allowed_cidrs entry")
	}
	for _, value := range cfg.AllowedCIDRs {
		if _, _, err := net.ParseCIDR(value); err != nil {
			return fmt.Errorf("metrics_scrape.allowed_cidrs contains invalid CIDR %q", value)
		}
	}
	return nil
}

func validateFlowRollupConfig(cfg FlowRollupConfig) error {
	if cfg.ScanInterval <= 0 || cfg.LateArrivalWindow < 0 || cfg.BootstrapLookback <= 0 ||
		cfg.MaxTenantsPerScan <= 0 || cfg.MaxTenantsPerScan > 10_000 ||
		cfg.MaxBucketsPerSeriesScan <= 0 || cfg.MaxBucketsPerScan <= 0 ||
		cfg.WorkerConcurrency <= 0 || cfg.WorkerConcurrency > 1_024 || cfg.LeaseFor <= 0 ||
		cfg.MaxAttempts == 0 || cfg.RetryBase <= 0 {
		return errors.New("flow_rollup intervals, lookback, budgets, concurrency, lease, and retry values are invalid")
	}
	if !cfg.Enabled {
		return nil
	}
	if cfg.ClickHouseAddress == "" || cfg.ClickHouseDatabase == "" || cfg.ClickHouseUser == "" ||
		cfg.ClickHouseMaxConns <= 0 || cfg.ClickHouseMaxConns > 1_024 || cfg.ClickHouseMinConns < 0 ||
		cfg.ClickHouseMinConns > cfg.ClickHouseMaxConns || cfg.ClickHouseDialTimeout <= 0 || cfg.ClickHouseReadTimeout <= 0 || cfg.ClickHouseOperationTimeout <= 0 {
		return errors.New("enabled flow_rollup requires valid ClickHouse endpoint, identity, connection limits, and timeouts")
	}
	if (cfg.ClickHouseCertFile == "") != (cfg.ClickHouseKeyFile == "") {
		return errors.New("flow_rollup ClickHouse TLS certificate and key must be configured together")
	}
	if !cfg.ClickHouseTLS && (cfg.ClickHouseCAFile != "" || cfg.ClickHouseCertFile != "" || cfg.ClickHouseKeyFile != "" || cfg.ClickHouseServerName != "") {
		return errors.New("flow_rollup ClickHouse TLS parameters require TLS to be enabled")
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
