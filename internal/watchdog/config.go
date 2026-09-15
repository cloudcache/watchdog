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

	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowworker"
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
	defaultExportWorkerConcurrency     = 2
	defaultSNMPCollectorInterval       = time.Minute
	defaultSNMPCollectorPollLimit      = 500
	defaultSNMPDiscoveryInterval       = 30 * time.Second
	defaultSNMPDiscoveryBatch          = 10
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
	defaultFlowRollupReaperInterval    = time.Minute
	defaultFlowRollupReaperBucketScan  = 720
	defaultFlowRollupReaperRetryCap    = uint32(5)
	defaultFlowRollupReaperReconcile   = 2 * time.Hour
	defaultFlowRollupWorkerConcurrency = 2
	defaultFlowRollupLease             = 2 * time.Minute
	defaultFlowRollupMaxAttempts       = uint32(5)
	defaultFlowRollupRetryBase         = 30 * time.Second
	defaultFlowStorageScanInterval     = 5 * time.Minute
	defaultFlowStoragePolicyScanLimit  = 500
	defaultFlowStoragePartitionBudget  = 100
	defaultFlowStorageConcurrency      = 2
	defaultFlowStorageLease            = 30 * time.Minute
	defaultFlowStorageMaxAttempts      = uint32(5)
	defaultFlowStorageRetryBase        = time.Minute
	defaultFlowReconciliationCron      = "*/5 * * * *"
	defaultFlowReconciliationBatches   = 5_000
	defaultFlowReconciliationFacts     = 500_000
	defaultFlowReconciliationReadBytes = uint64(512 << 20)
	defaultFlowReconciliationLease     = 5 * time.Minute
	defaultFlowReconciliationAttempts  = uint32(5)
	defaultFlowReconciliationRetryBase = 30 * time.Second
	defaultAddressLibraryDir           = "address-artifacts"
	defaultAddressLibraryBatchSize     = 1_000
	defaultAddressLibraryWorkers       = 1
	defaultAddressSnapshotMaxBytes     = 512 << 20
	defaultAddressObjectRetention      = 30 * 24 * time.Hour
	defaultAddressObjectGCInterval     = 5 * time.Minute
	defaultAddressObjectGCBatch        = 100
	defaultQueryVMMaxConcurrent        = 8
	defaultQueryCHMaxConcurrent        = 16
)

type BackendConfig struct {
	MySQL              MySQLConfig              `yaml:"mysql"`
	VictoriaMetrics    VictoriaMetricsConfig    `yaml:"victoriametrics"`
	MetricsScrape      MetricsScrapeConfig      `yaml:"metrics_scrape"`
	Export             ExportConfig             `yaml:"export"`
	AddressLibrary     AddressLibraryConfig     `yaml:"address_library"`
	SNMPCollector      SNMPCollectorConfig      `yaml:"snmp_collector"`
	FlowGeo            FlowGeoConfig            `yaml:"flow_geo"`
	FlowRollup         FlowRollupConfig         `yaml:"flow_rollup"`
	FlowStorage        FlowStorageConfig        `yaml:"flow_storage"`
	FlowReconciliation FlowReconciliationConfig `yaml:"flow_reconciliation"`
	QueryGateway       QueryGatewayConfig       `yaml:"query_gateway"`

	CollectorPrincipalProvider RemoteCollectorPrincipalProviderConfig `yaml:"collector_principal_provider"`
	CollectorPlanSigning       CollectorPlanSigningConfig             `yaml:"collector_plan_signing"`

	SNMP          SNMPConfig          `yaml:"snmp"`
	SNMPTrapAgent SNMPTrapAgentConfig `yaml:"snmp_trap_agent"`
	Agent         AgentClientConfig   `yaml:"agent"`
}

// MetricsScrapeConfig exposes the composed hub metrics on the existing HTTP
// listener. TokenFile is used instead of an inline token so production secrets
// do not enter YAML, process arguments, or environment values.
type MetricsScrapeConfig struct {
	Enabled      bool     `yaml:"enabled"`
	TokenFile    string   `yaml:"token_file"`
	AllowedCIDRs []string `yaml:"allowed_cidrs"`
}

// QueryGatewayConfig controls provider admission only. Endpoints and secrets
// remain in their existing provider sections so one dependency has one source
// of truth.
type QueryGatewayConfig struct {
	Enabled                   bool `yaml:"enabled"`
	VictoriaMetricsEnabled    bool `yaml:"victoriametrics_enabled"`
	VictoriaMetricsConcurrent int  `yaml:"victoriametrics_max_concurrent"`
	ClickHouseEnabled         bool `yaml:"clickhouse_enabled"`
	ClickHouseConcurrent      int  `yaml:"clickhouse_max_concurrent"`
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

	// Reaper (F1/F2): the completion-watermark loop that re-drives failed/missing
	// buckets and reconciles late base data. ReaperInterval <= 0 disables it.
	ReaperInterval          time.Duration `yaml:"reaper_interval"`
	ReaperMaxBucketsPerScan int           `yaml:"reaper_max_buckets_per_scan"`
	ReaperRetryCap          uint32        `yaml:"reaper_retry_cap"`
	ReaperReconcileWindow   time.Duration `yaml:"reaper_reconcile_window"`

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

// FlowStorageConfig controls only the sealed-day lifecycle. ClickHouse
// connectivity remains in flow_rollup during the compatibility window so the
// hub has one pool and one secret source; lifecycle policy values themselves
// are tenant-owned immutable revisions in MySQL, not static config.
type FlowStorageConfig struct {
	Enabled              bool          `yaml:"enabled"`
	ScanInterval         time.Duration `yaml:"scan_interval"`
	MaxPoliciesPerScan   int           `yaml:"max_policies_per_scan"`
	MaxPartitionsPerScan int           `yaml:"max_partitions_per_scan"`
	WorkerConcurrency    int           `yaml:"worker_concurrency"`
	LeaseFor             time.Duration `yaml:"lease_for"`
	MaxAttempts          uint32        `yaml:"max_attempts"`
	RetryBase            time.Duration `yaml:"retry_base"`
}

// FlowReconciliationConfig owns only the system-scope Kafka→ClickHouse audit.
// ClickHouse connectivity is shared with flow_rollup; Kafka secrets remain in
// owner-only files and are loaded once at startup.
type FlowReconciliationConfig struct {
	Enabled               bool             `yaml:"enabled"`
	SourceStreamID        string           `yaml:"source_stream_id"`
	KafkaBrokers          []string         `yaml:"kafka_brokers"`
	KafkaTopic            string           `yaml:"kafka_topic"`
	KafkaConsumerGroup    string           `yaml:"kafka_consumer_group"`
	KafkaClientID         string           `yaml:"kafka_client_id"`
	ScheduleCron          string           `yaml:"schedule_cron"`
	BootstrapOffsets      map[int32]uint64 `yaml:"bootstrap_offsets"`
	MaxBatches            int              `yaml:"max_batches"`
	MaxFactRows           int              `yaml:"max_fact_rows"`
	MaxReadBytes          uint64           `yaml:"max_read_bytes"`
	LeaseFor              time.Duration    `yaml:"lease_for"`
	MaxAttempts           uint32           `yaml:"max_attempts"`
	RetryBase             time.Duration    `yaml:"retry_base"`
	KafkaTLS              bool             `yaml:"kafka_tls"`
	KafkaCAFile           string           `yaml:"kafka_tls_ca"`
	KafkaCertFile         string           `yaml:"kafka_tls_cert"`
	KafkaKeyFile          string           `yaml:"kafka_tls_key"`
	KafkaServerName       string           `yaml:"kafka_tls_server_name"`
	KafkaSASLMechanism    string           `yaml:"kafka_sasl_mechanism"`
	KafkaSASLUsername     string           `yaml:"kafka_sasl_username"`
	KafkaSASLPasswordFile string           `yaml:"kafka_sasl_password_file"`
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

// CollectorPlanSigningConfig identifies the platform-global signing key. The
// private key is loaded from an owner-only file and never stored in MySQL.
type CollectorPlanSigningConfig struct {
	KeyID          string `yaml:"key_id"`
	PrivateKeyFile string `yaml:"private_key_file"`
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

type ExportConfig struct {
	Dir               string        `yaml:"dir"`
	WorkerInterval    time.Duration `yaml:"worker_interval"`
	WorkerBatch       int           `yaml:"worker_batch"`
	WorkerConcurrency int           `yaml:"worker_concurrency"`
	Metric            string        `yaml:"metric"`
}

type AddressLibraryConfig struct {
	Dir               string                             `yaml:"dir"`
	OwnerTenantID     ID                                 `yaml:"owner_tenant_id"`
	MaxUploadBytes    int64                              `yaml:"max_upload_bytes"`
	MaxSnapshotBytes  int                                `yaml:"max_snapshot_bytes"`
	ImportBatchSize   int                                `yaml:"import_batch_size"`
	WorkerConcurrency int                                `yaml:"worker_concurrency"`
	ObjectRetention   time.Duration                      `yaml:"object_retention"`
	ObjectGCInterval  time.Duration                      `yaml:"object_gc_interval"`
	ObjectGCBatch     int                                `yaml:"object_gc_batch"`
	TrustedKeys       []AddressDimensionTrustedKeyConfig `yaml:"trusted_keys"`
}

type AddressDimensionTrustedKeyConfig struct {
	TenantID      ID     `yaml:"tenant_id"`
	KeyID         string `yaml:"key_id"`
	PublicKeyFile string `yaml:"public_key_file"`
}

type SNMPCollectorConfig struct {
	TenantID          ID            `yaml:"tenant_id"`
	Interval          time.Duration `yaml:"interval"`
	PollLimit         int           `yaml:"poll_limit"`
	DiscoveryInterval time.Duration `yaml:"discovery_interval"`
	DiscoveryBatch    int           `yaml:"discovery_batch"`
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
		QueryGateway: QueryGatewayConfig{
			Enabled: true, VictoriaMetricsEnabled: true,
			VictoriaMetricsConcurrent: defaultQueryVMMaxConcurrent,
			ClickHouseConcurrent:      defaultQueryCHMaxConcurrent,
		},
		Export: ExportConfig{
			Dir:               defaultExportDir,
			WorkerInterval:    defaultExportWorkerInterval,
			WorkerBatch:       defaultExportWorkerBatch,
			WorkerConcurrency: defaultExportWorkerConcurrency,
			Metric:            MetricSNMPIfInBps,
		},
		AddressLibrary: AddressLibraryConfig{
			Dir: defaultAddressLibraryDir, OwnerTenantID: "tenant_dev", MaxUploadBytes: DefaultAddressArtifactMaxBytes,
			MaxSnapshotBytes: defaultAddressSnapshotMaxBytes,
			ImportBatchSize:  defaultAddressLibraryBatchSize, WorkerConcurrency: defaultAddressLibraryWorkers,
			ObjectRetention: defaultAddressObjectRetention, ObjectGCInterval: defaultAddressObjectGCInterval,
			ObjectGCBatch: defaultAddressObjectGCBatch,
		},
		SNMPCollector: SNMPCollectorConfig{
			TenantID:          "tenant_dev",
			Interval:          defaultSNMPCollectorInterval,
			PollLimit:         defaultSNMPCollectorPollLimit,
			DiscoveryInterval: defaultSNMPDiscoveryInterval,
			DiscoveryBatch:    defaultSNMPDiscoveryBatch,
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
			ReaperInterval: defaultFlowRollupReaperInterval, ReaperMaxBucketsPerScan: defaultFlowRollupReaperBucketScan,
			ReaperRetryCap: defaultFlowRollupReaperRetryCap, ReaperReconcileWindow: defaultFlowRollupReaperReconcile,
			WorkerConcurrency: defaultFlowRollupWorkerConcurrency, LeaseFor: defaultFlowRollupLease,
			MaxAttempts: defaultFlowRollupMaxAttempts, RetryBase: defaultFlowRollupRetryBase,
			ClickHouseAddress: "127.0.0.1:9000", ClickHouseDatabase: "watchdog_flow", ClickHouseUser: "default",
			ClickHouseMaxConns: 2, ClickHouseMinConns: 0,
			ClickHouseDialTimeout: 3 * time.Second, ClickHouseReadTimeout: 90 * time.Second,
			ClickHouseOperationTimeout: 5 * time.Minute,
		},
		FlowStorage: FlowStorageConfig{
			ScanInterval: defaultFlowStorageScanInterval, MaxPoliciesPerScan: defaultFlowStoragePolicyScanLimit,
			MaxPartitionsPerScan: defaultFlowStoragePartitionBudget, WorkerConcurrency: defaultFlowStorageConcurrency,
			LeaseFor: defaultFlowStorageLease, MaxAttempts: defaultFlowStorageMaxAttempts, RetryBase: defaultFlowStorageRetryBase,
		},
		FlowReconciliation: FlowReconciliationConfig{
			KafkaBrokers: []string{"127.0.0.1:9092"}, KafkaTopic: "watchdog.flow.raw-v1",
			KafkaConsumerGroup: "watchdog-flow-worker-v1", KafkaClientID: "watchdog-flow-reconciliation",
			ScheduleCron: defaultFlowReconciliationCron, BootstrapOffsets: map[int32]uint64{},
			MaxBatches: defaultFlowReconciliationBatches, MaxFactRows: defaultFlowReconciliationFacts,
			MaxReadBytes: defaultFlowReconciliationReadBytes, LeaseFor: defaultFlowReconciliationLease,
			MaxAttempts: defaultFlowReconciliationAttempts, RetryBase: defaultFlowReconciliationRetryBase,
			KafkaSASLMechanism: string(flowstream.SASLNone),
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
	if cfg.QueryGateway.Enabled, err = getEnvBool("WATCHDOG_QUERY_GATEWAY_ENABLED", cfg.QueryGateway.Enabled); err != nil {
		return err
	}
	if cfg.QueryGateway.VictoriaMetricsEnabled, err = getEnvBool("WATCHDOG_QUERY_VM_ENABLED", cfg.QueryGateway.VictoriaMetricsEnabled); err != nil {
		return err
	}
	if cfg.QueryGateway.VictoriaMetricsConcurrent, err = getEnvInt("WATCHDOG_QUERY_VM_MAX_CONCURRENT", cfg.QueryGateway.VictoriaMetricsConcurrent, 1); err != nil {
		return err
	}
	if cfg.QueryGateway.ClickHouseEnabled, err = getEnvBool("WATCHDOG_QUERY_CLICKHOUSE_ENABLED", cfg.QueryGateway.ClickHouseEnabled); err != nil {
		return err
	}
	if cfg.QueryGateway.ClickHouseConcurrent, err = getEnvInt("WATCHDOG_QUERY_CLICKHOUSE_MAX_CONCURRENT", cfg.QueryGateway.ClickHouseConcurrent, 1); err != nil {
		return err
	}
	if cfg.MetricsScrape.Enabled, err = getEnvBool("WATCHDOG_METRICS_SCRAPE_ENABLED", cfg.MetricsScrape.Enabled); err != nil {
		return err
	}
	cfg.MetricsScrape.TokenFile = getEnv("WATCHDOG_METRICS_SCRAPE_TOKEN_FILE", cfg.MetricsScrape.TokenFile)
	cfg.MetricsScrape.AllowedCIDRs = getEnvCommaList("WATCHDOG_METRICS_SCRAPE_ALLOWED_CIDRS", cfg.MetricsScrape.AllowedCIDRs)
	cfg.FlowGeo.Path = getEnv("WATCHDOG_FLOW_GEO_PATH", cfg.FlowGeo.Path)
	cfg.FlowGeo.HistoricalPaths = getEnvCommaList("WATCHDOG_FLOW_GEO_HISTORICAL_PATHS", cfg.FlowGeo.HistoricalPaths)
	if cfg.FlowRollup.Enabled, err = getEnvBool("WATCHDOG_FLOW_ROLLUP_ENABLED", cfg.FlowRollup.Enabled); err != nil {
		return err
	}
	if cfg.FlowStorage.Enabled, err = getEnvBool("WATCHDOG_FLOW_STORAGE_ENABLED", cfg.FlowStorage.Enabled); err != nil {
		return err
	}
	if cfg.FlowStorage.ScanInterval, err = getEnvDuration("WATCHDOG_FLOW_STORAGE_SCAN_INTERVAL", cfg.FlowStorage.ScanInterval); err != nil {
		return err
	}
	if cfg.FlowStorage.MaxPoliciesPerScan, err = getEnvInt("WATCHDOG_FLOW_STORAGE_MAX_POLICIES_PER_SCAN", cfg.FlowStorage.MaxPoliciesPerScan, 1); err != nil {
		return err
	}
	if cfg.FlowStorage.MaxPartitionsPerScan, err = getEnvInt("WATCHDOG_FLOW_STORAGE_MAX_PARTITIONS_PER_SCAN", cfg.FlowStorage.MaxPartitionsPerScan, 1); err != nil {
		return err
	}
	if cfg.FlowStorage.WorkerConcurrency, err = getEnvInt("WATCHDOG_FLOW_STORAGE_WORKER_CONCURRENCY", cfg.FlowStorage.WorkerConcurrency, 1); err != nil {
		return err
	}
	if cfg.FlowStorage.LeaseFor, err = getEnvDuration("WATCHDOG_FLOW_STORAGE_LEASE_FOR", cfg.FlowStorage.LeaseFor); err != nil {
		return err
	}
	if cfg.FlowStorage.MaxAttempts, err = getEnvUint32("WATCHDOG_FLOW_STORAGE_MAX_ATTEMPTS", cfg.FlowStorage.MaxAttempts); err != nil {
		return err
	}
	if cfg.FlowStorage.RetryBase, err = getEnvDuration("WATCHDOG_FLOW_STORAGE_RETRY_BASE", cfg.FlowStorage.RetryBase); err != nil {
		return err
	}
	if cfg.FlowReconciliation.Enabled, err = getEnvBool("WATCHDOG_FLOW_RECONCILIATION_ENABLED", cfg.FlowReconciliation.Enabled); err != nil {
		return err
	}
	cfg.FlowReconciliation.SourceStreamID = getEnv("WATCHDOG_FLOW_RECONCILIATION_SOURCE_STREAM_ID", cfg.FlowReconciliation.SourceStreamID)
	cfg.FlowReconciliation.KafkaBrokers = getEnvCommaList("WATCHDOG_FLOW_RECONCILIATION_KAFKA_BROKERS", cfg.FlowReconciliation.KafkaBrokers)
	cfg.FlowReconciliation.KafkaTopic = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_TOPIC", cfg.FlowReconciliation.KafkaTopic)
	cfg.FlowReconciliation.KafkaConsumerGroup = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_CONSUMER_GROUP", cfg.FlowReconciliation.KafkaConsumerGroup)
	cfg.FlowReconciliation.KafkaClientID = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_CLIENT_ID", cfg.FlowReconciliation.KafkaClientID)
	cfg.FlowReconciliation.ScheduleCron = getEnv("WATCHDOG_FLOW_RECONCILIATION_SCHEDULE_CRON", cfg.FlowReconciliation.ScheduleCron)
	if cfg.FlowReconciliation.BootstrapOffsets, err = getEnvPartitionOffsets("WATCHDOG_FLOW_RECONCILIATION_BOOTSTRAP_OFFSETS", cfg.FlowReconciliation.BootstrapOffsets); err != nil {
		return err
	}
	if cfg.FlowReconciliation.MaxBatches, err = getEnvInt("WATCHDOG_FLOW_RECONCILIATION_MAX_BATCHES", cfg.FlowReconciliation.MaxBatches, 1); err != nil {
		return err
	}
	if cfg.FlowReconciliation.MaxFactRows, err = getEnvInt("WATCHDOG_FLOW_RECONCILIATION_MAX_FACT_ROWS", cfg.FlowReconciliation.MaxFactRows, 1); err != nil {
		return err
	}
	if cfg.FlowReconciliation.MaxReadBytes, err = getEnvUint64("WATCHDOG_FLOW_RECONCILIATION_MAX_READ_BYTES", cfg.FlowReconciliation.MaxReadBytes, 1); err != nil {
		return err
	}
	if cfg.FlowReconciliation.LeaseFor, err = getEnvDuration("WATCHDOG_FLOW_RECONCILIATION_LEASE_FOR", cfg.FlowReconciliation.LeaseFor); err != nil {
		return err
	}
	if cfg.FlowReconciliation.MaxAttempts, err = getEnvUint32("WATCHDOG_FLOW_RECONCILIATION_MAX_ATTEMPTS", cfg.FlowReconciliation.MaxAttempts); err != nil {
		return err
	}
	if cfg.FlowReconciliation.RetryBase, err = getEnvDuration("WATCHDOG_FLOW_RECONCILIATION_RETRY_BASE", cfg.FlowReconciliation.RetryBase); err != nil {
		return err
	}
	if cfg.FlowReconciliation.KafkaTLS, err = getEnvBool("WATCHDOG_FLOW_RECONCILIATION_KAFKA_TLS", cfg.FlowReconciliation.KafkaTLS); err != nil {
		return err
	}
	cfg.FlowReconciliation.KafkaCAFile = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_TLS_CA", cfg.FlowReconciliation.KafkaCAFile)
	cfg.FlowReconciliation.KafkaCertFile = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_TLS_CERT", cfg.FlowReconciliation.KafkaCertFile)
	cfg.FlowReconciliation.KafkaKeyFile = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_TLS_KEY", cfg.FlowReconciliation.KafkaKeyFile)
	cfg.FlowReconciliation.KafkaServerName = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_TLS_SERVER_NAME", cfg.FlowReconciliation.KafkaServerName)
	cfg.FlowReconciliation.KafkaSASLMechanism = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_SASL_MECHANISM", cfg.FlowReconciliation.KafkaSASLMechanism)
	cfg.FlowReconciliation.KafkaSASLUsername = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_SASL_USERNAME", cfg.FlowReconciliation.KafkaSASLUsername)
	cfg.FlowReconciliation.KafkaSASLPasswordFile = getEnv("WATCHDOG_FLOW_RECONCILIATION_KAFKA_SASL_PASSWORD_FILE", cfg.FlowReconciliation.KafkaSASLPasswordFile)
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
	if cfg.FlowRollup.ReaperInterval, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_REAPER_INTERVAL", cfg.FlowRollup.ReaperInterval); err != nil {
		return err
	}
	if cfg.FlowRollup.ReaperMaxBucketsPerScan, err = getEnvInt("WATCHDOG_FLOW_ROLLUP_REAPER_MAX_BUCKETS_PER_SCAN", cfg.FlowRollup.ReaperMaxBucketsPerScan, 1); err != nil {
		return err
	}
	if cfg.FlowRollup.ReaperRetryCap, err = getEnvUint32("WATCHDOG_FLOW_ROLLUP_REAPER_RETRY_CAP", cfg.FlowRollup.ReaperRetryCap); err != nil {
		return err
	}
	if cfg.FlowRollup.ReaperReconcileWindow, err = getEnvDuration("WATCHDOG_FLOW_ROLLUP_REAPER_RECONCILE_WINDOW", cfg.FlowRollup.ReaperReconcileWindow); err != nil {
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
	cfg.CollectorPlanSigning.KeyID = getEnv("WATCHDOG_COLLECTOR_PLAN_SIGNING_KEY_ID", cfg.CollectorPlanSigning.KeyID)
	cfg.CollectorPlanSigning.PrivateKeyFile = getEnv("WATCHDOG_COLLECTOR_PLAN_SIGNING_PRIVATE_KEY_FILE", cfg.CollectorPlanSigning.PrivateKeyFile)
	cfg.Export.Dir = getEnv("WATCHDOG_EXPORT_DIR", cfg.Export.Dir)
	if cfg.Export.WorkerInterval, err = getEnvDuration("WATCHDOG_EXPORT_WORKER_INTERVAL", cfg.Export.WorkerInterval); err != nil {
		return err
	}
	if cfg.Export.WorkerBatch, err = getEnvInt("WATCHDOG_EXPORT_WORKER_BATCH", cfg.Export.WorkerBatch, 1); err != nil {
		return err
	}
	if cfg.Export.WorkerConcurrency, err = getEnvInt("WATCHDOG_EXPORT_WORKER_CONCURRENCY", cfg.Export.WorkerConcurrency, 1); err != nil {
		return err
	}
	cfg.Export.Metric = getEnv("WATCHDOG_EXPORT_METRIC", cfg.Export.Metric)
	cfg.AddressLibrary.Dir = getEnv("WATCHDOG_ADDRESS_LIBRARY_DIR", cfg.AddressLibrary.Dir)
	cfg.AddressLibrary.OwnerTenantID = ID(getEnv("WATCHDOG_ADDRESS_LIBRARY_OWNER_TENANT_ID", string(cfg.AddressLibrary.OwnerTenantID)))
	if cfg.AddressLibrary.MaxUploadBytes, err = getEnvInt64("WATCHDOG_ADDRESS_LIBRARY_MAX_UPLOAD_BYTES", cfg.AddressLibrary.MaxUploadBytes, 1); err != nil {
		return err
	}
	if cfg.AddressLibrary.MaxSnapshotBytes, err = getEnvInt("WATCHDOG_ADDRESS_LIBRARY_MAX_SNAPSHOT_BYTES", cfg.AddressLibrary.MaxSnapshotBytes, 1); err != nil {
		return err
	}
	if cfg.AddressLibrary.ImportBatchSize, err = getEnvInt("WATCHDOG_ADDRESS_LIBRARY_IMPORT_BATCH_SIZE", cfg.AddressLibrary.ImportBatchSize, 1); err != nil {
		return err
	}
	if cfg.AddressLibrary.WorkerConcurrency, err = getEnvInt("WATCHDOG_ADDRESS_LIBRARY_WORKER_CONCURRENCY", cfg.AddressLibrary.WorkerConcurrency, 1); err != nil {
		return err
	}
	if cfg.AddressLibrary.ObjectRetention, err = getEnvDuration("WATCHDOG_ADDRESS_LIBRARY_OBJECT_RETENTION", cfg.AddressLibrary.ObjectRetention); err != nil {
		return err
	}
	if cfg.AddressLibrary.ObjectGCInterval, err = getEnvDuration("WATCHDOG_ADDRESS_LIBRARY_OBJECT_GC_INTERVAL", cfg.AddressLibrary.ObjectGCInterval); err != nil {
		return err
	}
	if cfg.AddressLibrary.ObjectGCBatch, err = getEnvInt("WATCHDOG_ADDRESS_LIBRARY_OBJECT_GC_BATCH", cfg.AddressLibrary.ObjectGCBatch, 1); err != nil {
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

func getEnvUint64(key string, fallback, minimum uint64) (uint64, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < minimum {
		return fallback, fmt.Errorf("%s must be an integer >= %d", key, minimum)
	}
	return parsed, nil
}

func getEnvPartitionOffsets(key string, fallback map[int32]uint64) (map[int32]uint64, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	result := make(map[int32]uint64)
	if strings.TrimSpace(value) == "" {
		return result, nil
	}
	for _, part := range splitConfigList(value) {
		pair := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(pair) != 2 {
			return fallback, fmt.Errorf("%s must use partition=offset pairs", key)
		}
		partition, partitionErr := strconv.ParseInt(strings.TrimSpace(pair[0]), 10, 32)
		offset, offsetErr := strconv.ParseUint(strings.TrimSpace(pair[1]), 10, 64)
		if partitionErr != nil || partition < 0 || offsetErr != nil {
			return fallback, fmt.Errorf("%s contains invalid partition=offset pair %q", key, part)
		}
		if _, exists := result[int32(partition)]; exists {
			return fallback, fmt.Errorf("%s contains duplicate partition %d", key, partition)
		}
		result[int32(partition)] = offset
	}
	return result, nil
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
	cfg.FlowReconciliation.SourceStreamID = strings.TrimSpace(cfg.FlowReconciliation.SourceStreamID)
	cfg.FlowReconciliation.KafkaBrokers = normalizeStringList(cfg.FlowReconciliation.KafkaBrokers)
	cfg.FlowReconciliation.KafkaTopic = strings.TrimSpace(cfg.FlowReconciliation.KafkaTopic)
	cfg.FlowReconciliation.KafkaConsumerGroup = strings.TrimSpace(cfg.FlowReconciliation.KafkaConsumerGroup)
	cfg.FlowReconciliation.KafkaClientID = strings.TrimSpace(cfg.FlowReconciliation.KafkaClientID)
	cfg.FlowReconciliation.ScheduleCron = strings.TrimSpace(cfg.FlowReconciliation.ScheduleCron)
	cfg.FlowReconciliation.KafkaCAFile = cleanOptionalConfigPath(cfg.FlowReconciliation.KafkaCAFile)
	cfg.FlowReconciliation.KafkaCertFile = cleanOptionalConfigPath(cfg.FlowReconciliation.KafkaCertFile)
	cfg.FlowReconciliation.KafkaKeyFile = cleanOptionalConfigPath(cfg.FlowReconciliation.KafkaKeyFile)
	cfg.FlowReconciliation.KafkaServerName = strings.TrimSpace(cfg.FlowReconciliation.KafkaServerName)
	cfg.FlowReconciliation.KafkaSASLMechanism = strings.TrimSpace(cfg.FlowReconciliation.KafkaSASLMechanism)
	cfg.FlowReconciliation.KafkaSASLUsername = strings.TrimSpace(cfg.FlowReconciliation.KafkaSASLUsername)
	cfg.FlowReconciliation.KafkaSASLPasswordFile = cleanOptionalConfigPath(cfg.FlowReconciliation.KafkaSASLPasswordFile)
	cfg.Export.Dir = strings.TrimSpace(cfg.Export.Dir)
	cfg.Export.Metric = strings.TrimSpace(cfg.Export.Metric)
	cfg.AddressLibrary.Dir = strings.TrimSpace(cfg.AddressLibrary.Dir)
	cfg.AddressLibrary.OwnerTenantID = ID(strings.TrimSpace(string(cfg.AddressLibrary.OwnerTenantID)))
	for index := range cfg.AddressLibrary.TrustedKeys {
		key := &cfg.AddressLibrary.TrustedKeys[index]
		key.TenantID = ID(strings.TrimSpace(string(key.TenantID)))
		key.KeyID = strings.TrimSpace(key.KeyID)
		key.PublicKeyFile = cleanOptionalConfigPath(key.PublicKeyFile)
	}
	cfg.SNMPCollector.TenantID = ID(strings.TrimSpace(string(cfg.SNMPCollector.TenantID)))
	cfg.CollectorPrincipalProvider.Name = strings.TrimSpace(cfg.CollectorPrincipalProvider.Name)
	cfg.CollectorPrincipalProvider.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.CollectorPrincipalProvider.BaseURL), "/")
	cfg.CollectorPrincipalProvider.PolicyRevision = strings.TrimSpace(cfg.CollectorPrincipalProvider.PolicyRevision)
	cfg.CollectorPrincipalProvider.TLSCAFile = cleanOptionalConfigPath(cfg.CollectorPrincipalProvider.TLSCAFile)
	cfg.CollectorPrincipalProvider.TLSCertFile = cleanOptionalConfigPath(cfg.CollectorPrincipalProvider.TLSCertFile)
	cfg.CollectorPrincipalProvider.TLSKeyFile = cleanOptionalConfigPath(cfg.CollectorPrincipalProvider.TLSKeyFile)
	cfg.CollectorPrincipalProvider.TLSServerName = strings.TrimSpace(cfg.CollectorPrincipalProvider.TLSServerName)
	cfg.CollectorPlanSigning.KeyID = strings.TrimSpace(cfg.CollectorPlanSigning.KeyID)
	cfg.CollectorPlanSigning.PrivateKeyFile = cleanOptionalConfigPath(cfg.CollectorPlanSigning.PrivateKeyFile)
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
	if err := validateQueryGatewayConfig(cfg.QueryGateway); err != nil {
		return err
	}
	if err := validateFlowRollupConfig(cfg.FlowRollup, cfg.FlowRollup.Enabled || cfg.FlowStorage.Enabled || cfg.FlowReconciliation.Enabled || (cfg.QueryGateway.Enabled && cfg.QueryGateway.ClickHouseEnabled)); err != nil {
		return err
	}
	if err := validateFlowStorageConfig(cfg.FlowStorage); err != nil {
		return err
	}
	if err := validateFlowReconciliationConfig(cfg.FlowReconciliation); err != nil {
		return err
	}
	if cfg.FlowRollup.Enabled && cfg.FlowStorage.Enabled {
		return errors.New("flow_rollup and flow_storage cannot be enabled together; complete the Storage V2 maintenance-window cutover first")
	}
	if cfg.CollectorPrincipalProvider.Enabled {
		if err := validateRemoteCollectorPrincipalProviderConfig(cfg.CollectorPrincipalProvider); err != nil {
			return err
		}
	}
	if (cfg.CollectorPlanSigning.KeyID == "") != (cfg.CollectorPlanSigning.PrivateKeyFile == "") {
		return errors.New("collector_plan_signing.key_id and private_key_file must be configured together")
	}
	if cfg.CollectorPlanSigning.KeyID != "" && !validCollectorPlanSigningKeyID(cfg.CollectorPlanSigning.KeyID) {
		return errors.New("collector_plan_signing.key_id is invalid")
	}
	if cfg.Export.Dir == "" || cfg.Export.WorkerInterval <= 0 || cfg.Export.WorkerBatch <= 0 {
		return errors.New("export dir, worker_interval, and worker_batch must be configured with positive worker values")
	}
	if cfg.Export.WorkerConcurrency < 1 || cfg.Export.WorkerConcurrency > 32 {
		return errors.New("export.worker_concurrency must be between 1 and 32")
	}
	if cfg.Export.Metric != MetricSNMPIfInBps && cfg.Export.Metric != MetricSNMPIfOutBps {
		return fmt.Errorf("export.metric must be %q or %q", MetricSNMPIfInBps, MetricSNMPIfOutBps)
	}
	if cfg.AddressLibrary.Dir == "" || cfg.AddressLibrary.OwnerTenantID == "" || cfg.AddressLibrary.MaxUploadBytes <= 0 || cfg.AddressLibrary.MaxUploadBytes > 16<<30 ||
		cfg.AddressLibrary.MaxSnapshotBytes <= 0 || int64(cfg.AddressLibrary.MaxSnapshotBytes) > 4<<30 ||
		cfg.AddressLibrary.ImportBatchSize <= 0 || cfg.AddressLibrary.ImportBatchSize > maxAddressImportBatch ||
		cfg.AddressLibrary.WorkerConcurrency <= 0 || cfg.AddressLibrary.WorkerConcurrency > 32 ||
		cfg.AddressLibrary.ObjectRetention <= 0 || cfg.AddressLibrary.ObjectGCInterval <= 0 ||
		cfg.AddressLibrary.ObjectGCBatch <= 0 || cfg.AddressLibrary.ObjectGCBatch > 1_000 {
		return errors.New("address_library dir, upload limit, batch size, and worker concurrency are invalid")
	}
	trustedKeys := make(map[string]struct{}, len(cfg.AddressLibrary.TrustedKeys))
	for _, key := range cfg.AddressLibrary.TrustedKeys {
		if key.TenantID == "" || key.KeyID == "" || len(key.KeyID) > 128 || key.PublicKeyFile == "" {
			return errors.New("address_library.trusted_keys require tenant_id, key_id, and public_key_file")
		}
		if key.TenantID != cfg.AddressLibrary.OwnerTenantID {
			return fmt.Errorf("address_library.trusted_keys tenant %q must match owner_tenant_id %q", key.TenantID, cfg.AddressLibrary.OwnerTenantID)
		}
		lookup := addressDimensionTrustedKeyLookup(key.TenantID, key.KeyID)
		if _, exists := trustedKeys[lookup]; exists {
			return fmt.Errorf("address_library.trusted_keys contains duplicate key_id %q for tenant %q", key.KeyID, key.TenantID)
		}
		trustedKeys[lookup] = struct{}{}
	}
	if cfg.SNMPCollector.Interval <= 0 || cfg.SNMPCollector.PollLimit <= 0 || cfg.SNMPCollector.DiscoveryInterval <= 0 || cfg.SNMPCollector.DiscoveryBatch <= 0 {
		return errors.New("snmp_collector interval, limit, and batch values must be positive")
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

func validateQueryGatewayConfig(cfg QueryGatewayConfig) error {
	if cfg.VictoriaMetricsConcurrent < 1 || cfg.VictoriaMetricsConcurrent > 4096 ||
		cfg.ClickHouseConcurrent < 1 || cfg.ClickHouseConcurrent > 4096 {
		return errors.New("query_gateway provider concurrency must be between 1 and 4096")
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

func validateFlowRollupConfig(cfg FlowRollupConfig, requireClickHouse bool) error {
	if cfg.ScanInterval <= 0 || cfg.LateArrivalWindow < 0 || cfg.BootstrapLookback <= 0 ||
		cfg.MaxTenantsPerScan <= 0 || cfg.MaxTenantsPerScan > 10_000 ||
		cfg.MaxBucketsPerSeriesScan <= 0 || cfg.MaxBucketsPerScan <= 0 ||
		cfg.WorkerConcurrency <= 0 || cfg.WorkerConcurrency > 1_024 || cfg.LeaseFor <= 0 ||
		cfg.MaxAttempts == 0 || cfg.RetryBase <= 0 {
		return errors.New("flow_rollup intervals, lookback, budgets, concurrency, lease, and retry values are invalid")
	}
	if !requireClickHouse {
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

func validateFlowStorageConfig(cfg FlowStorageConfig) error {
	if cfg.ScanInterval <= 0 || cfg.MaxPoliciesPerScan < 1 || cfg.MaxPoliciesPerScan > 10_000 ||
		cfg.MaxPartitionsPerScan < 1 || cfg.MaxPartitionsPerScan > 100_000 ||
		cfg.WorkerConcurrency < 1 || cfg.WorkerConcurrency > 128 || cfg.LeaseFor < time.Minute ||
		cfg.MaxAttempts < 1 || cfg.MaxAttempts > 100 || cfg.RetryBase <= 0 {
		return errors.New("flow_storage scan, budget, concurrency, lease, and retry values are invalid")
	}
	return nil
}

func validateFlowReconciliationConfig(cfg FlowReconciliationConfig) error {
	if cfg.MaxBatches < 1 || cfg.MaxBatches > 10_000 || cfg.MaxFactRows < 1 || cfg.MaxFactRows > 1_000_000 ||
		cfg.MaxReadBytes < 1 || cfg.LeaseFor < time.Minute || cfg.MaxAttempts < 1 || cfg.MaxAttempts > 100 || cfg.RetryBase <= 0 {
		return errors.New("flow_reconciliation scan budgets, lease, and retry values are invalid")
	}
	if _, err := nextOperationJobScheduleTime(cfg.ScheduleCron, "UTC", time.Now()); err != nil {
		return fmt.Errorf("flow_reconciliation.schedule_cron: %w", err)
	}
	if !cfg.Enabled {
		return nil
	}
	if !flowworker.ValidSourceStreamID(cfg.SourceStreamID) {
		return errors.New("enabled flow_reconciliation requires a valid source_stream_id")
	}
	if cfg.KafkaSASLMechanism == "" {
		cfg.KafkaSASLMechanism = string(flowstream.SASLNone)
	}
	password := ""
	if cfg.KafkaSASLPasswordFile != "" {
		password = "configured-in-secret-file"
	}
	kafka := flowstream.KafkaConfig{
		Brokers: cfg.KafkaBrokers, Topic: cfg.KafkaTopic, ClientID: cfg.KafkaClientID,
		TLS: flowstream.TLSConfig{Enabled: cfg.KafkaTLS, CAFile: cfg.KafkaCAFile, CertFile: cfg.KafkaCertFile,
			KeyFile: cfg.KafkaKeyFile, ServerName: cfg.KafkaServerName},
		SASL: flowstream.SASLConfig{Mechanism: flowstream.SASLMechanism(cfg.KafkaSASLMechanism), Username: cfg.KafkaSASLUsername, Password: password},
	}
	if err := kafka.Validate(); err != nil {
		return fmt.Errorf("flow_reconciliation Kafka: %w", err)
	}
	if strings.TrimSpace(cfg.KafkaConsumerGroup) == "" || len(cfg.KafkaConsumerGroup) > 255 {
		return errors.New("enabled flow_reconciliation requires a valid kafka_consumer_group")
	}
	for partition := range cfg.BootstrapOffsets {
		if partition < 0 {
			return errors.New("flow_reconciliation bootstrap partition must not be negative")
		}
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

func ValidateSNMPCollectorConfig(cfg SNMPCollectorConfig) error {
	if cfg.TenantID == "" {
		return errors.New("snmp_collector.tenant_id is required")
	}
	if cfg.Interval <= 0 || cfg.PollLimit <= 0 || cfg.DiscoveryInterval <= 0 || cfg.DiscoveryBatch <= 0 {
		return errors.New("snmp_collector interval, limit, and batch values must be positive")
	}
	return nil
}
