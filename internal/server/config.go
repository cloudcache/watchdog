package server

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"gopkg.in/yaml.v3"
)

// Config is the KISS watchdog-server configuration, loaded from a YAML file.
// The file is the source of truth; only secrets (DB DSN, admin password) may be
// overridden by environment variables for deployment.
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	MySQL      MySQLConfig      `yaml:"mysql"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Flow       FlowConfig       `yaml:"flow"`
	AgentPlans AgentPlansConfig `yaml:"agent_plans"`
	Address    AddressConfig    `yaml:"address"`
	SNMP       SNMPConfig       `yaml:"snmp"`
	Billing    BillingConfig    `yaml:"billing"`
	Admin      AdminConfig      `yaml:"admin"`
}

// AgentPlansConfig keeps the one installation-wide signing root on disk. The
// public key is copied to agents during provisioning; private material never
// appears in an API response or MySQL.
type AgentPlansConfig struct {
	SigningKeyID      string        `yaml:"signing_key_id"`
	SigningPrivateKey string        `yaml:"signing_private_key"`
	DefaultTTL        time.Duration `yaml:"default_ttl"`
}

// ServerConfig — frontend and backend are separate builds, so the server never hosts static files.
type ServerConfig struct {
	Listen  string   `yaml:"listen"`  // e.g. "127.0.0.1:8091"
	Origins []string `yaml:"origins"` // CORS allowlist for the separate frontend build
}

type MySQLConfig struct {
	DSN string `yaml:"dsn"` // go-sql-driver DSN (the only management authority)
}

type ClickHouseConfig struct {
	Address      string `yaml:"address"` // host:port; flow facts + SNMP/system time-series + log/alert
	Database     string `yaml:"database"`
	Username     string `yaml:"username"`
	PasswordFile string `yaml:"password_file"`
	// OperationTimeout bounds interactive statements at the transport layer.
	// BatchOperationTimeout is separate because rollup/reconciliation statements
	// legitimately run longer and must not inherit an implicit library default.
	OperationTimeout      time.Duration `yaml:"operation_timeout"`
	BatchOperationTimeout time.Duration `yaml:"batch_operation_timeout"`
	// MaxConns/MinConns size the interactive pool (flow queries, SNMP reads,
	// billing). BatchMaxConns sizes a separate pool for the background rollup,
	// reclassification, reconciliation and VPN jobs, so a long job cannot exhaust
	// the interactive pool and stall user queries behind an Acquire wait.
	MaxConns      int32                `yaml:"max_conns"`
	MinConns      int32                `yaml:"min_conns"`
	BatchMaxConns int32                `yaml:"batch_max_conns"`
	TLS           flowstream.TLSConfig `yaml:"tls"`
}

// KafkaConfig is the RawFlow transport (buffering/replay only, never a query store).
type KafkaConfig struct {
	Brokers          []string `yaml:"brokers"`
	Topic            string   `yaml:"topic"`
	ConsumerGroup    string   `yaml:"consumer_group"`
	SASLMechanism    string   `yaml:"sasl_mechanism"`
	SASLUsername     string   `yaml:"sasl_username"`
	SASLPasswordFile string   `yaml:"sasl_password_file"`
}

// FlowConfig contains runtime locations and workers only. Flow retention is an
// immutable, auditable management-plane policy in MySQL; keeping a second set
// of day-count knobs here would create a non-functional competing authority.
type FlowConfig struct {
	Geo            FlowGeoConfig            `yaml:"geo"`
	VPN            FlowVPNConfig            `yaml:"vpn"`
	Export         FlowExportConfig         `yaml:"export"`
	Query          FlowQueryConfig          `yaml:"query"`
	HotRollup      FlowHotRollupConfig      `yaml:"hot_rollup"`
	Reconciliation FlowReconciliationConfig `yaml:"reconciliation"`
}

// FlowQueryConfig owns the operational limits for interactive and asynchronous
// Flow reads. Keeping these values in deployment configuration avoids coupling
// browser request deadlines and ClickHouse execution limits to release builds.
type FlowQueryConfig struct {
	ExecutionTimeout       time.Duration `yaml:"execution_timeout"`
	SynchronousTimeout     time.Duration `yaml:"synchronous_timeout"`
	SynchronousMaxRange    time.Duration `yaml:"synchronous_max_range"`
	PanelConcurrency       int           `yaml:"panel_concurrency"`
	AsyncPollInterval      time.Duration `yaml:"async_poll_interval"`
	AsyncWorkerConcurrency int           `yaml:"async_worker_concurrency"`
	AsyncWorkerPoll        time.Duration `yaml:"async_worker_poll_interval"`
	AsyncWorkerLease       time.Duration `yaml:"async_worker_lease"`
	AsyncWorkerMaxAttempts uint32        `yaml:"async_worker_max_attempts"`
	AsyncWorkerRetryBase   time.Duration `yaml:"async_worker_retry_base"`
	AsyncResultDir         string        `yaml:"async_result_dir"`
	AsyncResultRetention   time.Duration `yaml:"async_result_retention"`
}

// FlowHotRollupConfig controls the non-destructive recent aggregate cache. It
// uses the same generation-marked rollup as the reconciled lifecycle archive,
// but generations stay below 2^32 so a later policy archive always supersedes
// them. This is query acceleration only; it never authorizes raw deletion.
type FlowHotRollupConfig struct {
	Enabled                 bool          `yaml:"enabled"`
	ScanInterval            time.Duration `yaml:"scan_interval"`
	SealDelay               time.Duration `yaml:"seal_delay"`
	MinuteLookback          time.Duration `yaml:"minute_lookback"`
	HourLookback            time.Duration `yaml:"hour_lookback"`
	MinuteLateArrivalWindow time.Duration `yaml:"minute_late_arrival_window"`
	HourLateArrivalWindow   time.Duration `yaml:"hour_late_arrival_window"`
	RepairInterval          time.Duration `yaml:"repair_interval"`
	MaxMinuteBucketsPerRun  int           `yaml:"max_minute_buckets_per_run"`
	MaxHourBucketsPerRun    int           `yaml:"max_hour_buckets_per_run"`
	MaxThreads              uint64        `yaml:"max_threads"`
	Priority                uint64        `yaml:"priority"`
	MaxMemoryBytes          uint64        `yaml:"max_memory_bytes"`
	MinimumGeneration       uint64        `yaml:"minimum_generation"`
}

// FlowReconciliationConfig schedules cold-path Kafka-to-ClickHouse count and
// counter reconciliation. BootstrapOffsets are consulted only for partitions
// without a durable MySQL watermark; an omitted entry never means offset zero.
type FlowReconciliationConfig struct {
	Enabled          bool              `yaml:"enabled"`
	SourceStreamID   string            `yaml:"source_stream_id"`
	Interval         time.Duration     `yaml:"interval"`
	BootstrapOffsets map[uint32]uint64 `yaml:"bootstrap_offsets"`
	MaxBatches       int               `yaml:"max_batches"`
	MaxFactRows      int               `yaml:"max_fact_rows"`
	MaxReadBytes     uint64            `yaml:"max_read_bytes"`
}

// FlowExportConfig drives the async flow-record detail export worker: it writes CSV
// or Parquet artifacts to Dir and serves them until Retention elapses after
// completion.
type FlowExportConfig struct {
	Dir       string        `yaml:"dir"`
	Retention time.Duration `yaml:"retention"`
}

// FlowVPNConfig drives the background VPN detection pipeline: per closed window it
// materializes candidates from flow_records, scores them against the active rule
// set, and writes findings. Disabled by default; the risk thresholds are the one
// rule-set-level policy (individual rules carry only weights).
type FlowVPNConfig struct {
	Enabled           bool    `yaml:"enabled"`
	WindowSeconds     int     `yaml:"window_seconds"`   // candidate window size (default 300)
	IntervalSeconds   int     `yaml:"interval_seconds"` // scheduler tick (default 300)
	LagSeconds        int     `yaml:"lag_seconds"`      // only process windows closed at least this long ago (default 120)
	MaxCandidates     uint32  `yaml:"max_candidates"`   // per-window cap (default 50000)
	MediumThreshold   uint16  `yaml:"medium_threshold"` // 0<medium<high<critical<=100
	HighThreshold     uint16  `yaml:"high_threshold"`
	CriticalThreshold uint16  `yaml:"critical_threshold"`
	ProbeThreshold    uint16  `yaml:"probe_threshold"`
	MinCompleteness   float64 `yaml:"min_completeness"`
}

// AddressConfig controls the address-library source-import store: where uploaded
// MMDB/IPDB artifacts are kept and the per-upload size ceiling.
type AddressConfig struct {
	ArtifactDir    string `yaml:"artifact_dir"`     // local dir for uploaded source databases
	MaxUploadBytes int64  `yaml:"max_upload_bytes"` // 0 -> 2 GiB default
	SnapshotDir    string `yaml:"snapshot_dir"`     // local dir for published WADS dimension objects
}

// SNMPConfig layers optional LibreNMS definitions/vendor MIBs over the
// collector's embedded standard MIB bundle. An empty definitions_dir keeps
// generic IF/IP/BGP/ENTITY discovery available without OS-specific labels.
type SNMPConfig struct {
	DefinitionsDir     string        `yaml:"definitions_dir"`
	DefinitionsVersion string        `yaml:"definitions_version"`
	MIBDirs            []string      `yaml:"mib_dirs"`
	MIBLoad            string        `yaml:"mib_load"`
	PollInterval       time.Duration `yaml:"poll_interval"`
	PollLimit          int           `yaml:"poll_limit"`
	PollConcurrency    int           `yaml:"poll_concurrency"`
	// AutoDiscover runs the server-side discovery reconcile loop so that binding
	// an SNMP profile to a network device actually starts collection: it
	// discovers devices that have a profile but no enabled recipes, and (like
	// LibreNMS separating discovery from polling) re-discovers healthy devices
	// every RediscoverInterval to pick up new interfaces/sensors over time.
	AutoDiscover       bool          `yaml:"auto_discover"`
	RediscoverInterval time.Duration `yaml:"rediscover_interval"`
	ExportDir          string        `yaml:"export_dir"`
	ExportRetention    time.Duration `yaml:"export_retention"`
	// QueryMaxIntermediateRows bounds synchronous chart expansion before
	// correction/aggregation/downsampling. It is not the final chart point
	// count and it is unrelated to billing formula or account limits.
	QueryMaxIntermediateRows uint32        `yaml:"query_max_intermediate_rows"`
	QueryMaxExecutionTime    time.Duration `yaml:"query_max_execution_time"`
	QueryMaxRowsToRead       uint64        `yaml:"query_max_rows_to_read"`
	QueryMaxBytesToRead      uint64        `yaml:"query_max_bytes_to_read"`
	QueryMaxMemoryBytes      uint64        `yaml:"query_max_memory_bytes"`
}

type BillingConfig struct {
	ExportDir           string        `yaml:"export_dir"`
	ExportRetention     time.Duration `yaml:"export_retention"`
	MaxAccountPorts     int           `yaml:"max_account_ports"`
	MaxPageSize         int           `yaml:"max_page_size"`
	MaxPeriodDuration   time.Duration `yaml:"max_period_duration"`
	MaxExportRows       int           `yaml:"max_export_rows"`
	MaxPublicationRefs  int           `yaml:"max_publication_refs"`
	MaxPublicationBytes int           `yaml:"max_publication_bytes"`
	WorkerPollInterval  time.Duration `yaml:"worker_poll_interval"`
	WorkerLease         time.Duration `yaml:"worker_lease"`
	WorkerMaxAttempts   uint32        `yaml:"worker_max_attempts"`
	WorkerRetryBase     time.Duration `yaml:"worker_retry_base"`
}

// AdminConfig is used only to bootstrap the first administrator on an empty install.
type AdminConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"` // optional explicit password for unattended installation
}

// DefaultConfigPath is used when neither --config nor WATCHDOG_CONFIG is set.
const DefaultConfigPath = "config/watchdog.yaml"

func defaultConfig() Config {
	return Config{
		Server: ServerConfig{Listen: "127.0.0.1:8091", Origins: []string{"http://127.0.0.1:8090"}},
		MySQL:  MySQLConfig{DSN: "root:@tcp(127.0.0.1:3306)/watchdog?parseTime=true&loc=UTC&charset=utf8mb4"},
		ClickHouse: ClickHouseConfig{
			Address: "127.0.0.1:9000", Database: "watchdog_flow", Username: "default",
			OperationTimeout: 2 * time.Minute, BatchOperationTimeout: 15 * time.Minute,
			MaxConns: 8, MinConns: 1, BatchMaxConns: 1,
		},
		Kafka: KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "watchdog.flow.raw", ConsumerGroup: "watchdog-flow-worker"},
		Flow: FlowConfig{
			Query: FlowQueryConfig{
				ExecutionTimeout: 2 * time.Minute, SynchronousTimeout: 25 * time.Second,
				SynchronousMaxRange: time.Hour, PanelConcurrency: 3, AsyncPollInterval: time.Second,
					AsyncWorkerConcurrency: 1,
				AsyncWorkerPoll:        500 * time.Millisecond, AsyncWorkerLease: 30 * time.Second,
				AsyncWorkerMaxAttempts: 3, AsyncWorkerRetryBase: 5 * time.Second,
				AsyncResultDir: "data/flow-query-results", AsyncResultRetention: 24 * time.Hour,
			},
			HotRollup: FlowHotRollupConfig{
				Enabled: false, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
				MinuteLookback: 6 * time.Hour, HourLookback: 72 * time.Hour,
				MinuteLateArrivalWindow: 30 * time.Minute, HourLateArrivalWindow: 2 * time.Hour,
				RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 60, MaxHourBucketsPerRun: 4,
				MaxThreads: 4, Priority: 10, MaxMemoryBytes: 6 << 30,
			},
			Reconciliation: FlowReconciliationConfig{
				Interval: 5 * time.Minute, MaxBatches: 1000, MaxFactRows: 250_000, MaxReadBytes: 512 << 20,
			},
		},
		AgentPlans: AgentPlansConfig{SigningKeyID: "watchdog-agent-plan-v1", SigningPrivateKey: "data/agent-plan-ed25519.pem", DefaultTTL: 365 * 24 * time.Hour},
		Address:    AddressConfig{ArtifactDir: "data/address-artifacts", MaxUploadBytes: 2 << 30, SnapshotDir: "data/dimension-snapshots"},
		SNMP: SNMPConfig{
			PollInterval: time.Minute, PollLimit: 500, PollConcurrency: 32,
			AutoDiscover: true, RediscoverInterval: 6 * time.Hour,
			ExportDir: "data/snmp-exports", ExportRetention: 24 * time.Hour,
			QueryMaxIntermediateRows: snmpch.HardMaxAggregateRows,
			QueryMaxExecutionTime:    15 * time.Second, QueryMaxRowsToRead: 50_000_000,
			QueryMaxBytesToRead: 4 << 30, QueryMaxMemoryBytes: 2 << 30,
		},
		Billing: BillingConfig{
			ExportDir: "data/billing-exports", ExportRetention: 7 * 24 * time.Hour,
			MaxAccountPorts: 1000, MaxPageSize: 500, MaxPeriodDuration: 400 * 24 * time.Hour, MaxExportRows: 100000,
			MaxPublicationRefs: 10000, MaxPublicationBytes: 8 << 20,
			WorkerPollInterval: 2 * time.Second, WorkerLease: 30 * time.Second, WorkerMaxAttempts: 5, WorkerRetryBase: 30 * time.Second,
		},
		Admin: AdminConfig{Username: "admin"},
	}
}

// LoadConfig reads the YAML config at path (or WATCHDOG_CONFIG, or the default).
// A missing file is not fatal: sensible defaults are used so the server can start
// out of the box; a present file overrides them. Secret env overrides are applied last.
func LoadConfig(path string) (Config, error) {
	cfg := defaultConfig()
	if path == "" {
		path = getenv("WATCHDOG_CONFIG", DefaultConfigPath)
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := rejectDeprecatedFlowLifecycleConfig(data); err != nil {
			return Config{}, fmt.Errorf("parse config %s: %w", path, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %s: %w", path, err)
		}
	case os.IsNotExist(err):
		// keep defaults; the server logs that it is running without a config file
	default:
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	applySecretEnvOverrides(&cfg)
	if cfg.Admin.Username == "" {
		cfg.Admin.Username = "admin"
	}
	if err := validateClickHouseConfig(cfg.ClickHouse); err != nil {
		return Config{}, fmt.Errorf("validate ClickHouse: %w", err)
	}
	if err := validateFlowReconciliationConfig(cfg); err != nil {
		return Config{}, fmt.Errorf("validate flow reconciliation: %w", err)
	}
	if err := validateFlowQueryConfig(cfg.Flow.Query); err != nil {
		return Config{}, fmt.Errorf("validate flow query: %w", err)
	}
	if err := validateFlowHotRollupConfig(cfg.Flow.HotRollup); err != nil {
		return Config{}, fmt.Errorf("validate flow hot rollup: %w", err)
	}
	if err := validateBillingConfig(cfg.Billing); err != nil {
		return Config{}, fmt.Errorf("validate billing: %w", err)
	}
	if err := validateSNMPConfig(cfg.SNMP); err != nil {
		return Config{}, fmt.Errorf("validate SNMP: %w", err)
	}
	return cfg, nil
}

func validateClickHouseConfig(cfg ClickHouseConfig) error {
	if cfg.OperationTimeout <= 0 || cfg.BatchOperationTimeout <= 0 {
		return errors.New("interactive and batch operation timeouts must be positive")
	}
	return nil
}

func validateFlowQueryConfig(cfg FlowQueryConfig) error {
	if cfg.ExecutionTimeout <= 0 || cfg.SynchronousTimeout <= 0 || cfg.SynchronousMaxRange <= 0 {
		return errors.New("execution timeout, synchronous timeout and synchronous max range must be positive")
	}
	if cfg.PanelConcurrency < 1 || cfg.PanelConcurrency > 32 {
		return errors.New("panel concurrency must be between 1 and 32")
	}
	if cfg.AsyncWorkerConcurrency < 1 || cfg.AsyncWorkerConcurrency > 8 {
		return errors.New("async worker concurrency must be between 1 and 8")
	}
	if cfg.AsyncPollInterval < 100*time.Millisecond || cfg.AsyncWorkerPoll <= 0 || cfg.AsyncWorkerLease < 3*time.Second ||
		cfg.AsyncWorkerMaxAttempts == 0 || cfg.AsyncWorkerRetryBase <= 0 || cfg.AsyncResultRetention <= 0 {
		return errors.New("async poll, worker lease/retry and result retention limits are invalid")
	}
	if strings.TrimSpace(cfg.AsyncResultDir) == "" {
		return errors.New("async result directory is required")
	}
	return nil
}

func validateFlowHotRollupConfig(cfg FlowHotRollupConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.ScanInterval < 10*time.Second || cfg.SealDelay < time.Minute ||
		cfg.MinuteLookback < time.Minute || cfg.MinuteLookback > 48*time.Hour || cfg.HourLookback < time.Hour ||
		cfg.MinuteLateArrivalWindow < 0 || cfg.MinuteLateArrivalWindow > cfg.MinuteLookback ||
		cfg.HourLateArrivalWindow < 0 || cfg.HourLateArrivalWindow > cfg.HourLookback ||
		cfg.RepairInterval < time.Minute || cfg.MinuteLookback%time.Minute != 0 ||
		cfg.HourLookback%time.Hour != 0 || cfg.MinuteLateArrivalWindow%time.Minute != 0 ||
		cfg.HourLateArrivalWindow%time.Hour != 0 || cfg.MaxMinuteBucketsPerRun < 1 ||
		cfg.MaxMinuteBucketsPerRun > 1440 || cfg.MaxHourBucketsPerRun < 1 || cfg.MaxHourBucketsPerRun > 168 ||
		cfg.MaxThreads > 256 || cfg.Priority > 10_000 ||
		cfg.MinimumGeneration >= lifecycleGenerationFloor ||
		(cfg.MaxMemoryBytes > 0 && cfg.MaxMemoryBytes < 1<<30) {
		return errors.New("hot rollup intervals, lookback or bucket budget are invalid")
	}
	return nil
}

func validateSNMPConfig(cfg SNMPConfig) error {
	return snmpch.ValidateQueryLimits(snmpQueryLimits(cfg))
}

func snmpQueryLimits(cfg SNMPConfig) snmpch.QueryLimits {
	return snmpch.QueryLimits{
		MaxResultRows: cfg.QueryMaxIntermediateRows, MaxExecutionTime: cfg.QueryMaxExecutionTime,
		MaxRowsToRead: cfg.QueryMaxRowsToRead, MaxBytesToRead: cfg.QueryMaxBytesToRead,
		MaxMemoryBytes: cfg.QueryMaxMemoryBytes,
	}
}

func validateBillingConfig(cfg BillingConfig) error {
	if cfg.MaxAccountPorts <= 0 || cfg.MaxPageSize <= 0 || cfg.MaxPeriodDuration <= 0 || cfg.MaxExportRows <= 0 ||
		cfg.MaxPublicationRefs <= 0 || cfg.MaxPublicationBytes <= 0 {
		return errors.New("billing limits must be positive")
	}
	if cfg.WorkerPollInterval <= 0 || cfg.WorkerLease < 3*time.Second || cfg.WorkerMaxAttempts == 0 || cfg.WorkerRetryBase <= 0 {
		return errors.New("billing worker poll, lease, attempts and retry base must be positive; lease must be at least 3s")
	}
	if cfg.MaxAccountPorts > billing.HardMaxAccountPorts || cfg.MaxPeriodDuration > billing.HardMaxPeriodDuration {
		return fmt.Errorf("billing account ports and period duration cannot exceed current reader ceilings (%d ports, %s)", billing.HardMaxAccountPorts, billing.HardMaxPeriodDuration)
	}
	return nil
}

func validateFlowReconciliationConfig(cfg Config) error {
	reconciliation := cfg.Flow.Reconciliation
	if !reconciliation.Enabled {
		return nil
	}
	if reconciliation.Interval < time.Minute {
		return errors.New("interval must be at least one minute")
	}
	_, err := flowReconciliationPayload(cfg)
	return err
}

func rejectDeprecatedFlowLifecycleConfig(data []byte) error {
	var document struct {
		Flow struct {
			RetentionRawDays    *int `yaml:"retention_raw_days"`
			DownsampleAfterDays *int `yaml:"downsample_after_days"`
			Rollup1mDays        *int `yaml:"rollup_1m_days"`
			Rollup1hDays        *int `yaml:"rollup_1h_days"`
		} `yaml:"flow"`
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		return err
	}
	if document.Flow.RetentionRawDays != nil || document.Flow.DownsampleAfterDays != nil ||
		document.Flow.Rollup1mDays != nil || document.Flow.Rollup1hDays != nil {
		return errors.New("flow retention_raw_days/downsample_after_days/rollup_*_days were removed; publish the global Flow lifecycle policy through the management API")
	}
	return nil
}

// applySecretEnvOverrides lets secrets be injected without committing them to the file.
func applySecretEnvOverrides(cfg *Config) {
	if v := strings.TrimSpace(os.Getenv("WATCHDOG_MYSQL_DSN")); v != "" {
		cfg.MySQL.DSN = v
	}
	if v := strings.TrimSpace(os.Getenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE")); v != "" {
		cfg.ClickHouse.PasswordFile = v
	}
	if v := os.Getenv("WATCHDOG_ADMIN_PASSWORD"); v != "" {
		cfg.Admin.Password = v
	}
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
