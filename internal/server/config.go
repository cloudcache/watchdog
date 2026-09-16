package server

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

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
}

// KafkaConfig is the RawFlow transport (buffering/replay only, never a query store).
type KafkaConfig struct {
	Brokers          []string `yaml:"brokers"`
	Topic            string   `yaml:"topic"`
	ConsumerGroup    string   `yaml:"consumer_group"`
	SASLUsername     string   `yaml:"sasl_username"`
	SASLPasswordFile string   `yaml:"sasl_password_file"`
}

// FlowConfig contains runtime locations and workers only. Flow retention is an
// immutable, auditable management-plane policy in MySQL; keeping a second set
// of day-count knobs here would create a non-functional competing authority.
type FlowConfig struct {
	Geo    FlowGeoConfig    `yaml:"geo"`
	VPN    FlowVPNConfig    `yaml:"vpn"`
	Export FlowExportConfig `yaml:"export"`
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
	ExportDir          string        `yaml:"export_dir"`
	ExportRetention    time.Duration `yaml:"export_retention"`
}

type BillingConfig struct {
	ExportDir       string        `yaml:"export_dir"`
	ExportRetention time.Duration `yaml:"export_retention"`
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
		Server:     ServerConfig{Listen: "127.0.0.1:8091", Origins: []string{"http://127.0.0.1:8090"}},
		MySQL:      MySQLConfig{DSN: "root:@tcp(127.0.0.1:3306)/watchdog?parseTime=true&loc=UTC&charset=utf8mb4"},
		ClickHouse: ClickHouseConfig{Address: "127.0.0.1:9000", Database: "watchdog_flow", Username: "default"},
		Kafka:      KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "watchdog.flow.raw", ConsumerGroup: "watchdog-flow-worker"},
		AgentPlans: AgentPlansConfig{SigningKeyID: "watchdog-agent-plan-v1", SigningPrivateKey: "data/agent-plan-ed25519.pem", DefaultTTL: 365 * 24 * time.Hour},
		Address:    AddressConfig{ArtifactDir: "data/address-artifacts", MaxUploadBytes: 2 << 30, SnapshotDir: "data/dimension-snapshots"},
		SNMP:       SNMPConfig{PollInterval: time.Minute, PollLimit: 500, PollConcurrency: 32, ExportDir: "data/snmp-exports", ExportRetention: 24 * time.Hour},
		Billing:    BillingConfig{ExportDir: "data/billing-exports", ExportRetention: 7 * 24 * time.Hour},
		Admin:      AdminConfig{Username: "admin"},
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
	return cfg, nil
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
