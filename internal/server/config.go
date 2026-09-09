package server

import (
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

// FlowConfig is the explicit global retention/downsample policy (no hardcoded 30 days).
type FlowConfig struct {
	RetentionRawDays    int `yaml:"retention_raw_days"`
	DownsampleAfterDays int `yaml:"downsample_after_days"`
	Rollup1mDays        int `yaml:"rollup_1m_days"`
	Rollup1hDays        int `yaml:"rollup_1h_days"`
}

// AddressConfig controls the address-library source-import store: where uploaded
// MMDB/IPDB artifacts are kept and the per-upload size ceiling.
type AddressConfig struct {
	ArtifactDir    string                    `yaml:"artifact_dir"`     // local dir for uploaded source databases
	MaxUploadBytes int64                     `yaml:"max_upload_bytes"` // 0 -> 2 GiB default
	SnapshotDir    string                    `yaml:"snapshot_dir"`     // local dir for published WADS dimension objects
	TrustedKeys    []AddressTrustedKeyConfig `yaml:"trusted_keys"`     // ed25519 dimension-publication approval keys
}

// AddressTrustedKeyConfig is one trusted ed25519 dimension-publication signing key.
type AddressTrustedKeyConfig struct {
	KeyID         string `yaml:"key_id"`
	PublicKeyFile string `yaml:"public_key_file"`
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
	Password string `yaml:"password"` // empty -> a random password is generated and logged once
}

// DefaultConfigPath is used when neither --config nor WATCHDOG_CONFIG is set.
const DefaultConfigPath = "config/watchdog.yaml"

func defaultConfig() Config {
	return Config{
		Server:     ServerConfig{Listen: "127.0.0.1:8091", Origins: []string{"http://127.0.0.1:8090"}},
		MySQL:      MySQLConfig{DSN: "root:@tcp(127.0.0.1:3306)/watchdog?parseTime=true&loc=UTC&charset=utf8mb4"},
		ClickHouse: ClickHouseConfig{Address: "127.0.0.1:9000", Database: "watchdog_flow", Username: "default"},
		Kafka:      KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "watchdog.flow.raw", ConsumerGroup: "watchdog-flow-worker"},
		Flow:       FlowConfig{RetentionRawDays: 365, DownsampleAfterDays: 365, Rollup1mDays: 180, Rollup1hDays: 400},
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

// applySecretEnvOverrides lets secrets be injected without committing them to the file.
func applySecretEnvOverrides(cfg *Config) {
	if v := strings.TrimSpace(os.Getenv("WATCHDOG_MYSQL_DSN")); v != "" {
		cfg.MySQL.DSN = v
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
