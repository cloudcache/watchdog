package server

import (
	"fmt"
	"os"
	"strings"

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
	Admin      AdminConfig      `yaml:"admin"`
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
