package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigYAMLAndSecretOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "watchdog.yaml")
	if err := os.WriteFile(path, []byte(`
server:
  listen: "127.0.0.1:19091"
  origins: ["http://127.0.0.1:8090"]
mysql:
  dsn: "file-user@tcp(localhost:3306)/from_file"
admin:
  username: "operator"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WATCHDOG_MYSQL_DSN", "env-user@tcp(localhost:3306)/from_env")
	t.Setenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE", "/run/secrets/watchdog-clickhouse")
	t.Setenv("WATCHDOG_ADMIN_PASSWORD", "env-secret")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen != "127.0.0.1:19091" || cfg.Admin.Username != "operator" {
		t.Fatalf("yaml was not loaded: %+v", cfg)
	}
	if cfg.MySQL.DSN != "env-user@tcp(localhost:3306)/from_env" || cfg.Admin.Password != "env-secret" {
		t.Fatal("secret environment overrides were not applied")
	}
	if cfg.ClickHouse.PasswordFile != "/run/secrets/watchdog-clickhouse" {
		t.Fatal("ClickHouse secret-file override was not applied")
	}
}

func TestLoadConfigMissingFileUsesDefaults(t *testing.T) {
	t.Setenv("WATCHDOG_MYSQL_DSN", "")
	t.Setenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE", "")
	t.Setenv("WATCHDOG_ADMIN_PASSWORD", "")
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen == "" || cfg.MySQL.DSN == "" || cfg.Admin.Username != "admin" {
		t.Fatalf("incomplete defaults: %+v", cfg)
	}
}

func TestValidDatabaseName(t *testing.T) {
	for _, name := range []string{"watchdog", "watchdog_test", "watchdog$1"} {
		if !validDatabaseName(name) {
			t.Errorf("expected %q to be valid", name)
		}
	}
	for _, name := range []string{"", "watchdog-test", "bad`name", "bad name"} {
		if validDatabaseName(name) {
			t.Errorf("expected %q to be rejected", name)
		}
	}
}
