package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigReadsOnlyTrapAgentSection(t *testing.T) {
	clearConfigEnvironment(t)
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	contents := `server:
  listen: "127.0.0.1:8091"
snmp_trap_agent:
  api_url: " http://127.0.0.1:8091/ "
  token: "secret"
  listen: " :1162 "
flow:
  retention_raw_days: 365
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err = normalizeAndValidateConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIURL != "http://127.0.0.1:8091" || cfg.Token != "secret" || cfg.Listen != ":1162" {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestLoadConfigAcceptsRepositoryConfigurations(t *testing.T) {
	clearConfigEnvironment(t)
	for _, name := range []string{"watchdog.example.yaml", "watchdog.dev.yaml"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(filepath.Join("..", "..", "config", name))
			if err != nil {
				t.Fatal(err)
			}
			cfg, err = normalizeAndValidateConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.APIURL != "http://127.0.0.1:8091" {
				t.Fatalf("API URL = %q", cfg.APIURL)
			}
		})
	}
}

func TestLoadConfigEnvironmentOverridesYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte("snmp_trap_agent:\n  api_url: http://old.invalid\n  token: old\n  listen: :162\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WATCHDOG_SNMP_TRAP_API_URL", "https://watchdog.example.com/")
	t.Setenv("WATCHDOG_SNMP_TRAP_TOKEN", "new-secret")
	t.Setenv("WATCHDOG_SNMP_TRAP_LISTEN", "127.0.0.1:2162")
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err = normalizeAndValidateConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIURL != "https://watchdog.example.com" || cfg.Token != "new-secret" || cfg.Listen != "127.0.0.1:2162" {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestLoadConfigRejectsUnknownTrapAgentField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte("snmp_trap_agent:\n  api_url: http://127.0.0.1:8091\n  token: secret\n  listen: :162\n  typo: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "field typo not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestNormalizeAndValidateConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  config
		want string
	}{
		{name: "missing API", cfg: config{Token: "secret", Listen: ":162"}, want: "api_url"},
		{name: "invalid API", cfg: config{APIURL: "localhost:8091", Token: "secret", Listen: ":162"}, want: "absolute http(s)"},
		{name: "query in API", cfg: config{APIURL: "http://localhost:8091?x=1", Token: "secret", Listen: ":162"}, want: "query or fragment"},
		{name: "missing token", cfg: config{APIURL: "http://localhost:8091", Listen: ":162"}, want: "token is required"},
		{name: "invalid listen", cfg: config{APIURL: "http://localhost:8091", Token: "secret", Listen: "162"}, want: "host:port"},
		{name: "zero port", cfg: config{APIURL: "http://localhost:8091", Token: "secret", Listen: ":0"}, want: "between 1 and 65535"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeAndValidateConfig(test.cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"WATCHDOG_CONFIG",
		"WATCHDOG_SNMP_TRAP_API_URL",
		"WATCHDOG_SNMP_TRAP_TOKEN",
		"WATCHDOG_SNMP_TRAP_LISTEN",
	} {
		value, ok := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if ok {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}
