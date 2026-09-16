package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAgentConfigReadsOnlyAgentSectionAndAppliesEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	content := "mysql:\n  unrelated: true\nagent:\n  hub_url: http://127.0.0.1:8091/\n  agent_id: yaml-agent\n  token: yaml-token\n  interval: 30s\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WATCHDOG_AGENT_ID", "env-agent")
	cfg, err := loadAgentConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HubURL != "http://127.0.0.1:8091/" || cfg.AgentID != "env-agent" || cfg.Token != "yaml-token" || cfg.Interval != 30*time.Second {
		t.Fatalf("config = %+v", cfg)
	}
	cfg, err = normalizeAndValidateAgentConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HubURL != "http://127.0.0.1:8091" {
		t.Fatalf("normalized hub URL = %q", cfg.HubURL)
	}
}

func TestLoadAgentConfigRejectsUnknownAgentField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watchdog.yaml")
	if err := os.WriteFile(path, []byte("agent:\n  unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAgentConfig(path); err == nil || !strings.Contains(err.Error(), "field unknown") {
		t.Fatalf("unknown agent field error = %v", err)
	}
}

func TestLoadAgentConfigRejectsInvalidIntervalEnvironment(t *testing.T) {
	t.Setenv("WATCHDOG_AGENT_INTERVAL", "not-a-duration")
	if _, err := loadAgentConfig(""); err == nil {
		t.Fatal("invalid WATCHDOG_AGENT_INTERVAL accepted")
	}
}
