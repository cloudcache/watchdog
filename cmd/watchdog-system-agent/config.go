package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type agentClientConfig struct {
	HubURL   string        `yaml:"hub_url"`
	AgentID  string        `yaml:"agent_id"`
	Token    string        `yaml:"token"`
	Interval time.Duration `yaml:"interval"`
}

// loadAgentConfig reads only the standalone system agent's section from the
// shared YAML file. Unrelated platform configuration is intentionally outside
// this process's lifecycle and validation boundary.
func loadAgentConfig(path string) (agentClientConfig, error) {
	cfg := agentClientConfig{Interval: time.Minute}
	if strings.TrimSpace(path) == "" {
		path = strings.TrimSpace(os.Getenv("WATCHDOG_CONFIG"))
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, err
		}
		var document struct {
			Agent yaml.Node `yaml:"agent"`
		}
		if err := yaml.Unmarshal(data, &document); err != nil {
			return cfg, fmt.Errorf("decode watchdog config %q: %w", path, err)
		}
		if document.Agent.Kind != 0 {
			section, err := yaml.Marshal(&document.Agent)
			if err != nil {
				return cfg, fmt.Errorf("decode agent config: %w", err)
			}
			decoder := yaml.NewDecoder(strings.NewReader(string(section)))
			decoder.KnownFields(true)
			if err := decoder.Decode(&cfg); err != nil {
				return cfg, fmt.Errorf("decode watchdog config %q agent: %w", path, err)
			}
		}
	}
	if value, ok := os.LookupEnv("WATCHDOG_AGENT_HUB_URL"); ok {
		cfg.HubURL = value
	}
	if value, ok := os.LookupEnv("WATCHDOG_AGENT_ID"); ok {
		cfg.AgentID = value
	}
	if value, ok := os.LookupEnv("WATCHDOG_AGENT_TOKEN"); ok {
		cfg.Token = value
	}
	if value, ok := os.LookupEnv("WATCHDOG_AGENT_INTERVAL"); ok {
		interval, err := time.ParseDuration(value)
		if err != nil {
			return cfg, fmt.Errorf("WATCHDOG_AGENT_INTERVAL: %w", err)
		}
		cfg.Interval = interval
	}
	return cfg, nil
}

func normalizeAndValidateAgentConfig(cfg agentClientConfig) (agentClientConfig, error) {
	cfg.HubURL = strings.TrimRight(strings.TrimSpace(cfg.HubURL), "/")
	cfg.AgentID = strings.TrimSpace(cfg.AgentID)
	parsed, err := url.Parse(cfg.HubURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return cfg, errors.New("agent.hub_url must be an absolute http(s) URL without query or fragment")
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
