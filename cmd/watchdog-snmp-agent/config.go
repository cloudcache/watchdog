package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultListenAddress = ":162"

type config struct {
	APIURL string `yaml:"api_url"`
	Token  string `yaml:"token"`
	Listen string `yaml:"listen"`
}

type trapVarBind struct {
	OID   string `json:"oid"`
	Value string `json:"value"`
}

// loadConfig reads only the snmp_trap_agent section. This lets the standalone
// process share a platform YAML file without depending on the platform's full
// configuration schema or its retired runtime package.
func loadConfig(path string) (config, error) {
	cfg := config{Listen: defaultListenAddress}
	if strings.TrimSpace(path) == "" {
		path = strings.TrimSpace(os.Getenv("WATCHDOG_CONFIG"))
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, err
		}
		var document struct {
			SNMPTrapAgent yaml.Node `yaml:"snmp_trap_agent"`
		}
		if err := yaml.Unmarshal(data, &document); err != nil {
			return cfg, fmt.Errorf("decode watchdog config %q: %w", path, err)
		}
		if document.SNMPTrapAgent.Kind != 0 {
			section, err := yaml.Marshal(&document.SNMPTrapAgent)
			if err != nil {
				return cfg, fmt.Errorf("decode snmp_trap_agent config: %w", err)
			}
			decoder := yaml.NewDecoder(strings.NewReader(string(section)))
			decoder.KnownFields(true)
			if err := decoder.Decode(&cfg); err != nil {
				return cfg, fmt.Errorf("decode watchdog config %q snmp_trap_agent: %w", path, err)
			}
		}
	}
	applyEnvironment(&cfg)
	return cfg, nil
}

func applyEnvironment(cfg *config) {
	if value, ok := os.LookupEnv("WATCHDOG_SNMP_TRAP_API_URL"); ok {
		cfg.APIURL = value
	}
	if value, ok := os.LookupEnv("WATCHDOG_SNMP_TRAP_TOKEN"); ok {
		cfg.Token = value
	}
	if value, ok := os.LookupEnv("WATCHDOG_SNMP_TRAP_LISTEN"); ok {
		cfg.Listen = value
	}
}

func normalizeAndValidateConfig(cfg config) (config, error) {
	cfg.APIURL = strings.TrimRight(strings.TrimSpace(cfg.APIURL), "/")
	cfg.Listen = strings.TrimSpace(cfg.Listen)
	parsed, err := url.Parse(cfg.APIURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return cfg, errors.New("snmp_trap_agent.api_url must be an absolute http(s) URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return cfg, errors.New("snmp_trap_agent.api_url must not contain a query or fragment")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return cfg, errors.New("snmp_trap_agent.token is required")
	}
	_, portText, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return cfg, fmt.Errorf("snmp_trap_agent.listen must be a host:port listen address: %w", err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return cfg, errors.New("snmp_trap_agent.listen must contain a port between 1 and 65535")
	}
	return cfg, nil
}
