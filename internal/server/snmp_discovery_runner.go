package server

import (
	"strings"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

func newSNMPDiscoveryRunner(cfg SNMPConfig) (snmpDiscoveryRunner, error) {
	if len(cfg.MIBDirs) > 0 || strings.TrimSpace(cfg.MIBLoad) != "" {
		if err := snmpdomain.ConfigureSNMPMIBRegistry(snmpdomain.SNMPConfig{MIBDirs: cfg.MIBDirs, MIBLoad: cfg.MIBLoad}); err != nil {
			return nil, err
		}
	}
	var definitions []snmpdomain.OSDefinition
	if dir := strings.TrimSpace(cfg.DefinitionsDir); dir != "" {
		parsed, err := snmpdomain.ParseLibrenmsDefinitions(dir, cfg.DefinitionsVersion)
		if err != nil {
			return nil, err
		}
		definitions = parsed.OSDefinitions
	}
	registry := snmpdomain.DefaultSNMPCollectorModuleRegistry()
	return snmpdomain.DiscoveryEngine{
		Query: snmpdomain.NewGoSNMPQueryEngine(), OSDefinitions: definitions,
		ModuleDefinitions: map[string]snmpdomain.ModuleDefinition{},
		Modules:           registry.DiscoveryModulesForDefinitions(nil),
	}, nil
}

var _ snmpDiscoveryRunner = snmpdomain.DiscoveryEngine{}
