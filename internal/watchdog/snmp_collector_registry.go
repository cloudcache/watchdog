package watchdog

import (
	"context"
	"sort"
)

type SNMPCollectorModuleRegistry struct {
	discoveryModules map[string]SNMPCollectorDiscoveryModule
}

func DefaultSNMPCollectorModuleRegistry() SNMPCollectorModuleRegistry {
	return NewSNMPCollectorModuleRegistry([]SNMPCollectorDiscoveryModule{
		SNMPEntityPhysicalDiscoveryModule{},
		SNMPPortsDiscoveryModule{},
		SNMPSensorsDiscoveryModule{},
		SNMPProcessorsDiscoveryModule{},
		SNMPMemoryDiscoveryModule{},
		SNMPStorageDiscoveryModule{},
		SNMPVLANsDiscoveryModule{},
		SNMPLAGsDiscoveryModule{},
		SNMPBGPDiscoveryModule{},
	})
}

func NewSNMPCollectorModuleRegistry(modules []SNMPCollectorDiscoveryModule) SNMPCollectorModuleRegistry {
	registry := SNMPCollectorModuleRegistry{discoveryModules: map[string]SNMPCollectorDiscoveryModule{}}
	for _, module := range modules {
		if module == nil || module.Name() == "" {
			continue
		}
		registry.discoveryModules[module.Name()] = module
	}
	return registry
}

func (r SNMPCollectorModuleRegistry) DiscoveryModule(name string) (SNMPCollectorDiscoveryModule, bool) {
	module, ok := r.discoveryModules[name]
	return module, ok
}

func (r SNMPCollectorModuleRegistry) DiscoveryModuleNames() []string {
	names := make([]string, 0, len(r.discoveryModules))
	for name := range r.discoveryModules {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r SNMPCollectorModuleRegistry) DiscoveryModulesForDefinitions(definitions []SNMPCollectorModuleDefinition) []SNMPCollectorDiscoveryModule {
	if len(definitions) == 0 {
		names := r.DiscoveryModuleNames()
		modules := make([]SNMPCollectorDiscoveryModule, 0, len(names))
		for _, name := range names {
			modules = append(modules, r.discoveryModules[name])
		}
		return modules
	}
	modules := make([]SNMPCollectorDiscoveryModule, 0, len(definitions))
	seen := map[string]struct{}{}
	for _, definition := range definitions {
		if definition.ModuleName == "" || definition.ModuleType != SNMPCollectorModuleDiscovery {
			continue
		}
		if _, ok := seen[definition.ModuleName]; ok {
			continue
		}
		module, ok := r.discoveryModules[definition.ModuleName]
		if !ok {
			continue
		}
		modules = append(modules, module)
		seen[definition.ModuleName] = struct{}{}
	}
	return modules
}

func NewSNMPDiscoveryEngineFromRepository(ctx context.Context, repository SNMPCollectorRepository, query SNMPCollectorQueryEngine, registry SNMPCollectorModuleRegistry) (SNMPDiscoveryEngine, error) {
	if registry.discoveryModules == nil {
		registry = DefaultSNMPCollectorModuleRegistry()
	}
	osDefinitions, err := repository.ListSNMPOSDefinitions(ctx)
	if err != nil {
		return SNMPDiscoveryEngine{}, err
	}
	moduleDefinitions, err := repository.ListSNMPModuleDefinitions(ctx, SNMPCollectorModuleDiscovery)
	if err != nil {
		return SNMPDiscoveryEngine{}, err
	}
	byName := make(map[string]SNMPCollectorModuleDefinition, len(moduleDefinitions))
	for _, definition := range moduleDefinitions {
		if definition.ModuleName != "" {
			byName[definition.ModuleName] = definition
		}
	}
	return SNMPDiscoveryEngine{
		Query:             query,
		OSDefinitions:     osDefinitions,
		ModuleDefinitions: byName,
		Modules:           registry.DiscoveryModulesForDefinitions(moduleDefinitions),
	}, nil
}
