package snmpdomain

import "sort"

type SNMPCollectorModuleRegistry struct {
	discoveryModules map[string]DiscoveryModule
}

func DefaultSNMPCollectorModuleRegistry() SNMPCollectorModuleRegistry {
	return NewSNMPCollectorModuleRegistry([]DiscoveryModule{
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

func NewSNMPCollectorModuleRegistry(modules []DiscoveryModule) SNMPCollectorModuleRegistry {
	registry := SNMPCollectorModuleRegistry{discoveryModules: map[string]DiscoveryModule{}}
	for _, module := range modules {
		if module == nil || module.Name() == "" {
			continue
		}
		registry.discoveryModules[module.Name()] = module
	}
	return registry
}

func (r SNMPCollectorModuleRegistry) DiscoveryModule(name string) (DiscoveryModule, bool) {
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

func (r SNMPCollectorModuleRegistry) DiscoveryModulesForDefinitions(definitions []ModuleDefinition) []DiscoveryModule {
	if len(definitions) == 0 {
		names := r.DiscoveryModuleNames()
		modules := make([]DiscoveryModule, 0, len(names))
		for _, name := range names {
			modules = append(modules, r.discoveryModules[name])
		}
		return modules
	}
	modules := make([]DiscoveryModule, 0, len(definitions))
	seen := map[string]struct{}{}
	for _, definition := range definitions {
		if definition.ModuleName == "" || definition.ModuleType != ModuleDiscovery {
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
