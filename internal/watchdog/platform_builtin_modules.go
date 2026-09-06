package watchdog

import (
	"net/http"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

// Builtin module descriptors validate the PLAT-02 registry contract with the
// subsystems that already exist. Routes and workers stay wired through the
// current runtime for now and migrate behind RegisterRoutes incrementally; the
// descriptors, resource tree and target kinds are authoritative immediately.

// baseModule provides no-op registration hooks so builtin modules only
// implement what they own.
type baseModule struct{ descriptor ModuleDescriptor }

func (m baseModule) Descriptor() ModuleDescriptor                { return m.descriptor }
func (baseModule) RegisterResources(*ResourceRegistry) error     { return nil }
func (baseModule) RegisterTargetKinds(*TargetKindRegistry) error { return nil }
func (baseModule) RegisterDatasets(*DatasetRegistry) error       { return nil }
func (baseModule) RegisterRoutes(*http.ServeMux) error           { return nil }

type coreModule struct{ baseModule }

func newCoreModule() coreModule {
	return coreModule{baseModule{ModuleDescriptor{
		Key:            "core",
		Version:        "1",
		DisplayName:    "Watchdog Core",
		DefaultEnabled: true,
	}}}
}

func (coreModule) RegisterResources(registry *ResourceRegistry) error {
	// Tenant is the root: it has no parent.
	if err := registry.Register("core", ResourceTenant, ""); err != nil {
		return err
	}
	if err := registry.Register("core", ResourceTarget, ResourceTenant); err != nil {
		return err
	}
	if err := registry.Register("core", ResourceExportTask, ResourceTenant); err != nil {
		return err
	}
	return nil
}

type hostModule struct{ baseModule }

func newHostModule() hostModule {
	return hostModule{baseModule{ModuleDescriptor{
		Key:            "host",
		Version:        "1",
		DisplayName:    "Hosts",
		Dependencies:   []string{"core"},
		DefaultEnabled: true,
	}}}
}

func (hostModule) RegisterTargetKinds(registry *TargetKindRegistry) error {
	if err := registry.Register(TargetKindDescriptor{
		// Kind value stays "system" until the schema rename migration lands;
		// the taxonomy name is Hosts (§7.1).
		Key:                 TargetKindSystem,
		ModuleKey:           "host",
		DisplayName:         "Hosts",
		Readiness:           TargetKindGA,
		MenuGroup:           "resources",
		AllowedCapabilities: []string{"system", "storage"},
		DetailTabs:          []string{"overview", "containers", "systemd", "smart"},
	}); err != nil {
		return err
	}
	// Storage rides on the system agent's storage capabilities (§7.1): SMART
	// today, RAID and cluster probes as they land.
	return registry.Register(TargetKindDescriptor{
		Key:                 TargetKind("storage"),
		ModuleKey:           "host",
		DisplayName:         "Storage",
		Readiness:           TargetKindBeta,
		MenuGroup:           "resources",
		AllowedCapabilities: []string{"storage"},
		DetailTabs:          []string{"overview", "disks"},
	})
}

func (hostModule) RegisterDatasets(registry *DatasetRegistry) error {
	return registry.Register(DatasetDescriptor{
		Key:           "host.agent_metrics",
		ModuleKey:     "host",
		Provider:      DatasetProviderVM,
		TimeField:     "time",
		Metrics:       metricNamesForFamilies("system", "gpu", "container"),
		GroupByFields: []string{"target_id"},
		FilterFields:  []string{"target_id"},
		ValueLayers:   []QueryValueLayer{QueryValueCustomer},
		MaxRangeDays:  400,
		MaxResultRows: 250_000,
	})
}

type edgeModule struct{ baseModule }

func newEdgeModule() edgeModule {
	return edgeModule{baseModule{ModuleDescriptor{
		Key:          "edge",
		Version:      "1",
		DisplayName:  "Edge",
		Dependencies: []string{"core"},
		// Planned: holds the key and permission slot without shipping an
		// entry point until the edge probe agent exists.
		DefaultEnabled: false,
	}}}
}

type flowModule struct{ baseModule }

func newFlowModule() flowModule {
	return flowModule{baseModule{ModuleDescriptor{
		Key:            "flow",
		Version:        "1",
		DisplayName:    "Flow",
		Dependencies:   []string{"core", "network"},
		DefaultEnabled: true,
	}}}
}

func (flowModule) RegisterDatasets(registry *DatasetRegistry) error {
	metrics := flowquery.Metrics()
	metricNames := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		metricNames = append(metricNames, string(metric.Name))
	}
	dimensions := flowquery.Dimensions()
	groupByFields := make([]string, 0, len(dimensions))
	for _, dimension := range dimensions {
		groupByFields = append(groupByFields, string(dimension.Kind))
	}
	return registry.Register(DatasetDescriptor{
		Key: FlowTrafficDataset, ModuleKey: "flow", Provider: DatasetProviderClickHouse,
		TimeField: "bucket_start", Metrics: metricNames, GroupByFields: groupByFields,
		FilterFields: []string{
			"directions", "categories", "businesses", "target_ids", "device_ids", "exporter_ids",
			"dimension_values", "dimension_snapshot_ids", "geo_versions", "classification_versions",
		},
		ValueLayers: []QueryValueLayer{QueryValueCustomer}, MaxRangeDays: 400, MaxResultRows: 250_000,
	})
}

func (edgeModule) RegisterTargetKinds(registry *TargetKindRegistry) error {
	return registry.Register(TargetKindDescriptor{
		Key:                 TargetKind("edge"),
		ModuleKey:           "edge",
		DisplayName:         "Edge",
		Readiness:           TargetKindPlanned,
		MenuGroup:           "resources",
		AllowedCapabilities: []string{"edge_probe"},
	})
}

type networkModule struct{ baseModule }

func newNetworkModule() networkModule {
	return networkModule{baseModule{ModuleDescriptor{
		Key:            "network",
		Version:        "1",
		DisplayName:    "Network",
		Dependencies:   []string{"core"},
		DefaultEnabled: true,
	}}}
}

func (networkModule) RegisterResources(registry *ResourceRegistry) error {
	return registry.Register("network", ResourcePort, ResourceTarget)
}

func (networkModule) RegisterTargetKinds(registry *TargetKindRegistry) error {
	if err := registry.Register(TargetKindDescriptor{
		Key:                 TargetKindNetwork,
		ModuleKey:           "network",
		DisplayName:         "Network",
		Readiness:           TargetKindGA,
		MenuGroup:           "resources",
		AllowedCapabilities: []string{"snmp"},
		DetailTabs:          []string{"overview", "ports", "inventory", "bgp", "events"},
	}); err != nil {
		return err
	}
	// Core (BGP) rides on network collection today; BMP and the IP-library
	// lookup page are planned extensions of the same kind family.
	return registry.Register(TargetKindDescriptor{
		Key:                 TargetKind("core"),
		ModuleKey:           "network",
		DisplayName:         "Core (BGP)",
		Readiness:           TargetKindGA,
		MenuGroup:           "resources",
		AllowedCapabilities: []string{"snmp", "bmp"},
		DetailTabs:          []string{"sessions"},
	})
}

func (networkModule) RegisterDatasets(registry *DatasetRegistry) error {
	return registry.Register(DatasetDescriptor{
		Key:           "network.snmp_interface",
		ModuleKey:     "network",
		Provider:      DatasetProviderVM,
		TimeField:     "time",
		Metrics:       metricNamesForFamilies("snmp_interface", "snmp_optics", "snmp_device", "bgp"),
		GroupByFields: []string{"device_id", "port_id", "if_name"},
		FilterFields:  []string{"target_id", "device_id", "port_id"},
		ValueLayers:   []QueryValueLayer{QueryValueRaw, QueryValueSupplier, QueryValueCustomer},
		MaxRangeDays:  400,
		MaxResultRows: 250_000,
	})
}

func metricNamesForFamilies(families ...string) []string {
	allowed := make(map[string]struct{}, len(families))
	for _, family := range families {
		allowed[family] = struct{}{}
	}
	metrics := make([]string, 0, len(MetricCatalog))
	for _, metric := range MetricCatalog {
		if _, ok := allowed[metric.Family]; ok {
			metrics = append(metrics, metric.Name)
		}
	}
	return metrics
}

// NewBuiltinPlatformRegistries builds the registries with every builtin
// module registered; the hub and tests share this single composition point.
func NewBuiltinPlatformRegistries() (*PlatformRegistries, error) {
	registries := NewPlatformRegistries()
	if err := registries.RegisterModules(newCoreModule(), newHostModule(), newNetworkModule(), newEdgeModule(), newFlowModule()); err != nil {
		return nil, err
	}
	return registries, nil
}
