package watchdog

import (
	"context"
	"strings"
	"testing"
)

type stubModule struct {
	baseModule
	kinds []TargetKindDescriptor
}

func (m stubModule) RegisterTargetKinds(registry *TargetKindRegistry) error {
	for _, kind := range m.kinds {
		if err := registry.Register(kind); err != nil {
			return err
		}
	}
	return nil
}

func newStubModule(key string, deps ...string) stubModule {
	return stubModule{baseModule: baseModule{ModuleDescriptor{Key: key, Version: "1", DisplayName: key, Dependencies: deps, DefaultEnabled: true}}}
}

func TestModuleRegistryRejectsDuplicates(t *testing.T) {
	registry := NewModuleRegistry()
	if err := registry.Register(newStubModule("a")); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(newStubModule("a")); err == nil {
		t.Fatal("duplicate module key must be rejected")
	}
}

func TestModuleRegistryResolvesDependencyOrder(t *testing.T) {
	registry := NewModuleRegistry()
	for _, module := range []stubModule{newStubModule("c", "b"), newStubModule("b", "a"), newStubModule("a")} {
		if err := registry.Register(module); err != nil {
			t.Fatal(err)
		}
	}
	ordered, err := registry.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, module := range ordered {
		keys = append(keys, module.Descriptor().Key)
	}
	if strings.Join(keys, ",") != "a,b,c" {
		t.Fatalf("order = %v", keys)
	}
}

func TestModuleRegistryDetectsMissingAndCyclicDependencies(t *testing.T) {
	registry := NewModuleRegistry()
	if err := registry.Register(newStubModule("a", "ghost")); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(); err == nil {
		t.Fatal("missing dependency must fail resolve")
	}

	cyclic := NewModuleRegistry()
	if err := cyclic.Register(newStubModule("x", "y")); err != nil {
		t.Fatal(err)
	}
	if err := cyclic.Register(newStubModule("y", "x")); err != nil {
		t.Fatal(err)
	}
	if _, err := cyclic.Resolve(); err == nil {
		t.Fatal("cycle must fail resolve")
	}
}

func TestResourceRegistryParentChain(t *testing.T) {
	registry := NewResourceRegistry()
	if err := registry.Register("core", ResourceTenant, ""); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("core", ResourceTarget, ResourceTenant); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("network", ResourcePort, ResourceTarget); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("other", ResourcePort, ResourceTenant); err == nil {
		t.Fatal("duplicate resource type must be rejected")
	}
	chain := registry.Ancestors(ResourcePort)
	if len(chain) != 2 || chain[0] != ResourceTarget || chain[1] != ResourceTenant {
		t.Fatalf("ancestors = %v", chain)
	}
	if err := registry.Register("core", ResourceType("loop"), ResourceType("loop")); err == nil {
		t.Fatal("self-parent must be rejected")
	}
}

func TestTargetKindRegistryValidatesReadiness(t *testing.T) {
	registry := NewTargetKindRegistry()
	if err := registry.Register(TargetKindDescriptor{Key: "host", ModuleKey: "host", Readiness: "someday"}); err == nil {
		t.Fatal("invalid readiness must be rejected")
	}
	if err := registry.Register(TargetKindDescriptor{Key: "edge", ModuleKey: "edge", Readiness: TargetKindPlanned}); err != nil {
		t.Fatal(err)
	}
	if registry.Available("edge") {
		t.Fatal("planned kinds must not be available")
	}
	if err := registry.Register(TargetKindDescriptor{Key: "edge", ModuleKey: "again", Readiness: TargetKindGA}); err == nil {
		t.Fatal("duplicate kind must be rejected")
	}
}

func TestDatasetRegistryValidatesProvider(t *testing.T) {
	registry := NewDatasetRegistry()
	if err := registry.Register(DatasetDescriptor{Key: "x", Provider: "postgres"}); err == nil {
		t.Fatal("unknown provider must be rejected")
	}
	if err := registry.Register(DatasetDescriptor{Key: "x", Provider: DatasetProviderClickHouse}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(DatasetDescriptor{Key: "x", Provider: DatasetProviderVM}); err == nil {
		t.Fatal("duplicate dataset key must be rejected")
	}
}

func TestBuiltinPlatformRegistriesComposeFiveKinds(t *testing.T) {
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	kinds := registries.TargetKinds.List()
	byKey := map[TargetKind]TargetKindDescriptor{}
	for _, kind := range kinds {
		byKey[kind.Key] = kind
	}
	for _, expected := range []TargetKind{TargetKindSystem, TargetKindNetwork, "storage", "edge", "core"} {
		if _, ok := byKey[expected]; !ok {
			t.Fatalf("missing target kind %q (have %v)", expected, kinds)
		}
	}
	if byKey["edge"].Readiness != TargetKindPlanned {
		t.Fatalf("edge readiness = %s", byKey["edge"].Readiness)
	}
	if registries.TargetKinds.Available("edge") {
		t.Fatal("edge must not be available while planned")
	}
	if !registries.TargetKinds.Available(TargetKindNetwork) {
		t.Fatal("network must be available")
	}
	if chain := registries.Resources.Ancestors(ResourcePort); len(chain) != 2 {
		t.Fatalf("port ancestors = %v", chain)
	}
	if _, ok := registries.Datasets.Get("network.snmp_interface"); !ok {
		t.Fatal("network dataset must be registered")
	}
	if _, ok := registries.Datasets.Get("host.agent_metrics"); !ok {
		t.Fatal("host metrics dataset must be registered")
	}
	flowDataset, ok := registries.Datasets.Get(FlowTrafficDataset)
	if !ok || flowDataset.Provider != DatasetProviderClickHouse || flowDataset.ModuleKey != "flow" {
		t.Fatalf("Flow dataset = %#v, found = %v", flowDataset, ok)
	}
	if module, ok := registries.Modules.Get("flow"); !ok || !module.Descriptor().DefaultEnabled {
		t.Fatalf("Flow module is not registered and enabled: %#v, found = %v", module, ok)
	}
	metricOwners := map[string]string{}
	for _, dataset := range registries.Datasets.List() {
		for _, metric := range dataset.Metrics {
			if owner := metricOwners[metric]; owner != "" {
				t.Fatalf("metric %q is registered by both %q and %q", metric, owner, dataset.Key)
			}
			metricOwners[metric] = dataset.Key
		}
	}
	for _, metric := range MetricCatalog {
		if metricOwners[metric.Name] == "" {
			t.Fatalf("catalog metric %q has no query dataset", metric.Name)
		}
	}
}

// fakeTenantModuleRepository remains a shared QueryGateway test fixture while
// the legacy registry closure is retired. It no longer backs an HTTP API.
type fakeTenantModuleRepository struct {
	states map[string]bool
	sets   []string
}

func (f *fakeTenantModuleRepository) ListTenantModuleStates(context.Context, ID) (map[string]bool, error) {
	if f.states == nil {
		return map[string]bool{}, nil
	}
	return f.states, nil
}

func (f *fakeTenantModuleRepository) SetTenantModuleEnabled(_ context.Context, _ ID, moduleKey string, enabled bool, _ ID) error {
	if f.states == nil {
		f.states = map[string]bool{}
	}
	f.states[moduleKey] = enabled
	f.sets = append(f.sets, moduleKey)
	return nil
}
