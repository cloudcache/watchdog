package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func newModuleTestRouter(t *testing.T, repo TenantModuleRepository) http.Handler {
	t.Helper()
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	return NewAPIV1Router(APIV1RouterConfig{
		Auth:          identityAdminTestAuth,
		Registries:    registries,
		TenantModules: repo,
		Audit:         &recordingAuditRepository{},
	})
}

func TestModuleAPIListsEffectiveStates(t *testing.T) {
	router := newModuleTestRouter(t, &fakeTenantModuleRepository{states: map[string]bool{"network": false}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/modules", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"Key":"network","Version":"1","DisplayName":"Network","Dependencies":["core"],"DefaultEnabled":true,"Enabled":false`) {
		t.Fatalf("network override missing: %s", body)
	}
	if !strings.Contains(body, `"Key":"edge"`) || !strings.Contains(body, `"DefaultEnabled":false`) {
		t.Fatalf("edge module missing: %s", body)
	}
}

func TestModuleAPIPutValidatesAndAudits(t *testing.T) {
	repo := &fakeTenantModuleRepository{}
	router := newModuleTestRouter(t, repo)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/tenants/tenant-a/modules",
		strings.NewReader(`{"modules":{"ghost":true}}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown module status = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/tenants/tenant-a/modules",
		strings.NewReader(`{"modules":{"core":false}}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("core disable status = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/tenants/tenant-b/modules",
		strings.NewReader(`{"modules":{"network":false}}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("foreign tenant status = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/tenants/tenant-a/modules",
		strings.NewReader(`{"modules":{"network":false}}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("valid put status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.sets) != 1 || repo.states["network"] != false {
		t.Fatalf("repo state = %+v", repo)
	}
}

func TestModuleAPIListsTargetKinds(t *testing.T) {
	router := newModuleTestRouter(t, &fakeTenantModuleRepository{})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/modules/target-kinds", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	for _, expected := range []string{`"Key":"network"`, `"Key":"edge"`, `"Readiness":"planned"`, `"MenuGroup":"resources"`} {
		if !strings.Contains(rec.Body.String(), expected) {
			t.Fatalf("missing %s in %s", expected, rec.Body.String())
		}
	}
}
