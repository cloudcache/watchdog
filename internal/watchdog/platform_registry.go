package watchdog

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// PLAT-02 registry contracts. Modules compile into the binary and describe
// themselves here; nothing is loaded at runtime. The registries give the core
// one place to answer "which modules exist, what resources do they own, what
// target kinds and datasets do they expose" instead of hardcoded switches.

type ModuleDescriptor struct {
	Key            string
	Version        string
	DisplayName    string
	Dependencies   []string
	DefaultEnabled bool
}

// PlatformModule is the compile-time module contract. Registration hooks are
// optional at this stage of the migration: existing subsystems register their
// descriptors first and move their routes/workers behind the interface
// incrementally.
type PlatformModule interface {
	Descriptor() ModuleDescriptor
	RegisterResources(*ResourceRegistry) error
	RegisterTargetKinds(*TargetKindRegistry) error
	RegisterDatasets(*DatasetRegistry) error
	RegisterRoutes(mux *http.ServeMux) error
}

type ModuleRegistry struct {
	mu      sync.RWMutex
	modules map[string]PlatformModule
	order   []string
}

func NewModuleRegistry() *ModuleRegistry {
	return &ModuleRegistry{modules: map[string]PlatformModule{}}
}

func (r *ModuleRegistry) Register(module PlatformModule) error {
	if module == nil {
		return fmt.Errorf("module is required")
	}
	descriptor := module.Descriptor()
	if descriptor.Key == "" {
		return fmt.Errorf("module key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.modules[descriptor.Key]; exists {
		return fmt.Errorf("module %q is already registered", descriptor.Key)
	}
	r.modules[descriptor.Key] = module
	r.order = append(r.order, descriptor.Key)
	return nil
}

func (r *ModuleRegistry) Get(key string) (PlatformModule, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	module, ok := r.modules[key]
	return module, ok
}

// Resolve returns modules in dependency order and rejects missing or cyclic
// dependencies. Registration order breaks ties so output is deterministic.
func (r *ModuleRegistry) Resolve() ([]PlatformModule, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	const (
		unvisited = 0
		visiting  = 1
		done      = 2
	)
	state := map[string]int{}
	var ordered []PlatformModule
	var visit func(key string, path []string) error
	visit = func(key string, path []string) error {
		switch state[key] {
		case done:
			return nil
		case visiting:
			return fmt.Errorf("module dependency cycle: %v -> %s", path, key)
		}
		module, ok := r.modules[key]
		if !ok {
			return fmt.Errorf("module %q depends on unregistered module (path %v)", key, path)
		}
		state[key] = visiting
		for _, dep := range module.Descriptor().Dependencies {
			if err := visit(dep, append(path, key)); err != nil {
				return err
			}
		}
		state[key] = done
		ordered = append(ordered, module)
		return nil
	}
	for _, key := range r.order {
		if err := visit(key, nil); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func (r *ModuleRegistry) Descriptors() []ModuleDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	descriptors := make([]ModuleDescriptor, 0, len(r.order))
	for _, key := range r.order {
		descriptors = append(descriptors, r.modules[key].Descriptor())
	}
	return descriptors
}

// ResourceRegistry owns the resource-type tree that RBAC inheritance walks,
// replacing the hardcoded port→target→tenant chain in resourceMatches.
type ResourceRegistry struct {
	mu      sync.RWMutex
	parents map[ResourceType]ResourceType
	owners  map[ResourceType]string
}

func NewResourceRegistry() *ResourceRegistry {
	return &ResourceRegistry{parents: map[ResourceType]ResourceType{}, owners: map[ResourceType]string{}}
}

func (r *ResourceRegistry) Register(moduleKey string, resourceType, parent ResourceType) error {
	if resourceType == "" {
		return fmt.Errorf("resource type is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, exists := r.owners[resourceType]; exists {
		return fmt.Errorf("resource type %q is already registered by module %q", resourceType, owner)
	}
	// Reject a parent chain that would loop back through this type.
	for cursor := parent; cursor != ""; cursor = r.parents[cursor] {
		if cursor == resourceType {
			return fmt.Errorf("resource type %q parent chain forms a cycle", resourceType)
		}
	}
	r.owners[resourceType] = moduleKey
	if parent != "" {
		r.parents[resourceType] = parent
	}
	return nil
}

func (r *ResourceRegistry) Parent(resourceType ResourceType) (ResourceType, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	parent, ok := r.parents[resourceType]
	return parent, ok
}

// Ancestors returns the parent chain from the immediate parent upward.
func (r *ResourceRegistry) Ancestors(resourceType ResourceType) []ResourceType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var chain []ResourceType
	for cursor, ok := r.parents[resourceType]; ok; cursor, ok = r.parents[cursor] {
		chain = append(chain, cursor)
	}
	return chain
}

// Target kinds follow the frozen five-kind taxonomy (platform architecture
// §7.1): host / network / storage / edge / core. Readiness gates whether the
// kind is exposed to users; planned kinds hold their key, permissions and
// documentation slot without rendering an entry point.
type TargetKindReadiness string

const (
	TargetKindGA      TargetKindReadiness = "ga"
	TargetKindBeta    TargetKindReadiness = "beta"
	TargetKindPlanned TargetKindReadiness = "planned"
)

type TargetKindDescriptor struct {
	Key                 TargetKind
	ModuleKey           string
	DisplayName         string
	Readiness           TargetKindReadiness
	MenuGroup           string
	AllowedCapabilities []string
	DetailTabs          []string
}

type TargetKindRegistry struct {
	mu    sync.RWMutex
	kinds map[TargetKind]TargetKindDescriptor
}

func NewTargetKindRegistry() *TargetKindRegistry {
	return &TargetKindRegistry{kinds: map[TargetKind]TargetKindDescriptor{}}
}

func (r *TargetKindRegistry) Register(descriptor TargetKindDescriptor) error {
	if descriptor.Key == "" {
		return fmt.Errorf("target kind key is required")
	}
	switch descriptor.Readiness {
	case TargetKindGA, TargetKindBeta, TargetKindPlanned:
	default:
		return fmt.Errorf("target kind %q readiness must be ga, beta or planned", descriptor.Key)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, exists := r.kinds[descriptor.Key]; exists {
		return fmt.Errorf("target kind %q is already registered by module %q", descriptor.Key, existing.ModuleKey)
	}
	r.kinds[descriptor.Key] = descriptor
	return nil
}

func (r *TargetKindRegistry) Get(kind TargetKind) (TargetKindDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	descriptor, ok := r.kinds[kind]
	return descriptor, ok
}

// Available reports whether targets of this kind may be created: the kind must
// be registered and past the planned stage.
func (r *TargetKindRegistry) Available(kind TargetKind) bool {
	descriptor, ok := r.Get(kind)
	return ok && descriptor.Readiness != TargetKindPlanned
}

func (r *TargetKindRegistry) List() []TargetKindDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	kinds := make([]TargetKindDescriptor, 0, len(r.kinds))
	for _, descriptor := range r.kinds {
		kinds = append(kinds, descriptor)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i].Key < kinds[j].Key })
	return kinds
}

type DatasetProviderKind string

const (
	DatasetProviderVM         DatasetProviderKind = "vm"
	DatasetProviderClickHouse DatasetProviderKind = "clickhouse"
)

type DatasetDescriptor struct {
	Key           string              `json:"key"`
	ModuleKey     string              `json:"module_key"`
	Provider      DatasetProviderKind `json:"provider"`
	TimeField     string              `json:"time_field"`
	Metrics       []string            `json:"metrics,omitempty"`
	GroupByFields []string            `json:"group_by_fields,omitempty"`
	FilterFields  []string            `json:"filter_fields,omitempty"`
	ValueLayers   []QueryValueLayer   `json:"value_layers,omitempty"`
	MaxRangeDays  int                 `json:"max_range_days"`
	MaxResultRows uint32              `json:"max_result_rows"`
}

type DatasetRegistry struct {
	mu       sync.RWMutex
	datasets map[string]DatasetDescriptor
}

func NewDatasetRegistry() *DatasetRegistry {
	return &DatasetRegistry{datasets: map[string]DatasetDescriptor{}}
}

func (r *DatasetRegistry) Register(descriptor DatasetDescriptor) error {
	if descriptor.Key == "" {
		return fmt.Errorf("dataset key is required")
	}
	switch descriptor.Provider {
	case DatasetProviderVM, DatasetProviderClickHouse:
	default:
		return fmt.Errorf("dataset %q provider must be vm or clickhouse", descriptor.Key)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, exists := r.datasets[descriptor.Key]; exists {
		return fmt.Errorf("dataset %q is already registered by module %q", descriptor.Key, existing.ModuleKey)
	}
	r.datasets[descriptor.Key] = descriptor
	return nil
}

func (r *DatasetRegistry) Get(key string) (DatasetDescriptor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	descriptor, ok := r.datasets[key]
	return descriptor, ok
}

func (r *DatasetRegistry) List() []DatasetDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	datasets := make([]DatasetDescriptor, 0, len(r.datasets))
	for _, descriptor := range r.datasets {
		datasets = append(datasets, descriptor)
	}
	sort.Slice(datasets, func(i, j int) bool { return datasets[i].Key < datasets[j].Key })
	return datasets
}

// PlatformRegistries bundles the four registries a module receives during
// registration.
type PlatformRegistries struct {
	Modules     *ModuleRegistry
	Resources   *ResourceRegistry
	TargetKinds *TargetKindRegistry
	Datasets    *DatasetRegistry
}

func NewPlatformRegistries() *PlatformRegistries {
	return &PlatformRegistries{
		Modules:     NewModuleRegistry(),
		Resources:   NewResourceRegistry(),
		TargetKinds: NewTargetKindRegistry(),
		Datasets:    NewDatasetRegistry(),
	}
}

// RegisterModules registers each module and immediately applies its
// resource/target-kind/dataset registrations in dependency order.
func (p *PlatformRegistries) RegisterModules(modules ...PlatformModule) error {
	for _, module := range modules {
		if err := p.Modules.Register(module); err != nil {
			return err
		}
	}
	ordered, err := p.Modules.Resolve()
	if err != nil {
		return err
	}
	for _, module := range ordered {
		if err := module.RegisterResources(p.Resources); err != nil {
			return fmt.Errorf("module %s resources: %w", module.Descriptor().Key, err)
		}
		if err := module.RegisterTargetKinds(p.TargetKinds); err != nil {
			return fmt.Errorf("module %s target kinds: %w", module.Descriptor().Key, err)
		}
		if err := module.RegisterDatasets(p.Datasets); err != nil {
			return fmt.Errorf("module %s datasets: %w", module.Descriptor().Key, err)
		}
	}
	return nil
}
