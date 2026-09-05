package flowplan

import (
	"errors"
	"net/netip"
	"reflect"
	"sync/atomic"
)

var (
	ErrRegistryVersionUnavailable = errors.New("flow registry version is unavailable")
	ErrRegistryVersionConflict    = errors.New("flow registry version is immutable")
)

type registryKey struct {
	collectorID string
	revision    uint64
}

type catalogState struct {
	registries map[registryKey]*Registry
}

// Catalog retains immutable plan revisions needed by RawFlow records already
// in Kafka. Resolve never falls forward to the current plan.
type Catalog struct {
	state atomic.Pointer[catalogState]
}

func NewCatalog(registries ...*Registry) (*Catalog, error) {
	catalog := &Catalog{}
	catalog.state.Store(&catalogState{registries: map[registryKey]*Registry{}})
	for _, registry := range registries {
		if err := catalog.Install(registry); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *Catalog) Install(registry *Registry) error {
	if c == nil || registry == nil {
		return ErrRegistryVersionUnavailable
	}
	plan := registry.Plan()
	key := registryKey{collectorID: plan.CollectorID, revision: plan.Revision}
	for {
		current := c.state.Load()
		if current == nil {
			empty := &catalogState{registries: map[registryKey]*Registry{}}
			if !c.state.CompareAndSwap(nil, empty) {
				continue
			}
			current = empty
		}
		if installed := current.registries[key]; installed != nil {
			if reflect.DeepEqual(installed.Plan(), plan) {
				return nil
			}
			return ErrRegistryVersionConflict
		}
		next := &catalogState{registries: make(map[registryKey]*Registry, len(current.registries)+1)}
		for installedKey, installed := range current.registries {
			next.registries[installedKey] = installed
		}
		next.registries[key] = registry
		if c.state.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (c *Catalog) Resolve(collectorID string, revision uint64, protocol Protocol, source netip.Addr, observationDomainID uint64) (SourceBinding, error) {
	if c == nil || collectorID == "" || revision == 0 {
		return SourceBinding{}, ErrRegistryVersionUnavailable
	}
	state := c.state.Load()
	if state == nil {
		return SourceBinding{}, ErrRegistryVersionUnavailable
	}
	registry := state.registries[registryKey{collectorID: collectorID, revision: revision}]
	if registry == nil {
		return SourceBinding{}, ErrRegistryVersionUnavailable
	}
	binding, ok := registry.Admit(protocol, source, observationDomainID)
	if !ok {
		return SourceBinding{}, ErrRegistryVersionUnavailable
	}
	return binding, nil
}
