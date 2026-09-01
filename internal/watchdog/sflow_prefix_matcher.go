package watchdog

import (
	"net/netip"
	"sync"

	"github.com/gaissmai/bart"
)

type PrefixMatcher struct {
	mu   sync.RWMutex
	tree *bart.Table[map[string]string]
	sets []AddressSet
}

func NewPrefixMatcher() *PrefixMatcher {
	return &PrefixMatcher{
		tree: &bart.Table[map[string]string]{},
	}
}

func (m *PrefixMatcher) LoadPrefixes(prefixes []AddressPrefix) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tree = &bart.Table[map[string]string]{}
	for _, p := range prefixes {
		prefix, err := netip.ParsePrefix(p.CIDR)
		if err != nil {
			continue
		}
		m.tree.Insert(prefix, p.Labels)
	}
}

func (m *PrefixMatcher) LoadSets(sets []AddressSet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	enabled := make([]AddressSet, 0, len(sets))
	for _, s := range sets {
		if s.Enabled {
			enabled = append(enabled, s)
		}
	}
	m.sets = enabled
}

func (m *PrefixMatcher) Match(ipStr string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	addr, err := netip.ParseAddr(ipStr)
	if err != nil {
		return nil
	}
	labels, ok := m.tree.Lookup(addr)
	if !ok {
		return nil
	}
	return labels
}

func (m *PrefixMatcher) MatchSets(labels map[string]string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var matched []string
	for _, set := range m.sets {
		if matchSelector(set.Selector, labels) {
			matched = append(matched, set.Name)
		}
	}
	return matched
}

func matchSelector(selector map[string]any, labels map[string]string) bool {
	labelsRaw, ok := selector["labels"]
	if !ok {
		return false
	}
	labelsMap, ok := labelsRaw.(map[string]any)
	if !ok {
		return false
	}
	for key, expected := range labelsMap {
		actual, ok := labels[key]
		if !ok {
			return false
		}
		switch exp := expected.(type) {
		case string:
			if actual != exp {
				return false
			}
		case []any:
			found := false
			for _, e := range exp {
				if s, ok := e.(string); ok && s == actual {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}
