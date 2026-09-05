package flowdimension

import (
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"github.com/gaissmai/bart"
)

const (
	maxAddressSetReferences      = 256
	maxAddressSetDependencyDepth = 64
)

type pendingAddressSet struct {
	compiled   compiledAddressSet
	includeIDs []string
	excludeIDs []string
}

type addressSetDirectives struct {
	include []int
	exclude []int
}

func compileAddressSetDefinitions(definitions []AddressSetDefinition, maxCIDRs int) ([]compiledAddressSet, []netip.Prefix, error) {
	allIDs := make(map[string]struct{}, len(definitions))
	pending := make([]pendingAddressSet, 0, len(definitions))
	totalCIDRs := 0
	for index, definition := range definitions {
		if !validIdentifier(definition.ID, 128) {
			return nil, nil, fmt.Errorf("address_sets[%d].id is invalid", index)
		}
		if _, exists := allIDs[definition.ID]; exists {
			return nil, nil, fmt.Errorf("address_sets[%d] duplicates id %q", index, definition.ID)
		}
		allIDs[definition.ID] = struct{}{}
		if !definition.Enabled {
			continue
		}

		direction := BusinessDirection(strings.ToLower(strings.TrimSpace(definition.MatchDirection)))
		if direction == "" {
			direction = DirectionBoth
		}
		if direction != DirectionIn && direction != DirectionOut && direction != DirectionBoth {
			return nil, nil, fmt.Errorf("address_sets[%d].match_direction must be in, out, or both", index)
		}
		var selector map[string][]string
		var err error
		if len(definition.Selector.Labels) > 0 {
			selector, err = cloneAndValidateSelector(definition.Selector.Labels)
			if err != nil {
				return nil, nil, fmt.Errorf("address_sets[%d].selector: %w", index, err)
			}
		}
		members, err := canonicalSetPrefixes(definition.Members)
		if err != nil {
			return nil, nil, fmt.Errorf("address_sets[%d].members: %w", index, err)
		}
		excludeMembers, err := canonicalSetPrefixes(definition.ExcludeMembers)
		if err != nil {
			return nil, nil, fmt.Errorf("address_sets[%d].exclude_members: %w", index, err)
		}
		includeIDs, err := canonicalSetReferences(definition.IncludeSetIDs)
		if err != nil {
			return nil, nil, fmt.Errorf("address_sets[%d].include_set_ids: %w", index, err)
		}
		excludeIDs, err := canonicalSetReferences(definition.ExcludeSetIDs)
		if err != nil {
			return nil, nil, fmt.Errorf("address_sets[%d].exclude_set_ids: %w", index, err)
		}
		if len(selector) == 0 && len(members) == 0 && len(includeIDs) == 0 {
			return nil, nil, fmt.Errorf("address_sets[%d] selector must contain 1..64 labels unless a member or included set is present", index)
		}
		totalCIDRs += len(members) + len(excludeMembers)
		if totalCIDRs > maxCIDRs {
			return nil, nil, fmt.Errorf("address-set CIDRs exceed compilation limit %d", maxCIDRs)
		}
		pending = append(pending, pendingAddressSet{
			compiled: compiledAddressSet{
				id: definition.ID, direction: direction, selector: selector,
				members: members, excludeMembers: excludeMembers,
			},
			includeIDs: includeIDs,
			excludeIDs: excludeIDs,
		})
	}

	sort.Slice(pending, func(i, j int) bool { return pending[i].compiled.id < pending[j].compiled.id })
	indexByID := make(map[string]int, len(pending))
	sets := make([]compiledAddressSet, len(pending))
	for index := range pending {
		sets[index] = pending[index].compiled
		indexByID[sets[index].id] = index
	}
	for index := range pending {
		for _, id := range pending[index].includeIDs {
			dependency, exists := indexByID[id]
			if !exists {
				return nil, nil, fmt.Errorf("address set %q includes missing or disabled set %q", sets[index].id, id)
			}
			sets[index].include = append(sets[index].include, dependency)
		}
		for _, id := range pending[index].excludeIDs {
			dependency, exists := indexByID[id]
			if !exists {
				return nil, nil, fmt.Errorf("address set %q excludes missing or disabled set %q", sets[index].id, id)
			}
			sets[index].exclude = append(sets[index].exclude, dependency)
		}
	}
	if _, err := addressSetTopologicalOrder(sets); err != nil {
		return nil, nil, err
	}
	membershipPrefixes := make([]netip.Prefix, 0, totalCIDRs)
	for _, set := range sets {
		membershipPrefixes = append(membershipPrefixes, set.members...)
		membershipPrefixes = append(membershipPrefixes, set.excludeMembers...)
	}
	return sets, membershipPrefixes, nil
}

func canonicalSetPrefixes(values []string) ([]netip.Prefix, error) {
	unique := make(map[netip.Prefix]struct{}, len(values))
	for _, value := range values {
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix != prefix.Masked() || value != prefix.String() {
			return nil, errors.New("members must be canonical IPv4 or IPv6 CIDRs")
		}
		unique[prefix] = struct{}{}
	}
	prefixes := make([]netip.Prefix, 0, len(unique))
	for prefix := range unique {
		prefixes = append(prefixes, prefix)
	}
	sort.Slice(prefixes, func(i, j int) bool { return comparePrefixes(prefixes[i], prefixes[j]) < 0 })
	return prefixes, nil
}

func canonicalSetReferences(values []string) ([]string, error) {
	if len(values) > maxAddressSetReferences {
		return nil, fmt.Errorf("reference count exceeds limit %d", maxAddressSetReferences)
	}
	canonical := append([]string(nil), values...)
	for _, value := range canonical {
		if !validIdentifier(value, 128) {
			return nil, errors.New("references contain an invalid set ID")
		}
	}
	sort.Strings(canonical)
	return slicesCompact(canonical), nil
}

func addressSetTopologicalOrder(sets []compiledAddressSet) ([]int, error) {
	states := make([]uint8, len(sets))
	depths := make([]int, len(sets))
	order := make([]int, 0, len(sets))
	var visit func(int) (int, error)
	visit = func(index int) (int, error) {
		switch states[index] {
		case 1:
			return 0, fmt.Errorf("address-set dependency cycle contains %q", sets[index].id)
		case 2:
			return depths[index], nil
		}
		states[index] = 1
		depth := 1
		for _, dependency := range sets[index].include {
			dependencyDepth, err := visit(dependency)
			if err != nil {
				return 0, err
			}
			depth = max(depth, dependencyDepth+1)
		}
		for _, dependency := range sets[index].exclude {
			dependencyDepth, err := visit(dependency)
			if err != nil {
				return 0, err
			}
			depth = max(depth, dependencyDepth+1)
		}
		if depth > maxAddressSetDependencyDepth {
			return 0, fmt.Errorf("address-set dependency depth exceeds limit %d at %q", maxAddressSetDependencyDepth, sets[index].id)
		}
		states[index] = 2
		depths[index] = depth
		order = append(order, index)
		return depth, nil
	}
	for index := range sets {
		if _, err := visit(index); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func compileAddressSetMemberships(
	primary *bart.Table[compiledPrefix], primaryPrefixes, membershipPrefixes []netip.Prefix,
	sets []compiledAddressSet, endpointLimit int, evaluationLimit int64,
) (*bart.Table[compiledAddressSetMembership], int, error) {
	memberships := &bart.Table[compiledAddressSetMembership]{}
	if len(sets) == 0 {
		return memberships, 0, nil
	}
	nodes := uniqueSetBoundaryPrefixes(primaryPrefixes, membershipPrefixes)
	topological, err := addressSetTopologicalOrder(sets)
	if err != nil {
		return nil, 0, err
	}
	directives := buildAddressSetDirectiveTree(sets)
	selectorIndex := buildAddressSetSelectorIndex(sets)
	dependents := buildAddressSetDependents(sets)

	maxLocalIn, maxLocalOut := 0, 0
	maxRemoteIn, maxRemoteOut := 0, 0
	var evaluations int64
	base := make([]bool, len(sets))
	directExclude := make([]bool, len(sets))
	considered := make([]bool, len(sets))
	relevant := make([]bool, len(sets))
	matched := make([]bool, len(sets))
	queue := make([]int, 0, len(sets))
	countEvaluation := func() error {
		if evaluations >= evaluationLimit {
			return fmt.Errorf("dimension bundle address-set evaluations exceed compilation limit %d", evaluationLimit)
		}
		evaluations++
		return nil
	}
	for _, node := range nodes {
		clear(base)
		clear(directExclude)
		clear(considered)
		clear(relevant)
		clear(matched)
		prefix, prefixMatched := primary.Lookup(node.Addr())
		if prefixMatched {
			for key, value := range prefix.labels {
				for _, setIndex := range selectorIndex[key][value] {
					if considered[setIndex] {
						continue
					}
					considered[setIndex] = true
					if err := countEvaluation(); err != nil {
						return nil, 0, err
					}
					if selectorMatches(sets[setIndex].selector, prefix.labels) {
						base[setIndex] = true
					}
				}
			}
		}
		for _, directive := range directives.Supernets(node) {
			for _, setIndex := range directive.include {
				if !considered[setIndex] {
					considered[setIndex] = true
					if err := countEvaluation(); err != nil {
						return nil, 0, err
					}
				}
				base[setIndex] = true
			}
			for _, setIndex := range directive.exclude {
				directExclude[setIndex] = true
			}
		}
		queue = markAddressSetRelevant(base, dependents, relevant, queue[:0])
		for setIndex, isRelevant := range relevant {
			if isRelevant && !considered[setIndex] {
				considered[setIndex] = true
				if err := countEvaluation(); err != nil {
					return nil, 0, err
				}
			}
		}
		for _, setIndex := range topological {
			if !relevant[setIndex] {
				continue
			}
			member := base[setIndex]
			if !member {
				member = anySetMatched(matched, sets[setIndex].include)
			}
			if member && (directExclude[setIndex] || anySetMatched(matched, sets[setIndex].exclude)) {
				member = false
			}
			matched[setIndex] = member
		}
		value := compiledAddressSetMembership{}
		for setIndex, member := range matched {
			if !member {
				continue
			}
			switch sets[setIndex].direction {
			case DirectionIn:
				value.in = append(value.in, sets[setIndex].id)
			case DirectionOut:
				value.out = append(value.out, sets[setIndex].id)
			case DirectionBoth:
				value.in = append(value.in, sets[setIndex].id)
				value.out = append(value.out, sets[setIndex].id)
			}
		}
		if len(value.in) > endpointLimit || len(value.out) > endpointLimit {
			return nil, 0, fmt.Errorf("dimension bundle address-set endpoint expansion exceeds per-record limit %d", endpointLimit)
		}
		memberships.Insert(node, value)
		local := prefixMatched && prefix.labels["flow"] == "local"
		if local {
			maxLocalIn = max(maxLocalIn, len(value.in))
			maxLocalOut = max(maxLocalOut, len(value.out))
		} else {
			maxRemoteIn = max(maxRemoteIn, len(value.in))
			maxRemoteOut = max(maxRemoteOut, len(value.out))
		}
	}
	maximum := max(maxLocalIn+maxRemoteIn, maxLocalOut+maxRemoteOut)
	if maximum > endpointLimit {
		return nil, 0, fmt.Errorf("dimension bundle address-set expansion %d exceeds per-record limit %d", maximum, endpointLimit)
	}
	return memberships, maximum, nil
}

func uniqueSetBoundaryPrefixes(primary, membership []netip.Prefix) []netip.Prefix {
	unique := make(map[netip.Prefix]struct{}, len(primary)+len(membership))
	for _, prefix := range primary {
		unique[prefix] = struct{}{}
	}
	for _, prefix := range membership {
		unique[prefix] = struct{}{}
	}
	result := make([]netip.Prefix, 0, len(unique))
	for prefix := range unique {
		result = append(result, prefix)
	}
	sort.Slice(result, func(i, j int) bool { return comparePrefixes(result[i], result[j]) < 0 })
	return result
}

func comparePrefixes(left, right netip.Prefix) int {
	if compared := left.Addr().Compare(right.Addr()); compared != 0 {
		return compared
	}
	return left.Bits() - right.Bits()
}

func buildAddressSetDirectiveTree(sets []compiledAddressSet) *bart.Table[addressSetDirectives] {
	byPrefix := make(map[netip.Prefix]addressSetDirectives)
	for setIndex, set := range sets {
		for _, prefix := range set.members {
			value := byPrefix[prefix]
			value.include = append(value.include, setIndex)
			byPrefix[prefix] = value
		}
		for _, prefix := range set.excludeMembers {
			value := byPrefix[prefix]
			value.exclude = append(value.exclude, setIndex)
			byPrefix[prefix] = value
		}
	}
	tree := &bart.Table[addressSetDirectives]{}
	for prefix, value := range byPrefix {
		tree.Insert(prefix, value)
	}
	return tree
}

func buildAddressSetSelectorIndex(sets []compiledAddressSet) map[string]map[string][]int {
	index := make(map[string]map[string][]int)
	for setIndex, set := range sets {
		if len(set.selector) == 0 {
			continue
		}
		anchorKey := ""
		var anchorValues []string
		for key, values := range set.selector {
			if anchorKey == "" || len(values) < len(anchorValues) || (len(values) == len(anchorValues) && key < anchorKey) {
				anchorKey, anchorValues = key, values
			}
		}
		if index[anchorKey] == nil {
			index[anchorKey] = make(map[string][]int)
		}
		for _, value := range anchorValues {
			index[anchorKey][value] = append(index[anchorKey][value], setIndex)
		}
	}
	return index
}

func buildAddressSetDependents(sets []compiledAddressSet) [][]int {
	dependents := make([][]int, len(sets))
	for setIndex, set := range sets {
		for _, dependency := range set.include {
			dependents[dependency] = append(dependents[dependency], setIndex)
		}
		for _, dependency := range set.exclude {
			dependents[dependency] = append(dependents[dependency], setIndex)
		}
	}
	return dependents
}

func markAddressSetRelevant(base []bool, dependents [][]int, relevant []bool, queue []int) []int {
	for index, matched := range base {
		if matched {
			relevant[index] = true
			queue = append(queue, index)
		}
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, dependent := range dependents[current] {
			if !relevant[dependent] {
				relevant[dependent] = true
				queue = append(queue, dependent)
			}
		}
	}
	return queue[:0]
}

func anySetMatched(matched []bool, indexes []int) bool {
	for _, index := range indexes {
		if matched[index] {
			return true
		}
	}
	return false
}
