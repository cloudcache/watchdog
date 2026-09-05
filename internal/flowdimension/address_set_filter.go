package flowdimension

import (
	"errors"
	"sort"
)

const maxAddressSetFilterIDs = 256

var (
	ErrAddressSetFilterLimit     = errors.New("address-set filter contains too many IDs")
	ErrAddressSetFilterInvalidID = errors.New("address-set filter contains an invalid ID")
)

// AddressSetFilter represents (union(include_any) AND intersection(include_all))
// MINUS union(exclude_any). Empty lists do not add a condition.
type AddressSetFilter struct {
	IncludeAny []string `json:"include_any"`
	IncludeAll []string `json:"include_all"`
	ExcludeAny []string `json:"exclude_any"`
}

// CompiledAddressSetFilter is immutable and safe to reuse across records.
// It evaluates the membership IDs stored on the fact, so a record that belongs
// to two selected sets is still counted once.
type CompiledAddressSetFilter struct {
	includeAny []string
	includeAll []string
	excludeAny []string
}

func CompileAddressSetFilter(filter AddressSetFilter) (CompiledAddressSetFilter, error) {
	if len(filter.IncludeAny)+len(filter.IncludeAll)+len(filter.ExcludeAny) > maxAddressSetFilterIDs {
		return CompiledAddressSetFilter{}, ErrAddressSetFilterLimit
	}
	includeAny, err := canonicalFilterIDs(filter.IncludeAny)
	if err != nil {
		return CompiledAddressSetFilter{}, err
	}
	includeAll, err := canonicalFilterIDs(filter.IncludeAll)
	if err != nil {
		return CompiledAddressSetFilter{}, err
	}
	excludeAny, err := canonicalFilterIDs(filter.ExcludeAny)
	if err != nil {
		return CompiledAddressSetFilter{}, err
	}
	return CompiledAddressSetFilter{
		includeAny: includeAny,
		includeAll: includeAll,
		excludeAny: excludeAny,
	}, nil
}

// Canonical returns sorted, deduplicated copies for storage/query compilers.
// Callers cannot mutate the compiled predicate through the returned slices.
func (f CompiledAddressSetFilter) Canonical() AddressSetFilter {
	return AddressSetFilter{
		IncludeAny: append([]string(nil), f.includeAny...),
		IncludeAll: append([]string(nil), f.includeAll...),
		ExcludeAny: append([]string(nil), f.excludeAny...),
	}
}

func (f CompiledAddressSetFilter) Matches(membership AddressSetMembership) bool {
	return f.MatchesIDs(membership.ids)
}

func (f CompiledAddressSetFilter) MatchesIDs(membershipIDs []string) bool {
	if len(f.includeAny) > 0 && !containsAnyID(membershipIDs, f.includeAny) {
		return false
	}
	if !containsAllIDs(membershipIDs, f.includeAll) {
		return false
	}
	return !containsAnyID(membershipIDs, f.excludeAny)
}

func canonicalFilterIDs(ids []string) ([]string, error) {
	if len(ids) > maxAddressSetFilterIDs {
		return nil, ErrAddressSetFilterLimit
	}
	canonical := append([]string(nil), ids...)
	for _, id := range canonical {
		if !validIdentifier(id, 128) {
			return nil, ErrAddressSetFilterInvalidID
		}
	}
	sort.Strings(canonical)
	return slicesCompact(canonical), nil
}

func slicesCompact(values []string) []string {
	if len(values) < 2 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

func containsAnyID(membershipIDs, requested []string) bool {
	for _, id := range requested {
		for _, membershipID := range membershipIDs {
			if membershipID == id {
				return true
			}
		}
	}
	return false
}

func containsAllIDs(membershipIDs, requested []string) bool {
	for _, id := range requested {
		found := false
		for _, membershipID := range membershipIDs {
			if membershipID == id {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
