package address

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

type AddressDimensionDraftSource struct {
	Prefixes  []AddressPrefix
	Sets      []AddressSet
	Geography []GeoDictionaryNode
	Operators []ISPOperator
	Sources   []AddressDimensionSource
}

type AddressDimensionDraft struct {
	Prefixes    []flowdimension.PrefixDefinition     `json:"prefixes"`
	AddressSets []flowdimension.AddressSetDefinition `json:"address_sets"`
	Operators   []flowdimension.OperatorDefinition   `json:"operators"`
	GeoNodes    []flowdimension.GeoNodeDefinition    `json:"geo_nodes"`
	Sources     []AddressDimensionSource             `json:"sources,omitempty"`
}

func CompileAddressDimensionDraft(source AddressDimensionDraftSource) (AddressDimensionDraft, string, error) {
	draft := AddressDimensionDraft{
		Prefixes:    make([]flowdimension.PrefixDefinition, 0, len(source.Prefixes)),
		AddressSets: make([]flowdimension.AddressSetDefinition, 0, len(source.Sets)),
	}
	var geography map[ID]GeoDictionaryNode
	var err error
	draft.GeoNodes, geography, err = canonicalAddressDimensionGeoNodes(source.Geography)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	draft.Operators, err = canonicalAddressDimensionOperators(source.Operators)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	operators := make(map[ID]ISPOperator, len(source.Operators))
	for _, item := range source.Operators {
		operators[item.ID] = item
	}
	draft.Sources, err = canonicalAddressDimensionSources(source.Sources)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	for _, prefix := range source.Prefixes {
		definition, err := compileAddressPrefixDefinition(prefix, geography, operators)
		if err != nil {
			return AddressDimensionDraft{}, "", err
		}
		draft.Prefixes = append(draft.Prefixes, definition)
	}
	for _, set := range source.Sets {
		definition, err := compileAddressSetDefinition(set, geography, operators)
		if err != nil {
			return AddressDimensionDraft{}, "", err
		}
		draft.AddressSets = append(draft.AddressSets, definition)
	}
	sort.Slice(draft.Prefixes, func(i, j int) bool {
		left, _ := netip.ParsePrefix(draft.Prefixes[i].CIDR)
		right, _ := netip.ParsePrefix(draft.Prefixes[j].CIDR)
		if left.Addr().Compare(right.Addr()) != 0 {
			return left.Addr().Compare(right.Addr()) < 0
		}
		return left.Bits() < right.Bits()
	})
	sort.Slice(draft.AddressSets, func(i, j int) bool { return draft.AddressSets[i].ID < draft.AddressSets[j].ID })
	data, err := json.Marshal(draft)
	if err != nil {
		return AddressDimensionDraft{}, "", err
	}
	digest := sha256.Sum256(data)
	return draft, "sha256:" + hex.EncodeToString(digest[:]), nil
}

func canonicalAddressDimensionGeoNodes(items []GeoDictionaryNode) ([]flowdimension.GeoNodeDefinition, map[ID]GeoDictionaryNode, error) {
	definitions := make([]flowdimension.GeoNodeDefinition, 0, len(items))
	lookup := make(map[ID]GeoDictionaryNode, len(items))
	for _, item := range items {
		if item.ID == "" {
			return nil, nil, fmt.Errorf("%w: Geo node ID is required", ErrAddressDimensionInvalid)
		}
		if _, exists := lookup[item.ID]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate Geo node id %s", ErrAddressDimensionInvalid, item.ID)
		}
		normalized, err := normalizeGeoDictionaryNode(item)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrAddressDimensionInvalid, err)
		}
		lookup[normalized.ID] = normalized
		definitions = append(definitions, flowdimension.GeoNodeDefinition{
			ID: string(normalized.ID), Kind: normalized.Kind, Code: normalized.Code,
			Name: normalized.Name, ParentID: string(normalized.ParentID), Enabled: normalized.Enabled,
		})
	}
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].ID < definitions[j].ID })
	return definitions, lookup, nil
}

func canonicalAddressDimensionOperators(items []ISPOperator) ([]flowdimension.OperatorDefinition, error) {
	definitions := make([]flowdimension.OperatorDefinition, 0, len(items))
	seenIDs := make(map[ID]struct{}, len(items))
	seenFlowIDs := make(map[uint16]struct{}, len(items))
	enabledASNOwners := make(map[uint32]ID)
	for _, item := range items {
		if item.ID == "" || item.FlowISPID == 0 {
			return nil, fmt.Errorf("%w: operator identity and non-zero Flow ISP id are required", ErrAddressDimensionInvalid)
		}
		if _, exists := seenIDs[item.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate operator id %s", ErrAddressDimensionInvalid, item.ID)
		}
		if _, exists := seenFlowIDs[item.FlowISPID]; exists {
			return nil, fmt.Errorf("%w: duplicate Flow ISP id %d", ErrAddressDimensionInvalid, item.FlowISPID)
		}
		item.ASNs = append([]uint32(nil), item.ASNs...)
		normalized, err := normalizeISPOperator(item)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrAddressDimensionInvalid, err)
		}
		if normalized.Enabled {
			for _, asn := range normalized.ASNs {
				if owner, exists := enabledASNOwners[asn]; exists {
					return nil, fmt.Errorf("%w: ASN %d is assigned to enabled operators %s and %s", ErrAddressDimensionInvalid, asn, owner, normalized.ID)
				}
				enabledASNOwners[asn] = normalized.ID
			}
		}
		definitions = append(definitions, flowdimension.OperatorDefinition{
			ID: string(normalized.ID), FlowISPID: normalized.FlowISPID, Code: normalized.Code,
			Name: normalized.Name, ShortName: normalized.ShortName, Category: normalized.Category,
			ASNs: append(make([]uint32, 0, len(normalized.ASNs)), normalized.ASNs...), Enabled: normalized.Enabled,
		})
		seenIDs[item.ID] = struct{}{}
		seenFlowIDs[item.FlowISPID] = struct{}{}
	}
	sort.Slice(definitions, func(i, j int) bool {
		if definitions[i].FlowISPID != definitions[j].FlowISPID {
			return definitions[i].FlowISPID < definitions[j].FlowISPID
		}
		return definitions[i].ID < definitions[j].ID
	})
	return definitions, nil
}

func canonicalAddressDimensionSources(sources []AddressDimensionSource) ([]AddressDimensionSource, error) {
	result := append(make([]AddressDimensionSource, 0, len(sources)), sources...)
	sort.Slice(result, func(i, j int) bool {
		return addressDimensionSourceRank(result[i].Slot) < addressDimensionSourceRank(result[j].Slot)
	})
	seen := make(map[string]struct{}, len(result))
	for _, source := range result {
		if addressDimensionSourceRank(source.Slot) < 0 || source.ImportID == "" || source.SlotRowVersion == 0 ||
			len(source.ChecksumSHA256) != 64 || strings.ToLower(source.ChecksumSHA256) != source.ChecksumSHA256 {
			return nil, fmt.Errorf("%w: invalid address import source manifest", ErrAddressDimensionInvalid)
		}
		if decoded, err := hex.DecodeString(source.ChecksumSHA256); err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("%w: invalid address import source checksum", ErrAddressDimensionInvalid)
		}
		if _, exists := seen[source.Slot]; exists {
			return nil, fmt.Errorf("%w: duplicate address import source slot %s", ErrAddressDimensionInvalid, source.Slot)
		}
		seen[source.Slot] = struct{}{}
	}
	return result, nil
}

// Sources are ordered from the broad fallback to the dimension-specific
// generations: combined, then geo/asn, then the manual Prefixes in this draft.
func addressDimensionSourceRank(slot string) int {
	switch slot {
	case AddressImportSlotCombined:
		return 0
	case AddressImportSlotGeo:
		return 1
	case AddressImportSlotASN:
		return 2
	default:
		return -1
	}
}

func compileAddressPrefixDefinition(prefix AddressPrefix, geography map[ID]GeoDictionaryNode, operators map[ID]ISPOperator) (flowdimension.PrefixDefinition, error) {
	parsed, err := netip.ParsePrefix(prefix.CIDR)
	if err != nil || prefix.CIDR != parsed.Masked().String() {
		return flowdimension.PrefixDefinition{}, fmt.Errorf("%w: prefix %s is not canonical", ErrAddressDimensionInvalid, prefix.ID)
	}
	labels := make(map[string]string, len(prefix.Labels)+14)
	for key, value := range prefix.Labels {
		labels[key] = value
	}
	if parsed.Addr().Is4() {
		labels["ip.family"] = "4"
	} else {
		labels["ip.family"] = "6"
	}
	if prefix.GeoLeafID != "" {
		seen := map[ID]bool{}
		current := prefix.GeoLeafID
		for depth := 0; current != ""; depth++ {
			if depth >= 5 || seen[current] {
				return flowdimension.PrefixDefinition{}, fmt.Errorf("%w: geography cycle for prefix %s", ErrAddressDimensionInvalid, prefix.ID)
			}
			seen[current] = true
			node, ok := geography[current]
			if !ok || !node.Enabled {
				return flowdimension.PrefixDefinition{}, fmt.Errorf("%w: prefix %s references missing or disabled geography %s", ErrAddressDimensionInvalid, prefix.ID, current)
			}
			labels["geo."+node.Kind] = node.Code
			labels["geo."+node.Kind+"_id"] = string(node.ID)
			current = node.ParentID
		}
	}
	if prefix.OperatorID != "" {
		operator, ok := operators[prefix.OperatorID]
		if !ok || !operator.Enabled {
			return flowdimension.PrefixDefinition{}, fmt.Errorf("%w: prefix %s references missing or disabled operator %s", ErrAddressDimensionInvalid, prefix.ID, prefix.OperatorID)
		}
		labels["operator.id"] = string(operator.ID)
		labels["operator.code"] = operator.Code
		labels["operator.category"] = operator.Category
	}
	if prefix.ASN != nil {
		labels["asn"] = strconv.FormatUint(uint64(*prefix.ASN), 10)
	}
	return flowdimension.PrefixDefinition{ID: prefix.ID, CIDR: prefix.CIDR, Labels: labels}, nil
}

func compileAddressSetDefinition(set AddressSet, geography map[ID]GeoDictionaryNode, operators map[ID]ISPOperator) (flowdimension.AddressSetDefinition, error) {
	var selector struct {
		Labels      map[string][]string `json:"labels"`
		GeoNodeIDs  []ID                `json:"geo_node_ids"`
		OperatorIDs []ID                `json:"operator_ids"`
		ASNs        []uint32            `json:"asns"`
		Families    []int               `json:"families"`
	}
	data, err := json.Marshal(set.Selector)
	if err != nil || json.Unmarshal(data, &selector) != nil {
		return flowdimension.AddressSetDefinition{}, fmt.Errorf("%w: address set %s selector", ErrAddressDimensionInvalid, set.ID)
	}
	labels := make(map[string][]string, len(selector.Labels)+8)
	for key, values := range selector.Labels {
		labels[key] = append([]string(nil), values...)
	}
	for _, nodeID := range selector.GeoNodeIDs {
		node, ok := geography[nodeID]
		if !ok || !node.Enabled {
			return flowdimension.AddressSetDefinition{}, fmt.Errorf("%w: address set %s references missing or disabled geography %s", ErrAddressDimensionInvalid, set.ID, nodeID)
		}
		key := "geo." + node.Kind + "_id"
		labels[key] = append(labels[key], string(node.ID))
	}
	for _, operatorID := range selector.OperatorIDs {
		operator, ok := operators[operatorID]
		if !ok || !operator.Enabled {
			return flowdimension.AddressSetDefinition{}, fmt.Errorf("%w: address set %s references missing or disabled operator %s", ErrAddressDimensionInvalid, set.ID, operatorID)
		}
		labels["operator.id"] = append(labels["operator.id"], string(operator.ID))
	}
	for _, asn := range selector.ASNs {
		labels["asn"] = append(labels["asn"], strconv.FormatUint(uint64(asn), 10))
	}
	for _, family := range selector.Families {
		labels["ip.family"] = append(labels["ip.family"], strconv.Itoa(family))
	}
	for key, values := range labels {
		sort.Strings(values)
		labels[key] = compactDimensionStrings(values)
	}
	return flowdimension.AddressSetDefinition{
		ID: set.ID, Name: set.Name, Selector: flowdimension.LabelSelector{Labels: labels},
		Members: append([]string(nil), set.ExplicitMembers...), ExcludeMembers: append([]string(nil), set.ExplicitExcludeMembers...),
		IncludeSetIDs: append([]string(nil), set.IncludeSetIDs...), ExcludeSetIDs: append([]string(nil), set.ExcludeSetIDs...),
		MatchDirection: set.MatchDirection, Enabled: set.Enabled,
	}, nil
}

func compactDimensionStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if strings.TrimSpace(value) != "" && value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

// addressBundleDomain is the single-domain identity written to the bundle's
// TenantID field. flowdimension requires a non-empty identifier there (it is not
// modified in this port); with no tenant, a fixed sentinel keeps the bundle valid
// and its bytes deterministic.
const addressBundleDomain = "default"

// encodeAddressDimensionBundle serializes the compiled draft to the WADS input
// bundle and compiles it. De-tenanted: the bundle identity is the fixed domain.
func encodeAddressDimensionBundle(draft AddressDimensionDraft, snapshotID string, version uint64, effectiveFrom time.Time) ([]byte, *flowdimension.CompiledSnapshot, string, error) {
	bundle := flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: snapshotID, TenantID: addressBundleDomain,
		Version: version, EffectiveFrom: effectiveFrom.UTC(), Prefixes: draft.Prefixes, AddressSets: draft.AddressSets,
		Operators: draft.Operators, GeoNodes: draft.GeoNodes,
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		return nil, nil, "", err
	}
	digest := sha256.Sum256(data)
	checksum := "sha256:" + hex.EncodeToString(digest[:])
	compiled, err := flowdimension.DecodeAndCompileBundle(data, checksum, flowdimension.CompileLimits{})
	if err != nil {
		return nil, nil, "", fmt.Errorf("%w: %v", ErrAddressDimensionInvalid, err)
	}
	return data, compiled, checksum, nil
}
