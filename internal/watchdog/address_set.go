package watchdog

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"
)

type AddressPrefix struct {
	ID           string            `json:"id"`
	TenantID     ID                `json:"tenant_id"`
	CIDR         string            `json:"cidr"`
	Family       uint8             `json:"family"`
	PrefixLength uint8             `json:"prefix_length"`
	Labels       map[string]string `json:"labels"`
	GeoLeafID    ID                `json:"geo_leaf_id,omitempty"`
	OperatorID   ID                `json:"operator_id,omitempty"`
	ASN          *uint32           `json:"asn,omitempty"`
	Source       string            `json:"source"`
	RowVersion   uint64            `json:"row_version"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

type AddressPrefixListFilter struct {
	Search     string
	Family     uint8
	Source     string
	GeoLeafID  ID
	OperatorID ID
	ASN        *uint32
	Limit      int
	Cursor     string
}

type AddressSet struct {
	ID                     string         `json:"id"`
	TenantID               ID             `json:"tenant_id"`
	Name                   string         `json:"name"`
	Description            string         `json:"description"`
	Selector               map[string]any `json:"selector"`
	ExplicitMembers        []string       `json:"explicit_members"`
	ExplicitExcludeMembers []string       `json:"explicit_exclude_members"`
	IncludeSetIDs          []string       `json:"include_set_ids"`
	ExcludeSetIDs          []string       `json:"exclude_set_ids"`
	MatchDirection         string         `json:"match_direction"`
	Enabled                bool           `json:"enabled"`
	RowVersion             uint64         `json:"row_version"`
	CreatedAt              time.Time      `json:"created_at"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

type AddressSetListFilter struct {
	Search         string
	MatchDirection string
	Enabled        *bool
	Limit          int
	Cursor         string
}

type AddressSetRepository interface {
	ListAddressPrefixes(ctx context.Context, tenantID ID) ([]AddressPrefix, error)
	ListAddressPrefixesPage(ctx context.Context, tenantID ID, filter AddressPrefixListFilter) ([]AddressPrefix, string, error)
	GetAddressPrefix(ctx context.Context, tenantID ID, prefixID string) (AddressPrefix, error)
	UpsertAddressPrefix(ctx context.Context, prefix AddressPrefix) (AddressPrefix, error)
	UpdateAddressPrefix(ctx context.Context, prefix AddressPrefix, expectedVersion uint64) (AddressPrefix, error)
	DeleteAddressPrefix(ctx context.Context, tenantID ID, prefixID string) error
	DeleteAddressPrefixVersion(ctx context.Context, tenantID ID, prefixID string, expectedVersion uint64) error
	ListAddressSets(ctx context.Context, tenantID ID) ([]AddressSet, error)
	ListAddressSetsPage(ctx context.Context, tenantID ID, filter AddressSetListFilter) ([]AddressSet, string, error)
	GetAddressSet(ctx context.Context, tenantID ID, setID string) (AddressSet, error)
	UpsertAddressSet(ctx context.Context, set AddressSet) (AddressSet, error)
	UpdateAddressSet(ctx context.Context, set AddressSet, expectedVersion uint64) (AddressSet, error)
	DeleteAddressSet(ctx context.Context, tenantID ID, setID string) error
	DeleteAddressSetVersion(ctx context.Context, tenantID ID, setID string, expectedVersion uint64) error
	PrepareAddressPrefixRevision(ctx context.Context, tenantID, actorID ID, operations []AddressPrefixBatchOperation) (AddressDraftRevision, error)
	ListAddressDraftRevisions(ctx context.Context, tenantID ID, filter AddressDraftRevisionListFilter) ([]AddressDraftRevision, string, error)
	GetAddressDraftRevision(ctx context.Context, tenantID, revisionID ID) (AddressDraftRevision, error)
	ApplyAddressDraftRevision(ctx context.Context, tenantID, actorID, revisionID ID, expectedVersion uint64) (AddressDraftRevision, error)
}

func normalizeAddressSet(set AddressSet) (AddressSet, error) {
	set.Name = strings.TrimSpace(set.Name)
	set.Description = strings.TrimSpace(set.Description)
	set.MatchDirection = strings.ToLower(strings.TrimSpace(set.MatchDirection))
	if set.MatchDirection == "" {
		set.MatchDirection = "both"
	}
	if set.Name == "" || len(set.Name) > 190 || (set.MatchDirection != "in" && set.MatchDirection != "out" && set.MatchDirection != "both") {
		return AddressSet{}, fmt.Errorf("%w: address set name or match_direction", ErrAddressTaxonomyInvalid)
	}
	if set.Selector == nil {
		set.Selector = map[string]any{}
	}
	var err error
	set.Selector, err = normalizeAddressSetSelector(set.Selector)
	if err != nil {
		return AddressSet{}, err
	}
	set.ExplicitMembers, err = normalizeAddressExpressionList(set.ExplicitMembers)
	if err != nil {
		return AddressSet{}, fmt.Errorf("explicit_members: %w", err)
	}
	set.ExplicitExcludeMembers, err = normalizeAddressExpressionList(set.ExplicitExcludeMembers)
	if err != nil {
		return AddressSet{}, fmt.Errorf("explicit_exclude_members: %w", err)
	}
	set.IncludeSetIDs, err = normalizeAddressSetIDs(set.ID, set.IncludeSetIDs)
	if err != nil {
		return AddressSet{}, err
	}
	set.ExcludeSetIDs, err = normalizeAddressSetIDs(set.ID, set.ExcludeSetIDs)
	if err != nil {
		return AddressSet{}, err
	}
	if len(set.Selector) == 0 && len(set.ExplicitMembers) == 0 && len(set.IncludeSetIDs) == 0 {
		return AddressSet{}, fmt.Errorf("%w: selector, explicit member, or included set is required", ErrAddressTaxonomyInvalid)
	}
	return set, nil
}

func normalizeAddressSetSelector(selector map[string]any) (map[string]any, error) {
	for key := range selector {
		switch key {
		case "labels", "geo_node_ids", "operator_ids", "asns", "families":
		default:
			return nil, fmt.Errorf("%w: unsupported selector field %q", ErrAddressTaxonomyInvalid, key)
		}
	}
	data, err := json.Marshal(selector)
	if err != nil {
		return nil, fmt.Errorf("%w: selector values", ErrAddressTaxonomyInvalid)
	}
	var wire struct {
		Labels      map[string]json.RawMessage `json:"labels"`
		GeoNodeIDs  []ID                       `json:"geo_node_ids"`
		OperatorIDs []ID                       `json:"operator_ids"`
		ASNs        []uint32                   `json:"asns"`
		Families    []int                      `json:"families"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, fmt.Errorf("%w: selector value types", ErrAddressTaxonomyInvalid)
	}
	result := make(map[string]any, len(selector))
	if len(wire.Labels) > 64 {
		return nil, fmt.Errorf("%w: selector has more than 64 label keys", ErrAddressTaxonomyInvalid)
	}
	labels := make(map[string]any, len(wire.Labels))
	for rawKey, rawValue := range wire.Labels {
		key := strings.TrimSpace(rawKey)
		if key == "" || len(key) > 64 {
			return nil, fmt.Errorf("%w: selector label key", ErrAddressTaxonomyInvalid)
		}
		if _, exists := labels[key]; exists {
			return nil, fmt.Errorf("%w: duplicate selector label key %q", ErrAddressTaxonomyInvalid, key)
		}
		var values []string
		var single string
		if string(rawValue) != "null" && json.Unmarshal(rawValue, &single) == nil {
			values = []string{single}
		} else if err := json.Unmarshal(rawValue, &values); err != nil {
			return nil, fmt.Errorf("%w: selector label %q must be a string or string array", ErrAddressTaxonomyInvalid, key)
		}
		if len(values) == 0 || len(values) > 256 {
			return nil, fmt.Errorf("%w: selector label %q value count", ErrAddressTaxonomyInvalid, key)
		}
		for index := range values {
			values[index] = strings.TrimSpace(values[index])
			if values[index] == "" || len(values[index]) > 255 {
				return nil, fmt.Errorf("%w: selector label %q value", ErrAddressTaxonomyInvalid, key)
			}
		}
		sort.Strings(values)
		values = compactStrings(values)
		labels[key] = values
	}
	if len(labels) > 0 {
		result["labels"] = labels
	}
	geoNodeIDs, err := normalizeSelectorIDs(wire.GeoNodeIDs, 1_000)
	if err != nil {
		return nil, fmt.Errorf("geo_node_ids: %w", err)
	}
	if len(geoNodeIDs) > 0 {
		result["geo_node_ids"] = geoNodeIDs
	}
	operatorIDs, err := normalizeSelectorIDs(wire.OperatorIDs, 1_000)
	if err != nil {
		return nil, fmt.Errorf("operator_ids: %w", err)
	}
	if len(operatorIDs) > 0 {
		result["operator_ids"] = operatorIDs
	}
	if len(wire.ASNs) > 10_000 {
		return nil, fmt.Errorf("%w: selector ASN list exceeds 10000", ErrAddressTaxonomyInvalid)
	}
	sort.Slice(wire.ASNs, func(i, j int) bool { return wire.ASNs[i] < wire.ASNs[j] })
	wire.ASNs = compactUint32s(wire.ASNs)
	for _, asn := range wire.ASNs {
		if asn == 0 {
			return nil, fmt.Errorf("%w: selector ASN zero is reserved for unknown", ErrAddressTaxonomyInvalid)
		}
	}
	if len(wire.ASNs) > 0 {
		result["asns"] = wire.ASNs
	}
	sort.Ints(wire.Families)
	wire.Families = compactInts(wire.Families)
	for _, family := range wire.Families {
		if family != 4 && family != 6 {
			return nil, fmt.Errorf("%w: selector family must be 4 or 6", ErrAddressTaxonomyInvalid)
		}
	}
	if len(wire.Families) > 0 {
		result["families"] = wire.Families
	}
	return result, nil
}

func normalizeSelectorIDs(values []ID, limit int) ([]ID, error) {
	if len(values) > limit {
		return nil, fmt.Errorf("%w: selector id list exceeds %d", ErrAddressTaxonomyInvalid, limit)
	}
	for index := range values {
		values[index] = ID(strings.TrimSpace(string(values[index])))
		if values[index] == "" || len(values[index]) > 36 {
			return nil, fmt.Errorf("%w: selector id", ErrAddressTaxonomyInvalid)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	output := values[:0]
	for _, value := range values {
		if len(output) == 0 || output[len(output)-1] != value {
			output = append(output, value)
		}
	}
	return output, nil
}

func compactStrings(values []string) []string {
	output := values[:0]
	for _, value := range values {
		if len(output) == 0 || output[len(output)-1] != value {
			output = append(output, value)
		}
	}
	return output
}

func compactUint32s(values []uint32) []uint32 {
	output := values[:0]
	for _, value := range values {
		if len(output) == 0 || output[len(output)-1] != value {
			output = append(output, value)
		}
	}
	return output
}

func compactInts(values []int) []int {
	output := values[:0]
	for _, value := range values {
		if len(output) == 0 || output[len(output)-1] != value {
			output = append(output, value)
		}
	}
	return output
}

func normalizeAddressPrefix(prefix AddressPrefix) (AddressPrefix, [16]byte, [16]byte, error) {
	prefix.CIDR = strings.TrimSpace(prefix.CIDR)
	preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: []string{prefix.CIDR}})
	if err != nil {
		return AddressPrefix{}, [16]byte{}, [16]byte{}, err
	}
	if len(preview.Result) != 1 {
		return AddressPrefix{}, [16]byte{}, [16]byte{}, fmt.Errorf("%w: one prefix record cannot expand to %d CIDRs; use a set operation", ErrAddressTaxonomyInvalid, len(preview.Result))
	}
	prefix.CIDR = preview.Result[0]
	parsed, err := netip.ParsePrefix(prefix.CIDR)
	if err != nil {
		return AddressPrefix{}, [16]byte{}, [16]byte{}, err
	}
	prefix.Family = 6
	if parsed.Addr().Is4() {
		prefix.Family = 4
	}
	prefix.PrefixLength = uint8(parsed.Bits())
	start, end := addressPrefixBounds(parsed)
	if prefix.Labels == nil {
		prefix.Labels = map[string]string{}
	}
	if len(prefix.Labels) > 128 {
		return AddressPrefix{}, [16]byte{}, [16]byte{}, fmt.Errorf("%w: address prefix has more than 128 labels", ErrAddressTaxonomyInvalid)
	}
	cleanLabels := make(map[string]string, len(prefix.Labels))
	for key, value := range prefix.Labels {
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if key == "" || len(key) > 64 || len(value) > 255 {
			return AddressPrefix{}, [16]byte{}, [16]byte{}, fmt.Errorf("%w: address prefix label", ErrAddressTaxonomyInvalid)
		}
		cleanLabels[key] = value
	}
	prefix.Labels = cleanLabels
	prefix.Source = strings.ToLower(strings.TrimSpace(prefix.Source))
	if prefix.Source == "" {
		prefix.Source = "manual"
	}
	if len(prefix.Source) > 32 {
		return AddressPrefix{}, [16]byte{}, [16]byte{}, fmt.Errorf("%w: address prefix source", ErrAddressTaxonomyInvalid)
	}
	return prefix, start, end, nil
}

func normalizeAddressExpressionList(values []string) ([]string, error) {
	if len(values) == 0 {
		return []string{}, nil
	}
	preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: "normalize", Left: values})
	if err != nil {
		return nil, err
	}
	return preview.Result, nil
}

func normalizeAddressSetIDs(self string, values []string) ([]string, error) {
	if len(values) > 1_000 {
		return nil, fmt.Errorf("%w: address set reference list exceeds 1000", ErrAddressTaxonomyInvalid)
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("%w: empty address set reference", ErrAddressTaxonomyInvalid)
		}
		if self != "" && value == self {
			return nil, ErrAddressTaxonomyCycle
		}
		result = append(result, value)
	}
	sort.Strings(result)
	output := result[:0]
	for _, value := range result {
		if len(output) == 0 || output[len(output)-1] != value {
			output = append(output, value)
		}
	}
	return output, nil
}
