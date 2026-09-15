package address

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"
)

// Geographic hierarchy kinds. continent→country→province→city form the parented
// hierarchy; region and the three flat base-data kinds (search_engine,
// cloud_provider, natural_region) stand alone (no parent).
const (
	GeoKindContinent     = "continent"
	GeoKindRegion        = "region"
	GeoKindCountry       = "country"
	GeoKindProvince      = "province"
	GeoKindCity          = "city"
	GeoKindSearchEngine  = "search_engine"
	GeoKindCloudProvider = "cloud_provider"
	GeoKindNaturalRegion = "natural_region"
)

// AddressPrefix is a manually-maintained prefix with geo/operator/ASN attribution.
type AddressPrefix struct {
	ID           string            `json:"id"`
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
	Offset     int
	Cursor     string
	Sort       string
	Desc       bool
	TableMode  bool
}

// AddressSet is a label-selector grouping with set algebra (union/intersection/difference).
type AddressSet struct {
	ID                     string         `json:"id"`
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
	Offset         int
	Cursor         string
	Sort           string
	Desc           bool
	TableMode      bool
}

// GeoDictionaryNode is one node of the continent/region/country/province/city tree.
type GeoDictionaryNode struct {
	ID         ID        `json:"id"`
	Kind       string    `json:"kind"`
	Code       string    `json:"code"`
	ParentID   ID        `json:"parent_id,omitempty"`
	Name       string    `json:"name"`
	ShortName  string    `json:"short_name,omitempty"`
	SortOrder  int       `json:"sort_order"`
	Enabled    bool      `json:"enabled"`
	RowVersion uint64    `json:"row_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ISPOperator is one operator identity; FlowISPID is the stable UInt16 Flow id.
type ISPOperator struct {
	ID         ID        `json:"id"`
	FlowISPID  uint16    `json:"flow_isp_id"`
	Code       string    `json:"code"`
	Name       string    `json:"name"`
	ShortName  string    `json:"short_name,omitempty"`
	Category   string    `json:"category"`
	ASNs       []uint32  `json:"asns"`
	SortOrder  int       `json:"sort_order"`
	Enabled    bool      `json:"enabled"`
	RowVersion uint64    `json:"row_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// GeoLineSelector is the include criteria of a line/region-group (大区组). Within
// a dimension the values are OR'd; across dimensions they are AND'd (a prefix
// matches when it satisfies the geo AND operator AND asn constraints that are
// set). An empty dimension is no constraint.
type GeoLineSelector struct {
	GeoNodeIDs  []ID     `json:"geo_node_ids,omitempty"`
	Families    []int    `json:"families,omitempty"`
	OperatorIDs []ID     `json:"operator_ids,omitempty"`
	ASNs        []uint32 `json:"asns,omitempty"`
}

// GeoLine (线路/大区组) is a named group: an include selector plus explicit CIDR
// members (unioned with the selector) minus the effective sets of exclude lines
// (recursively). Also carries a single operator_id + address_set_id for legacy
// routing.
type GeoLine struct {
	ID             ID              `json:"id"`
	ParentID       ID              `json:"parent_id,omitempty"`
	Code           string          `json:"code"`
	Name           string          `json:"name"`
	Description    string          `json:"description,omitempty"`
	GeoSelector    GeoLineSelector `json:"geo_selector"`
	Members        []string        `json:"members,omitempty"`
	ExcludeLineIDs []ID            `json:"exclude_line_ids,omitempty"`
	OperatorID     ID              `json:"operator_id,omitempty"`
	AddressSetID   string          `json:"address_set_id,omitempty"`
	SortOrder      int             `json:"sort_order"`
	Enabled        bool            `json:"enabled"`
	RowVersion     uint64          `json:"row_version"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type AddressTaxonomyListFilter struct {
	Search    string
	Kind      string
	ParentID  ID
	Enabled   *bool
	Limit     int
	Offset    int
	Cursor    string
	Sort      string
	Desc      bool
	TableMode bool
}

// --- normalizers (pure; reused verbatim, tenant fields removed) -------------

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

func normalizeGeoDictionaryNode(node GeoDictionaryNode) (GeoDictionaryNode, error) {
	node.Kind = strings.ToLower(strings.TrimSpace(node.Kind))
	node.Code = strings.TrimSpace(node.Code)
	node.Name = strings.TrimSpace(node.Name)
	node.ShortName = strings.TrimSpace(node.ShortName)
	if !validGeoKind(node.Kind) || node.Code == "" || len(node.Code) > 64 || node.Name == "" || len(node.Name) > 190 || len(node.ShortName) > 190 {
		return GeoDictionaryNode{}, fmt.Errorf("%w: kind, code, or name", ErrAddressTaxonomyInvalid)
	}
	return node, nil
}

func validGeoKind(kind string) bool {
	switch kind {
	case GeoKindContinent, GeoKindRegion, GeoKindCountry, GeoKindProvince, GeoKindCity,
		GeoKindSearchEngine, GeoKindCloudProvider, GeoKindNaturalRegion:
		return true
	default:
		return false
	}
}

func validGeoParentKind(parent, child string) bool {
	parentOrder := map[string]int{GeoKindContinent: 0, GeoKindRegion: 1, GeoKindCountry: 2, GeoKindProvince: 3, GeoKindCity: 4}
	p, parentOK := parentOrder[parent]
	c, childOK := parentOrder[child]
	return parentOK && childOK && p < c
}

func normalizeISPOperator(operator ISPOperator) (ISPOperator, error) {
	operator.Code = strings.TrimSpace(operator.Code)
	operator.Name = strings.TrimSpace(operator.Name)
	operator.ShortName = strings.TrimSpace(operator.ShortName)
	operator.Category = strings.ToLower(strings.TrimSpace(operator.Category))
	if operator.Category == "" {
		operator.Category = "other"
	}
	if operator.Code == "" || len(operator.Code) > 64 || operator.Name == "" || len(operator.Name) > 190 || len(operator.ShortName) > 190 || len(operator.Category) > 32 {
		return ISPOperator{}, fmt.Errorf("%w: operator code, name, or category", ErrAddressTaxonomyInvalid)
	}
	if len(operator.ASNs) > 10_000 {
		return ISPOperator{}, fmt.Errorf("%w: operator ASN list exceeds 10000", ErrAddressTaxonomyInvalid)
	}
	sort.Slice(operator.ASNs, func(i, j int) bool { return operator.ASNs[i] < operator.ASNs[j] })
	result := operator.ASNs[:0]
	for _, asn := range operator.ASNs {
		if asn == 0 {
			return ISPOperator{}, fmt.Errorf("%w: ASN zero is reserved for unknown", ErrAddressTaxonomyInvalid)
		}
		if len(result) == 0 || result[len(result)-1] != asn {
			result = append(result, asn)
		}
	}
	operator.ASNs = result
	return operator, nil
}

func normalizeGeoLine(line GeoLine) (GeoLine, error) {
	line.Code = strings.TrimSpace(line.Code)
	line.Name = strings.TrimSpace(line.Name)
	line.Description = strings.TrimSpace(line.Description)
	if line.Code == "" || len(line.Code) > 64 || line.Name == "" || len(line.Name) > 190 || len(line.Description) > 65_535 {
		return GeoLine{}, fmt.Errorf("%w: line code, name, or description", ErrAddressTaxonomyInvalid)
	}
	if len(line.GeoSelector.GeoNodeIDs) > 1_000 {
		return GeoLine{}, fmt.Errorf("%w: line geo selector exceeds 1000 nodes", ErrAddressTaxonomyInvalid)
	}
	ids := append([]ID(nil), line.GeoSelector.GeoNodeIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	line.GeoSelector.GeoNodeIDs = ids[:0]
	for _, id := range ids {
		if id == "" {
			return GeoLine{}, fmt.Errorf("%w: empty geo node id", ErrAddressTaxonomyInvalid)
		}
		if len(line.GeoSelector.GeoNodeIDs) == 0 || line.GeoSelector.GeoNodeIDs[len(line.GeoSelector.GeoNodeIDs)-1] != id {
			line.GeoSelector.GeoNodeIDs = append(line.GeoSelector.GeoNodeIDs, id)
		}
	}
	families := append([]int(nil), line.GeoSelector.Families...)
	sort.Ints(families)
	line.GeoSelector.Families = families[:0]
	for _, family := range families {
		if family != 4 && family != 6 {
			return GeoLine{}, fmt.Errorf("%w: line family must be 4 or 6", ErrAddressTaxonomyInvalid)
		}
		if len(line.GeoSelector.Families) == 0 || line.GeoSelector.Families[len(line.GeoSelector.Families)-1] != family {
			line.GeoSelector.Families = append(line.GeoSelector.Families, family)
		}
	}
	line.GeoSelector.OperatorIDs = dedupSortedIDs(line.GeoSelector.OperatorIDs)
	if len(line.GeoSelector.OperatorIDs) > 1_000 {
		return GeoLine{}, fmt.Errorf("%w: line selector exceeds 1000 operators", ErrAddressTaxonomyInvalid)
	}
	asns := append([]uint32(nil), line.GeoSelector.ASNs...)
	sort.Slice(asns, func(i, j int) bool { return asns[i] < asns[j] })
	line.GeoSelector.ASNs = asns[:0]
	for _, asn := range asns {
		if len(line.GeoSelector.ASNs) == 0 || line.GeoSelector.ASNs[len(line.GeoSelector.ASNs)-1] != asn {
			line.GeoSelector.ASNs = append(line.GeoSelector.ASNs, asn)
		}
	}
	if len(line.GeoSelector.ASNs) > 10_000 {
		return GeoLine{}, fmt.Errorf("%w: line selector exceeds 10000 asns", ErrAddressTaxonomyInvalid)
	}
	members := make([]string, 0, len(line.Members))
	seenMember := map[string]bool{}
	for _, raw := range line.Members {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(raw)
		if err != nil {
			return GeoLine{}, fmt.Errorf("%w: line member %q is not a valid CIDR", ErrAddressTaxonomyInvalid, raw)
		}
		canonical := prefix.Masked().String()
		if !seenMember[canonical] {
			seenMember[canonical] = true
			members = append(members, canonical)
		}
	}
	if len(members) > 10_000 {
		return GeoLine{}, fmt.Errorf("%w: line exceeds 10000 members", ErrAddressTaxonomyInvalid)
	}
	line.Members = members
	line.ExcludeLineIDs = dedupSortedIDs(line.ExcludeLineIDs)
	for _, id := range line.ExcludeLineIDs {
		if id == line.ID {
			return GeoLine{}, fmt.Errorf("%w: line cannot exclude itself", ErrAddressTaxonomyInvalid)
		}
	}
	if len(line.ExcludeLineIDs) > 1_000 {
		return GeoLine{}, fmt.Errorf("%w: line exceeds 1000 exclude references", ErrAddressTaxonomyInvalid)
	}
	return line, nil
}

// dedupSortedIDs returns the non-empty ids sorted and de-duplicated.
func dedupSortedIDs(in []ID) []ID {
	ids := append([]ID(nil), in...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]ID, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}
