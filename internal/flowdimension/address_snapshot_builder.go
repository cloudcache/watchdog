// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
)

const (
	AddressSnapshotSourceCombined = "combined"
	AddressSnapshotSourceGeo      = "geo"
	AddressSnapshotSourceASN      = "asn"
)

// AddressSnapshotBuildGeo is the source-side representation used before the
// WADS string dictionary is assigned. IDs are stable identities; codes and
// display strings are metadata only.
type AddressSnapshotBuildGeo struct {
	CountryCode string
	AdminCode   string
	Subdivision string
	City        string
	ContinentID string
	RegionID    string
	CountryID   string
	ProvinceID  string
	CityID      string
}

// AddressSnapshotBuildRange is one inclusive, already-normalized source
// range. Ranges inside one source generation must be ordered, disjoint, and
// contain every durable row exactly once.
type AddressSnapshotBuildRange struct {
	Start netip.Addr
	End   netip.Addr
	Geo   AddressSnapshotBuildGeo
	ISPID uint16
	ASN   uint32
}

type AddressSnapshotBuildSource struct {
	Slot           string
	ImportID       string
	ChecksumSHA256 string
	SlotRowVersion uint64
	RowCountV4     uint64
	RowCountV6     uint64
	Ranges         []AddressSnapshotBuildRange
}

type AddressSnapshotBuildGeoNode struct {
	ID       string
	Kind     string
	Code     string
	Name     string
	ParentID string
	Enabled  bool
}

type AddressSnapshotBuildOperator struct {
	ID        uint16
	StableID  string
	Code      string
	Name      string
	ShortName string
	Category  string
	ASNs      []uint32
	Enabled   bool
}

type AddressSnapshotBuildInput struct {
	Definition        *CompiledSnapshot
	BuilderVersion    string
	Sources           []AddressSnapshotBuildSource
	SupplierGeoNodes  []AddressSnapshotBuildGeoNode
	SupplierOperators []AddressSnapshotBuildOperator
}

type AddressSnapshotBuildResult struct {
	Artifact       AddressSnapshotArtifact
	Data           []byte
	ChecksumSHA256 string
}

// BuildAddressSnapshot performs a deterministic two-pass merge. The first
// pass discovers the string/value dictionaries; the second emits coalesced
// ranges. Large base sources are never expanded to individual addresses.
func BuildAddressSnapshot(input AddressSnapshotBuildInput, limits AddressSnapshotLimits) (AddressSnapshotBuildResult, error) {
	return BuildAddressSnapshotContext(context.Background(), input, limits)
}

// BuildAddressSnapshotContext is the operation-job entry point. It checks
// cancellation between bounded build chunks; the non-context wrapper remains
// for deterministic tooling and tests.
func BuildAddressSnapshotContext(ctx context.Context, input AddressSnapshotBuildInput, limits AddressSnapshotLimits) (AddressSnapshotBuildResult, error) {
	if ctx == nil {
		return AddressSnapshotBuildResult{}, errors.New("address snapshot build context is required")
	}
	if err := ctx.Err(); err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	limits, err := normalizeAddressSnapshotLimits(limits)
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	if input.Definition == nil || input.Definition.metadata.SchemaVersion < 3 || !validText(input.BuilderVersion, 64) {
		return AddressSnapshotBuildResult{}, fmt.Errorf("%w: schema v3 definition and builder version are required", ErrInvalidAddressSnapshot)
	}
	sources, layers, err := prepareAddressSnapshotBuildSources(ctx, input.Sources)
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	manualV4, manualV6, err := input.Definition.addressSnapshotManualRanges()
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	if len(manualV4) != 0 {
		layers.v4 = append(layers.v4, manualV4)
	}
	if len(manualV6) != 0 {
		layers.v6 = append(layers.v6, manualV6)
	}

	customerOperators, customerByID, customerByFlowID, customerByASN := addressSnapshotCustomerOperators(input.Definition)
	geoNodes, err := prepareAddressSnapshotGeoNodes(input.SupplierGeoNodes, input.Definition.GeoNodeDefinitions())
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	supplierOperators, err := prepareAddressSnapshotSupplierOperators(input.SupplierOperators)
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	operators := append(supplierOperators, customerOperators...)

	sets := input.Definition.AddressSetMetadata()
	sort.Slice(sets, func(i, j int) bool { return sets[i].ID < sets[j].ID })
	stringSet := make(map[string]struct{})
	collect := func(values ...string) {
		for _, value := range values {
			if value != "" {
				stringSet[value] = struct{}{}
			}
		}
	}
	for _, source := range sources {
		collect(source.Slot, source.ImportID, source.ChecksumSHA256)
	}
	for _, node := range geoNodes {
		collect(node.ID, node.Kind, node.Code, node.Name, node.ParentID)
	}
	for _, operator := range operators {
		collect(operator.StableID, operator.Code, operator.Name, operator.ShortName, operator.Category)
	}
	for _, set := range sets {
		collect(set.ID, set.Name)
	}

	values := make(map[addressSnapshotTextValue]struct{})
	segmentCountV4, segmentCountV6 := 0, 0
	firstPass := func(family int, start, end netip.Addr, active []*addressSnapshotBuildLayerRange) error {
		if (segmentCountV4+segmentCountV6)&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		value, err := composeAddressSnapshotTextValue(active, customerByID, customerByFlowID, customerByASN)
		if err != nil {
			return err
		}
		if value.zero() {
			return nil
		}
		if family == 4 {
			segmentCountV4++
			if segmentCountV4 > limits.MaxIPv4Ranges {
				return fmt.Errorf("%w: IPv4 range count exceeds build limit", ErrInvalidAddressSnapshot)
			}
		} else {
			segmentCountV6++
			if segmentCountV6 > limits.MaxIPv6Ranges {
				return fmt.Errorf("%w: IPv6 range count exceeds build limit", ErrInvalidAddressSnapshot)
			}
		}
		values[value] = struct{}{}
		value.collectStrings(collect)
		return nil
	}
	if err := walkAddressSnapshotBuildLayers(4, layers.v4, firstPass); err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	if err := walkAddressSnapshotBuildLayers(6, layers.v6, firstPass); err != nil {
		return AddressSnapshotBuildResult{}, err
	}

	stringsInput := make([]string, 0, len(stringSet))
	for value := range stringSet {
		stringsInput = append(stringsInput, value)
	}
	dictionary, err := CanonicalAddressSnapshotStrings(stringsInput...)
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	stringIndexes := make(map[string]uint32, len(dictionary))
	for index, value := range dictionary {
		stringIndexes[value] = uint32(index)
	}
	artifact := AddressSnapshotArtifact{
		SnapshotID: input.Definition.metadata.SnapshotID, TenantID: input.Definition.metadata.TenantID,
		Version: input.Definition.metadata.Version, EffectiveFrom: input.Definition.metadata.EffectiveFrom,
		BuilderVersion: input.BuilderVersion, SourceManifestSHA256: addressSnapshotSourceManifestSHA256(sources), Strings: dictionary,
		Sources:     encodeAddressSnapshotSources(sources, stringIndexes),
		GeoNodes:    encodeAddressSnapshotGeoNodes(geoNodes, stringIndexes),
		Operators:   encodeAddressSnapshotOperators(operators, stringIndexes),
		AddressSets: encodeAddressSnapshotSets(sets, stringIndexes),
		Values:      []AddressSnapshotValue{{}},
	}

	type encodedValue struct {
		text    addressSnapshotTextValue
		value   AddressSnapshotValue
		encoded []byte
	}
	encodedValues := make([]encodedValue, 0, len(values))
	valueOrdinal := 0
	for value := range values {
		if valueOrdinal&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return AddressSnapshotBuildResult{}, err
			}
		}
		valueOrdinal++
		encoded, err := value.encode(stringIndexes)
		if err != nil {
			return AddressSnapshotBuildResult{}, err
		}
		encodedValues = append(encodedValues, encodedValue{text: value, value: encoded, encoded: marshalAddressSnapshotValue(encoded)})
	}
	sort.Slice(encodedValues, func(i, j int) bool { return bytes.Compare(encodedValues[i].encoded, encodedValues[j].encoded) < 0 })
	valueIndexes := make(map[addressSnapshotTextValue]uint32, len(encodedValues))
	for _, value := range encodedValues {
		if len(artifact.Values) > 1 && bytes.Equal(marshalAddressSnapshotValue(artifact.Values[len(artifact.Values)-1]), value.encoded) {
			return AddressSnapshotBuildResult{}, fmt.Errorf("%w: two logical values collapse to one encoded value", ErrInvalidAddressSnapshot)
		}
		valueIndexes[value.text] = uint32(len(artifact.Values))
		artifact.Values = append(artifact.Values, value.value)
	}

	secondPass := func(family int, start, end netip.Addr, active []*addressSnapshotBuildLayerRange) error {
		if (len(artifact.IPv4Ranges)+len(artifact.IPv6Ranges))&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		value, err := composeAddressSnapshotTextValue(active, customerByID, customerByFlowID, customerByASN)
		if err != nil || value.zero() {
			return err
		}
		valueIndex, exists := valueIndexes[value]
		if !exists {
			return fmt.Errorf("%w: second build pass produced an unknown value", ErrInvalidAddressSnapshot)
		}
		if family == 4 {
			rangeValue, err := AddressSnapshotIPv4(start, end, valueIndex)
			if err != nil {
				return err
			}
			artifact.IPv4Ranges = appendAddressSnapshotIPv4Range(artifact.IPv4Ranges, rangeValue)
		} else {
			rangeValue, err := AddressSnapshotIPv6(start, end, valueIndex)
			if err != nil {
				return err
			}
			artifact.IPv6Ranges = appendAddressSnapshotIPv6Range(artifact.IPv6Ranges, rangeValue)
		}
		return nil
	}
	if err := walkAddressSnapshotBuildLayers(4, layers.v4, secondPass); err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	if err := walkAddressSnapshotBuildLayers(6, layers.v6, secondPass); err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	data, err := EncodeAddressSnapshot(artifact, limits)
	if err != nil {
		return AddressSnapshotBuildResult{}, err
	}
	decoded, err := DecodeAddressSnapshot(data, limits)
	if err != nil || decoded.SnapshotID != artifact.SnapshotID || decoded.Version != artifact.Version {
		return AddressSnapshotBuildResult{}, fmt.Errorf("%w: builder round-trip verification failed: %v", ErrInvalidAddressSnapshot, err)
	}
	digest := sha256.Sum256(data)
	return AddressSnapshotBuildResult{Artifact: artifact, Data: data, ChecksumSHA256: "sha256:" + hex.EncodeToString(digest[:])}, nil
}

type addressSnapshotPreparedSource struct {
	Slot           string
	ImportID       string
	ChecksumSHA256 string
	SlotRowVersion uint64
	RowCountV4     uint64
	RowCountV6     uint64
}

type addressSnapshotBuildLayers struct {
	v4 [][]addressSnapshotBuildLayerRange
	v6 [][]addressSnapshotBuildLayerRange
}

type addressSnapshotBuildLayerRange struct {
	start  netip.Addr
	end    netip.Addr
	slot   string
	source AddressSnapshotBuildRange
	manual addressSnapshotManualValue
}

func prepareAddressSnapshotBuildSources(ctx context.Context, input []AddressSnapshotBuildSource) ([]addressSnapshotPreparedSource, addressSnapshotBuildLayers, error) {
	sources := append([]AddressSnapshotBuildSource(nil), input...)
	sort.Slice(sources, func(i, j int) bool {
		return addressSnapshotSourceRank(sources[i].Slot) < addressSnapshotSourceRank(sources[j].Slot)
	})
	prepared := make([]addressSnapshotPreparedSource, 0, len(sources))
	layers := addressSnapshotBuildLayers{}
	for index, source := range sources {
		if addressSnapshotSourceRank(source.Slot) < 0 || source.ImportID == "" || !validSnapshotSHA256(source.ChecksumSHA256) || source.SlotRowVersion == 0 ||
			(index > 0 && sources[index-1].Slot == source.Slot) {
			return nil, addressSnapshotBuildLayers{}, fmt.Errorf("%w: source manifest is invalid", ErrInvalidAddressSnapshot)
		}
		v4, v6 := make([]addressSnapshotBuildLayerRange, 0), make([]addressSnapshotBuildLayerRange, 0)
		var last4, last6 netip.Addr
		seenV6 := false
		for rangeIndex, item := range source.Ranges {
			if rangeIndex&4095 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, addressSnapshotBuildLayers{}, err
				}
			}
			start, end := item.Start.Unmap(), item.End.Unmap()
			if !start.IsValid() || !end.IsValid() || start.BitLen() != end.BitLen() || start.Compare(end) > 0 {
				return nil, addressSnapshotBuildLayers{}, fmt.Errorf("%w: source %s range %d is invalid", ErrInvalidAddressSnapshot, source.Slot, rangeIndex)
			}
			layer := addressSnapshotBuildLayerRange{start: start, end: end, slot: source.Slot, source: item}
			if start.Is4() {
				if seenV6 || (last4.IsValid() && last4.Compare(start) >= 0) {
					return nil, addressSnapshotBuildLayers{}, fmt.Errorf("%w: source %s IPv4 ranges are not canonical", ErrInvalidAddressSnapshot, source.Slot)
				}
				last4 = end
				v4 = append(v4, layer)
			} else {
				seenV6 = true
				if last6.IsValid() && last6.Compare(start) >= 0 {
					return nil, addressSnapshotBuildLayers{}, fmt.Errorf("%w: source %s IPv6 ranges are not canonical", ErrInvalidAddressSnapshot, source.Slot)
				}
				last6 = end
				v6 = append(v6, layer)
			}
		}
		// RowCountV4/V6 are provenance counts for the immutable import, not
		// canonical range counts: longest-prefix normalization can both split and
		// coalesce source rows. The repository adapter verifies the exact input
		// counts before the normalized ranges enter this pure builder.
		prepared = append(prepared, addressSnapshotPreparedSource{
			Slot: source.Slot, ImportID: source.ImportID, ChecksumSHA256: source.ChecksumSHA256,
			SlotRowVersion: source.SlotRowVersion, RowCountV4: source.RowCountV4, RowCountV6: source.RowCountV6,
		})
		if len(v4) != 0 {
			layers.v4 = append(layers.v4, v4)
		}
		if len(v6) != 0 {
			layers.v6 = append(layers.v6, v6)
		}
	}
	return prepared, layers, nil
}

func addressSnapshotSourceRank(slot string) int {
	switch slot {
	case AddressSnapshotSourceCombined:
		return 0
	case AddressSnapshotSourceGeo:
		return 1
	case AddressSnapshotSourceASN:
		return 2
	default:
		return -1
	}
}

type addressSnapshotManualValue struct {
	prefix      compiledPrefix
	hasPrefix   bool
	membership  compiledAddressSetMembership
	override    compiledGeoOverride
	hasOverride bool
}

func (s *CompiledSnapshot) addressSnapshotManualRanges() ([]addressSnapshotBuildLayerRange, []addressSnapshotBuildLayerRange, error) {
	boundaries4 := make(map[netip.Addr]struct{})
	boundaries6 := make(map[netip.Addr]struct{})
	collect := func(prefix netip.Prefix) {
		prefix = prefix.Masked()
		boundaries := boundaries6
		if prefix.Addr().Is4() {
			boundaries = boundaries4
		}
		boundaries[prefix.Addr()] = struct{}{}
		if next := addressSnapshotPrefixEnd(prefix).Next(); next.IsValid() {
			boundaries[next] = struct{}{}
		}
	}
	for prefix := range s.prefixes.All() {
		collect(prefix)
	}
	for prefix := range s.addressSets.All() {
		collect(prefix)
	}
	for prefix := range s.geoOverrides.All() {
		collect(prefix)
	}
	build := func(family int, boundarySet map[netip.Addr]struct{}) []addressSnapshotBuildLayerRange {
		boundaries := make([]netip.Addr, 0, len(boundarySet))
		for address := range boundarySet {
			boundaries = append(boundaries, address)
		}
		sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Compare(boundaries[j]) < 0 })
		result := make([]addressSnapshotBuildLayerRange, 0, len(boundaries))
		for index, start := range boundaries {
			end := addressSnapshotMaxAddress(family)
			if index+1 < len(boundaries) {
				end = boundaries[index+1].Prev()
			}
			manual := addressSnapshotManualValue{}
			manual.prefix, manual.hasPrefix = s.prefixes.Lookup(start)
			manual.membership, _ = s.addressSets.Lookup(start)
			manual.override, manual.hasOverride = s.geoOverrides.Lookup(start)
			if !manual.hasPrefix && !manual.hasOverride && len(manual.membership.in) == 0 && len(manual.membership.out) == 0 {
				continue
			}
			result = append(result, addressSnapshotBuildLayerRange{start: start, end: end, slot: "manual", manual: manual})
		}
		return result
	}
	return build(4, boundaries4), build(6, boundaries6), nil
}

func addressSnapshotPrefixEnd(prefix netip.Prefix) netip.Addr {
	prefix = prefix.Masked()
	if prefix.Addr().Is4() {
		value := prefix.Addr().As4()
		for bit := prefix.Bits(); bit < 32; bit++ {
			value[bit/8] |= 1 << (7 - uint(bit%8))
		}
		return netip.AddrFrom4(value)
	}
	value := prefix.Addr().As16()
	for bit := prefix.Bits(); bit < 128; bit++ {
		value[bit/8] |= 1 << (7 - uint(bit%8))
	}
	return netip.AddrFrom16(value)
}

func addressSnapshotMaxAddress(family int) netip.Addr {
	if family == 4 {
		return netip.AddrFrom4([4]byte{0xff, 0xff, 0xff, 0xff})
	}
	return netip.AddrFrom16([16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
}

func walkAddressSnapshotBuildLayers(family int, layers [][]addressSnapshotBuildLayerRange, visit func(int, netip.Addr, netip.Addr, []*addressSnapshotBuildLayerRange) error) error {
	indexes := make([]int, len(layers))
	active := make([]*addressSnapshotBuildLayerRange, len(layers))
	var position netip.Addr
	for {
		if !position.IsValid() {
			for layerIndex := range layers {
				if len(layers[layerIndex]) != 0 && (!position.IsValid() || layers[layerIndex][0].start.Compare(position) < 0) {
					position = layers[layerIndex][0].start
				}
			}
			if !position.IsValid() {
				return nil
			}
		}
		var next netip.Addr
		clear(active)
		for layerIndex := range layers {
			for indexes[layerIndex] < len(layers[layerIndex]) && layers[layerIndex][indexes[layerIndex]].end.Compare(position) < 0 {
				indexes[layerIndex]++
			}
			if indexes[layerIndex] >= len(layers[layerIndex]) {
				continue
			}
			item := &layers[layerIndex][indexes[layerIndex]]
			candidate := item.start
			if item.start.Compare(position) <= 0 {
				active[layerIndex] = item
				candidate = item.end.Next()
			}
			if candidate.IsValid() && (!next.IsValid() || candidate.Compare(next) < 0) {
				next = candidate
			}
		}
		end := addressSnapshotMaxAddress(family)
		if next.IsValid() {
			end = next.Prev()
		}
		if err := visit(family, position, end, active); err != nil {
			return err
		}
		if !next.IsValid() {
			return nil
		}
		position = next
	}
}

type addressSnapshotTextGeo struct {
	CountryCode, AdminCode, Subdivision, City            string
	ContinentID, RegionID, CountryID, ProvinceID, CityID string
}

type addressSnapshotTextValue struct {
	SupplierGeo, CustomerGeo                    addressSnapshotTextGeo
	SupplierISPID, CustomerISPID                uint16
	SupplierASN, CustomerASN                    uint32
	CustomerOverrideBits                        uint8
	Local                                       bool
	PrimaryPrefixID, PrimaryPrefixCIDR          string
	Business, InAddressSetIDs, OutAddressSetIDs string
}

func composeAddressSnapshotTextValue(active []*addressSnapshotBuildLayerRange, customerByID map[string]OperatorDefinition, customerByFlowID map[uint16]OperatorDefinition, customerByASN map[uint32]uint16) (addressSnapshotTextValue, error) {
	var value addressSnapshotTextValue
	var manual *addressSnapshotManualValue
	for _, layer := range active {
		if layer == nil {
			continue
		}
		switch layer.slot {
		case AddressSnapshotSourceCombined:
			value.SupplierGeo = addressSnapshotTextGeoFromBuild(layer.source.Geo)
			value.SupplierISPID, value.SupplierASN = layer.source.ISPID, layer.source.ASN
		case AddressSnapshotSourceGeo:
			value.SupplierGeo = addressSnapshotTextGeoFromBuild(layer.source.Geo)
		case AddressSnapshotSourceASN:
			value.SupplierISPID, value.SupplierASN = layer.source.ISPID, layer.source.ASN
		case "manual":
			manual = &layer.manual
		}
	}
	value.CustomerGeo = value.SupplierGeo
	value.CustomerASN = value.SupplierASN
	value.CustomerISPID = customerByASN[value.CustomerASN]
	if manual == nil {
		return value, nil
	}
	if manual.hasPrefix {
		labels := manual.prefix.labels
		value.Local = labels["flow"] == "local"
		value.PrimaryPrefixID, value.PrimaryPrefixCIDR = manual.prefix.id, manual.prefix.cidr
		value.Business = businessLabel(manual.prefix)
		if geo, bits, exists := addressSnapshotCustomerGeoFromLabels(labels); exists {
			value.CustomerGeo = geo
			value.CustomerOverrideBits |= bits
		}
		if rawASN := labels["asn"]; rawASN != "" {
			asn, err := strconv.ParseUint(rawASN, 10, 32)
			if err != nil {
				return addressSnapshotTextValue{}, fmt.Errorf("%w: invalid manual ASN", ErrInvalidAddressSnapshot)
			}
			value.CustomerASN = uint32(asn)
			value.CustomerOverrideBits |= uint8(GeoOverrideASN)
			value.CustomerISPID = customerByASN[value.CustomerASN]
		}
		if operatorID := labels["operator.id"]; operatorID != "" {
			operator, exists := customerByID[operatorID]
			if !exists || !operator.Enabled {
				return addressSnapshotTextValue{}, fmt.Errorf("%w: manual prefix references unknown customer operator", ErrInvalidAddressSnapshot)
			}
			value.CustomerISPID = operator.FlowISPID
			value.CustomerOverrideBits |= uint8(GeoOverrideISPID)
		}
	}
	if manual.hasOverride {
		applyAddressSnapshotLegacyGeoOverride(&value, manual.override)
		if manual.override.fields&GeoOverrideISPID != 0 {
			if operator, exists := customerByFlowID[manual.override.info.ISPID]; !exists || !operator.Enabled {
				return addressSnapshotTextValue{}, fmt.Errorf("%w: legacy ISP override is outside customer operator namespace", ErrInvalidAddressSnapshot)
			}
		}
	}
	value.InAddressSetIDs = strings.Join(manual.membership.in, "\x00")
	value.OutAddressSetIDs = strings.Join(manual.membership.out, "\x00")
	return value, nil
}

func addressSnapshotTextGeoFromBuild(value AddressSnapshotBuildGeo) addressSnapshotTextGeo {
	return addressSnapshotTextGeo{
		CountryCode: value.CountryCode, AdminCode: value.AdminCode, Subdivision: value.Subdivision, City: value.City,
		ContinentID: value.ContinentID, RegionID: value.RegionID, CountryID: value.CountryID,
		ProvinceID: value.ProvinceID, CityID: value.CityID,
	}
}

func addressSnapshotCustomerGeoFromLabels(labels map[string]string) (addressSnapshotTextGeo, uint8, bool) {
	result := addressSnapshotTextGeo{
		ContinentID: labels["geo.continent_id"], RegionID: labels["geo.region_id"], CountryID: labels["geo.country_id"],
		ProvinceID: labels["geo.province_id"], CityID: labels["geo.city_id"],
		CountryCode: labels["geo.country"], Subdivision: labels["geo.province"], City: labels["geo.city"],
	}
	if result.ContinentID == "" && result.RegionID == "" && result.CountryID == "" && result.ProvinceID == "" && result.CityID == "" {
		return addressSnapshotTextGeo{}, 0, false
	}
	bits := uint8(0)
	if result.CountryID != "" || result.CountryCode != "" {
		bits |= uint8(GeoOverrideCountry)
	}
	if result.City != "" {
		result.AdminCode = result.City
		bits |= uint8(GeoOverrideAdminCode | GeoOverrideCity)
	} else if result.Subdivision != "" {
		result.AdminCode = result.Subdivision
		bits |= uint8(GeoOverrideAdminCode | GeoOverrideSubdivision)
	}
	return result, bits, true
}

func applyAddressSnapshotLegacyGeoOverride(value *addressSnapshotTextValue, override compiledGeoOverride) {
	if override.fields&GeoOverrideCountry != 0 {
		value.CustomerGeo.CountryCode = override.info.Country
		value.CustomerGeo.ContinentID, value.CustomerGeo.RegionID, value.CustomerGeo.CountryID = "", "", ""
		value.CustomerGeo.ProvinceID, value.CustomerGeo.CityID = "", ""
	}
	if override.fields&GeoOverrideAdminCode != 0 {
		value.CustomerGeo.AdminCode = override.info.AdminCode
		value.CustomerGeo.ProvinceID, value.CustomerGeo.CityID = "", ""
	}
	if override.fields&GeoOverrideSubdivision != 0 {
		value.CustomerGeo.Subdivision = override.info.Subdivision
	}
	if override.fields&GeoOverrideCity != 0 {
		value.CustomerGeo.City = override.info.City
		value.CustomerGeo.CityID = ""
	}
	if override.fields&GeoOverrideISPID != 0 {
		value.CustomerISPID = override.info.ISPID
	}
	if override.fields&GeoOverrideASN != 0 {
		value.CustomerASN = override.info.ASN
	}
	value.CustomerOverrideBits |= uint8(override.fields)
}

func (value addressSnapshotTextValue) zero() bool {
	return value == (addressSnapshotTextValue{})
}

func (value addressSnapshotTextValue) collectStrings(collect func(...string)) {
	for _, geo := range []addressSnapshotTextGeo{value.SupplierGeo, value.CustomerGeo} {
		collect(geo.CountryCode, geo.AdminCode, geo.Subdivision, geo.City, geo.ContinentID, geo.RegionID, geo.CountryID, geo.ProvinceID, geo.CityID)
	}
	collect(value.PrimaryPrefixID, value.PrimaryPrefixCIDR, value.Business)
	for _, encoded := range []string{value.InAddressSetIDs, value.OutAddressSetIDs} {
		if encoded != "" {
			collect(strings.Split(encoded, "\x00")...)
		}
	}
}

func (value addressSnapshotTextValue) encode(indexes map[string]uint32) (AddressSnapshotValue, error) {
	ref := func(value string) (uint32, error) {
		index, exists := indexes[value]
		if !exists {
			return 0, fmt.Errorf("%w: string dictionary reference is missing", ErrInvalidAddressSnapshot)
		}
		return index, nil
	}
	encodeGeo := func(value addressSnapshotTextGeo) (AddressSnapshotGeoValue, error) {
		fields := []string{value.CountryCode, value.AdminCode, value.Subdivision, value.City, value.ContinentID, value.RegionID, value.CountryID, value.ProvinceID, value.CityID}
		encoded := make([]uint32, len(fields))
		for position, field := range fields {
			var err error
			encoded[position], err = ref(field)
			if err != nil {
				return AddressSnapshotGeoValue{}, err
			}
		}
		return AddressSnapshotGeoValue{
			CountryCode: encoded[0], AdminCode: encoded[1], Subdivision: encoded[2], City: encoded[3], ContinentID: encoded[4],
			RegionID: encoded[5], CountryID: encoded[6], ProvinceID: encoded[7], CityID: encoded[8],
		}, nil
	}
	supplierGeo, err := encodeGeo(value.SupplierGeo)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	customerGeo, err := encodeGeo(value.CustomerGeo)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	primaryID, err := ref(value.PrimaryPrefixID)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	primaryCIDR, err := ref(value.PrimaryPrefixCIDR)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	business, err := ref(value.Business)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	encodeSets := func(encoded string) ([]uint32, error) {
		if encoded == "" {
			return nil, nil
		}
		values := strings.Split(encoded, "\x00")
		result := make([]uint32, len(values))
		for position, item := range values {
			var err error
			result[position], err = ref(item)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	inSets, err := encodeSets(value.InAddressSetIDs)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	outSets, err := encodeSets(value.OutAddressSetIDs)
	if err != nil {
		return AddressSnapshotValue{}, err
	}
	return AddressSnapshotValue{
		SupplierGeo: supplierGeo, CustomerGeo: customerGeo, SupplierISPID: value.SupplierISPID, CustomerISPID: value.CustomerISPID,
		SupplierASN: value.SupplierASN, CustomerASN: value.CustomerASN, CustomerOverrideBits: value.CustomerOverrideBits,
		Local: value.Local, PrimaryPrefixID: primaryID, PrimaryPrefixCIDR: primaryCIDR, Business: business,
		InAddressSetIDs: inSets, OutAddressSetIDs: outSets,
	}, nil
}

type addressSnapshotTextGeoNode struct {
	Namespace                      uint8
	ID, Kind, Code, Name, ParentID string
	Enabled                        bool
}

func prepareAddressSnapshotGeoNodes(supplier []AddressSnapshotBuildGeoNode, customer []GeoNodeDefinition) ([]addressSnapshotTextGeoNode, error) {
	byKey := make(map[string]addressSnapshotTextGeoNode, len(supplier)*2+len(customer))
	put := func(node addressSnapshotTextGeoNode) error {
		if node.ID == "" || node.Kind == "" || node.Code == "" || node.Name == "" {
			return fmt.Errorf("%w: Geo build node is incomplete", ErrInvalidAddressSnapshot)
		}
		key := strconv.Itoa(int(node.Namespace)) + "\x00" + node.ID
		if _, exists := byKey[key]; exists {
			return fmt.Errorf("%w: duplicate namespaced Geo id", ErrInvalidAddressSnapshot)
		}
		byKey[key] = node
		return nil
	}
	for _, node := range supplier {
		supplierNode := addressSnapshotTextGeoNode{Namespace: AddressSnapshotGeoSupplier, ID: node.ID, Kind: node.Kind, Code: node.Code, Name: node.Name, ParentID: node.ParentID, Enabled: node.Enabled}
		if err := put(supplierNode); err != nil {
			return nil, err
		}
		customerNode := supplierNode
		customerNode.Namespace = AddressSnapshotGeoCustomer
		if err := put(customerNode); err != nil {
			return nil, err
		}
	}
	for _, node := range customer {
		item := addressSnapshotTextGeoNode{Namespace: AddressSnapshotGeoCustomer, ID: node.ID, Kind: node.Kind, Code: node.Code, Name: node.Name, ParentID: node.ParentID, Enabled: node.Enabled}
		key := strconv.Itoa(int(item.Namespace)) + "\x00" + item.ID
		byKey[key] = item
	}
	result := make([]addressSnapshotTextGeoNode, 0, len(byKey))
	for _, node := range byKey {
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

type addressSnapshotTextOperator struct {
	Namespace                                 uint8
	ID                                        uint16
	StableID, Code, Name, ShortName, Category string
	ASNs                                      []uint32
	Enabled                                   bool
}

func prepareAddressSnapshotSupplierOperators(input []AddressSnapshotBuildOperator) ([]addressSnapshotTextOperator, error) {
	result := make([]addressSnapshotTextOperator, len(input))
	for index, operator := range input {
		asns := append([]uint32(nil), operator.ASNs...)
		sort.Slice(asns, func(i, j int) bool { return asns[i] < asns[j] })
		asns = compactUint32s(asns)
		result[index] = addressSnapshotTextOperator{
			Namespace: AddressSnapshotOperatorSupplier, ID: operator.ID, StableID: operator.StableID, Code: operator.Code,
			Name: operator.Name, ShortName: operator.ShortName, Category: operator.Category, ASNs: asns, Enabled: operator.Enabled,
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func addressSnapshotCustomerOperators(snapshot *CompiledSnapshot) ([]addressSnapshotTextOperator, map[string]OperatorDefinition, map[uint16]OperatorDefinition, map[uint32]uint16) {
	definitions := snapshot.OperatorDefinitions()
	result := make([]addressSnapshotTextOperator, 0, len(definitions))
	byID := make(map[string]OperatorDefinition, len(definitions))
	byFlowID := make(map[uint16]OperatorDefinition, len(definitions))
	byASN := make(map[uint32]uint16)
	for _, operator := range definitions {
		result = append(result, addressSnapshotTextOperator{
			Namespace: AddressSnapshotOperatorCustomer, ID: operator.FlowISPID, StableID: operator.ID, Code: operator.Code,
			Name: operator.Name, ShortName: operator.ShortName, Category: operator.Category, ASNs: append([]uint32(nil), operator.ASNs...), Enabled: operator.Enabled,
		})
		byID[operator.ID] = operator
		byFlowID[operator.FlowISPID] = operator
		if operator.Enabled {
			for _, asn := range operator.ASNs {
				byASN[asn] = operator.FlowISPID
			}
		}
	}
	return result, byID, byFlowID, byASN
}

func compactUint32s(values []uint32) []uint32 {
	if len(values) == 0 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func encodeAddressSnapshotSources(values []addressSnapshotPreparedSource, indexes map[string]uint32) []AddressSnapshotSource {
	result := make([]AddressSnapshotSource, len(values))
	for index, value := range values {
		result[index] = AddressSnapshotSource{
			Slot: indexes[value.Slot], ImportID: indexes[value.ImportID], ChecksumSHA256: indexes[value.ChecksumSHA256],
			SlotRowVersion: value.SlotRowVersion, RowCountV4: value.RowCountV4, RowCountV6: value.RowCountV6,
		}
	}
	return result
}

func encodeAddressSnapshotGeoNodes(values []addressSnapshotTextGeoNode, indexes map[string]uint32) []AddressSnapshotGeoNode {
	result := make([]AddressSnapshotGeoNode, len(values))
	for index, value := range values {
		result[index] = AddressSnapshotGeoNode{
			Namespace: value.Namespace, ID: indexes[value.ID], Kind: indexes[value.Kind], Code: indexes[value.Code],
			Name: indexes[value.Name], ParentID: indexes[value.ParentID], Enabled: value.Enabled,
		}
	}
	return result
}

func encodeAddressSnapshotOperators(values []addressSnapshotTextOperator, indexes map[string]uint32) []AddressSnapshotOperator {
	result := make([]AddressSnapshotOperator, len(values))
	for index, value := range values {
		result[index] = AddressSnapshotOperator{
			Namespace: value.Namespace, ID: value.ID, StableID: indexes[value.StableID], Code: indexes[value.Code], Name: indexes[value.Name],
			ShortName: indexes[value.ShortName], Category: indexes[value.Category], ASNs: append([]uint32(nil), value.ASNs...), Enabled: value.Enabled,
		}
	}
	return result
}

func encodeAddressSnapshotSets(values []AddressSetMetadata, indexes map[string]uint32) []AddressSnapshotSet {
	result := make([]AddressSnapshotSet, len(values))
	for index, value := range values {
		result[index] = AddressSnapshotSet{ID: indexes[value.ID], Name: indexes[value.Name], Enabled: value.Enabled}
	}
	return result
}

func addressSnapshotSourceManifestSHA256(sources []addressSnapshotPreparedSource) string {
	hash := sha256.New()
	writeString := func(value string) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	writeUint64 := func(value uint64) {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], value)
		_, _ = hash.Write(encoded[:])
	}
	for _, source := range sources {
		writeString(source.Slot)
		writeString(source.ImportID)
		writeString(source.ChecksumSHA256)
		writeUint64(source.SlotRowVersion)
		writeUint64(source.RowCountV4)
		writeUint64(source.RowCountV6)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func appendAddressSnapshotIPv4Range(ranges []AddressSnapshotIPv4Range, value AddressSnapshotIPv4Range) []AddressSnapshotIPv4Range {
	if len(ranges) != 0 {
		last := &ranges[len(ranges)-1]
		if last.ValueIndex == value.ValueIndex && last.End != ^uint32(0) && last.End+1 == value.Start {
			last.End = value.End
			return ranges
		}
	}
	return append(ranges, value)
}

func appendAddressSnapshotIPv6Range(ranges []AddressSnapshotIPv6Range, value AddressSnapshotIPv6Range) []AddressSnapshotIPv6Range {
	if len(ranges) != 0 {
		last := &ranges[len(ranges)-1]
		lastEnd := netip.AddrFrom16(last.End)
		if last.ValueIndex == value.ValueIndex && lastEnd.Next().IsValid() && lastEnd.Next() == netip.AddrFrom16(value.Start) {
			last.End = value.End
			return ranges
		}
	}
	return append(ranges, value)
}
