// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"unsafe"
)

// AddressSnapshotResolution is the immutable supplier/customer attribution
// compiled into one AddressSnap range. Supplier and customer ISP identities
// deliberately remain in separate namespaces.
type AddressSnapshotResolution struct {
	SupplierGeo            GeoInfo
	CustomerGeo            GeoInfo
	SupplierASN            uint32
	CustomerASN            uint32
	CustomerOverrideFields GeoOverrideFields
}

type compiledAddressSnapshotValue struct {
	resolution AddressSnapshotResolution
	local      bool
	prefixID   string
	prefixCIDR string
	business   string
	inSets     []string
	outSets    []string
	internal   []string
}

// AddressSnapshotIndex is immutable after construction. Its hot path performs
// only bounded binary searches and slice/string reads; it does no DB, file, or
// network I/O.
type AddressSnapshotIndex struct {
	metadata SnapshotMetadata
	values   []compiledAddressSnapshotValue
	ipv4     []AddressSnapshotIPv4Range
	ipv6     []AddressSnapshotIPv6Range
}

// DecodeAndCompileAddressSnapshot verifies the immutable object identity and
// WADS container before resolving dictionary references into a read-only index.
func DecodeAndCompileAddressSnapshot(data []byte, expectedChecksum string, limits AddressSnapshotLimits) (*AddressSnapshotIndex, error) {
	normalizedLimits, err := normalizeAddressSnapshotLimits(limits)
	if err != nil {
		return nil, err
	}
	want, err := parseSHA256Checksum(expectedChecksum)
	if err != nil {
		return nil, err
	}
	got := sha256.Sum256(data)
	if got != want {
		return nil, errors.New("address snapshot checksum mismatch")
	}
	artifact, err := DecodeAddressSnapshot(data, normalizedLimits)
	if err != nil {
		return nil, err
	}
	if err := validateAddressSnapshotIndexMemory(data, artifact, normalizedLimits.MaxDecodedMemoryBytes); err != nil {
		return nil, err
	}
	return compileAddressSnapshotIndex(artifact, "sha256:"+hex.EncodeToString(got[:]))
}

func validateAddressSnapshotIndexMemory(data []byte, artifact AddressSnapshotArtifact, maximum uint64) error {
	// DecodeAddressSnapshot already validated this header. Count its conservative
	// decode footprint plus the extra resolved value and membership arrays that
	// stay resident after compilation.
	uncompressed := binary.BigEndian.Uint64(data[20:28])
	resident := uint64(len(data)) + uncompressed*2
	resident += uint64(len(artifact.Values)) * (uint64(unsafe.Sizeof(compiledAddressSnapshotValue{})) + 32)
	membershipReferences := uint64(0)
	for _, value := range artifact.Values {
		membershipReferences += uint64(len(value.InAddressSetIDs) + len(value.OutAddressSetIDs))
	}
	resident += membershipReferences * 2 * uint64(unsafe.Sizeof(string("")))
	if resident > maximum {
		return fmt.Errorf("%w: compiled index memory %d exceeds %d", ErrInvalidAddressSnapshot, resident, maximum)
	}
	return nil
}

func compileAddressSnapshotIndex(artifact AddressSnapshotArtifact, checksum string) (*AddressSnapshotIndex, error) {
	stringsAt := func(reference uint32) string { return artifact.Strings[reference] }
	geo := func(value AddressSnapshotGeoValue, ispID uint16, asn uint32, source string) GeoInfo {
		country := stringsAt(value.CountryCode)
		if country == "" {
			country = GeoUnknownCountry
		}
		return GeoInfo{
			Country: country, AdminCode: stringsAt(value.AdminCode), Subdivision: stringsAt(value.Subdivision), City: stringsAt(value.City),
			ContinentID: stringsAt(value.ContinentID), RegionID: stringsAt(value.RegionID), CountryID: stringsAt(value.CountryID),
			ProvinceID: stringsAt(value.ProvinceID), CityID: stringsAt(value.CityID), ISPID: ispID, ASN: asn,
			Version: artifact.SnapshotID, Source: source,
		}
	}
	membership := func(references []uint32) []string {
		if len(references) == 0 {
			return nil
		}
		result := make([]string, len(references))
		for index, reference := range references {
			result[index] = stringsAt(reference)
		}
		return result
	}
	union := func(left, right []string) []string {
		if len(left) == 0 {
			return right
		}
		if len(right) == 0 {
			return left
		}
		result := make([]string, 0, len(left)+len(right))
		for leftIndex, rightIndex := 0, 0; leftIndex < len(left) || rightIndex < len(right); {
			switch {
			case rightIndex == len(right) || (leftIndex < len(left) && left[leftIndex] < right[rightIndex]):
				result = append(result, left[leftIndex])
				leftIndex++
			case leftIndex == len(left) || right[rightIndex] < left[leftIndex]:
				result = append(result, right[rightIndex])
				rightIndex++
			default:
				result = append(result, left[leftIndex])
				leftIndex++
				rightIndex++
			}
		}
		return result
	}

	values := make([]compiledAddressSnapshotValue, len(artifact.Values))
	prefixes := make(map[string]struct{})
	maxSets := 0
	for index, value := range artifact.Values {
		overrides := GeoOverrideFields(value.CustomerOverrideBits)
		customerSource := GeoSchemaV2
		if overrides != 0 {
			customerSource = "flow_geo_override"
		}
		inSets := membership(value.InAddressSetIDs)
		outSets := membership(value.OutAddressSetIDs)
		internal := union(inSets, outSets)
		if len(internal) > maxSets {
			maxSets = len(internal)
		}
		prefixID := stringsAt(value.PrimaryPrefixID)
		if prefixID != "" {
			prefixes[prefixID] = struct{}{}
		}
		business := stringsAt(value.Business)
		if business == "" {
			business = UnassignedDimensionID
		}
		values[index] = compiledAddressSnapshotValue{
			resolution: AddressSnapshotResolution{
				SupplierGeo: geo(value.SupplierGeo, value.SupplierISPID, value.SupplierASN, GeoSchemaV2),
				CustomerGeo: geo(value.CustomerGeo, value.CustomerISPID, value.CustomerASN, customerSource),
				SupplierASN: value.SupplierASN, CustomerASN: value.CustomerASN, CustomerOverrideFields: overrides,
			},
			local: value.Local, prefixID: prefixID, prefixCIDR: stringsAt(value.PrimaryPrefixCIDR), business: business,
			inSets: inSets, outSets: outSets, internal: internal,
		}
	}
	enabledSets := 0
	for _, set := range artifact.AddressSets {
		if set.Enabled {
			enabledSets++
		}
	}
	return &AddressSnapshotIndex{
		metadata: SnapshotMetadata{
			SchemaVersion: uint32(AddressSnapshotFormatVersion), SnapshotID: artifact.SnapshotID, TenantID: artifact.TenantID,
			Version: artifact.Version, EffectiveFrom: artifact.EffectiveFrom, Checksum: checksum, PrefixCount: len(prefixes),
			OperatorCount: len(artifact.Operators), GeoNodeCount: len(artifact.GeoNodes), EnabledAddressSetCount: enabledSets,
			MaxAddressSetsPerRecord: maxSets,
		},
		values: values, ipv4: artifact.IPv4Ranges, ipv6: artifact.IPv6Ranges,
	}, nil
}

func (i *AddressSnapshotIndex) Metadata() SnapshotMetadata {
	if i == nil {
		return SnapshotMetadata{}
	}
	return i.metadata
}

// ResolveAddress returns both supplier provenance and customer attribution
// from the same immutable AddressSnap generation.
func (i *AddressSnapshotIndex) ResolveAddress(address netip.Addr) (AddressSnapshotResolution, bool) {
	value, matched := i.lookup(address)
	if !matched {
		return AddressSnapshotResolution{}, false
	}
	return value.resolution, true
}

func (i *AddressSnapshotIndex) ClassifyEndpoints(source, destination netip.Addr) ClassifiedEndpoints {
	result := ClassifiedEndpoints{Direction: DirectionAmbiguous, Business: UnassignedDimensionID}
	if i == nil {
		return result
	}
	result.SnapshotID, result.Version = i.metadata.SnapshotID, i.metadata.Version
	if !source.IsValid() || !destination.IsValid() {
		return result
	}
	source, destination = source.Unmap(), destination.Unmap()
	sourceValue, sourceMatched := i.lookup(source)
	destinationValue, destinationMatched := i.lookup(destination)
	sourceLocal := sourceMatched && sourceValue.local
	destinationLocal := destinationMatched && destinationValue.local
	switch {
	case sourceLocal && !destinationLocal:
		result.Direction = DirectionOut
		result.Local = addressSnapshotEndpoint(source, EndpointSrc, sourceValue, sourceMatched, result.Direction)
		result.Remote = addressSnapshotEndpoint(destination, EndpointDst, destinationValue, destinationMatched, result.Direction)
		result.Business = sourceValue.business
	case !sourceLocal && destinationLocal:
		result.Direction = DirectionIn
		result.Local = addressSnapshotEndpoint(destination, EndpointDst, destinationValue, destinationMatched, result.Direction)
		result.Remote = addressSnapshotEndpoint(source, EndpointSrc, sourceValue, sourceMatched, result.Direction)
		result.Business = destinationValue.business
	case sourceLocal && destinationLocal:
		result.Direction = DirectionInternal
		result.Local = addressSnapshotEndpoint(source, EndpointSrc, sourceValue, sourceMatched, result.Direction)
		result.Remote = addressSnapshotEndpoint(destination, EndpointDst, destinationValue, destinationMatched, result.Direction)
		result.Business = sourceValue.business
	default:
		result.Direction = DirectionTransit
	}
	return result
}

func addressSnapshotEndpoint(address netip.Addr, side EndpointSide, value *compiledAddressSnapshotValue, matched bool, direction BusinessDirection) EndpointDimension {
	result := EndpointDimension{IP: address, Side: side, PrefixID: UnassignedDimensionID}
	if !matched {
		return result
	}
	if value.prefixID != "" {
		result.PrefixID, result.PrefixCIDR = value.prefixID, value.prefixCIDR
	}
	switch direction {
	case DirectionIn:
		result.AddressSets.ids = value.inSets
	case DirectionOut:
		result.AddressSets.ids = value.outSets
	case DirectionInternal:
		result.AddressSets.ids = value.internal
	}
	return result
}

func (i *AddressSnapshotIndex) lookup(address netip.Addr) (*compiledAddressSnapshotValue, bool) {
	if i == nil || !address.IsValid() {
		return nil, false
	}
	address = address.Unmap()
	if address.Is4() {
		key := addressToUint32(address)
		position := sort.Search(len(i.ipv4), func(position int) bool { return i.ipv4[position].End >= key })
		if position >= len(i.ipv4) || i.ipv4[position].Start > key {
			return nil, false
		}
		return &i.values[i.ipv4[position].ValueIndex], true
	}
	key := address.As16()
	position := sort.Search(len(i.ipv6), func(position int) bool { return compareAddress16(i.ipv6[position].End, key) >= 0 })
	if position >= len(i.ipv6) || compareAddress16(i.ipv6[position].Start, key) > 0 {
		return nil, false
	}
	return &i.values[i.ipv6[position].ValueIndex], true
}

func compareAddress16(left, right [16]byte) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}
