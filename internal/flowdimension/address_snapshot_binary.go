// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net/netip"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const (
	AddressSnapshotFormatVersion = uint16(1)

	addressSnapshotHeaderSize = 36
	addressSnapshotFlagZstd   = uint16(1)

	defaultMaxAddressSnapshotCompressedBytes   = 512 << 20
	defaultMaxAddressSnapshotUncompressedBytes = 1 << 30
	defaultMaxAddressSnapshotDecodedMemory     = 2 << 30
	defaultMaxAddressSnapshotStrings           = 2_000_000
	defaultMaxAddressSnapshotStringBytes       = 256 << 20
	defaultMaxAddressSnapshotSources           = 8
	defaultMaxAddressSnapshotGeoNodes          = 1_000_000
	defaultMaxAddressSnapshotOperators         = 65_535
	defaultMaxAddressSnapshotSets              = 100_000
	defaultMaxAddressSnapshotValues            = 5_000_000
	defaultMaxAddressSnapshotIPv4Ranges        = 20_000_000
	defaultMaxAddressSnapshotIPv6Ranges        = 20_000_000
	defaultMaxAddressSnapshotSetsPerValue      = 32
	defaultMaxAddressSnapshotZstdWindow        = 128 << 20
)

var (
	addressSnapshotMagic       = [4]byte{'W', 'A', 'D', 'S'}
	addressSnapshotCRC32CTable = crc32.MakeTable(crc32.Castagnoli)
	ErrInvalidAddressSnapshot  = errors.New("invalid address snapshot")
)

type AddressSnapshotLimits struct {
	MaxCompressedBytes    int
	MaxUncompressedBytes  int
	MaxDecodedMemoryBytes uint64
	MaxStrings            int
	MaxStringBytes        int
	MaxSources            int
	MaxGeoNodes           int
	MaxOperators          int
	MaxAddressSets        int
	MaxValues             int
	MaxIPv4Ranges         int
	MaxIPv6Ranges         int
	MaxSetsPerValue       int
	MaxZstdWindowBytes    uint64
}

type AddressSnapshotArtifact struct {
	SnapshotID           string
	TenantID             string
	Version              uint64
	EffectiveFrom        time.Time
	BuilderVersion       string
	SourceManifestSHA256 string
	Strings              []string
	Sources              []AddressSnapshotSource
	GeoNodes             []AddressSnapshotGeoNode
	Operators            []AddressSnapshotOperator
	AddressSets          []AddressSnapshotSet
	Values               []AddressSnapshotValue
	IPv4Ranges           []AddressSnapshotIPv4Range
	IPv6Ranges           []AddressSnapshotIPv6Range
}

// AddressSnapshotSource pins one immutable management-plane import generation.
// String fields are indexes into AddressSnapshotArtifact.Strings.
type AddressSnapshotSource struct {
	Slot           uint32
	ImportID       uint32
	ChecksumSHA256 uint32
	SlotRowVersion uint64
	RowCountV4     uint64
	RowCountV6     uint64
}

type AddressSnapshotGeoNode struct {
	Namespace uint8
	ID        uint32
	Kind      uint32
	Code      uint32
	Name      uint32
	ParentID  uint32
	Enabled   bool
}

const (
	AddressSnapshotGeoSupplier = uint8(1)
	AddressSnapshotGeoCustomer = uint8(2)
)

const (
	AddressSnapshotOperatorSupplier = uint8(1)
	AddressSnapshotOperatorCustomer = uint8(2)
)

type AddressSnapshotOperator struct {
	Namespace uint8
	ID        uint16
	StableID  uint32
	Code      uint32
	Name      uint32
	ShortName uint32
	Category  uint32
	ASNs      []uint32
	Enabled   bool
}

type AddressSnapshotSet struct {
	ID      uint32
	Name    uint32
	Enabled bool
}

// AddressSnapshotGeoValue stores both legacy display codes and stable
// five-level taxonomy IDs. Every field is a string-dictionary index.
type AddressSnapshotGeoValue struct {
	CountryCode uint32
	AdminCode   uint32
	Subdivision uint32
	City        uint32
	ContinentID uint32
	RegionID    uint32
	CountryID   uint32
	ProvinceID  uint32
	CityID      uint32
}

// AddressSnapshotValue is dictionary encoded once and referenced by ranges.
// Supplier and customer namespaces remain separate so a tenant correction can
// never overwrite supplier provenance merely because their numeric IDs match.
type AddressSnapshotValue struct {
	SupplierGeo          AddressSnapshotGeoValue
	CustomerGeo          AddressSnapshotGeoValue
	SupplierISPID        uint16
	CustomerISPID        uint16
	SupplierASN          uint32
	CustomerASN          uint32
	CustomerOverrideBits uint8
	Local                bool
	PrimaryPrefixID      uint32
	PrimaryPrefixCIDR    uint32
	Business             uint32
	InAddressSetIDs      []uint32
	OutAddressSetIDs     []uint32
}

type AddressSnapshotIPv4Range struct {
	Start      uint32
	End        uint32
	ValueIndex uint32
}

type AddressSnapshotIPv6Range struct {
	Start      [16]byte
	End        [16]byte
	ValueIndex uint32
}

// EncodeAddressSnapshot emits a deterministic WADS v1 container. The caller
// must provide canonical tables; rejecting non-canonical inputs prevents two
// builders from publishing different bytes for the same logical generation.
func EncodeAddressSnapshot(snapshot AddressSnapshotArtifact, limits AddressSnapshotLimits) ([]byte, error) {
	limits, err := normalizeAddressSnapshotLimits(limits)
	if err != nil {
		return nil, err
	}
	if err := validateAddressSnapshot(snapshot, limits); err != nil {
		return nil, err
	}
	payload, err := marshalAddressSnapshotPayload(snapshot, limits)
	if err != nil {
		return nil, err
	}
	if len(payload) > limits.MaxUncompressedBytes {
		return nil, fmt.Errorf("%w: payload exceeds uncompressed byte limit", ErrInvalidAddressSnapshot)
	}
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderCRC(false),
	)
	if err != nil {
		return nil, fmt.Errorf("create address snapshot encoder: %w", err)
	}
	compressed := encoder.EncodeAll(payload, nil)
	encoder.Close()
	if len(compressed) == 0 || len(compressed) > limits.MaxCompressedBytes {
		return nil, fmt.Errorf("%w: payload exceeds compressed byte limit", ErrInvalidAddressSnapshot)
	}

	result := make([]byte, addressSnapshotHeaderSize+len(compressed))
	copy(result[:4], addressSnapshotMagic[:])
	binary.BigEndian.PutUint16(result[4:6], AddressSnapshotFormatVersion)
	binary.BigEndian.PutUint16(result[6:8], addressSnapshotFlagZstd)
	binary.BigEndian.PutUint32(result[8:12], addressSnapshotHeaderSize)
	binary.BigEndian.PutUint64(result[12:20], uint64(len(compressed)))
	binary.BigEndian.PutUint64(result[20:28], uint64(len(payload)))
	binary.BigEndian.PutUint32(result[28:32], crc32.Checksum(payload, addressSnapshotCRC32CTable))
	copy(result[addressSnapshotHeaderSize:], compressed)
	return result, nil
}

// DecodeAddressSnapshot checks all allocation and reference boundaries before
// returning data. Trust remains external: callers must verify the object SHA
// and signed publication metadata before invoking this decoder.
func DecodeAddressSnapshot(data []byte, limits AddressSnapshotLimits) (AddressSnapshotArtifact, error) {
	limits, err := normalizeAddressSnapshotLimits(limits)
	if err != nil {
		return AddressSnapshotArtifact{}, err
	}
	if len(data) < addressSnapshotHeaderSize || len(data) > addressSnapshotHeaderSize+limits.MaxCompressedBytes {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: container size is outside limits", ErrInvalidAddressSnapshot)
	}
	if !bytes.Equal(data[:4], addressSnapshotMagic[:]) {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: magic mismatch", ErrInvalidAddressSnapshot)
	}
	if binary.BigEndian.Uint16(data[4:6]) != AddressSnapshotFormatVersion {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: unsupported format version", ErrInvalidAddressSnapshot)
	}
	if binary.BigEndian.Uint16(data[6:8]) != addressSnapshotFlagZstd || binary.BigEndian.Uint32(data[8:12]) != addressSnapshotHeaderSize || binary.BigEndian.Uint32(data[32:36]) != 0 {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: unsupported flags or header", ErrInvalidAddressSnapshot)
	}
	compressedLength := binary.BigEndian.Uint64(data[12:20])
	uncompressedLength := binary.BigEndian.Uint64(data[20:28])
	if compressedLength == 0 || compressedLength > uint64(limits.MaxCompressedBytes) ||
		uncompressedLength == 0 || uncompressedLength > uint64(limits.MaxUncompressedBytes) ||
		compressedLength != uint64(len(data)-addressSnapshotHeaderSize) {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: payload length mismatch", ErrInvalidAddressSnapshot)
	}
	if compressedLength+uncompressedLength*2 > limits.MaxDecodedMemoryBytes {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: declared decoded memory exceeds limit", ErrInvalidAddressSnapshot)
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(uint64(limits.MaxUncompressedBytes)),
		zstd.WithDecoderMaxWindow(limits.MaxZstdWindowBytes),
	)
	if err != nil {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: create decoder: %v", ErrInvalidAddressSnapshot, err)
	}
	payload, err := decoder.DecodeAll(data[addressSnapshotHeaderSize:], make([]byte, 0, int(uncompressedLength)))
	decoder.Close()
	if err != nil {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: decompress payload: %v", ErrInvalidAddressSnapshot, err)
	}
	if uint64(len(payload)) != uncompressedLength || crc32.Checksum(payload, addressSnapshotCRC32CTable) != binary.BigEndian.Uint32(data[28:32]) {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: uncompressed length or CRC mismatch", ErrInvalidAddressSnapshot)
	}
	snapshot, err := unmarshalAddressSnapshotPayload(payload, limits)
	if err != nil {
		return AddressSnapshotArtifact{}, err
	}
	if err := validateAddressSnapshot(snapshot, limits); err != nil {
		return AddressSnapshotArtifact{}, err
	}
	return snapshot, nil
}

func normalizeAddressSnapshotLimits(limits AddressSnapshotLimits) (AddressSnapshotLimits, error) {
	defaults := []struct {
		value *int
		set   int
	}{
		{&limits.MaxCompressedBytes, defaultMaxAddressSnapshotCompressedBytes},
		{&limits.MaxUncompressedBytes, defaultMaxAddressSnapshotUncompressedBytes},
		{&limits.MaxStrings, defaultMaxAddressSnapshotStrings},
		{&limits.MaxStringBytes, defaultMaxAddressSnapshotStringBytes},
		{&limits.MaxSources, defaultMaxAddressSnapshotSources},
		{&limits.MaxGeoNodes, defaultMaxAddressSnapshotGeoNodes},
		{&limits.MaxOperators, defaultMaxAddressSnapshotOperators},
		{&limits.MaxAddressSets, defaultMaxAddressSnapshotSets},
		{&limits.MaxValues, defaultMaxAddressSnapshotValues},
		{&limits.MaxIPv4Ranges, defaultMaxAddressSnapshotIPv4Ranges},
		{&limits.MaxIPv6Ranges, defaultMaxAddressSnapshotIPv6Ranges},
		{&limits.MaxSetsPerValue, defaultMaxAddressSnapshotSetsPerValue},
	}
	for _, item := range defaults {
		if *item.value == 0 {
			*item.value = item.set
		}
		if *item.value < 0 {
			return AddressSnapshotLimits{}, fmt.Errorf("%w: limits must not be negative", ErrInvalidAddressSnapshot)
		}
	}
	if limits.MaxZstdWindowBytes == 0 {
		limits.MaxZstdWindowBytes = defaultMaxAddressSnapshotZstdWindow
	}
	if limits.MaxDecodedMemoryBytes == 0 {
		limits.MaxDecodedMemoryBytes = defaultMaxAddressSnapshotDecodedMemory
	}
	if limits.MaxUncompressedBytes < 1 || limits.MaxCompressedBytes < 1 || limits.MaxZstdWindowBytes < 1 || limits.MaxDecodedMemoryBytes < 1 {
		return AddressSnapshotLimits{}, fmt.Errorf("%w: byte limits must be positive", ErrInvalidAddressSnapshot)
	}
	return limits, nil
}

func validateAddressSnapshot(snapshot AddressSnapshotArtifact, limits AddressSnapshotLimits) error {
	if !validIdentifier(snapshot.SnapshotID, 128) || !validIdentifier(snapshot.TenantID, 64) || snapshot.Version == 0 || !validText(snapshot.BuilderVersion, 64) || !validSnapshotSHA256(snapshot.SourceManifestSHA256) {
		return fmt.Errorf("%w: metadata is invalid", ErrInvalidAddressSnapshot)
	}
	_, offset := snapshot.EffectiveFrom.Zone()
	if snapshot.EffectiveFrom.IsZero() || offset != 0 || snapshot.EffectiveFrom.Second() != 0 || snapshot.EffectiveFrom.Nanosecond() != 0 {
		return fmt.Errorf("%w: effective time must be a UTC minute boundary", ErrInvalidAddressSnapshot)
	}
	counts := []struct {
		name  string
		value int
		limit int
	}{
		{"strings", len(snapshot.Strings), limits.MaxStrings}, {"sources", len(snapshot.Sources), limits.MaxSources},
		{"geo nodes", len(snapshot.GeoNodes), limits.MaxGeoNodes}, {"operators", len(snapshot.Operators), limits.MaxOperators},
		{"address sets", len(snapshot.AddressSets), limits.MaxAddressSets}, {"values", len(snapshot.Values), limits.MaxValues},
		{"IPv4 ranges", len(snapshot.IPv4Ranges), limits.MaxIPv4Ranges}, {"IPv6 ranges", len(snapshot.IPv6Ranges), limits.MaxIPv6Ranges},
	}
	for _, count := range counts {
		if count.value > count.limit {
			return fmt.Errorf("%w: %s count %d exceeds %d", ErrInvalidAddressSnapshot, count.name, count.value, count.limit)
		}
	}
	if len(snapshot.Strings) == 0 || snapshot.Strings[0] != "" || len(snapshot.Values) == 0 || !zeroAddressSnapshotValue(snapshot.Values[0]) {
		return fmt.Errorf("%w: string and value index zero must be reserved", ErrInvalidAddressSnapshot)
	}
	stringBytes := 0
	for index, value := range snapshot.Strings {
		if !utf8.ValidString(value) || len(value) > 65_535 || (index > 0 && (value == "" || strings.TrimSpace(value) != value || snapshot.Strings[index-1] >= value)) {
			return fmt.Errorf("%w: string dictionary is not canonical at %d", ErrInvalidAddressSnapshot, index)
		}
		stringBytes += len(value)
		if stringBytes > limits.MaxStringBytes {
			return fmt.Errorf("%w: string bytes exceed limit", ErrInvalidAddressSnapshot)
		}
	}
	validRef := func(reference uint32) bool { return uint64(reference) < uint64(len(snapshot.Strings)) }

	for index, source := range snapshot.Sources {
		if source.Slot == 0 || source.ImportID == 0 || source.ChecksumSHA256 == 0 || !validRef(source.Slot) || !validRef(source.ImportID) || !validRef(source.ChecksumSHA256) || source.SlotRowVersion == 0 || !validSnapshotSHA256(snapshot.Strings[source.ChecksumSHA256]) ||
			addressSnapshotSourceRank(snapshot.Strings[source.Slot]) < 0 ||
			(index > 0 && addressSnapshotSourceRank(snapshot.Strings[snapshot.Sources[index-1].Slot]) >= addressSnapshotSourceRank(snapshot.Strings[source.Slot])) {
			return fmt.Errorf("%w: source table is not canonical at %d", ErrInvalidAddressSnapshot, index)
		}
	}
	geoNodes, err := validateAddressSnapshotGeoNodes(snapshot.GeoNodes, snapshot.Strings, validRef)
	if err != nil {
		return err
	}
	operatorIDs := map[uint8]map[uint16]struct{}{
		AddressSnapshotOperatorSupplier: {},
		AddressSnapshotOperatorCustomer: {},
	}
	operatorStableIDs := make(map[[2]uint32]struct{}, len(snapshot.Operators))
	for index, operator := range snapshot.Operators {
		if (operator.Namespace != AddressSnapshotOperatorSupplier && operator.Namespace != AddressSnapshotOperatorCustomer) || operator.ID == 0 ||
			!validRef(operator.StableID) || !validRef(operator.Code) || !validRef(operator.Name) || !validRef(operator.ShortName) || !validRef(operator.Category) ||
			operator.Code == 0 || operator.Name == 0 || operator.Category == 0 ||
			len(operator.ASNs) > defaultMaxASNsPerAddressSnapshotOperator ||
			(index > 0 && !addressSnapshotOperatorLess(snapshot.Operators[index-1], operator)) {
			return fmt.Errorf("%w: operator table is not canonical at %d", ErrInvalidAddressSnapshot, index)
		}
		stableKey := [2]uint32{uint32(operator.Namespace), operator.StableID}
		if _, exists := operatorStableIDs[stableKey]; exists {
			return fmt.Errorf("%w: duplicate namespaced operator stable id", ErrInvalidAddressSnapshot)
		}
		operatorStableIDs[stableKey] = struct{}{}
		operatorIDs[operator.Namespace][operator.ID] = struct{}{}
		for position, asn := range operator.ASNs {
			if asn == 0 || (position > 0 && operator.ASNs[position-1] >= asn) {
				return fmt.Errorf("%w: operator ASN table is not canonical", ErrInvalidAddressSnapshot)
			}
		}
	}
	setIDs := make(map[uint32]struct{}, len(snapshot.AddressSets))
	for index, set := range snapshot.AddressSets {
		if set.ID == 0 || !validRef(set.ID) || !validRef(set.Name) || (index > 0 && snapshot.AddressSets[index-1].ID >= set.ID) {
			return fmt.Errorf("%w: address-set table is not canonical at %d", ErrInvalidAddressSnapshot, index)
		}
		if set.Enabled {
			setIDs[set.ID] = struct{}{}
		}
	}
	for index, value := range snapshot.Values {
		if err := validateAddressSnapshotValue(value, validRef, setIDs, operatorIDs, geoNodes, snapshot.GeoNodes, snapshot.Strings, limits.MaxSetsPerValue); err != nil {
			return fmt.Errorf("%w: value %d: %v", ErrInvalidAddressSnapshot, index, err)
		}
		if index > 1 && bytes.Compare(marshalAddressSnapshotValue(snapshot.Values[index-1]), marshalAddressSnapshotValue(value)) >= 0 {
			return fmt.Errorf("%w: value table is not canonical at %d", ErrInvalidAddressSnapshot, index)
		}
	}
	if err := validateAddressSnapshotIPv4Ranges(snapshot.IPv4Ranges, len(snapshot.Values)); err != nil {
		return err
	}
	if err := validateAddressSnapshotIPv6Ranges(snapshot.IPv6Ranges, len(snapshot.Values)); err != nil {
		return err
	}
	return nil
}

type addressSnapshotGeoNodeKey struct {
	namespace uint8
	id        uint32
}

func validateAddressSnapshotGeoNodes(nodes []AddressSnapshotGeoNode, dictionary []string, validRef func(uint32) bool) (map[addressSnapshotGeoNodeKey]int, error) {
	type namespacedID struct {
		namespace uint8
		value     uint32
	}
	idOwners := make(map[namespacedID]int, len(nodes))
	result := make(map[addressSnapshotGeoNodeKey]int, len(nodes))
	for index, node := range nodes {
		if (node.Namespace != AddressSnapshotGeoSupplier && node.Namespace != AddressSnapshotGeoCustomer) || node.ID == 0 || node.Kind == 0 || node.Code == 0 || node.Name == 0 || !validRef(node.ID) || !validRef(node.Kind) || !validRef(node.Code) || !validRef(node.Name) || !validRef(node.ParentID) ||
			(index > 0 && !addressSnapshotGeoNodeLess(nodes[index-1], node)) {
			return nil, fmt.Errorf("%w: Geo node table is not canonical at %d", ErrInvalidAddressSnapshot, index)
		}
		id := namespacedID{namespace: node.Namespace, value: node.ID}
		if _, exists := idOwners[id]; exists {
			return nil, fmt.Errorf("%w: duplicate Geo id %q", ErrInvalidAddressSnapshot, dictionary[node.ID])
		}
		idOwners[id] = index
		result[addressSnapshotGeoNodeKey{namespace: node.Namespace, id: node.ID}] = index
	}
	states := make([]uint8, len(nodes))
	var visit func(int) error
	visit = func(index int) error {
		if states[index] == 1 {
			return fmt.Errorf("%w: Geo parent cycle", ErrInvalidAddressSnapshot)
		}
		if states[index] == 2 {
			return nil
		}
		states[index] = 1
		parent := nodes[index].ParentID
		if parent != 0 {
			parentIndex, exists := idOwners[namespacedID{namespace: nodes[index].Namespace, value: parent}]
			if !exists || parent == nodes[index].ID {
				return fmt.Errorf("%w: Geo parent is missing or self-referential", ErrInvalidAddressSnapshot)
			}
			if geoDefinitionKindRank(dictionary[nodes[parentIndex].Kind]) >= geoDefinitionKindRank(dictionary[nodes[index].Kind]) {
				return fmt.Errorf("%w: Geo parent hierarchy is not descending", ErrInvalidAddressSnapshot)
			}
			if nodes[index].Enabled && !nodes[parentIndex].Enabled {
				return fmt.Errorf("%w: enabled Geo node has disabled parent", ErrInvalidAddressSnapshot)
			}
			if err := visit(parentIndex); err != nil {
				return err
			}
		}
		states[index] = 2
		return nil
	}
	for index := range nodes {
		if err := visit(index); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func validateAddressSnapshotValue(value AddressSnapshotValue, validRef func(uint32) bool, setIDs map[uint32]struct{}, operatorIDs map[uint8]map[uint16]struct{}, geoNodes map[addressSnapshotGeoNodeKey]int, nodes []AddressSnapshotGeoNode, dictionary []string, maxSets int) error {
	for _, geo := range []AddressSnapshotGeoValue{value.SupplierGeo, value.CustomerGeo} {
		for _, reference := range []uint32{geo.CountryCode, geo.AdminCode, geo.Subdivision, geo.City, geo.ContinentID, geo.RegionID, geo.CountryID, geo.ProvinceID, geo.CityID} {
			if !validRef(reference) {
				return errors.New("Geo string reference is outside dictionary")
			}
		}
	}
	if err := validateAddressSnapshotGeoValue(value.SupplierGeo, AddressSnapshotGeoSupplier, geoNodes, nodes, dictionary); err != nil {
		return err
	}
	if err := validateAddressSnapshotGeoValue(value.CustomerGeo, AddressSnapshotGeoCustomer, geoNodes, nodes, dictionary); err != nil {
		return err
	}
	if !validRef(value.PrimaryPrefixID) || !validRef(value.PrimaryPrefixCIDR) || !validRef(value.Business) ||
		value.CustomerOverrideBits&^uint8(GeoOverrideKnownFields) != 0 || len(value.InAddressSetIDs) > maxSets || len(value.OutAddressSetIDs) > maxSets {
		return errors.New("scalar field or set count is invalid")
	}
	if (value.SupplierISPID != 0 && !addressSnapshotOperatorIDExists(operatorIDs, AddressSnapshotOperatorSupplier, value.SupplierISPID)) ||
		(value.CustomerISPID != 0 && !addressSnapshotOperatorIDExists(operatorIDs, AddressSnapshotOperatorCustomer, value.CustomerISPID)) {
		return errors.New("ISP value references an unknown operator namespace or ID")
	}
	for _, memberships := range [][]uint32{value.InAddressSetIDs, value.OutAddressSetIDs} {
		for index, setID := range memberships {
			if _, exists := setIDs[setID]; !exists || (index > 0 && memberships[index-1] >= setID) {
				return errors.New("address-set memberships are not canonical or reference an unknown set")
			}
		}
	}
	return nil
}

func addressSnapshotOperatorIDExists(values map[uint8]map[uint16]struct{}, namespace uint8, id uint16) bool {
	_, exists := values[namespace][id]
	return exists
}

func validateAddressSnapshotGeoValue(value AddressSnapshotGeoValue, namespace uint8, nodeIndexes map[addressSnapshotGeoNodeKey]int, nodes []AddressSnapshotGeoNode, dictionary []string) error {
	ids := []uint32{value.ContinentID, value.RegionID, value.CountryID, value.ProvinceID, value.CityID}
	kinds := []string{"continent", "region", "country", "province", "city"}
	lastNode := -1
	for position, id := range ids {
		if id == 0 {
			continue
		}
		nodeIndex, exists := nodeIndexes[addressSnapshotGeoNodeKey{namespace: namespace, id: id}]
		if !exists || dictionary[nodes[nodeIndex].Kind] != kinds[position] || !nodes[nodeIndex].Enabled {
			return errors.New("Geo path references a missing, disabled, or wrong-kind node")
		}
		if lastNode >= 0 && !addressSnapshotGeoAncestor(nodeIndexes, nodes, lastNode, nodeIndex, namespace) {
			return errors.New("Geo path nodes do not form one ancestry chain")
		}
		lastNode = nodeIndex
	}
	return nil
}

func addressSnapshotGeoAncestor(nodeIndexes map[addressSnapshotGeoNodeKey]int, nodes []AddressSnapshotGeoNode, ancestor, descendant int, namespace uint8) bool {
	for parentID := nodes[descendant].ParentID; parentID != 0; {
		parent, exists := nodeIndexes[addressSnapshotGeoNodeKey{namespace: namespace, id: parentID}]
		if !exists {
			return false
		}
		if parent == ancestor {
			return true
		}
		parentID = nodes[parent].ParentID
	}
	return false
}

func validateAddressSnapshotIPv4Ranges(ranges []AddressSnapshotIPv4Range, values int) error {
	for index, item := range ranges {
		if item.Start > item.End || uint64(item.ValueIndex) >= uint64(values) || (index > 0 && ranges[index-1].End >= item.Start) {
			return fmt.Errorf("%w: IPv4 ranges are unsorted, overlap, or reference an unknown value at %d", ErrInvalidAddressSnapshot, index)
		}
	}
	return nil
}

func validateAddressSnapshotIPv6Ranges(ranges []AddressSnapshotIPv6Range, values int) error {
	for index, item := range ranges {
		if bytes.Compare(item.Start[:], item.End[:]) > 0 || uint64(item.ValueIndex) >= uint64(values) || (index > 0 && bytes.Compare(ranges[index-1].End[:], item.Start[:]) >= 0) {
			return fmt.Errorf("%w: IPv6 ranges are unsorted, overlap, or reference an unknown value at %d", ErrInvalidAddressSnapshot, index)
		}
	}
	return nil
}

func validSnapshotSHA256(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func zeroAddressSnapshotValue(value AddressSnapshotValue) bool {
	return value.SupplierGeo == (AddressSnapshotGeoValue{}) && value.CustomerGeo == (AddressSnapshotGeoValue{}) &&
		value.SupplierISPID == 0 && value.CustomerISPID == 0 && value.SupplierASN == 0 && value.CustomerASN == 0 &&
		value.CustomerOverrideBits == 0 && !value.Local && value.PrimaryPrefixID == 0 && value.PrimaryPrefixCIDR == 0 &&
		value.Business == 0 && len(value.InAddressSetIDs) == 0 && len(value.OutAddressSetIDs) == 0
}

func addressSnapshotGeoNodeLess(left, right AddressSnapshotGeoNode) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	return left.ID < right.ID
}

func addressSnapshotOperatorLess(left, right AddressSnapshotOperator) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	return left.ID < right.ID
}

func marshalAddressSnapshotPayload(snapshot AddressSnapshotArtifact, limits AddressSnapshotLimits) ([]byte, error) {
	writer := addressSnapshotWriter{data: make([]byte, 0, min(limits.MaxUncompressedBytes, 1<<20)), limit: limits.MaxUncompressedBytes}
	writer.string16(snapshot.SnapshotID)
	writer.string16(snapshot.TenantID)
	writer.string16(snapshot.BuilderVersion)
	writer.string16(snapshot.SourceManifestSHA256)
	writer.u64(snapshot.Version)
	writer.i64(snapshot.EffectiveFrom.UnixMilli())
	for _, count := range []int{len(snapshot.Strings), len(snapshot.Sources), len(snapshot.GeoNodes), len(snapshot.Operators), len(snapshot.AddressSets), len(snapshot.Values), len(snapshot.IPv4Ranges), len(snapshot.IPv6Ranges)} {
		writer.u32(uint32(count))
	}
	for _, value := range snapshot.Strings {
		writer.string32(value)
	}
	for _, source := range snapshot.Sources {
		writer.u32(source.Slot)
		writer.u32(source.ImportID)
		writer.u32(source.ChecksumSHA256)
		writer.u64(source.SlotRowVersion)
		writer.u64(source.RowCountV4)
		writer.u64(source.RowCountV6)
	}
	for _, node := range snapshot.GeoNodes {
		writer.u8(node.Namespace)
		writer.u32(node.ID)
		writer.u32(node.Kind)
		writer.u32(node.Code)
		writer.u32(node.Name)
		writer.u32(node.ParentID)
		writer.boolean(node.Enabled)
	}
	for _, operator := range snapshot.Operators {
		writer.u8(operator.Namespace)
		writer.u16(operator.ID)
		writer.u32(operator.StableID)
		writer.u32(operator.Code)
		writer.u32(operator.Name)
		writer.u32(operator.ShortName)
		writer.u32(operator.Category)
		writer.boolean(operator.Enabled)
		writer.u32(uint32(len(operator.ASNs)))
		for _, asn := range operator.ASNs {
			writer.u32(asn)
		}
	}
	for _, set := range snapshot.AddressSets {
		writer.u32(set.ID)
		writer.u32(set.Name)
		writer.boolean(set.Enabled)
	}
	for _, value := range snapshot.Values {
		writer.bytes(marshalAddressSnapshotValue(value))
	}
	for _, item := range snapshot.IPv4Ranges {
		writer.u32(item.Start)
		writer.u32(item.End)
		writer.u32(item.ValueIndex)
	}
	for _, item := range snapshot.IPv6Ranges {
		writer.bytes(item.Start[:])
		writer.bytes(item.End[:])
		writer.u32(item.ValueIndex)
	}
	if writer.err != nil {
		return nil, writer.err
	}
	return writer.data, nil
}

func marshalAddressSnapshotValue(value AddressSnapshotValue) []byte {
	writer := addressSnapshotWriter{data: make([]byte, 0, 128), limit: 1 << 20}
	for _, geo := range []AddressSnapshotGeoValue{value.SupplierGeo, value.CustomerGeo} {
		for _, reference := range []uint32{geo.CountryCode, geo.AdminCode, geo.Subdivision, geo.City, geo.ContinentID, geo.RegionID, geo.CountryID, geo.ProvinceID, geo.CityID} {
			writer.u32(reference)
		}
	}
	writer.u16(value.SupplierISPID)
	writer.u16(value.CustomerISPID)
	writer.u32(value.SupplierASN)
	writer.u32(value.CustomerASN)
	writer.u8(value.CustomerOverrideBits)
	writer.boolean(value.Local)
	writer.u32(value.PrimaryPrefixID)
	writer.u32(value.PrimaryPrefixCIDR)
	writer.u32(value.Business)
	writer.u32(uint32(len(value.InAddressSetIDs)))
	for _, setID := range value.InAddressSetIDs {
		writer.u32(setID)
	}
	writer.u32(uint32(len(value.OutAddressSetIDs)))
	for _, setID := range value.OutAddressSetIDs {
		writer.u32(setID)
	}
	return writer.data
}

func unmarshalAddressSnapshotPayload(payload []byte, limits AddressSnapshotLimits) (AddressSnapshotArtifact, error) {
	reader := addressSnapshotReader{data: payload}
	var snapshot AddressSnapshotArtifact
	snapshot.SnapshotID = reader.string16()
	snapshot.TenantID = reader.string16()
	snapshot.BuilderVersion = reader.string16()
	snapshot.SourceManifestSHA256 = reader.string16()
	snapshot.Version = reader.u64()
	snapshot.EffectiveFrom = time.UnixMilli(reader.i64()).UTC()
	counts := [8]uint32{}
	for index := range counts {
		counts[index] = reader.u32()
	}
	limitsByCount := []int{limits.MaxStrings, limits.MaxSources, limits.MaxGeoNodes, limits.MaxOperators, limits.MaxAddressSets, limits.MaxValues, limits.MaxIPv4Ranges, limits.MaxIPv6Ranges}
	for index, count := range counts {
		if uint64(count) > uint64(limitsByCount[index]) {
			return AddressSnapshotArtifact{}, fmt.Errorf("%w: declared table count exceeds limit", ErrInvalidAddressSnapshot)
		}
	}
	if reader.err != nil {
		return AddressSnapshotArtifact{}, reader.err
	}
	minimumEncodedBytes := uint64(counts[0])*4 + uint64(counts[1])*36 + uint64(counts[2])*22 + uint64(counts[3])*28 +
		uint64(counts[4])*9 + uint64(counts[5])*106 + uint64(counts[6])*12 + uint64(counts[7])*36
	if minimumEncodedBytes > uint64(len(payload)-reader.position) {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: declared tables exceed payload", ErrInvalidAddressSnapshot)
	}
	estimatedDecodedMemory := uint64(len(payload))*2 + uint64(counts[0])*16 + uint64(counts[1])*40 + uint64(counts[2])*32 +
		uint64(counts[3])*64 + uint64(counts[4])*8 + uint64(counts[5])*176 + uint64(counts[6])*12 + uint64(counts[7])*36
	if estimatedDecodedMemory > limits.MaxDecodedMemoryBytes {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: decoded memory estimate exceeds limit", ErrInvalidAddressSnapshot)
	}
	snapshot.Strings = make([]string, counts[0])
	stringBytes := 0
	for index := range snapshot.Strings {
		snapshot.Strings[index] = reader.string32()
		stringBytes += len(snapshot.Strings[index])
		if stringBytes > limits.MaxStringBytes {
			return AddressSnapshotArtifact{}, fmt.Errorf("%w: string bytes exceed limit", ErrInvalidAddressSnapshot)
		}
	}
	snapshot.Sources = make([]AddressSnapshotSource, counts[1])
	for index := range snapshot.Sources {
		item := &snapshot.Sources[index]
		item.Slot, item.ImportID, item.ChecksumSHA256 = reader.u32(), reader.u32(), reader.u32()
		item.SlotRowVersion, item.RowCountV4, item.RowCountV6 = reader.u64(), reader.u64(), reader.u64()
	}
	snapshot.GeoNodes = make([]AddressSnapshotGeoNode, counts[2])
	for index := range snapshot.GeoNodes {
		item := &snapshot.GeoNodes[index]
		item.Namespace = reader.u8()
		item.ID, item.Kind, item.Code, item.Name, item.ParentID = reader.u32(), reader.u32(), reader.u32(), reader.u32(), reader.u32()
		item.Enabled = reader.boolean()
	}
	snapshot.Operators = make([]AddressSnapshotOperator, counts[3])
	for index := range snapshot.Operators {
		item := &snapshot.Operators[index]
		item.Namespace, item.ID = reader.u8(), reader.u16()
		item.StableID, item.Code, item.Name, item.ShortName, item.Category = reader.u32(), reader.u32(), reader.u32(), reader.u32(), reader.u32()
		item.Enabled = reader.boolean()
		asnCount := reader.u32()
		if uint64(asnCount) > uint64(defaultMaxASNsPerAddressSnapshotOperator) {
			return AddressSnapshotArtifact{}, fmt.Errorf("%w: operator ASN count exceeds limit", ErrInvalidAddressSnapshot)
		}
		item.ASNs = make([]uint32, asnCount)
		for position := range item.ASNs {
			item.ASNs[position] = reader.u32()
		}
	}
	snapshot.AddressSets = make([]AddressSnapshotSet, counts[4])
	for index := range snapshot.AddressSets {
		snapshot.AddressSets[index] = AddressSnapshotSet{ID: reader.u32(), Name: reader.u32(), Enabled: reader.boolean()}
	}
	snapshot.Values = make([]AddressSnapshotValue, counts[5])
	for index := range snapshot.Values {
		item := &snapshot.Values[index]
		item.SupplierGeo = reader.geoValue()
		item.CustomerGeo = reader.geoValue()
		item.SupplierISPID, item.CustomerISPID = reader.u16(), reader.u16()
		item.SupplierASN, item.CustomerASN = reader.u32(), reader.u32()
		item.CustomerOverrideBits, item.Local = reader.u8(), reader.boolean()
		item.PrimaryPrefixID, item.PrimaryPrefixCIDR, item.Business = reader.u32(), reader.u32(), reader.u32()
		item.InAddressSetIDs = reader.u32Slice(limits.MaxSetsPerValue)
		item.OutAddressSetIDs = reader.u32Slice(limits.MaxSetsPerValue)
	}
	snapshot.IPv4Ranges = make([]AddressSnapshotIPv4Range, counts[6])
	for index := range snapshot.IPv4Ranges {
		snapshot.IPv4Ranges[index] = AddressSnapshotIPv4Range{Start: reader.u32(), End: reader.u32(), ValueIndex: reader.u32()}
	}
	snapshot.IPv6Ranges = make([]AddressSnapshotIPv6Range, counts[7])
	for index := range snapshot.IPv6Ranges {
		item := &snapshot.IPv6Ranges[index]
		copy(item.Start[:], reader.take(16))
		copy(item.End[:], reader.take(16))
		item.ValueIndex = reader.u32()
	}
	if reader.err != nil {
		return AddressSnapshotArtifact{}, reader.err
	}
	if reader.position != len(payload) {
		return AddressSnapshotArtifact{}, fmt.Errorf("%w: trailing payload bytes", ErrInvalidAddressSnapshot)
	}
	return snapshot, nil
}

const defaultMaxASNsPerAddressSnapshotOperator = 10_000

type addressSnapshotWriter struct {
	data  []byte
	limit int
	err   error
}

func (writer *addressSnapshotWriter) bytes(value []byte) {
	if writer.err != nil {
		return
	}
	if len(value) > writer.limit-len(writer.data) {
		writer.err = fmt.Errorf("%w: payload exceeds byte limit", ErrInvalidAddressSnapshot)
		return
	}
	writer.data = append(writer.data, value...)
}

func (writer *addressSnapshotWriter) u8(value uint8) { writer.bytes([]byte{value}) }
func (writer *addressSnapshotWriter) boolean(value bool) {
	if value {
		writer.u8(1)
	} else {
		writer.u8(0)
	}
}
func (writer *addressSnapshotWriter) u16(value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	writer.bytes(encoded[:])
}
func (writer *addressSnapshotWriter) u32(value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	writer.bytes(encoded[:])
}
func (writer *addressSnapshotWriter) u64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	writer.bytes(encoded[:])
}
func (writer *addressSnapshotWriter) i64(value int64) { writer.u64(uint64(value)) }
func (writer *addressSnapshotWriter) string16(value string) {
	if len(value) > 65_535 {
		writer.err = fmt.Errorf("%w: metadata string is too long", ErrInvalidAddressSnapshot)
		return
	}
	writer.u16(uint16(len(value)))
	writer.bytes([]byte(value))
}
func (writer *addressSnapshotWriter) string32(value string) {
	writer.u32(uint32(len(value)))
	writer.bytes([]byte(value))
}

type addressSnapshotReader struct {
	data     []byte
	position int
	err      error
}

func (reader *addressSnapshotReader) take(length int) []byte {
	if reader.err != nil {
		return nil
	}
	if length < 0 || length > len(reader.data)-reader.position {
		reader.err = fmt.Errorf("%w: truncated payload", ErrInvalidAddressSnapshot)
		return nil
	}
	value := reader.data[reader.position : reader.position+length]
	reader.position += length
	return value
}
func (reader *addressSnapshotReader) u8() uint8 {
	value := reader.take(1)
	if len(value) == 0 {
		return 0
	}
	return value[0]
}
func (reader *addressSnapshotReader) boolean() bool {
	value := reader.u8()
	if value > 1 && reader.err == nil {
		reader.err = fmt.Errorf("%w: invalid boolean", ErrInvalidAddressSnapshot)
	}
	return value == 1
}
func (reader *addressSnapshotReader) u16() uint16 {
	value := reader.take(2)
	if len(value) != 2 {
		return 0
	}
	return binary.BigEndian.Uint16(value)
}
func (reader *addressSnapshotReader) u32() uint32 {
	value := reader.take(4)
	if len(value) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}
func (reader *addressSnapshotReader) u64() uint64 {
	value := reader.take(8)
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}
func (reader *addressSnapshotReader) i64() int64 { return int64(reader.u64()) }
func (reader *addressSnapshotReader) string16() string {
	return string(reader.take(int(reader.u16())))
}
func (reader *addressSnapshotReader) string32() string {
	length := reader.u32()
	if uint64(length) > uint64(len(reader.data)-reader.position) {
		reader.err = fmt.Errorf("%w: truncated string", ErrInvalidAddressSnapshot)
		return ""
	}
	return string(reader.take(int(length)))
}
func (reader *addressSnapshotReader) geoValue() AddressSnapshotGeoValue {
	return AddressSnapshotGeoValue{
		CountryCode: reader.u32(), AdminCode: reader.u32(), Subdivision: reader.u32(), City: reader.u32(),
		ContinentID: reader.u32(), RegionID: reader.u32(), CountryID: reader.u32(), ProvinceID: reader.u32(), CityID: reader.u32(),
	}
}
func (reader *addressSnapshotReader) u32Slice(maximum int) []uint32 {
	count := reader.u32()
	if uint64(count) > uint64(maximum) {
		reader.err = fmt.Errorf("%w: list count exceeds limit", ErrInvalidAddressSnapshot)
		return nil
	}
	if count == 0 {
		return nil
	}
	values := make([]uint32, count)
	for index := range values {
		values[index] = reader.u32()
	}
	return values
}

func AddressSnapshotIPv4(start, end netip.Addr, valueIndex uint32) (AddressSnapshotIPv4Range, error) {
	start, end = start.Unmap(), end.Unmap()
	if !start.Is4() || !end.Is4() {
		return AddressSnapshotIPv4Range{}, fmt.Errorf("%w: IPv4 range requires IPv4 addresses", ErrInvalidAddressSnapshot)
	}
	return AddressSnapshotIPv4Range{Start: addressToUint32(start), End: addressToUint32(end), ValueIndex: valueIndex}, nil
}

func AddressSnapshotIPv6(start, end netip.Addr, valueIndex uint32) (AddressSnapshotIPv6Range, error) {
	start, end = start.Unmap(), end.Unmap()
	if !start.Is6() || !end.Is6() {
		return AddressSnapshotIPv6Range{}, fmt.Errorf("%w: IPv6 range requires IPv6 addresses", ErrInvalidAddressSnapshot)
	}
	return AddressSnapshotIPv6Range{Start: start.As16(), End: end.As16(), ValueIndex: valueIndex}, nil
}

// CanonicalAddressSnapshotStrings returns index-zero plus sorted unique values.
// Builders use it before translating domain rows to dictionary indexes.
func CanonicalAddressSnapshotStrings(values ...string) ([]string, error) {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if !utf8.ValidString(value) || strings.TrimSpace(value) != value || len(value) > 65_535 {
			return nil, fmt.Errorf("%w: string dictionary value is invalid", ErrInvalidAddressSnapshot)
		}
		set[value] = struct{}{}
	}
	result := make([]string, 1, len(set)+1)
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result[1:])
	return result, nil
}
