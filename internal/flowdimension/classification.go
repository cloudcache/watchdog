package flowdimension

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gaissmai/bart"
)

const (
	LegacyClassificationSchemaVersion = 1
	ClassificationSchemaVersion       = 2
	defaultMaxClassificationBytes     = 16 << 20
	maxHomeISPIDs                     = 4_096
	maxHomeASNs                       = 4_096
)

var ErrNoClassificationSnapshot = errors.New("no classification snapshot for event time")

type RecordPolicy string

const (
	RecordPolicyCount RecordPolicy = "count"
	RecordPolicyDrop  RecordPolicy = "drop"
)

type RecordDisposition string

const (
	DispositionCount RecordDisposition = "count"
	DispositionDrop  RecordDisposition = "drop"
)

type ClassificationDefinition struct {
	Version             uint32
	EffectiveFrom       time.Time
	DimensionSnapshotID string
	HomeProvince        string
	HomeCity            string
	HomeISPIDs          []uint16
	HomeASNs            []uint32
	OverseasIncludesHMT bool
	InternalPolicy      RecordPolicy
	TransitPolicy       RecordPolicy
	DeviceProfiles      []ClassificationDeviceProfile
}

// ClassificationDeviceProfile contains only the customer address boundary of
// one observation device. Geography, operator and ASN remain properties of the
// paired AddressSnap and are never duplicated into Flow configuration.
type ClassificationDeviceProfile struct {
	DeviceID       string                       `json:"device_id"`
	SourcePrefixes []ClassificationSourcePrefix `json:"source_prefixes"`
}

// ClassificationSourcePrefix binds one operational customer boundary to the
// Flow observation device that owns it. It is deliberately separate from the
// shared Geo/operator address library; the paired AddressSnap remains the
// source of geography and operator attributes for every endpoint.
type ClassificationSourcePrefix struct {
	ID           string `json:"id"`
	CIDR         string `json:"cidr"`
	CustomerID   string `json:"customer_id,omitempty"`
	CustomerName string `json:"customer_name,omitempty"`
}

// ClassificationBundle is the immutable wire object published by the control
// plane. ClassificationDefinition remains the programmatic compile input so a
// schema version is never silently optional on the wire.
type ClassificationBundle struct {
	SchemaVersion       uint32                        `json:"schema_version"`
	Version             uint32                        `json:"version"`
	EffectiveFrom       time.Time                     `json:"effective_from"`
	DimensionSnapshotID string                        `json:"dimension_snapshot_id"`
	HomeProvince        string                        `json:"home_province"`
	HomeCity            string                        `json:"home_city"`
	HomeISPIDs          []uint16                      `json:"home_isp_ids"`
	HomeASNs            []uint32                      `json:"home_asns"`
	OverseasIncludesHMT bool                          `json:"overseas_includes_hmt"`
	InternalPolicy      RecordPolicy                  `json:"internal_policy"`
	TransitPolicy       RecordPolicy                  `json:"transit_policy"`
	DeviceProfiles      []ClassificationDeviceProfile `json:"device_profiles,omitempty"`
}

type ClassificationCompileLimits struct {
	MaxBundleBytes int
}

type ClassificationMetadata struct {
	Version             uint32
	EffectiveFrom       time.Time
	DimensionSnapshotID string
	InternalPolicy      RecordPolicy
	TransitPolicy       RecordPolicy
	Checksum            string
}

// EncodeClassificationBundle validates and canonicalizes the control-plane
// definition before producing the immutable worker object.
func EncodeClassificationBundle(definition ClassificationDefinition) ([]byte, string, error) {
	if _, err := CompileClassification(definition); err != nil {
		return nil, "", err
	}
	ispIDs := append([]uint16(nil), definition.HomeISPIDs...)
	if ispIDs == nil {
		ispIDs = []uint16{}
	}
	sort.Slice(ispIDs, func(left, right int) bool { return ispIDs[left] < ispIDs[right] })
	asns := append([]uint32(nil), definition.HomeASNs...)
	if asns == nil {
		asns = []uint32{}
	}
	sort.Slice(asns, func(left, right int) bool { return asns[left] < asns[right] })
	profiles := canonicalClassificationDeviceProfiles(definition.DeviceProfiles)
	schemaVersion := uint32(LegacyClassificationSchemaVersion)
	if len(profiles) > 0 {
		schemaVersion = ClassificationSchemaVersion
	}
	bundle := ClassificationBundle{
		SchemaVersion: schemaVersion,
		Version:       definition.Version, EffectiveFrom: definition.EffectiveFrom.UTC(),
		DimensionSnapshotID: definition.DimensionSnapshotID,
		HomeProvince:        definition.HomeProvince, HomeCity: definition.HomeCity,
		HomeISPIDs: ispIDs, HomeASNs: asns, OverseasIncludesHMT: definition.OverseasIncludesHMT,
		InternalPolicy: definition.InternalPolicy, TransitPolicy: definition.TransitPolicy,
		DeviceProfiles: profiles,
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(data)
	return data, "sha256:" + hex.EncodeToString(digest[:]), nil
}

// ClassificationSnapshot is immutable and is selected using the flow record's
// event time. Its dimension reference makes an incomplete control-plane
// publication fail closed instead of mixing independently current versions.
type ClassificationSnapshot struct {
	metadata       ClassificationMetadata
	home           HomeProfile
	deviceSources  map[string]*bart.Table[classificationSourceAttribution]
	deviceProfiles []ClassificationDeviceProfile
	legacyGlobal   bool
}

type classificationSourceAttribution struct {
	PrefixID     string
	CustomerID   string
	CustomerName string
}

func CompileClassification(definition ClassificationDefinition) (*ClassificationSnapshot, error) {
	return compileClassification(definition, "")
}

func DecodeAndCompileClassificationBundle(data []byte, expectedChecksum string, limits ClassificationCompileLimits) (*ClassificationSnapshot, error) {
	maxBytes := limits.MaxBundleBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxClassificationBytes
	}
	if maxBytes < 1 || len(data) == 0 || len(data) > maxBytes {
		return nil, fmt.Errorf("classification bundle size must be 1..%d bytes", maxBytes)
	}
	want, err := parseSHA256Checksum(expectedChecksum)
	if err != nil {
		return nil, err
	}
	got := sha256.Sum256(data)
	if got != want {
		return nil, errors.New("classification bundle checksum mismatch")
	}
	var bundle ClassificationBundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode classification bundle: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode classification bundle: %w", err)
	}
	if bundle.SchemaVersion != LegacyClassificationSchemaVersion && bundle.SchemaVersion != ClassificationSchemaVersion {
		return nil, fmt.Errorf("unsupported classification bundle schema_version %d", bundle.SchemaVersion)
	}
	if bundle.SchemaVersion == ClassificationSchemaVersion && len(bundle.DeviceProfiles) == 0 {
		return nil, errors.New("classification schema v2 requires device_profiles")
	}
	if bundle.SchemaVersion == LegacyClassificationSchemaVersion && len(bundle.DeviceProfiles) != 0 {
		return nil, errors.New("classification schema v1 does not support device_profiles")
	}
	return compileClassification(ClassificationDefinition{
		Version: bundle.Version, EffectiveFrom: bundle.EffectiveFrom,
		DimensionSnapshotID: bundle.DimensionSnapshotID, HomeProvince: bundle.HomeProvince,
		HomeCity: bundle.HomeCity, HomeISPIDs: bundle.HomeISPIDs, HomeASNs: bundle.HomeASNs,
		OverseasIncludesHMT: bundle.OverseasIncludesHMT,
		InternalPolicy:      bundle.InternalPolicy, TransitPolicy: bundle.TransitPolicy,
		DeviceProfiles: bundle.DeviceProfiles,
	}, "sha256:"+hex.EncodeToString(got[:]))
}

func compileClassification(definition ClassificationDefinition, checksum string) (*ClassificationSnapshot, error) {
	if definition.Version == 0 || !validIdentifier(definition.DimensionSnapshotID, 64) {
		return nil, errors.New("classification version and dimension snapshot are required")
	}
	effectiveFrom := definition.EffectiveFrom.UTC()
	_, offset := definition.EffectiveFrom.Zone()
	if effectiveFrom.IsZero() || offset != 0 || effectiveFrom.Second() != 0 || effectiveFrom.Nanosecond() != 0 {
		return nil, errors.New("classification effective_from must be a UTC minute boundary")
	}
	if !validRecordPolicy(definition.InternalPolicy) || !validRecordPolicy(definition.TransitPolicy) {
		return nil, errors.New("classification internal and transit policies must be count or drop")
	}
	if len(definition.DeviceProfiles) > 0 && (definition.HomeProvince != "" || definition.HomeCity != "" || len(definition.HomeISPIDs) > 0 || len(definition.HomeASNs) > 0) {
		return nil, errors.New("classification global home and device profiles cannot be combined")
	}
	legacyHome, err := compileHomeProfile(definition.HomeProvince, definition.HomeCity, definition.HomeISPIDs, definition.HomeASNs, definition.OverseasIncludesHMT, definition.Version)
	if err != nil {
		return nil, err
	}
	deviceSources := make(map[string]*bart.Table[classificationSourceAttribution], len(definition.DeviceProfiles))
	profiles := canonicalClassificationDeviceProfiles(definition.DeviceProfiles)
	for _, profile := range profiles {
		if !validIdentifier(profile.DeviceID, 128) {
			return nil, errors.New("classification device ID is invalid")
		}
		if _, exists := deviceSources[profile.DeviceID]; exists {
			return nil, errors.New("classification device IDs must be unique")
		}
		if len(profile.SourcePrefixes) == 0 {
			return nil, fmt.Errorf("classification device %s requires at least one source prefix", profile.DeviceID)
		}
		tree := &bart.Table[classificationSourceAttribution]{}
		prefixIDs := make(map[string]struct{}, len(profile.SourcePrefixes))
		prefixCIDRs := make(map[netip.Prefix]struct{}, len(profile.SourcePrefixes))
		for _, source := range profile.SourcePrefixes {
			if !validIdentifier(source.ID, 128) {
				return nil, fmt.Errorf("classification device %s source prefix ID is invalid", profile.DeviceID)
			}
			prefix, err := netip.ParsePrefix(source.CIDR)
			if err != nil || prefix != prefix.Masked() {
				return nil, fmt.Errorf("classification device %s source prefix must be canonical IPv4 or IPv6 CIDR", profile.DeviceID)
			}
			if _, exists := prefixIDs[source.ID]; exists {
				return nil, fmt.Errorf("classification device %s source prefix IDs must be unique", profile.DeviceID)
			}
			if _, exists := prefixCIDRs[prefix]; exists {
				return nil, fmt.Errorf("classification device %s source prefix CIDRs must be unique", profile.DeviceID)
			}
			source.CustomerID = strings.TrimSpace(source.CustomerID)
			source.CustomerName = strings.TrimSpace(source.CustomerName)
			if (source.CustomerID == "") != (source.CustomerName == "") ||
				(source.CustomerID != "" && (!validIdentifier(source.CustomerID, 128) || len(source.CustomerName) > 190)) {
				return nil, fmt.Errorf("classification device %s source prefix customer is invalid", profile.DeviceID)
			}
			prefixIDs[source.ID] = struct{}{}
			prefixCIDRs[prefix] = struct{}{}
			tree.Insert(prefix, classificationSourceAttribution{PrefixID: source.ID, CustomerID: source.CustomerID, CustomerName: source.CustomerName})
		}
		deviceSources[profile.DeviceID] = tree
	}
	return &ClassificationSnapshot{
		metadata: ClassificationMetadata{
			Version: definition.Version, EffectiveFrom: effectiveFrom,
			DimensionSnapshotID: definition.DimensionSnapshotID,
			InternalPolicy:      definition.InternalPolicy, TransitPolicy: definition.TransitPolicy,
			Checksum: checksum,
		},
		home: legacyHome, deviceSources: deviceSources, deviceProfiles: profiles, legacyGlobal: len(definition.DeviceProfiles) == 0,
	}, nil
}

func compileHomeProfile(province, city string, ispValues []uint16, asnValues []uint32, overseasIncludesHMT bool, version uint32) (HomeProfile, error) {
	if province != "" {
		if _, ok := provincePart(province); !ok || province[2:] != "0000" {
			return HomeProfile{}, errors.New("classification home province must be a canonical six-digit province code")
		}
	}
	if city != "" {
		cityPartValue, cityOK := cityPart(city)
		provincePartValue, provinceOK := provincePart(province)
		if !cityOK || !provinceOK || city[4:] != "00" || cityPartValue[:2] != provincePartValue {
			return HomeProfile{}, errors.New("classification home city must be a canonical city code inside the home province")
		}
	}
	if len(ispValues) > maxHomeISPIDs {
		return HomeProfile{}, fmt.Errorf("classification home ISP IDs exceed limit %d", maxHomeISPIDs)
	}
	if len(asnValues) > maxHomeASNs {
		return HomeProfile{}, fmt.Errorf("classification home ASNs exceed limit %d", maxHomeASNs)
	}
	ispIDs := append([]uint16(nil), ispValues...)
	sort.Slice(ispIDs, func(left, right int) bool { return ispIDs[left] < ispIDs[right] })
	ispSet := make(map[uint16]struct{}, len(ispIDs))
	for index, id := range ispIDs {
		if id == 0 || (index > 0 && id == ispIDs[index-1]) {
			return HomeProfile{}, errors.New("classification home ISP IDs must be non-zero and unique")
		}
		ispSet[id] = struct{}{}
	}
	asns := append([]uint32(nil), asnValues...)
	sort.Slice(asns, func(left, right int) bool { return asns[left] < asns[right] })
	asnSet := make(map[uint32]struct{}, len(asns))
	for index, asn := range asns {
		if asn == 0 || (index > 0 && asn == asns[index-1]) {
			return HomeProfile{}, errors.New("classification home ASNs must be non-zero and unique")
		}
		asnSet[asn] = struct{}{}
	}
	return HomeProfile{Province: province, City: city, ISPIDs: ispSet, ASNs: asnSet, OverseasIncludesHMT: overseasIncludesHMT, Version: version}, nil
}

func canonicalClassificationDeviceProfiles(values []ClassificationDeviceProfile) []ClassificationDeviceProfile {
	result := append([]ClassificationDeviceProfile(nil), values...)
	for index := range result {
		result[index].SourcePrefixes = append([]ClassificationSourcePrefix(nil), result[index].SourcePrefixes...)
		sort.Slice(result[index].SourcePrefixes, func(left, right int) bool {
			if result[index].SourcePrefixes[left].ID == result[index].SourcePrefixes[right].ID {
				return result[index].SourcePrefixes[left].CIDR < result[index].SourcePrefixes[right].CIDR
			}
			return result[index].SourcePrefixes[left].ID < result[index].SourcePrefixes[right].ID
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].DeviceID < result[right].DeviceID })
	return result
}

func (s *ClassificationSnapshot) Metadata() ClassificationMetadata {
	if s == nil {
		return ClassificationMetadata{}
	}
	return s.metadata
}

func (s *ClassificationSnapshot) Classify(direction BusinessDirection, remote GeoInfo) Category {
	if s == nil || !s.legacyGlobal {
		return CategoryUnknown
	}
	return ClassifyCategory(direction, remote, s.home)
}

// ClassifyForDevice selects the immutable local context using the device ID
// carried by the Flow record. Schema v1 publications retain their historical
// global behavior; schema v2 fails closed for an unconfigured device.
func (s *ClassificationSnapshot) ClassifyForDevice(deviceID string, direction BusinessDirection, remote GeoInfo) Category {
	if s == nil {
		return CategoryUnknown
	}
	if s.legacyGlobal {
		return ClassifyCategory(direction, remote, s.home)
	}
	return CategoryUnknown
}

// DeviceDirection selects the local endpoint using the customer source CIDRs
// configured for the observation device. Missing device configuration fails
// closed instead of borrowing another device's network boundary.
func (s *ClassificationSnapshot) DeviceDirection(deviceID string, source, destination netip.Addr) (BusinessDirection, bool) {
	direction, _, ok := s.DeviceDirectionAttribution(deviceID, source, destination)
	return direction, ok
}

// DeviceDirectionAttribution also returns the customer owning the local
// endpoint. Customer boundaries are independent from the shared Geo/operator
// snapshot; that snapshot still supplies both endpoints' geographic facts.
func (s *ClassificationSnapshot) DeviceDirectionAttribution(deviceID string, source, destination netip.Addr) (BusinessDirection, string, bool) {
	if s == nil || s.legacyGlobal {
		return DirectionAmbiguous, "", false
	}
	local, ok := s.deviceSources[deviceID]
	if !ok || !source.IsValid() || !destination.IsValid() {
		return DirectionAmbiguous, "", false
	}
	sourceAttribution, sourceLocal := local.Lookup(source.Unmap())
	destinationAttribution, destinationLocal := local.Lookup(destination.Unmap())
	switch {
	case sourceLocal && !destinationLocal:
		return DirectionOut, sourceAttribution.CustomerName, true
	case !sourceLocal && destinationLocal:
		return DirectionIn, destinationAttribution.CustomerName, true
	case sourceLocal && destinationLocal:
		return DirectionInternal, sourceAttribution.CustomerName, true
	default:
		return DirectionTransit, "", true
	}
}

func (s *ClassificationSnapshot) UsesDeviceSources() bool {
	return s != nil && !s.legacyGlobal
}

// ClassifyResolvedEndpoints compares the already-resolved local and remote
// address-library attributes. This keeps province/city/operator ownership in
// AddressSnap and performs no allocation or external lookup per record.
func (s *ClassificationSnapshot) ClassifyResolvedEndpoints(deviceID string, direction BusinessDirection, local, remote GeoInfo) Category {
	if s == nil {
		return CategoryUnknown
	}
	if s.legacyGlobal {
		return ClassifyCategory(direction, remote, s.home)
	}
	if _, ok := s.deviceSources[deviceID]; !ok {
		return CategoryUnknown
	}
	return ClassifyCategoryFromEndpoints(direction, local, remote)
}

func (s *ClassificationSnapshot) Disposition(direction BusinessDirection) RecordDisposition {
	if s == nil {
		return DispositionDrop
	}
	switch direction {
	case DirectionInternal:
		if s.metadata.InternalPolicy == RecordPolicyDrop {
			return DispositionDrop
		}
	case DirectionTransit:
		if s.metadata.TransitPolicy == RecordPolicyDrop {
			return DispositionDrop
		}
	}
	return DispositionCount
}

type ClassificationCatalog struct {
	state atomic.Pointer[classificationCatalogState]
}

type classificationCatalogState struct {
	versions []*ClassificationSnapshot
}

func NewClassificationCatalog(snapshots ...*ClassificationSnapshot) (*ClassificationCatalog, error) {
	catalog := &ClassificationCatalog{}
	catalog.state.Store(&classificationCatalogState{})
	for _, snapshot := range snapshots {
		if err := catalog.Install(snapshot); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *ClassificationCatalog) Install(snapshot *ClassificationSnapshot) error {
	if c == nil || snapshot == nil || snapshot.metadata.Version == 0 {
		return errors.New("compiled classification snapshot is required")
	}
	for {
		current := c.state.Load()
		if current == nil {
			current = &classificationCatalogState{}
		}
		items := append([]*ClassificationSnapshot(nil), current.versions...)
		for _, existing := range items {
			if existing == snapshot || sameClassification(existing, snapshot) {
				return nil
			}
			if existing.metadata.Version == snapshot.metadata.Version {
				return errors.New("classification version is immutable")
			}
			if existing.metadata.EffectiveFrom.Equal(snapshot.metadata.EffectiveFrom) {
				return errors.New("classification effective_from already exists")
			}
			if (existing.metadata.EffectiveFrom.Before(snapshot.metadata.EffectiveFrom) && existing.metadata.Version > snapshot.metadata.Version) ||
				(existing.metadata.EffectiveFrom.After(snapshot.metadata.EffectiveFrom) && existing.metadata.Version < snapshot.metadata.Version) {
				return errors.New("classification version and effective_from are not monotonic")
			}
		}
		items = append(items, snapshot)
		sort.Slice(items, func(left, right int) bool {
			return items[left].metadata.EffectiveFrom.Before(items[right].metadata.EffectiveFrom)
		})
		if c.state.CompareAndSwap(current, &classificationCatalogState{versions: items}) {
			return nil
		}
	}
}

func (c *ClassificationCatalog) Select(eventTime time.Time) (*ClassificationSnapshot, error) {
	if c == nil || eventTime.IsZero() {
		return nil, ErrNoClassificationSnapshot
	}
	state := c.state.Load()
	if state == nil {
		return nil, ErrNoClassificationSnapshot
	}
	items := state.versions
	position := sort.Search(len(items), func(position int) bool {
		return items[position].metadata.EffectiveFrom.After(eventTime)
	})
	if position == 0 {
		return nil, ErrNoClassificationSnapshot
	}
	return items[position-1], nil
}

func validRecordPolicy(policy RecordPolicy) bool {
	return policy == RecordPolicyCount || policy == RecordPolicyDrop
}

func sameClassification(left, right *ClassificationSnapshot) bool {
	if left.metadata != right.metadata || left.home.Province != right.home.Province || left.home.City != right.home.City ||
		left.home.OverseasIncludesHMT != right.home.OverseasIncludesHMT || len(left.home.ISPIDs) != len(right.home.ISPIDs) || len(left.home.ASNs) != len(right.home.ASNs) {
		return false
	}
	for id := range left.home.ISPIDs {
		if _, exists := right.home.ISPIDs[id]; !exists {
			return false
		}
	}
	for asn := range left.home.ASNs {
		if _, exists := right.home.ASNs[asn]; !exists {
			return false
		}
	}
	if len(left.deviceProfiles) != len(right.deviceProfiles) {
		return false
	}
	for index := range left.deviceProfiles {
		if left.deviceProfiles[index].DeviceID != right.deviceProfiles[index].DeviceID || len(left.deviceProfiles[index].SourcePrefixes) != len(right.deviceProfiles[index].SourcePrefixes) {
			return false
		}
		for prefixIndex := range left.deviceProfiles[index].SourcePrefixes {
			if left.deviceProfiles[index].SourcePrefixes[prefixIndex] != right.deviceProfiles[index].SourcePrefixes[prefixIndex] {
				return false
			}
		}
	}
	return true
}
