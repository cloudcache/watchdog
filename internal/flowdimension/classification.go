package flowdimension

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

const (
	ClassificationSchemaVersion   = 1
	defaultMaxClassificationBytes = 64 << 10
	maxHomeISPIDs                 = 4_096
	maxHomeASNs                   = 4_096
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
	TenantID            string
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
}

// ClassificationBundle is the immutable wire object published by the control
// plane. ClassificationDefinition remains the programmatic compile input so a
// schema version is never silently optional on the wire.
type ClassificationBundle struct {
	SchemaVersion       uint32       `json:"schema_version"`
	TenantID            string       `json:"tenant_id"`
	Version             uint32       `json:"version"`
	EffectiveFrom       time.Time    `json:"effective_from"`
	DimensionSnapshotID string       `json:"dimension_snapshot_id"`
	HomeProvince        string       `json:"home_province"`
	HomeCity            string       `json:"home_city"`
	HomeISPIDs          []uint16     `json:"home_isp_ids"`
	HomeASNs            []uint32     `json:"home_asns"`
	OverseasIncludesHMT bool         `json:"overseas_includes_hmt"`
	InternalPolicy      RecordPolicy `json:"internal_policy"`
	TransitPolicy       RecordPolicy `json:"transit_policy"`
}

type ClassificationCompileLimits struct {
	MaxBundleBytes int
}

type ClassificationMetadata struct {
	TenantID            string
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
	bundle := ClassificationBundle{
		SchemaVersion: ClassificationSchemaVersion, TenantID: definition.TenantID,
		Version: definition.Version, EffectiveFrom: definition.EffectiveFrom.UTC(),
		DimensionSnapshotID: definition.DimensionSnapshotID,
		HomeProvince:        definition.HomeProvince, HomeCity: definition.HomeCity,
		HomeISPIDs: ispIDs, HomeASNs: asns, OverseasIncludesHMT: definition.OverseasIncludesHMT,
		InternalPolicy: definition.InternalPolicy, TransitPolicy: definition.TransitPolicy,
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
	metadata ClassificationMetadata
	home     HomeProfile
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
	if bundle.SchemaVersion != ClassificationSchemaVersion {
		return nil, fmt.Errorf("unsupported classification bundle schema_version %d", bundle.SchemaVersion)
	}
	return compileClassification(ClassificationDefinition{
		TenantID: bundle.TenantID, Version: bundle.Version, EffectiveFrom: bundle.EffectiveFrom,
		DimensionSnapshotID: bundle.DimensionSnapshotID, HomeProvince: bundle.HomeProvince,
		HomeCity: bundle.HomeCity, HomeISPIDs: bundle.HomeISPIDs, HomeASNs: bundle.HomeASNs,
		OverseasIncludesHMT: bundle.OverseasIncludesHMT,
		InternalPolicy:      bundle.InternalPolicy, TransitPolicy: bundle.TransitPolicy,
	}, "sha256:"+hex.EncodeToString(got[:]))
}

func compileClassification(definition ClassificationDefinition, checksum string) (*ClassificationSnapshot, error) {
	if !validIdentifier(definition.TenantID, 64) || definition.Version == 0 || !validIdentifier(definition.DimensionSnapshotID, 64) {
		return nil, errors.New("classification tenant, version, and dimension snapshot are required")
	}
	effectiveFrom := definition.EffectiveFrom.UTC()
	_, offset := definition.EffectiveFrom.Zone()
	if effectiveFrom.IsZero() || offset != 0 || effectiveFrom.Second() != 0 || effectiveFrom.Nanosecond() != 0 {
		return nil, errors.New("classification effective_from must be a UTC minute boundary")
	}
	if definition.HomeProvince != "" {
		if _, ok := provincePart(definition.HomeProvince); !ok || definition.HomeProvince[2:] != "0000" {
			return nil, errors.New("classification home province must be a canonical six-digit province code")
		}
	}
	if definition.HomeCity != "" {
		city, cityOK := cityPart(definition.HomeCity)
		province, provinceOK := provincePart(definition.HomeProvince)
		if !cityOK || !provinceOK || definition.HomeCity[4:] != "00" || city[:2] != province {
			return nil, errors.New("classification home city must be a canonical city code inside the home province")
		}
	}
	if !validRecordPolicy(definition.InternalPolicy) || !validRecordPolicy(definition.TransitPolicy) {
		return nil, errors.New("classification internal and transit policies must be count or drop")
	}
	if len(definition.HomeISPIDs) > maxHomeISPIDs {
		return nil, fmt.Errorf("classification home ISP IDs exceed limit %d", maxHomeISPIDs)
	}
	if len(definition.HomeASNs) > maxHomeASNs {
		return nil, fmt.Errorf("classification home ASNs exceed limit %d", maxHomeASNs)
	}
	ispIDs := append([]uint16(nil), definition.HomeISPIDs...)
	sort.Slice(ispIDs, func(left, right int) bool { return ispIDs[left] < ispIDs[right] })
	ispSet := make(map[uint16]struct{}, len(ispIDs))
	for index, id := range ispIDs {
		if id == 0 || (index > 0 && id == ispIDs[index-1]) {
			return nil, errors.New("classification home ISP IDs must be non-zero and unique")
		}
		ispSet[id] = struct{}{}
	}
	asns := append([]uint32(nil), definition.HomeASNs...)
	sort.Slice(asns, func(left, right int) bool { return asns[left] < asns[right] })
	asnSet := make(map[uint32]struct{}, len(asns))
	for index, asn := range asns {
		if asn == 0 || (index > 0 && asn == asns[index-1]) {
			return nil, errors.New("classification home ASNs must be non-zero and unique")
		}
		asnSet[asn] = struct{}{}
	}
	return &ClassificationSnapshot{
		metadata: ClassificationMetadata{
			TenantID: definition.TenantID, Version: definition.Version, EffectiveFrom: effectiveFrom,
			DimensionSnapshotID: definition.DimensionSnapshotID,
			InternalPolicy:      definition.InternalPolicy, TransitPolicy: definition.TransitPolicy,
			Checksum: checksum,
		},
		home: HomeProfile{
			Province: definition.HomeProvince, City: definition.HomeCity, ISPIDs: ispSet, ASNs: asnSet,
			OverseasIncludesHMT: definition.OverseasIncludesHMT, Version: definition.Version,
		},
	}, nil
}

func (s *ClassificationSnapshot) Metadata() ClassificationMetadata {
	if s == nil {
		return ClassificationMetadata{}
	}
	return s.metadata
}

func (s *ClassificationSnapshot) Classify(direction BusinessDirection, remote GeoInfo) Category {
	if s == nil {
		return CategoryUnknown
	}
	return ClassifyCategory(direction, remote, s.home)
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
	byTenant map[string][]*ClassificationSnapshot
}

func NewClassificationCatalog(snapshots ...*ClassificationSnapshot) (*ClassificationCatalog, error) {
	catalog := &ClassificationCatalog{}
	catalog.state.Store(&classificationCatalogState{byTenant: map[string][]*ClassificationSnapshot{}})
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
			current = &classificationCatalogState{byTenant: map[string][]*ClassificationSnapshot{}}
		}
		next := &classificationCatalogState{byTenant: make(map[string][]*ClassificationSnapshot, len(current.byTenant)+1)}
		for tenantID, existing := range current.byTenant {
			next.byTenant[tenantID] = append([]*ClassificationSnapshot(nil), existing...)
		}
		items := next.byTenant[snapshot.metadata.TenantID]
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
		next.byTenant[snapshot.metadata.TenantID] = items
		if c.state.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (c *ClassificationCatalog) Select(tenantID string, eventTime time.Time) (*ClassificationSnapshot, error) {
	if c == nil || !validIdentifier(tenantID, 64) || eventTime.IsZero() {
		return nil, ErrNoClassificationSnapshot
	}
	state := c.state.Load()
	if state == nil {
		return nil, ErrNoClassificationSnapshot
	}
	items := state.byTenant[tenantID]
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
	return true
}
