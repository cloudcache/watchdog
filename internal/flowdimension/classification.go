package flowdimension

import (
	"errors"
	"sort"
	"sync/atomic"
	"time"
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
	OverseasIncludesHMT bool
	InternalPolicy      RecordPolicy
	TransitPolicy       RecordPolicy
}

type ClassificationMetadata struct {
	TenantID            string
	Version             uint32
	EffectiveFrom       time.Time
	DimensionSnapshotID string
	InternalPolicy      RecordPolicy
	TransitPolicy       RecordPolicy
}

// ClassificationSnapshot is immutable and is selected using the flow record's
// event time. Its dimension reference makes an incomplete control-plane
// publication fail closed instead of mixing independently current versions.
type ClassificationSnapshot struct {
	metadata ClassificationMetadata
	home     HomeProfile
}

func CompileClassification(definition ClassificationDefinition) (*ClassificationSnapshot, error) {
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
	ispIDs := append([]uint16(nil), definition.HomeISPIDs...)
	sort.Slice(ispIDs, func(left, right int) bool { return ispIDs[left] < ispIDs[right] })
	ispSet := make(map[uint16]struct{}, len(ispIDs))
	for index, id := range ispIDs {
		if id == 0 || (index > 0 && id == ispIDs[index-1]) {
			return nil, errors.New("classification home ISP IDs must be non-zero and unique")
		}
		ispSet[id] = struct{}{}
	}
	return &ClassificationSnapshot{
		metadata: ClassificationMetadata{
			TenantID: definition.TenantID, Version: definition.Version, EffectiveFrom: effectiveFrom,
			DimensionSnapshotID: definition.DimensionSnapshotID,
			InternalPolicy:      definition.InternalPolicy, TransitPolicy: definition.TransitPolicy,
		},
		home: HomeProfile{
			Province: definition.HomeProvince, City: definition.HomeCity, ISPIDs: ispSet,
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
		left.home.OverseasIncludesHMT != right.home.OverseasIncludesHMT || len(left.home.ISPIDs) != len(right.home.ISPIDs) {
		return false
	}
	for id := range left.home.ISPIDs {
		if _, exists := right.home.ISPIDs[id]; !exists {
			return false
		}
	}
	return true
}
