package flowworker

import (
	"errors"
	"net/netip"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

var ErrNoEnrichmentVersion = errors.New("no dimension and classification version pair for event time")

// DimensionSnapshot is the immutable hot-path contract shared by legacy JSON
// dimensions and the compiled WADS AddressSnap index.
type DimensionSnapshot interface {
	Metadata() flowdimension.SnapshotMetadata
	ClassifyEndpoints(source, destination netip.Addr) flowdimension.ClassifiedEndpoints
}

// EnrichmentVersion is an immutable, validated dimension/classification pair.
// The classification effective time is the pair's activation boundary. A
// Home-only change may therefore reference an older dimension snapshot.
type EnrichmentVersion struct {
	Dimension      DimensionSnapshot
	Classification *flowdimension.ClassificationSnapshot
}

type EnrichmentVersionMetadata struct {
	TenantID               string
	EffectiveFrom          time.Time
	DimensionSnapshotID    string
	DimensionVersion       uint64
	DimensionChecksum      string
	ClassificationVersion  uint32
	ClassificationChecksum string
}

func (v EnrichmentVersion) Metadata() EnrichmentVersionMetadata {
	dimension := v.Dimension.Metadata()
	classification := v.Classification.Metadata()
	return EnrichmentVersionMetadata{
		TenantID: classification.TenantID, EffectiveFrom: classification.EffectiveFrom,
		DimensionSnapshotID: dimension.SnapshotID, DimensionVersion: dimension.Version,
		DimensionChecksum: dimension.Checksum, ClassificationVersion: classification.Version,
		ClassificationChecksum: classification.Checksum,
	}
}

// EnrichmentVersionCatalog publishes a complete version pair with one CAS.
// Readers can never observe a new dimension with an old classification (or
// the reverse), and do not take a mutex on the per-record hot path.
type EnrichmentVersionCatalog struct {
	state atomic.Pointer[enrichmentVersionCatalogState]
}

type enrichmentVersionCatalogState struct {
	byTenant map[string][]EnrichmentVersion
}

func NewEnrichmentVersionCatalog(versions ...EnrichmentVersion) (*EnrichmentVersionCatalog, error) {
	catalog := &EnrichmentVersionCatalog{}
	catalog.state.Store(&enrichmentVersionCatalogState{byTenant: map[string][]EnrichmentVersion{}})
	for _, version := range versions {
		if err := catalog.Install(version); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *EnrichmentVersionCatalog) Install(version EnrichmentVersion) error {
	if c == nil || version.Dimension == nil || version.Classification == nil {
		return errors.New("compiled dimension and classification snapshots are required")
	}
	dimension := version.Dimension.Metadata()
	classification := version.Classification.Metadata()
	if dimension.TenantID != classification.TenantID || dimension.SnapshotID != classification.DimensionSnapshotID {
		return errors.New("dimension and classification identities do not form a version pair")
	}
	if dimension.EffectiveFrom.After(classification.EffectiveFrom) {
		return errors.New("classification cannot become effective before its dimension snapshot")
	}
	for {
		current := c.state.Load()
		if current == nil {
			current = &enrichmentVersionCatalogState{byTenant: map[string][]EnrichmentVersion{}}
		}
		next := &enrichmentVersionCatalogState{byTenant: make(map[string][]EnrichmentVersion, len(current.byTenant)+1)}
		for tenantID, existing := range current.byTenant {
			next.byTenant[tenantID] = append([]EnrichmentVersion(nil), existing...)
		}
		items := next.byTenant[classification.TenantID]
		dimensionSeen := false
		var installedDimension DimensionSnapshot
		for _, existing := range items {
			if sameEnrichmentVersion(existing, version) {
				return nil
			}
			existingDimension := existing.Dimension.Metadata()
			existingClassification := existing.Classification.Metadata()
			if sameDimensionReference(existing.Dimension, version.Dimension) {
				dimensionSeen = true
				installedDimension = existing.Dimension
			}
			if existingClassification.Version == classification.Version {
				return errors.New("classification version is immutable")
			}
			if existingClassification.EffectiveFrom.Equal(classification.EffectiveFrom) {
				return errors.New("enrichment version effective_from already exists")
			}
			if existingDimension.Version == dimension.Version && !sameDimensionReference(existing.Dimension, version.Dimension) {
				return errors.New("dimension version is immutable")
			}
			if existingClassification.EffectiveFrom.Before(classification.EffectiveFrom) {
				if existingClassification.Version > classification.Version || existingDimension.Version > dimension.Version {
					return errors.New("enrichment versions and effective_from are not monotonic")
				}
			} else if existingClassification.Version < classification.Version || existingDimension.Version < dimension.Version {
				return errors.New("enrichment versions and effective_from are not monotonic")
			}
		}
		if !dimensionSeen && !dimension.EffectiveFrom.Equal(classification.EffectiveFrom) {
			return errors.New("a new dimension snapshot must become effective with its classification")
		}
		if installedDimension != nil {
			version.Dimension = installedDimension
		}
		items = append(items, version)
		sort.Slice(items, func(left, right int) bool {
			return items[left].Classification.Metadata().EffectiveFrom.Before(items[right].Classification.Metadata().EffectiveFrom)
		})
		next.byTenant[classification.TenantID] = items
		if c.state.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (c *EnrichmentVersionCatalog) ClassificationVersion(tenantID string, version uint32) (EnrichmentVersion, bool) {
	if c == nil || !validIdentifier(tenantID, 64) || version == 0 {
		return EnrichmentVersion{}, false
	}
	state := c.state.Load()
	if state == nil {
		return EnrichmentVersion{}, false
	}
	for _, candidate := range state.byTenant[tenantID] {
		if candidate.Classification.Metadata().Version == version {
			return candidate, true
		}
	}
	return EnrichmentVersion{}, false
}

func (c *EnrichmentVersionCatalog) DimensionVersion(tenantID string, version uint64) (DimensionSnapshot, bool) {
	if c == nil || !validIdentifier(tenantID, 64) || version == 0 {
		return nil, false
	}
	state := c.state.Load()
	if state == nil {
		return nil, false
	}
	for _, candidate := range state.byTenant[tenantID] {
		dimension := candidate.Dimension
		if dimension.Metadata().Version == version {
			return dimension, true
		}
	}
	return nil, false
}

func (c *EnrichmentVersionCatalog) Select(tenantID string, eventTime time.Time) (EnrichmentVersion, error) {
	if c == nil || !validIdentifier(tenantID, 64) || eventTime.IsZero() {
		return EnrichmentVersion{}, ErrNoEnrichmentVersion
	}
	state := c.state.Load()
	if state == nil {
		return EnrichmentVersion{}, ErrNoEnrichmentVersion
	}
	items := state.byTenant[tenantID]
	position := sort.Search(len(items), func(position int) bool {
		return items[position].Classification.Metadata().EffectiveFrom.After(eventTime)
	})
	if position == 0 {
		return EnrichmentVersion{}, ErrNoEnrichmentVersion
	}
	return items[position-1], nil
}

func sameEnrichmentVersion(left, right EnrichmentVersion) bool {
	if left.Dimension == right.Dimension && left.Classification == right.Classification {
		return true
	}
	leftDimension, rightDimension := left.Dimension.Metadata(), right.Dimension.Metadata()
	leftClassification, rightClassification := left.Classification.Metadata(), right.Classification.Metadata()
	return sameDimensionReference(left.Dimension, right.Dimension) &&
		leftClassification == rightClassification &&
		leftClassification.Checksum != "" && rightClassification.Checksum != "" &&
		leftDimension.Checksum != "" && rightDimension.Checksum != ""
}

func sameDimensionReference(left, right DimensionSnapshot) bool {
	if left == right {
		return true
	}
	leftMetadata, rightMetadata := left.Metadata(), right.Metadata()
	return leftMetadata.SnapshotID == rightMetadata.SnapshotID &&
		leftMetadata.TenantID == rightMetadata.TenantID &&
		leftMetadata.Version == rightMetadata.Version &&
		leftMetadata.EffectiveFrom.Equal(rightMetadata.EffectiveFrom) &&
		leftMetadata.Checksum != "" && leftMetadata.Checksum == rightMetadata.Checksum
}
