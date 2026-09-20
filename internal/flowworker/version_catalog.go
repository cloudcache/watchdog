package flowworker

import (
	"errors"
	"net/netip"
	"sort"
	"sync"
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
		EffectiveFrom:       classification.EffectiveFrom,
		DimensionSnapshotID: dimension.SnapshotID, DimensionVersion: dimension.Version,
		DimensionChecksum: dimension.Checksum, ClassificationVersion: classification.Version,
		ClassificationChecksum: classification.Checksum,
	}
}

// EnrichmentVersionCatalog publishes a complete version pair with one atomic
// pointer store.
// Readers can never observe a new dimension with an old classification (or
// the reverse), and do not take a mutex on the per-record hot path.
type EnrichmentVersionCatalog struct {
	installMu sync.Mutex
	state     atomic.Pointer[enrichmentVersionCatalogState]
}

type enrichmentVersionCatalogState struct {
	versions []EnrichmentVersion
}

func NewEnrichmentVersionCatalog(versions ...EnrichmentVersion) (*EnrichmentVersionCatalog, error) {
	catalog := &EnrichmentVersionCatalog{}
	catalog.state.Store(&enrichmentVersionCatalogState{})
	for _, version := range versions {
		if err := catalog.Install(version); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func (c *EnrichmentVersionCatalog) Install(version EnrichmentVersion) error {
	return c.install(version, nil)
}

// installPrepared serializes the rare control-plane write path while readers
// remain lock-free. beforePublish may durably persist the already validated
// version; a persistence failure leaves the catalog pointer unchanged.
func (c *EnrichmentVersionCatalog) installPrepared(version EnrichmentVersion, beforePublish func() error) error {
	return c.install(version, beforePublish)
}

func (c *EnrichmentVersionCatalog) install(version EnrichmentVersion, beforePublish func() error) error {
	if c == nil || version.Dimension == nil || version.Classification == nil {
		return errors.New("compiled dimension and classification snapshots are required")
	}
	dimension := version.Dimension.Metadata()
	classification := version.Classification.Metadata()
	if dimension.SnapshotID != classification.DimensionSnapshotID {
		return errors.New("dimension and classification identities do not form a version pair")
	}
	// The pair's effective boundary is classification.EffectiveFrom. The address
	// object's baked EffectiveFrom is a build artifact and is not used to gate the
	// pair (a timing-agnostic object may be paired at any boundary), so it is not
	// compared here.
	c.installMu.Lock()
	defer c.installMu.Unlock()
	current := c.state.Load()
	if current == nil {
		current = &enrichmentVersionCatalogState{}
	}
	items := append([]EnrichmentVersion(nil), current.versions...)
	var installedDimension DimensionSnapshot
	for _, existing := range items {
		if sameEnrichmentVersion(existing, version) {
			if beforePublish != nil {
				return beforePublish()
			}
			return nil
		}
		existingDimension := existing.Dimension.Metadata()
		existingClassification := existing.Classification.Metadata()
		if sameDimensionReference(existing.Dimension, version.Dimension) {
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
	// A newly introduced dimension takes effect at its pair's boundary
	// (classification.EffectiveFrom); Select keys on that boundary, so a new
	// dimension cannot silently extend the previous pair's coverage. The object's
	// own baked EffectiveFrom is not consulted here (it is a build artifact), so
	// activating a snapshot with a stale time no longer diverges the pair. Version
	// monotonicity above still forbids going backwards.
	if installedDimension != nil {
		version.Dimension = installedDimension
	}
	items = append(items, version)
	sort.Slice(items, func(left, right int) bool {
		return items[left].Classification.Metadata().EffectiveFrom.Before(items[right].Classification.Metadata().EffectiveFrom)
	})
	if beforePublish != nil {
		if err := beforePublish(); err != nil {
			return err
		}
	}
	c.state.Store(&enrichmentVersionCatalogState{versions: items})
	return nil
}

// publishRestored atomically exposes a fully validated cold-start catalog. It
// deliberately refuses to replace a live catalog; remote updates must use
// Install so their monotonicity checks remain in force.
func (c *EnrichmentVersionCatalog) publishRestored(staged *EnrichmentVersionCatalog) error {
	if c == nil || staged == nil {
		return errors.New("restored enrichment version catalog is required")
	}
	restored := staged.state.Load()
	if restored == nil || len(restored.versions) == 0 {
		return errors.New("restored enrichment version catalog is empty")
	}
	c.installMu.Lock()
	defer c.installMu.Unlock()
	current := c.state.Load()
	if current != nil && len(current.versions) != 0 {
		return errors.New("cannot restore over a live enrichment version catalog")
	}
	c.state.Store(restored)
	return nil
}

func (c *EnrichmentVersionCatalog) ClassificationVersion(version uint32) (EnrichmentVersion, bool) {
	if c == nil || version == 0 {
		return EnrichmentVersion{}, false
	}
	state := c.state.Load()
	if state == nil {
		return EnrichmentVersion{}, false
	}
	for _, candidate := range state.versions {
		if candidate.Classification.Metadata().Version == version {
			return candidate, true
		}
	}
	return EnrichmentVersion{}, false
}

func (c *EnrichmentVersionCatalog) DimensionVersion(version uint64) (DimensionSnapshot, bool) {
	if c == nil || version == 0 {
		return nil, false
	}
	state := c.state.Load()
	if state == nil {
		return nil, false
	}
	for _, candidate := range state.versions {
		dimension := candidate.Dimension
		if dimension.Metadata().Version == version {
			return dimension, true
		}
	}
	return nil, false
}

func (c *EnrichmentVersionCatalog) Select(eventTime time.Time) (EnrichmentVersion, error) {
	if c == nil || eventTime.IsZero() {
		return EnrichmentVersion{}, ErrNoEnrichmentVersion
	}
	state := c.state.Load()
	if state == nil {
		return EnrichmentVersion{}, ErrNoEnrichmentVersion
	}
	items := state.versions
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
		leftMetadata.Version == rightMetadata.Version &&
		leftMetadata.EffectiveFrom.Equal(rightMetadata.EffectiveFrom) &&
		leftMetadata.Checksum != "" && leftMetadata.Checksum == rightMetadata.Checksum
}
