package flowworker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

const (
	defaultMaxDimensionObjectBytes       = 64 << 20
	defaultMaxAddressSnapshotObjectBytes = 512 << 20
	defaultMaxClassificationObjectBytes  = 64 << 10
	VersionObjectFormatJSON              = "json"
	VersionObjectFormatWADS              = "wads"
)

var (
	ErrInvalidVersionPublication = errors.New("invalid enrichment version publication")
	ErrVersionObjectUnavailable  = errors.New("enrichment version object unavailable")
	ErrVersionPersistence        = errors.New("enrichment version persistence failed")
	ErrVersionAcknowledgement    = errors.New("enrichment version acknowledgement failed")
)

type VersionObjectReference struct {
	ObjectRef           string `json:"object_ref"`
	Checksum            string `json:"checksum"`
	ObjectFormat        string `json:"object_format,omitempty"`
	ObjectFormatVersion uint16 `json:"object_format_version,omitempty"`
}

// EnrichmentVersionPublication is the least-privilege PLAT-04A contract used
// by a dimension worker. Declared metadata is checked against both downloaded
// objects before the pair becomes visible.
type EnrichmentVersionPublication struct {
	PublicationID               string                 `json:"publication_id"`
	TenantID                    string                 `json:"tenant_id"`
	DimensionSnapshotID         string                 `json:"dimension_snapshot_id"`
	DimensionVersion            uint64                 `json:"dimension_version"`
	DimensionEffectiveFrom      time.Time              `json:"dimension_effective_from"`
	Dimension                   VersionObjectReference `json:"dimension"`
	ClassificationVersion       uint32                 `json:"classification_version"`
	ClassificationEffectiveFrom time.Time              `json:"classification_effective_from"`
	Classification              VersionObjectReference `json:"classification"`
}

type VersionWorkerIdentity struct {
	WorkerID        string `json:"worker_id"`
	BootID          string `json:"boot_id"`
	SoftwareVersion string `json:"software_version"`
}

type EnrichmentVersionAcknowledgement struct {
	PublicationID               string    `json:"publication_id"`
	TenantID                    string    `json:"tenant_id"`
	WorkerID                    string    `json:"worker_id"`
	BootID                      string    `json:"boot_id"`
	SoftwareVersion             string    `json:"software_version"`
	DimensionSnapshotID         string    `json:"dimension_snapshot_id"`
	DimensionVersion            uint64    `json:"dimension_version"`
	DimensionChecksum           string    `json:"dimension_checksum"`
	DimensionEffectiveFrom      time.Time `json:"dimension_effective_from"`
	ClassificationVersion       uint32    `json:"classification_version"`
	ClassificationChecksum      string    `json:"classification_checksum"`
	ClassificationEffectiveFrom time.Time `json:"classification_effective_from"`
	InstalledAt                 time.Time `json:"installed_at"`
}

// VersionObjectSource owns transport, authentication, and object-ref policy.
// It must enforce maxBytes while reading; VersionLoader checks the returned
// length again so a faulty source cannot bypass the memory boundary.
type VersionObjectSource interface {
	Fetch(ctx context.Context, objectRef string, maxBytes int) ([]byte, error)
}

type EnrichmentVersionAcknowledger interface {
	Acknowledge(ctx context.Context, acknowledgement EnrichmentVersionAcknowledgement) error
}

type EnrichmentVersionPersistence interface {
	PersistVersion(ctx context.Context, publication EnrichmentVersionPublication) error
}

type VersionLoaderLimits struct {
	MaxDimensionObjectBytes       int
	MaxAddressSnapshotObjectBytes int
	MaxClassificationObjectBytes  int
	DimensionCompile              flowdimension.CompileLimits
	AddressSnapshot               flowdimension.AddressSnapshotLimits
}

type VersionLoader struct {
	source   VersionObjectSource
	acks     EnrichmentVersionAcknowledger
	catalog  *EnrichmentVersionCatalog
	identity VersionWorkerIdentity
	limits   VersionLoaderLimits
	persist  EnrichmentVersionPersistence
	now      func() time.Time
}

func NewVersionLoader(source VersionObjectSource, acks EnrichmentVersionAcknowledger, catalog *EnrichmentVersionCatalog, identity VersionWorkerIdentity, limits VersionLoaderLimits) (*VersionLoader, error) {
	return newVersionLoader(source, acks, catalog, identity, limits, nil)
}

func NewPersistentVersionLoader(source VersionObjectSource, acks EnrichmentVersionAcknowledger, catalog *EnrichmentVersionCatalog, identity VersionWorkerIdentity, limits VersionLoaderLimits, persistence EnrichmentVersionPersistence) (*VersionLoader, error) {
	if persistence == nil {
		return nil, errors.New("version persistence is required")
	}
	return newVersionLoader(source, acks, catalog, identity, limits, persistence)
}

func newVersionLoader(source VersionObjectSource, acks EnrichmentVersionAcknowledger, catalog *EnrichmentVersionCatalog, identity VersionWorkerIdentity, limits VersionLoaderLimits, persistence EnrichmentVersionPersistence) (*VersionLoader, error) {
	if source == nil || acks == nil || catalog == nil {
		return nil, errors.New("version object source, acknowledger, and catalog are required")
	}
	if !validIdentifier(identity.WorkerID, 128) || !validIdentifier(identity.BootID, 128) || !validText(identity.SoftwareVersion, 64) {
		return nil, errors.New("version worker identity is invalid")
	}
	if limits.MaxDimensionObjectBytes == 0 {
		limits.MaxDimensionObjectBytes = defaultMaxDimensionObjectBytes
	}
	if limits.MaxAddressSnapshotObjectBytes == 0 {
		limits.MaxAddressSnapshotObjectBytes = defaultMaxAddressSnapshotObjectBytes
	}
	if limits.MaxClassificationObjectBytes == 0 {
		limits.MaxClassificationObjectBytes = defaultMaxClassificationObjectBytes
	}
	if limits.MaxDimensionObjectBytes < 1 || limits.MaxAddressSnapshotObjectBytes < 1 || limits.MaxClassificationObjectBytes < 1 {
		return nil, errors.New("version object byte limits must be positive")
	}
	if limits.DimensionCompile.MaxBundleBytes < 0 {
		return nil, errors.New("dimension compile byte limit must not be negative")
	}
	if limits.DimensionCompile.MaxBundleBytes == 0 || limits.DimensionCompile.MaxBundleBytes > limits.MaxDimensionObjectBytes {
		limits.DimensionCompile.MaxBundleBytes = limits.MaxDimensionObjectBytes
	}
	return &VersionLoader{source: source, acks: acks, catalog: catalog, identity: identity, limits: limits, persist: persistence, now: time.Now}, nil
}

// Install downloads and compiles both objects before one atomic catalog
// publication. The ACK
// follows local visibility. If ACK transport fails, retrying Install is safe:
// checksummed pairs install idempotently and the ACK is attempted again.
func (l *VersionLoader) Install(ctx context.Context, publication EnrichmentVersionPublication) error {
	if l == nil || l.source == nil || l.acks == nil || l.catalog == nil {
		return errors.New("version loader is not initialized")
	}
	if err := validateVersionPublication(publication); err != nil {
		return err
	}
	if installed, exists := l.catalog.ClassificationVersion(publication.TenantID, publication.ClassificationVersion); exists {
		if err := matchPublicationMetadata(publication, installed.Dimension.Metadata(), installed.Classification.Metadata()); err != nil {
			return err
		}
		if err := l.persistVersion(ctx, publication); err != nil {
			return err
		}
		return l.acknowledge(ctx, publication)
	}

	var dimension DimensionSnapshot
	if installed, exists := l.catalog.DimensionVersion(publication.TenantID, publication.DimensionVersion); exists {
		if err := matchDimensionMetadata(publication, installed.Metadata()); err != nil {
			return err
		}
		dimension = installed
	} else {
		maxDimensionBytes := l.limits.MaxDimensionObjectBytes
		if publication.Dimension.ObjectFormat == VersionObjectFormatWADS {
			maxDimensionBytes = l.limits.MaxAddressSnapshotObjectBytes
		}
		dimensionData, err := l.source.Fetch(ctx, publication.Dimension.ObjectRef, maxDimensionBytes)
		if err != nil {
			return fmt.Errorf("%w: dimension: %w", ErrVersionObjectUnavailable, err)
		}
		if len(dimensionData) == 0 || len(dimensionData) > maxDimensionBytes {
			return fmt.Errorf("%w: dimension object exceeds byte boundary", ErrVersionObjectUnavailable)
		}
		switch publication.Dimension.ObjectFormat {
		case "", VersionObjectFormatJSON:
			dimension, err = flowdimension.DecodeAndCompileBundle(dimensionData, publication.Dimension.Checksum, l.limits.DimensionCompile)
		case VersionObjectFormatWADS:
			dimension, err = flowdimension.DecodeAndCompileAddressSnapshot(dimensionData, publication.Dimension.Checksum, l.limits.AddressSnapshot)
		default:
			err = fmt.Errorf("unsupported dimension object format %q", publication.Dimension.ObjectFormat)
		}
		if err != nil {
			return fmt.Errorf("%w: compile dimension: %w", ErrInvalidVersionPublication, err)
		}
		if err := matchDimensionMetadata(publication, dimension.Metadata()); err != nil {
			return err
		}
	}

	classificationData, err := l.source.Fetch(ctx, publication.Classification.ObjectRef, l.limits.MaxClassificationObjectBytes)
	if err != nil {
		return fmt.Errorf("%w: classification: %w", ErrVersionObjectUnavailable, err)
	}
	if len(classificationData) == 0 || len(classificationData) > l.limits.MaxClassificationObjectBytes {
		return fmt.Errorf("%w: classification object exceeds byte boundary", ErrVersionObjectUnavailable)
	}

	classification, err := flowdimension.DecodeAndCompileClassificationBundle(classificationData, publication.Classification.Checksum, flowdimension.ClassificationCompileLimits{MaxBundleBytes: l.limits.MaxClassificationObjectBytes})
	if err != nil {
		return fmt.Errorf("%w: compile classification: %w", ErrInvalidVersionPublication, err)
	}
	if err := matchPublicationMetadata(publication, dimension.Metadata(), classification.Metadata()); err != nil {
		return err
	}
	if err := l.catalog.installPrepared(EnrichmentVersion{Dimension: dimension, Classification: classification}, func() error {
		return l.persistVersion(ctx, publication)
	}); err != nil {
		if errors.Is(err, ErrVersionPersistence) {
			return err
		}
		return fmt.Errorf("%w: install version pair: %w", ErrInvalidVersionPublication, err)
	}
	return l.acknowledge(ctx, publication)
}

func (l *VersionLoader) persistVersion(ctx context.Context, publication EnrichmentVersionPublication) error {
	if l.persist == nil {
		return nil
	}
	if err := l.persist.PersistVersion(ctx, publication); err != nil {
		return fmt.Errorf("%w: %w", ErrVersionPersistence, err)
	}
	return nil
}

func (l *VersionLoader) acknowledge(ctx context.Context, publication EnrichmentVersionPublication) error {
	acknowledgement := EnrichmentVersionAcknowledgement{
		PublicationID: publication.PublicationID, TenantID: publication.TenantID,
		WorkerID: l.identity.WorkerID, BootID: l.identity.BootID, SoftwareVersion: l.identity.SoftwareVersion,
		DimensionSnapshotID: publication.DimensionSnapshotID, DimensionVersion: publication.DimensionVersion,
		DimensionChecksum: publication.Dimension.Checksum, DimensionEffectiveFrom: publication.DimensionEffectiveFrom.UTC(),
		ClassificationVersion:       publication.ClassificationVersion,
		ClassificationChecksum:      publication.Classification.Checksum,
		ClassificationEffectiveFrom: publication.ClassificationEffectiveFrom.UTC(), InstalledAt: l.now().UTC(),
	}
	if err := l.acks.Acknowledge(ctx, acknowledgement); err != nil {
		return fmt.Errorf("%w: %w", ErrVersionAcknowledgement, err)
	}
	return nil
}

func validateVersionPublication(publication EnrichmentVersionPublication) error {
	if !validIdentifier(publication.PublicationID, 128) || !validIdentifier(publication.TenantID, 64) ||
		!validIdentifier(publication.DimensionSnapshotID, 64) || publication.DimensionVersion == 0 || publication.ClassificationVersion == 0 {
		return fmt.Errorf("%w: identity and versions are required", ErrInvalidVersionPublication)
	}
	if !validUTCMinute(publication.DimensionEffectiveFrom) || !validUTCMinute(publication.ClassificationEffectiveFrom) ||
		publication.DimensionEffectiveFrom.After(publication.ClassificationEffectiveFrom) {
		return fmt.Errorf("%w: effective times must be ordered UTC minute boundaries", ErrInvalidVersionPublication)
	}
	for name, reference := range map[string]VersionObjectReference{"dimension": publication.Dimension, "classification": publication.Classification} {
		if !validObjectRef(reference.ObjectRef) || !validSHA256(reference.Checksum) {
			return fmt.Errorf("%w: %s object ref or checksum is invalid", ErrInvalidVersionPublication, name)
		}
	}
	if !validDimensionObjectFormat(publication.Dimension) || !validJSONReference(publication.Classification) {
		return fmt.Errorf("%w: object format or format version is invalid", ErrInvalidVersionPublication)
	}
	return nil
}

func validDimensionObjectFormat(reference VersionObjectReference) bool {
	switch reference.ObjectFormat {
	case "", VersionObjectFormatJSON:
		return reference.ObjectFormatVersion == 0
	case VersionObjectFormatWADS:
		return reference.ObjectFormatVersion == flowdimension.AddressSnapshotFormatVersion
	default:
		return false
	}
}

func validJSONReference(reference VersionObjectReference) bool {
	return (reference.ObjectFormat == "" || reference.ObjectFormat == VersionObjectFormatJSON) && reference.ObjectFormatVersion == 0
}

func matchPublicationMetadata(publication EnrichmentVersionPublication, dimension flowdimension.SnapshotMetadata, classification flowdimension.ClassificationMetadata) error {
	if err := matchDimensionMetadata(publication, dimension); err != nil {
		return err
	}
	if classification.TenantID != publication.TenantID || classification.Version != publication.ClassificationVersion ||
		!classification.EffectiveFrom.Equal(publication.ClassificationEffectiveFrom) ||
		classification.DimensionSnapshotID != publication.DimensionSnapshotID || classification.Checksum != publication.Classification.Checksum {
		return fmt.Errorf("%w: classification metadata differs from publication", ErrInvalidVersionPublication)
	}
	return nil
}

func matchDimensionMetadata(publication EnrichmentVersionPublication, dimension flowdimension.SnapshotMetadata) error {
	if dimension.SnapshotID != publication.DimensionSnapshotID || dimension.TenantID != publication.TenantID ||
		dimension.Version != publication.DimensionVersion || !dimension.EffectiveFrom.Equal(publication.DimensionEffectiveFrom) ||
		dimension.Checksum != publication.Dimension.Checksum {
		return fmt.Errorf("%w: dimension metadata differs from publication", ErrInvalidVersionPublication)
	}
	return nil
}

func validUTCMinute(value time.Time) bool {
	_, offset := value.Zone()
	return !value.IsZero() && offset == 0 && value.Second() == 0 && value.Nanosecond() == 0
}

func validObjectRef(value string) bool {
	return validText(value, 512)
}

func validSHA256(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == 32
}
