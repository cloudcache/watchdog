package flowworker

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

const (
	defaultMaxDeviceBoundaryObjectBytes       = 16 << 20
	defaultMaxClassificationPolicyObjectBytes = 64 << 10
)

var (
	ErrInvalidWorkerDeployment   = errors.New("invalid worker deployment")
	ErrDeploymentPersistence     = errors.New("worker deployment persistence failed")
	ErrDeploymentAcknowledgement = errors.New("worker deployment acknowledgement failed")
)

type WorkerDeploymentPersistence interface {
	PersistDeployment(context.Context, WorkerDeploymentManifest) error
}

type WorkerDeploymentAcknowledger interface {
	AcknowledgeDeployment(context.Context, WorkerDeploymentManifest, string, string, map[string]DeploymentInstalledArtifact, string, string, error) error
}

type DeploymentInstalledArtifact struct {
	Version  uint64 `json:"version"`
	Checksum string `json:"checksum"`
}

type DeploymentLoaderLimits struct {
	MaxAddressCatalogBytes       int
	MaxDeviceBoundaryBytes       int
	MaxClassificationPolicyBytes int
	AddressSnapshot              flowdimension.AddressSnapshotLimits
}

type DeploymentLoader struct {
	source  VersionObjectSource
	acks    WorkerDeploymentAcknowledger
	catalog *EnrichmentVersionCatalog
	limits  DeploymentLoaderLimits
	persist WorkerDeploymentPersistence
}

func NewDeploymentLoader(source VersionObjectSource, acks WorkerDeploymentAcknowledger, catalog *EnrichmentVersionCatalog, limits DeploymentLoaderLimits, persistence WorkerDeploymentPersistence) (*DeploymentLoader, error) {
	if source == nil || acks == nil || catalog == nil {
		return nil, errors.New("deployment object source, acknowledger, and catalog are required")
	}
	if limits.MaxAddressCatalogBytes == 0 {
		limits.MaxAddressCatalogBytes = defaultMaxAddressSnapshotObjectBytes
	}
	if limits.MaxDeviceBoundaryBytes == 0 {
		limits.MaxDeviceBoundaryBytes = defaultMaxDeviceBoundaryObjectBytes
	}
	if limits.MaxClassificationPolicyBytes == 0 {
		limits.MaxClassificationPolicyBytes = defaultMaxClassificationPolicyObjectBytes
	}
	if limits.MaxAddressCatalogBytes < 1 || limits.MaxDeviceBoundaryBytes < 1 || limits.MaxClassificationPolicyBytes < 1 {
		return nil, errors.New("deployment object byte limits must be positive")
	}
	return &DeploymentLoader{source: source, acks: acks, catalog: catalog, limits: limits, persist: persistence}, nil
}

// Install validates and compiles every referenced artifact before persisting
// the manifest and publishing one immutable catalog entry. Readers therefore
// never observe a partially installed deployment.
func (l *DeploymentLoader) Install(ctx context.Context, manifest WorkerDeploymentManifest, manifestChecksum string) error {
	if l == nil || l.source == nil || l.acks == nil || l.catalog == nil || ctx == nil {
		return errors.New("deployment loader is not initialized")
	}
	if err := ValidateWorkerDeploymentManifest(manifest); err != nil || !validSHA256(manifestChecksum) {
		if err == nil {
			err = errors.New("manifest checksum is invalid")
		}
		return fmt.Errorf("%w: %v", ErrInvalidWorkerDeployment, err)
	}
	if manifest.Generation > uint64(^uint32(0)) {
		return fmt.Errorf("%w: generation exceeds compatibility range", ErrInvalidWorkerDeployment)
	}
	if installed, exists := l.catalog.ClassificationVersion(uint32(manifest.Generation)); exists {
		if err := matchInstalledDeployment(manifest, manifestChecksum, installed); err != nil {
			return err
		}
		if err := l.persistDeployment(ctx, manifest); err != nil {
			return err
		}
		return l.acknowledgeInstalled(ctx, manifest, manifestChecksum)
	}

	addressReference, _ := deploymentArtifactByKind(manifest, ArtifactKindAddressCatalog, "global")
	dimension, reused := l.catalog.DimensionVersion(addressReference.Version)
	if reused && dimension.Metadata().Checksum != addressReference.Checksum {
		return fmt.Errorf("%w: address catalog version is already installed with different content", ErrInvalidWorkerDeployment)
	}
	if !reused {
		addressData, err := l.fetchArtifact(ctx, addressReference, l.limits.MaxAddressCatalogBytes)
		if err != nil {
			return err
		}
		dimension, err = flowdimension.DecodeAndCompileAddressSnapshot(addressData, addressReference.Checksum, l.limits.AddressSnapshot)
		if err != nil {
			return fmt.Errorf("%w: compile address catalog: %v", ErrInvalidWorkerDeployment, err)
		}
	}
	dimensionMetadata := dimension.Metadata()
	if dimensionMetadata.Version != addressReference.Version || dimensionMetadata.EffectiveFrom.After(manifest.EffectiveFrom) {
		return fmt.Errorf("%w: address catalog metadata differs from manifest", ErrInvalidWorkerDeployment)
	}

	policyReference, _ := deploymentArtifactByKind(manifest, ArtifactKindClassificationPolicy, "global")
	policyData, err := l.fetchArtifact(ctx, policyReference, l.limits.MaxClassificationPolicyBytes)
	if err != nil {
		return err
	}
	policy, err := flowdimension.DecodeClassificationPolicy(policyData, policyReference.Checksum, flowdimension.DeploymentArtifactLimits{
		MaxClassificationPolicyBytes: l.limits.MaxClassificationPolicyBytes,
	})
	if err != nil {
		return fmt.Errorf("%w: compile classification policy: %v", ErrInvalidWorkerDeployment, err)
	}

	boundaryReferences := deploymentBoundaryReferences(manifest)
	boundaries := make([]flowdimension.DeviceCustomerBoundary, 0, len(boundaryReferences))
	for _, reference := range boundaryReferences {
		data, err := l.fetchArtifact(ctx, reference, l.limits.MaxDeviceBoundaryBytes)
		if err != nil {
			return err
		}
		boundary, err := flowdimension.DecodeDeviceCustomerBoundary(data, reference.Checksum, flowdimension.DeploymentArtifactLimits{
			MaxDeviceBoundaryBytes: l.limits.MaxDeviceBoundaryBytes,
		})
		if err != nil {
			return fmt.Errorf("%w: compile device boundary %s: %v", ErrInvalidWorkerDeployment, reference.ScopeID, err)
		}
		if boundary.DeviceID != reference.ScopeID {
			return fmt.Errorf("%w: device boundary scope differs from manifest", ErrInvalidWorkerDeployment)
		}
		boundaries = append(boundaries, boundary)
	}

	classification, err := flowdimension.CompileDeploymentClassification(
		dimensionMetadata.SnapshotID, manifest.Generation, manifest.EffectiveFrom, policy, boundaries, manifestChecksum,
	)
	if err != nil {
		return fmt.Errorf("%w: assemble deployment runtime: %v", ErrInvalidWorkerDeployment, err)
	}
	if err := l.catalog.installPrepared(EnrichmentVersion{Dimension: dimension, Classification: classification}, func() error {
		return l.persistDeployment(ctx, manifest)
	}); err != nil {
		return errors.Join(ErrInvalidWorkerDeployment, fmt.Errorf("activate deployment: %w", err))
	}
	return l.acknowledgeActivation(ctx, manifest, manifestChecksum)
}

func (l *DeploymentLoader) fetchArtifact(ctx context.Context, reference DeploymentArtifactReference, maximum int) ([]byte, error) {
	if reference.SizeBytes == 0 || reference.SizeBytes > uint64(maximum) {
		return nil, fmt.Errorf("%w: %s/%s exceeds byte boundary", ErrInvalidWorkerDeployment, reference.Kind, reference.ScopeID)
	}
	data, err := l.source.Fetch(ctx, reference.ArtifactID, maximum)
	if err != nil {
		return nil, fmt.Errorf("%w: fetch %s/%s: %v", ErrVersionObjectUnavailable, reference.Kind, reference.ScopeID, err)
	}
	if uint64(len(data)) != reference.SizeBytes {
		return nil, fmt.Errorf("%w: %s/%s size differs from manifest", ErrVersionObjectIntegrity, reference.Kind, reference.ScopeID)
	}
	return data, nil
}

func (l *DeploymentLoader) persistDeployment(ctx context.Context, manifest WorkerDeploymentManifest) error {
	if l.persist == nil {
		return nil
	}
	if err := l.persist.PersistDeployment(ctx, manifest); err != nil {
		return fmt.Errorf("%w: %v", ErrDeploymentPersistence, err)
	}
	return nil
}

func (l *DeploymentLoader) acknowledgeInstalled(ctx context.Context, manifest WorkerDeploymentManifest, manifestChecksum string) error {
	if err := l.acks.AcknowledgeDeployment(ctx, manifest, manifestChecksum, "installed", installedDeploymentArtifacts(manifest), "", "", nil); err != nil {
		return fmt.Errorf("%w: %v", ErrDeploymentAcknowledgement, err)
	}
	return nil
}

func (l *DeploymentLoader) acknowledgeActivation(ctx context.Context, manifest WorkerDeploymentManifest, manifestChecksum string) error {
	verifiedErr := l.acks.AcknowledgeDeployment(ctx, manifest, manifestChecksum, "verified", installedDeploymentArtifacts(manifest), "", "", nil)
	installedErr := l.acknowledgeInstalled(ctx, manifest, manifestChecksum)
	if installedErr != nil {
		if verifiedErr != nil {
			return errors.Join(fmt.Errorf("%w: verified ACK: %v", ErrDeploymentAcknowledgement, verifiedErr), installedErr)
		}
		return installedErr
	}
	// The installed ACK is authoritative and records all prior milestones. A
	// transient verified-only ACK failure therefore needs no separate retry.
	return nil
}

func installedDeploymentArtifacts(manifest WorkerDeploymentManifest) map[string]DeploymentInstalledArtifact {
	result := make(map[string]DeploymentInstalledArtifact, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		result[artifact.ArtifactID] = DeploymentInstalledArtifact{Version: artifact.Version, Checksum: artifact.Checksum}
	}
	return result
}

func matchInstalledDeployment(manifest WorkerDeploymentManifest, checksum string, installed EnrichmentVersion) error {
	dimension := installed.Dimension.Metadata()
	classification := installed.Classification.Metadata()
	address, _ := deploymentArtifactByKind(manifest, ArtifactKindAddressCatalog, "global")
	if dimension.Version != address.Version || dimension.Checksum != address.Checksum ||
		classification.Version != uint32(manifest.Generation) || classification.Checksum != checksum ||
		!classification.EffectiveFrom.Equal(manifest.EffectiveFrom) {
		return fmt.Errorf("%w: generation is already installed with different content", ErrInvalidWorkerDeployment)
	}
	return nil
}

type discardDeploymentAcknowledgement struct{}

func (discardDeploymentAcknowledgement) AcknowledgeDeployment(context.Context, WorkerDeploymentManifest, string, string, map[string]DeploymentInstalledArtifact, string, string, error) error {
	return nil
}
