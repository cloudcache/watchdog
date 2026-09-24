package flowworker

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
)

const (
	WorkerDeploymentManifestSchemaVersion = uint16(1)
	WorkerDeploymentEnvelopeSchemaVersion = uint16(1)
	WorkerDeploymentSignatureAlgorithm    = "ed25519"
	maxWorkerDeploymentEnvelopeBytes      = 1 << 20
	ArtifactKindAddressCatalog            = "address_catalog"
	ArtifactKindDeviceBoundary            = "device_boundary"
	ArtifactKindClassificationPolicy      = "classification_policy"
)

// DeploymentArtifactReference describes one independently versioned,
// content-addressed object. ObjectRef is intentionally absent from the signed
// wire contract: workers download by artifact ID through a target-authorized
// endpoint and never receive a server filesystem path.
type DeploymentArtifactReference struct {
	ArtifactID    string `json:"artifact_id"`
	Kind          string `json:"kind"`
	ScopeID       string `json:"scope_id"`
	Version       uint64 `json:"version"`
	Format        string `json:"format"`
	FormatVersion uint16 `json:"format_version"`
	Checksum      string `json:"checksum"`
	SizeBytes     uint64 `json:"size_bytes"`
}

type WorkerDeploymentManifest struct {
	SchemaVersion uint16                        `json:"schema_version"`
	DeploymentID  string                        `json:"deployment_id"`
	Generation    uint64                        `json:"generation"`
	WorkerID      string                        `json:"worker_id"`
	EffectiveFrom time.Time                     `json:"effective_from"`
	Artifacts     []DeploymentArtifactReference `json:"artifacts"`
}

type SignedWorkerDeploymentManifest struct {
	SchemaVersion      uint16                   `json:"schema_version"`
	Manifest           WorkerDeploymentManifest `json:"manifest"`
	SignatureAlgorithm string                   `json:"signature_algorithm"`
	SigningKeyID       string                   `json:"signing_key_id"`
	SignedAtUnixMilli  int64                    `json:"signed_at_unix_ms"`
	Signature          []byte                   `json:"signature"`
}

func WorkerDeploymentSigningPayload(envelope SignedWorkerDeploymentManifest) ([]byte, error) {
	if err := validateSignedWorkerDeploymentMetadata(envelope); err != nil {
		return nil, err
	}
	payload := struct {
		EnvelopeVersion    uint16                   `json:"envelope_version"`
		Manifest           WorkerDeploymentManifest `json:"manifest"`
		SignatureAlgorithm string                   `json:"signature_algorithm"`
		SigningKeyID       string                   `json:"signing_key_id"`
		SignedAtUnixMilli  int64                    `json:"signed_at_unix_ms"`
	}{
		EnvelopeVersion: envelope.SchemaVersion, Manifest: normalizedWorkerDeploymentManifest(envelope.Manifest),
		SignatureAlgorithm: envelope.SignatureAlgorithm, SigningKeyID: envelope.SigningKeyID,
		SignedAtUnixMilli: envelope.SignedAtUnixMilli,
	}
	return json.Marshal(payload)
}

func MarshalSignedWorkerDeploymentManifest(envelope SignedWorkerDeploymentManifest) ([]byte, error) {
	if len(envelope.Signature) != ed25519.SignatureSize {
		return nil, errors.New("worker deployment signature size is invalid")
	}
	if _, err := WorkerDeploymentSigningPayload(envelope); err != nil {
		return nil, err
	}
	envelope.Manifest = normalizedWorkerDeploymentManifest(envelope.Manifest)
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(data) > maxWorkerDeploymentEnvelopeBytes {
		return nil, errors.New("signed worker deployment manifest is too large")
	}
	return data, nil
}

func VerifySignedWorkerDeploymentManifest(data []byte, trust *flowplan.TrustStore, now time.Time) (SignedWorkerDeploymentManifest, error) {
	if len(data) == 0 || len(data) > maxWorkerDeploymentEnvelopeBytes || trust == nil || now.IsZero() {
		return SignedWorkerDeploymentManifest{}, errors.New("signed worker deployment manifest or trust store is invalid")
	}
	var envelope SignedWorkerDeploymentManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return SignedWorkerDeploymentManifest{}, fmt.Errorf("decode worker deployment manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return SignedWorkerDeploymentManifest{}, errors.New("worker deployment manifest must contain exactly one JSON document")
	}
	canonical, err := MarshalSignedWorkerDeploymentManifest(envelope)
	if err != nil {
		return SignedWorkerDeploymentManifest{}, err
	}
	if !bytes.Equal(canonical, data) {
		return SignedWorkerDeploymentManifest{}, errors.New("worker deployment manifest is not canonical JSON")
	}
	key, err := trust.ResolveAt(envelope.SigningKeyID, now)
	if err != nil {
		return SignedWorkerDeploymentManifest{}, err
	}
	payload, err := WorkerDeploymentSigningPayload(envelope)
	if err != nil {
		return SignedWorkerDeploymentManifest{}, err
	}
	if !ed25519.Verify(key, payload, envelope.Signature) {
		return SignedWorkerDeploymentManifest{}, errors.New("worker deployment manifest signature verification failed")
	}
	return envelope, nil
}

func WorkerDeploymentManifestChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validateSignedWorkerDeploymentMetadata(envelope SignedWorkerDeploymentManifest) error {
	if envelope.SchemaVersion != WorkerDeploymentEnvelopeSchemaVersion ||
		envelope.SignatureAlgorithm != WorkerDeploymentSignatureAlgorithm ||
		!validText(envelope.SigningKeyID, 64) || envelope.SignedAtUnixMilli <= 0 {
		return errors.New("signed worker deployment metadata is invalid")
	}
	return ValidateWorkerDeploymentManifest(envelope.Manifest)
}

func ValidateWorkerDeploymentManifest(manifest WorkerDeploymentManifest) error {
	if manifest.SchemaVersion != WorkerDeploymentManifestSchemaVersion ||
		!validIdentifier(manifest.DeploymentID, 128) || !validIdentifier(manifest.WorkerID, 128) ||
		manifest.Generation == 0 || !validUTCMinute(manifest.EffectiveFrom) {
		return errors.New("worker deployment manifest metadata is invalid")
	}
	if len(manifest.Artifacts) < 2 {
		return errors.New("worker deployment requires address and policy artifacts")
	}
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	addressCount, policyCount := 0, 0
	for _, artifact := range manifest.Artifacts {
		if !validIdentifier(artifact.ArtifactID, 128) || !validText(artifact.ScopeID, 128) || artifact.Version == 0 ||
			!validSHA256(artifact.Checksum) || artifact.SizeBytes == 0 {
			return errors.New("worker deployment artifact reference is invalid")
		}
		key := artifact.Kind + "\x00" + artifact.ScopeID
		if _, exists := seen[key]; exists {
			return errors.New("worker deployment artifact kind and scope must be unique")
		}
		seen[key] = struct{}{}
		switch artifact.Kind {
		case ArtifactKindAddressCatalog:
			addressCount++
			if artifact.ScopeID != "global" || artifact.Format != VersionObjectFormatWADS || artifact.FormatVersion != flowdimension.AddressSnapshotFormatVersion {
				return errors.New("worker deployment address artifact is invalid")
			}
		case ArtifactKindClassificationPolicy:
			policyCount++
			if artifact.ScopeID != "global" || artifact.Format != VersionObjectFormatJSON || artifact.FormatVersion != flowdimension.ClassificationPolicySchemaVersion {
				return errors.New("worker deployment policy artifact is invalid")
			}
		case ArtifactKindDeviceBoundary:
			if artifact.ScopeID == "global" || artifact.Format != VersionObjectFormatJSON || artifact.FormatVersion != flowdimension.DeviceBoundarySchemaVersion {
				return errors.New("worker deployment device boundary artifact is invalid")
			}
		default:
			return fmt.Errorf("unsupported worker deployment artifact kind %q", artifact.Kind)
		}
	}
	if addressCount != 1 || policyCount != 1 {
		return errors.New("worker deployment requires exactly one address and one policy artifact")
	}
	return nil
}

func normalizedWorkerDeploymentManifest(manifest WorkerDeploymentManifest) WorkerDeploymentManifest {
	manifest.EffectiveFrom = manifest.EffectiveFrom.UTC()
	manifest.Artifacts = append([]DeploymentArtifactReference(nil), manifest.Artifacts...)
	sort.Slice(manifest.Artifacts, func(left, right int) bool {
		if manifest.Artifacts[left].Kind != manifest.Artifacts[right].Kind {
			return manifest.Artifacts[left].Kind < manifest.Artifacts[right].Kind
		}
		if manifest.Artifacts[left].ScopeID != manifest.Artifacts[right].ScopeID {
			return manifest.Artifacts[left].ScopeID < manifest.Artifacts[right].ScopeID
		}
		return manifest.Artifacts[left].ArtifactID < manifest.Artifacts[right].ArtifactID
	})
	return manifest
}

func deploymentArtifactByKind(manifest WorkerDeploymentManifest, kind, scope string) (DeploymentArtifactReference, bool) {
	for _, artifact := range manifest.Artifacts {
		if artifact.Kind == kind && artifact.ScopeID == scope {
			return artifact, true
		}
	}
	return DeploymentArtifactReference{}, false
}

func deploymentBoundaryReferences(manifest WorkerDeploymentManifest) []DeploymentArtifactReference {
	result := make([]DeploymentArtifactReference, 0)
	for _, artifact := range manifest.Artifacts {
		if artifact.Kind == ArtifactKindDeviceBoundary {
			result = append(result, artifact)
		}
	}
	return result
}

func validDeploymentFailureStage(value string) bool {
	switch strings.TrimSpace(value) {
	case "fetch", "verify", "compile", "persist", "activate", "ack":
		return true
	default:
		return false
	}
}
