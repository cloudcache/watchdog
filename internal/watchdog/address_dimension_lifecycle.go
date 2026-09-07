package watchdog

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DimensionPublicationSignatureAlgorithm = "ed25519"
	DimensionPublicationActivationPublish  = "publish"
	DimensionPublicationActivationRollback = "rollback"
	DimensionPublicationAckDownloaded      = "downloaded"
	DimensionPublicationAckInstalled       = "installed"
	DimensionPublicationAckFailed          = "failed"

	AddressDimensionSignatureAlgorithm = DimensionPublicationSignatureAlgorithm
	AddressDimensionActivationPublish  = DimensionPublicationActivationPublish
	AddressDimensionActivationRollback = DimensionPublicationActivationRollback
	AddressDimensionAckDownloaded      = DimensionPublicationAckDownloaded
	AddressDimensionAckInstalled       = DimensionPublicationAckInstalled
	AddressDimensionAckFailed          = DimensionPublicationAckFailed
)

var ErrAddressDimensionInvalidTransition = errors.New("address dimension lifecycle transition is invalid")

// DimensionPublicationApproval can only be persisted after
// VerifyDimensionPublicationApproval has bound its signature to the immutable
// snapshot metadata. The private proof prevents an HTTP decoder or another
// package from asserting verification.
type DimensionPublicationApproval struct {
	SnapshotID             ID
	SigningKeyID           string
	SignedAt               time.Time
	Signature              []byte
	verifiedEnvelopeSHA256 [sha256.Size]byte
}

type AddressDimensionApproval = DimensionPublicationApproval

type DimensionPublicationActivationRequest struct {
	SnapshotID         ID
	EffectiveFrom      time.Time
	ExpectedRowVersion uint64
}

type AddressDimensionActivationRequest = DimensionPublicationActivationRequest

type DimensionPublicationRollbackRequest struct {
	SnapshotID         ID
	EffectiveFrom      time.Time
	ExpectedRowVersion uint64
}

type AddressDimensionRollbackRequest = DimensionPublicationRollbackRequest

type DimensionPublicationRetireRequest struct {
	SnapshotID         ID
	ExpectedRowVersion uint64
	Reason             string
}

type AddressDimensionRetireRequest = DimensionPublicationRetireRequest

func VerifyAddressDimensionApproval(snapshot AddressDimensionSnapshot, signingKeyID string, signedAt time.Time, signature []byte, publicKey ed25519.PublicKey) (AddressDimensionApproval, error) {
	return VerifyDimensionPublicationApproval(snapshot, signingKeyID, signedAt, signature, publicKey)
}

func VerifyDimensionPublicationApproval(snapshot DimensionPublicationSnapshot, signingKeyID string, signedAt time.Time, signature []byte, publicKey ed25519.PublicKey) (DimensionPublicationApproval, error) {
	approval := DimensionPublicationApproval{
		SnapshotID: snapshot.ID, SigningKeyID: strings.TrimSpace(signingKeyID),
		SignedAt: signedAt.UTC(), Signature: append([]byte(nil), signature...),
	}
	payload, err := DimensionPublicationSigningPayload(snapshot, approval.SigningKeyID, approval.SignedAt)
	if err != nil {
		return DimensionPublicationApproval{}, err
	}
	if len(publicKey) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, payload, signature) {
		return DimensionPublicationApproval{}, fmt.Errorf("%w: signature verification failed", ErrAddressDimensionInvalid)
	}
	approval.verifiedEnvelopeSHA256 = addressDimensionApprovalDigest(payload, signature)
	return approval, nil
}

func AddressDimensionSigningPayload(snapshot AddressDimensionSnapshot, signingKeyID string, signedAt time.Time) ([]byte, error) {
	return DimensionPublicationSigningPayload(snapshot, signingKeyID, signedAt)
}

func DimensionPublicationSigningPayload(snapshot DimensionPublicationSnapshot, signingKeyID string, signedAt time.Time) ([]byte, error) {
	signingKeyID = strings.TrimSpace(signingKeyID)
	if snapshot.ID == "" || snapshot.TenantID == "" || snapshot.ModuleKey == "" || snapshot.DimensionKey == "" || snapshot.Version == 0 ||
		snapshot.ObjectRef == "" || !validSHA256Digest(snapshot.Checksum) || !validSHA256Digest(snapshot.DraftDigest) || snapshot.BundleSchemaVersion == 0 ||
		signingKeyID == "" || len(signingKeyID) > 128 || signedAt.IsZero() || signedAt.Location() != time.UTC || signedAt.Nanosecond()%int(time.Millisecond) != 0 {
		return nil, ErrAddressDimensionInvalid
	}
	sources, err := canonicalAddressDimensionSources(snapshot.SourceManifest)
	if err != nil {
		return nil, err
	}
	if snapshot.SourceManifestVersion > AddressDimensionSourceManifestV1 ||
		(snapshot.SourceManifestVersion == 0 && len(sources) != 0) {
		return nil, ErrAddressDimensionInvalid
	}
	sourcePrefixCount, err := countAddressDimensionSourcePrefixes(sources)
	if err != nil || sourcePrefixCount != snapshot.SourcePrefixCount {
		return nil, ErrAddressDimensionInvalid
	}
	payload := struct {
		SchemaVersion         uint16                   `json:"schema_version"`
		SnapshotID            ID                       `json:"snapshot_id"`
		TenantID              ID                       `json:"tenant_id"`
		ModuleKey             string                   `json:"module_key"`
		DimensionKey          string                   `json:"dimension_key"`
		Version               uint64                   `json:"version"`
		EffectiveUnixMilli    int64                    `json:"effective_unix_milli"`
		ObjectRef             string                   `json:"object_ref"`
		Checksum              string                   `json:"checksum"`
		DraftDigest           string                   `json:"draft_digest"`
		SourceManifestVersion uint16                   `json:"source_manifest_version"`
		SourceManifest        []AddressDimensionSource `json:"source_manifest"`
		SourcePrefixCount     uint64                   `json:"source_prefix_count"`
		BundleSchemaVersion   uint32                   `json:"bundle_schema_version"`
		SigningKeyID          string                   `json:"signing_key_id"`
		SignedAtUnixMilli     int64                    `json:"signed_at_unix_milli"`
	}{
		SchemaVersion: DimensionPublicationSigningPayloadV2, SnapshotID: snapshot.ID, TenantID: snapshot.TenantID,
		ModuleKey: snapshot.ModuleKey, DimensionKey: snapshot.DimensionKey,
		Version: snapshot.Version, EffectiveUnixMilli: snapshot.EffectiveFrom.UTC().UnixMilli(),
		ObjectRef: snapshot.ObjectRef, Checksum: snapshot.Checksum, DraftDigest: snapshot.DraftDigest,
		SourceManifestVersion: snapshot.SourceManifestVersion,
		SourceManifest:        sources, SourcePrefixCount: sourcePrefixCount,
		BundleSchemaVersion: snapshot.BundleSchemaVersion, SigningKeyID: signingKeyID,
		SignedAtUnixMilli: signedAt.UnixMilli(),
	}
	return json.Marshal(payload)
}

func validateVerifiedDimensionPublicationApproval(snapshot DimensionPublicationSnapshot, approval DimensionPublicationApproval) error {
	if approval.SnapshotID != snapshot.ID || approval.SigningKeyID == "" || len(approval.Signature) != ed25519.SignatureSize {
		return ErrAddressDimensionInvalid
	}
	payload, err := DimensionPublicationSigningPayload(snapshot, approval.SigningKeyID, approval.SignedAt)
	if err != nil {
		return err
	}
	if addressDimensionApprovalDigest(payload, approval.Signature) != approval.verifiedEnvelopeSHA256 {
		return errors.New("address dimension signature was not cryptographically verified")
	}
	return nil
}

func validateVerifiedAddressDimensionApproval(snapshot AddressDimensionSnapshot, approval AddressDimensionApproval) error {
	return validateVerifiedDimensionPublicationApproval(snapshot, approval)
}

func addressDimensionApprovalDigest(payload, signature []byte) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write(payload)
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(signature)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}
