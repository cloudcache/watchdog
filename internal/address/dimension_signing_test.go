package address

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"
)

// Faithful de-tenant ports of the pure signing/verify assertions from
// internal/watchdog/address_dimension_lifecycle_test.go. The fixture drops
// TenantID; every signed field, the V2(json)/V3(wads) schema selection, the
// immutable-snapshot proof binding, the unverified-signature rejection and the
// WADS artifact-metadata binding are preserved.
func TestAddressDimensionApprovalProofBindsImmutableSnapshot(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := addressDimensionSignatureFixture()
	signedAt := time.Date(2026, 9, 2, 1, 2, 3, 456000000, time.UTC)
	payload, err := AddressDimensionSigningPayload(snapshot, "dimension-key-1", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		SchemaVersion         uint16                   `json:"schema_version"`
		SourceManifestVersion uint16                   `json:"source_manifest_version"`
		SourceManifest        []AddressDimensionSource `json:"source_manifest"`
		SourcePrefixCount     uint64                   `json:"source_prefix_count"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SchemaVersion != AddressDimensionSigningPayloadV2 || envelope.SourceManifestVersion != AddressDimensionSourceManifestV1 ||
		len(envelope.SourceManifest) != 1 || envelope.SourcePrefixCount != 12 {
		t.Fatalf("unexpected signing envelope: %#v", envelope)
	}
	approval, err := VerifyAddressDimensionApproval(snapshot, "dimension-key-1", signedAt, ed25519.Sign(privateKey, payload), publicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateVerifiedAddressDimensionApproval(snapshot, approval); err != nil {
		t.Fatalf("validated approval rejected: %v", err)
	}
	tampered := snapshot
	tampered.Version++
	if err := validateVerifiedAddressDimensionApproval(tampered, approval); err == nil {
		t.Fatal("approval proof accepted a different snapshot version")
	}
	tampered = snapshot
	tampered.SourceManifest = append([]AddressDimensionSource(nil), snapshot.SourceManifest...)
	tampered.SourceManifest[0].ImportID = "01JOTHERIMPORT000000000001"
	if err := validateVerifiedAddressDimensionApproval(tampered, approval); err == nil {
		t.Fatal("approval proof accepted a different source generation")
	}
	approval.Signature[0] ^= 0xff
	if err := validateVerifiedAddressDimensionApproval(snapshot, approval); err == nil {
		t.Fatal("approval proof accepted a mutated signature")
	}
}

func TestAddressDimensionApprovalRejectsUnverifiedSignature(t *testing.T) {
	snapshot := addressDimensionSignatureFixture()
	approval := AddressDimensionApproval{
		SnapshotID: snapshot.ID, SigningKeyID: "dimension-key-1",
		SignedAt: time.Date(2026, 9, 2, 1, 2, 3, 0, time.UTC), Signature: make([]byte, ed25519.SignatureSize),
	}
	if err := validateVerifiedAddressDimensionApproval(snapshot, approval); err == nil {
		t.Fatal("unverified approval was accepted")
	}
}

func addressDimensionSignatureFixture() AddressDimensionSnapshot {
	digest := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	return AddressDimensionSnapshot{
		ID:        "01JDIMENSIONSNAPSHOT000001",
		ModuleKey: AddressDimensionModuleKey, DimensionKey: AddressDimensionKey,
		Version: 7, EffectiveFrom: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC),
		ObjectRef: "dimension-snapshots/snapshot/bundle.json", Checksum: digest, DraftDigest: digest,
		SourceManifestVersion: AddressDimensionSourceManifestV1,
		SourceManifest: []AddressDimensionSource{{
			Slot: AddressImportSlotGeo, ImportID: "01JADDRESSIMPORT0000000001", SlotRowVersion: 3,
			ChecksumSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RowCountV4: 10, RowCountV6: 2,
		}},
		SourcePrefixCount: 12, BundleSchemaVersion: 2,
	}
}

func TestAddressDimensionSigningPayloadRejectsInvalidSourceManifestVersion(t *testing.T) {
	snapshot := addressDimensionSignatureFixture()
	snapshot.SourceManifestVersion = 0
	if _, err := AddressDimensionSigningPayload(snapshot, "dimension-key-1", time.Date(2026, 9, 2, 1, 2, 3, 0, time.UTC)); err == nil {
		t.Fatal("version zero accepted a non-empty source manifest")
	}
	snapshot.SourceManifestVersion = AddressDimensionSourceManifestV1 + 1
	if _, err := AddressDimensionSigningPayload(snapshot, "dimension-key-1", time.Date(2026, 9, 2, 1, 2, 3, 0, time.UTC)); err == nil {
		t.Fatal("unsupported source manifest version was accepted")
	}
}

func TestAddressSnapshotSigningPayloadBindsBinaryArtifactMetadata(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := addressDimensionSignatureFixture()
	snapshot.ObjectRef = "dimension-snapshots/snapshot/address-snapshot.wads"
	snapshot.ObjectFormat = AddressSnapshotObjectFormat
	snapshot.ObjectFormatVersion = 1
	snapshot.BuilderVersion = AddressSnapshotBuilderVersion
	snapshot.BuildJobID = snapshot.ID
	signedAt := time.Date(2026, 9, 7, 2, 0, 0, 0, time.UTC)
	payload, err := AddressDimensionSigningPayload(snapshot, "dimension-key-1", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		SchemaVersion       uint16 `json:"schema_version"`
		ObjectFormat        string `json:"object_format"`
		ObjectFormatVersion uint16 `json:"object_format_version"`
		BuilderVersion      string `json:"builder_version"`
		BuildJobID          ID     `json:"build_job_id"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SchemaVersion != DimensionPublicationSigningPayloadV3 || envelope.ObjectFormat != AddressSnapshotObjectFormat || envelope.ObjectFormatVersion != 1 || envelope.BuilderVersion != AddressSnapshotBuilderVersion || envelope.BuildJobID != snapshot.ID {
		t.Fatalf("AddressSnap signing envelope = %#v", envelope)
	}
	approval, err := VerifyAddressDimensionApproval(snapshot, "dimension-key-1", signedAt, ed25519.Sign(privateKey, payload), publicKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*AddressDimensionSnapshot){
		func(item *AddressDimensionSnapshot) { item.ObjectFormatVersion++ },
		func(item *AddressDimensionSnapshot) { item.BuilderVersion += "-changed" },
		func(item *AddressDimensionSnapshot) { item.BuildJobID = "01JOTHERBUILDJOB000000001" },
	} {
		tampered := snapshot
		mutate(&tampered)
		if err := validateVerifiedAddressDimensionApproval(tampered, approval); err == nil {
			t.Fatal("approval proof accepted changed AddressSnap artifact metadata")
		}
	}
}
