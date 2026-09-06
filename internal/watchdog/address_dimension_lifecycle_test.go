package watchdog

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

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
		ID: "01JDIMENSIONSNAPSHOT000001", TenantID: "01JDIMENSIONTENANT00000001",
		ModuleKey: AddressDimensionModuleKey, DimensionKey: AddressDimensionKey,
		Version: 7, EffectiveFrom: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC),
		ObjectRef: "dimension/tenant/snapshot.json", Checksum: digest, DraftDigest: digest,
		BundleSchemaVersion: 2,
	}
}
