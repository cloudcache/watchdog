package address

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

// Engine-level integration (de-tenanted) proving the publication lifecycle end to
// end against real MySQL, with the restored ed25519 approval driven directly:
// build a WADS snapshot -> the trusted-approval gate rejects a pending activation
// -> verify+approve with ed25519 -> the envelope is persisted -> activate ->
// worker ACK -> consumer summary. The HTTP approve endpoint is covered in 05D.
func TestStoreAddressDimensionLifecycleWithEd25519Approval(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const actorID = ID("user_addr_lifecycle_test")
	seedAddressTestUser(t, db, actorID)

	// A ready, activated base import provides the source prefixes for the build.
	imp, err := store.CreateAddressImport(ctx, AddressImport{
		ID: "import_addr_lifecycle", SourceSlot: AddressImportSlotCombined, Format: AddressImportFormatMMDB,
		OriginalName: "base.mmdb", ArtifactRef: "address-imports/import_addr_lifecycle/source.mmdb",
		ChecksumSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SizeBytes: 1024, Status: AddressImportStatusQueued, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginAddressImport(ctx, imp.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, imp.ID, []AddressImportRecord{
		{Prefix: "192.0.2.0/24", CountryCode: "CN", CountryName: "China", ASN: 4134, Operator: "China Telecom", Source: "mmdb"},
		{Prefix: "2001:db8::/48", CountryCode: "US", ASN: 64496, Source: "mmdb"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAddressImport(ctx, imp.ID, AddressImportMetadata{Format: AddressImportFormatMMDB, IPVersion: 6}, "en"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateAddressImport(ctx, imp.ID, actorID, 0); err != nil {
		t.Fatal(err)
	}

	publisher, err := NewPublisher(store, DiskDimensionObjectStore{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	effective := time.Now().UTC().Truncate(time.Minute)
	preview, err := publisher.PreviewAddressDimension(ctx, effective)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := publisher.BuildAddressSnapshotPublication(ctx, actorID, "01JADDRLIFECYCLE000000001", AddressDimensionPublishRequest{
		EffectiveFrom: effective, PreviewDigest: preview.DraftDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending || snapshot.ObjectFormat != AddressSnapshotObjectFormat {
		t.Fatalf("built snapshot = %#v", snapshot)
	}

	// The trusted-approval gate must refuse to activate a pending (unsigned) snapshot.
	if _, err := publisher.ActivateDimensionPublication(ctx, actorID, AddressDimensionActivationRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: effective, ExpectedRowVersion: snapshot.RowVersion,
	}); err != ErrAddressDimensionInvalidTransition {
		t.Fatalf("pending activation error = %v, want ErrAddressDimensionInvalidTransition", err)
	}

	// Approve with a real ed25519 signature over the snapshot's signing payload.
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signedAt := time.Now().UTC().Truncate(time.Millisecond)
	payload, err := AddressDimensionSigningPayload(snapshot, "lifecycle-key", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := VerifyAddressDimensionApproval(snapshot, "lifecycle-key", signedAt, ed25519.Sign(privateKey, payload), publicKey)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := publisher.ApproveDimensionPublication(ctx, actorID, approval, snapshot.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if approved.ApprovalState != AddressDimensionApprovalApproved || approved.SignatureAlgorithm != AddressDimensionSignatureAlgorithm ||
		approved.SigningKeyID != "lifecycle-key" || len(approved.Signature) != ed25519.SignatureSize || approved.SignedAt == nil || approved.RowVersion != 2 {
		t.Fatalf("approved snapshot = %#v", approved)
	}

	// A tampered approval (different snapshot) must be refused at persist time.
	other := approved
	other.Version++
	if err := validateVerifiedDimensionPublicationApproval(other, approval); err == nil {
		t.Fatal("persist-time proof accepted a mutated snapshot")
	}

	// The trusted, approved snapshot activates.
	activation, err := publisher.ActivateDimensionPublication(ctx, actorID, AddressDimensionActivationRequest{
		SnapshotID: approved.ID, EffectiveFrom: effective, ExpectedRowVersion: approved.RowVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activation.Reason != AddressDimensionActivationPublish || activation.SnapshotID != approved.ID {
		t.Fatalf("activation = %#v", activation)
	}

	// A worker installs it; the consumer summary reflects one ready worker.
	ack, err := publisher.ReportDimensionPublicationAcknowledgement(ctx, AddressDimensionAcknowledgement{
		SnapshotID: approved.ID, WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.0.0",
		Checksum: approved.Checksum, State: AddressDimensionAckInstalled,
	})
	if err != nil || ack.State != AddressDimensionAckInstalled || ack.InstalledAt == nil {
		t.Fatalf("installed ack = %#v, %v", ack, err)
	}
	summary, err := publisher.GetAddressDimensionConsumerSummary(ctx, approved.ID)
	if err != nil || summary.Observed != 1 || summary.Ready != 1 {
		t.Fatalf("consumer summary = %#v, %v", summary, err)
	}
}
