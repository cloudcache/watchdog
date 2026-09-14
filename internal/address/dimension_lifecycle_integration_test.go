package address

import (
	"context"
	"testing"
	"time"
)

// Engine-level integration (de-tenanted) proving the publication lifecycle end to
// end against real MySQL: build a WADS snapshot -> the trusted-approval gate rejects
// a pending activation -> approve (an audited UI confirmation, no signature) ->
// activate -> worker ACK -> consumer summary. The HTTP approve endpoint is covered in 05D.
func TestStoreAddressDimensionLifecycleApproval(t *testing.T) {
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

	// Approve is an audited UI confirmation now: it records the deciding actor and
	// timestamp and carries no signature; accountability is the audit log + rollback.
	approved, err := publisher.ApproveDimensionPublication(ctx, actorID, snapshot.ID, snapshot.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if approved.ApprovalState != AddressDimensionApprovalApproved || approved.DecidedBy != actorID ||
		approved.DecidedAt == nil || len(approved.Signature) != 0 || approved.RowVersion != 2 {
		t.Fatalf("approved snapshot = %#v", approved)
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
