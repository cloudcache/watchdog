package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLAddressDimensionPreviewPublishAndDraftCAS(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address dimension integration test")
	}
	ctx := context.Background()
	store, err := OpenMySQLStore(ctx, MySQLConfig{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := ApplyMySQLMigrations(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	tenantID := ID("01JADDRESSDIMENSIONTENANT1")
	userID := ID("01JADDRESSDIMENSIONUSER001")
	_, _ = store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address Dimension', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id) VALUES (?, ?, 'dimension@test.invalid', 'Dimension', 'active', 'test', 'dimension-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	prefix, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000001", TenantID: tenantID, CIDR: "10.0.0.0/8",
		Labels: map[string]string{"flow": "local", "business": "private"}, Source: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	objects := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	publisher, err := NewMySQLAddressDimensionPublisher(store, objects)
	if err != nil {
		t.Fatal(err)
	}
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	preview, err := publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	prefix.Labels["business"] = "changed"
	prefix, err = store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest}); !errors.Is(err, ErrAddressDimensionDraftChanged) {
		t.Fatalf("expected draft CAS error, got %v", err)
	}
	preview, err = publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.PrefixCount != 1 || snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	path, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := flowdimension.DecodeAndCompileBundle(data, snapshot.Checksum, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classified := compiled.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.7"))
	if classified.Direction != flowdimension.DirectionOut || classified.Business != "changed" {
		t.Fatalf("published bundle classification = %#v", classified)
	}
	items, cursor, err := publisher.ListAddressDimensionSnapshots(ctx, tenantID, AddressDimensionListFilter{Limit: 10})
	if err != nil || len(items) != 1 || cursor != "" || items[0].ID != snapshot.ID {
		t.Fatalf("list snapshots = %#v %q %v", items, cursor, err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	approve := func(item AddressDimensionSnapshot) AddressDimensionSnapshot {
		t.Helper()
		signedAt := time.Now().UTC().Add(time.Second).Truncate(time.Millisecond)
		payload, payloadErr := AddressDimensionSigningPayload(item, "dimension-test-key", signedAt)
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		approval, verifyErr := VerifyAddressDimensionApproval(item, "dimension-test-key", signedAt, ed25519.Sign(privateKey, payload), publicKey)
		if verifyErr != nil {
			t.Fatal(verifyErr)
		}
		approved, approveErr := publisher.ApproveAddressDimension(ctx, tenantID, userID, item.RowVersion, approval)
		if approveErr != nil {
			t.Fatal(approveErr)
		}
		return approved
	}

	snapshot = approve(snapshot)
	if snapshot.ApprovalState != AddressDimensionApprovalApproved || snapshot.SignatureAlgorithm != AddressDimensionSignatureAlgorithm || snapshot.RowVersion != 2 {
		t.Fatalf("approved snapshot = %#v", snapshot)
	}
	activation, err := publisher.ActivateAddressDimension(ctx, tenantID, userID, AddressDimensionActivationRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: effective, ExpectedRowVersion: snapshot.RowVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activation.Reason != AddressDimensionActivationPublish || activation.SnapshotID != snapshot.ID {
		t.Fatalf("activation = %#v", activation)
	}
	snapshot, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := publisher.GetAddressDimensionActivationAt(ctx, tenantID, effective.Add(time.Minute))
	if err != nil || selected.SnapshotID != snapshot.ID {
		t.Fatalf("selected activation = %#v, %v", selected, err)
	}

	prefix.Labels["business"] = "second"
	prefix, err = store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	effective2 := effective.Add(time.Hour)
	preview2, err := publisher.PreviewAddressDimension(ctx, tenantID, effective2)
	if err != nil {
		t.Fatal(err)
	}
	snapshot2, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective2, PreviewDigest: preview2.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.ActivateAddressDimension(ctx, tenantID, userID, AddressDimensionActivationRequest{
		SnapshotID: snapshot2.ID, EffectiveFrom: effective2, ExpectedRowVersion: snapshot2.RowVersion,
	}); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("pending snapshot activation error = %v", err)
	}
	snapshot2 = approve(snapshot2)
	if _, err := publisher.ActivateAddressDimension(ctx, tenantID, userID, AddressDimensionActivationRequest{
		SnapshotID: snapshot2.ID, EffectiveFrom: effective2, ExpectedRowVersion: snapshot2.RowVersion,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot2, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot2.ID)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err = publisher.RetireAddressDimension(ctx, tenantID, userID, AddressDimensionRetireRequest{
		SnapshotID: snapshot.ID, ExpectedRowVersion: snapshot.RowVersion, Reason: "superseded",
	})
	if err != nil || snapshot.Status != AddressDimensionStatusRetired {
		t.Fatalf("retire old snapshot = %#v, %v", snapshot, err)
	}
	rollbackAt := effective2.Add(time.Hour)
	rollback, err := publisher.RollbackAddressDimension(ctx, tenantID, userID, AddressDimensionRollbackRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: rollbackAt, ExpectedRowVersion: snapshot.RowVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rollback.Reason != AddressDimensionActivationRollback || rollback.RollbackOfSnapshotID != snapshot2.ID {
		t.Fatalf("rollback activation = %#v", rollback)
	}
	selected, err = publisher.GetAddressDimensionActivationAt(ctx, tenantID, rollbackAt)
	if err != nil || selected.SnapshotID != snapshot.ID {
		t.Fatalf("rollback selection = %#v, %v", selected, err)
	}
	if _, err := publisher.RetireAddressDimension(ctx, tenantID, userID, AddressDimensionRetireRequest{
		SnapshotID: snapshot.ID, ExpectedRowVersion: snapshot.RowVersion + 1, Reason: "must replace latest first",
	}); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("retire latest activation error = %v", err)
	}
	if _, err := publisher.RetireAddressDimension(ctx, tenantID, userID, AddressDimensionRetireRequest{
		SnapshotID: snapshot2.ID, ExpectedRowVersion: snapshot2.RowVersion, Reason: "rolled back",
	}); err != nil {
		t.Fatal(err)
	}

	ack, err := publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot.ID, WorkerID: "worker-a", BootID: "boot-a",
		SoftwareVersion: "1.0.0", Checksum: snapshot.Checksum, State: AddressDimensionAckFailed,
		ErrorCode: "DOWNLOAD_FAILED", ErrorMessage: "temporary failure",
	})
	if err != nil || ack.State != AddressDimensionAckFailed || ack.InstalledAt != nil || ack.RowVersion != 1 {
		t.Fatalf("failed ack = %#v, %v", ack, err)
	}
	ack, err = publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot.ID, WorkerID: "worker-a", BootID: "boot-a",
		SoftwareVersion: "1.0.0", Checksum: snapshot.Checksum, State: AddressDimensionAckInstalled,
	})
	if err != nil || ack.State != AddressDimensionAckInstalled || ack.InstalledAt == nil || ack.RowVersion != 2 {
		t.Fatalf("installed ack = %#v, %v", ack, err)
	}
	if _, err := publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot.ID, WorkerID: "worker-a", BootID: "boot-a",
		SoftwareVersion: "1.0.0", Checksum: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", State: AddressDimensionAckInstalled,
	}); !errors.Is(err, ErrAddressDimensionInvalid) {
		t.Fatalf("mismatched ack checksum error = %v", err)
	}

	reference, err := publisher.ReportAddressDimensionReference(ctx, AddressDimensionReference{
		TenantID: tenantID, SnapshotID: snapshot.ID, ConsumerKind: "flow_fact", ConsumerID: "partition-0",
		MinEventTime: effective, MaxEventTime: effective.Add(time.Hour), RetainUntil: effective.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	reference, err = publisher.ReportAddressDimensionReference(ctx, AddressDimensionReference{
		TenantID: tenantID, SnapshotID: snapshot.ID, ConsumerKind: "flow_fact", ConsumerID: "partition-0",
		MinEventTime: effective.Add(-time.Hour), MaxEventTime: effective.Add(2 * time.Hour), RetainUntil: effective.Add(48 * time.Hour),
	})
	if err != nil || !reference.MinEventTime.Equal(effective.Add(-time.Hour)) || !reference.MaxEventTime.Equal(effective.Add(2*time.Hour)) || !reference.RetainUntil.Equal(effective.Add(48*time.Hour)) || reference.RowVersion != 2 {
		t.Fatalf("merged reference = %#v, %v", reference, err)
	}

	effective3 := rollbackAt.Add(time.Hour)
	preview3, err := publisher.PreviewAddressDimension(ctx, tenantID, effective3)
	if err != nil {
		t.Fatal(err)
	}
	snapshot3, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective3, PreviewDigest: preview3.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	snapshot3, err = publisher.RejectAddressDimension(ctx, tenantID, userID, snapshot3.ID, snapshot3.RowVersion, "review failed")
	if err != nil || snapshot3.ApprovalState != AddressDimensionApprovalRejected || snapshot3.DecisionReason != "review failed" {
		t.Fatalf("rejected snapshot = %#v, %v", snapshot3, err)
	}
	var auditCount int
	if err := store.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs
		WHERE tenant_id = ? AND resource_type = 'dimension_snapshot'
	`, tenantID).Scan(&auditCount); err != nil || auditCount < 6 {
		t.Fatalf("dimension lifecycle audit count = %d, %v", auditCount, err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID); err != nil {
		t.Fatalf("tenant lifecycle cascade: %v", err)
	}
	for _, table := range []string{"dimension_snapshots", "dimension_snapshot_activations", "dimension_snapshot_acks", "dimension_snapshot_references"} {
		var count int
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE tenant_id = ?`, tenantID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s rows after tenant cascade = %d, %v", table, count, err)
		}
	}
}
