package address

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/opjob"
)

// failAfterDimensionObjectRemove deletes the object then simulates a crash before
// the DB is marked, so the handler must recover idempotently on retry.
type failAfterDimensionObjectRemove struct {
	DiskDimensionObjectStore
	fail bool
}

func (s *failAfterDimensionObjectRemove) RemoveDimensionObject(ref string) error {
	if err := s.DiskDimensionObjectStore.RemoveDimensionObject(ref); err != nil {
		return err
	}
	if s.fail {
		s.fail = false
		return errors.New("simulated crash after object deletion")
	}
	return nil
}

// Faithful de-tenant port of internal/watchdog TestMySQLAddressDimensionObjectGCLifecycle:
// future activation / live reference / latest-installed all block GC; retention can
// only extend; the eligible retired object is collected with a crash-safe, idempotent
// handler that records exactly one destruction receipt.
func TestStoreAddressDimensionObjectGCLifecycle(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const userID = ID("01JADDRESSGCTESTUSER0001")
	seedAddressTestUser(t, db, userID)

	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	disk := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	crashingStore := &failAfterDimensionObjectRemove{DiskDimensionObjectStore: disk, fail: true}
	publisher, err := NewPublisher(store, crashingStore, WithObjectRetention(24*time.Hour), WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}

	insertSnapshot := func(snapshotID ID, version uint64, effective, retention time.Time) DimensionObject {
		t.Helper()
		object, saveErr := disk.SaveDimensionObject(ctx, snapshotID, []byte(`{"snapshot":"`+string(snapshotID)+`"}`))
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		if _, insertErr := db.ExecContext(ctx, `
			INSERT INTO dimension_snapshots (
				id, module_key, dimension_key, version, effective_from,
				object_ref, checksum, draft_digest, source_manifest_version, source_manifest,
				bundle_schema_version, status, approval_state, retention_until,
				row_version, created_by, retired_by, created_at, retired_at
			) VALUES (?, 'flow', 'address', ?, ?, ?, ?, ?, 0, JSON_ARRAY(), 1,
			          'retired', 'approved', ?, 1, ?, ?, ?, ?)
		`, snapshotID, version, effective, object.Ref, object.Checksum,
			"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			retention, userID, userID, effective, effective.Add(time.Hour)); insertErr != nil {
			t.Fatal(insertErr)
		}
		return object
	}

	oldID := ID("01JADDRESSGCOBJECTOLD0001")
	currentID := ID("01JADDRESSGCOBJECTNEW0001")
	oldObject := insertSnapshot(oldID, 1, now.Add(-72*time.Hour), now.Add(-time.Hour))
	_ = insertSnapshot(currentID, 2, now.Add(-48*time.Hour), now.Add(24*time.Hour))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_activations (id, module_key, dimension_key, snapshot_id, effective_from, reason, rollback_of_snapshot_id, created_by)
		VALUES
		('01JADDRESSGCACTIVATE00001', 'flow', 'address', ?, ?, 'publish', NULL, ?),
		('01JADDRESSGCACTIVATE00002', 'flow', 'address', ?, ?, 'publish', NULL, ?),
		('01JADDRESSGCACTIVATE00003', 'flow', 'address', ?, ?, 'rollback', ?, ?)
	`, oldID, now.Add(-72*time.Hour), userID,
		currentID, now.Add(-48*time.Hour), userID,
		oldID, now.Add(time.Hour), currentID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_references (
			snapshot_id, consumer_kind, consumer_id, min_event_time, max_event_time, retain_until, last_observed_at
		) VALUES (?, 'flow_fact', 'partition-0', ?, ?, ?, ?)
	`, oldID, now.Add(-24*time.Hour), now.Add(-2*time.Hour), now.Add(time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_acks (
			snapshot_id, worker_id, boot_id, software_version, checksum, state, attempted_at, installed_at
		) VALUES (?, 'worker-a', 'boot-a', '1.0.0', ?, 'installed', ?, ?)
	`, oldID, oldObject.Checksum, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	list := func() []AddressDimensionGCCandidate {
		t.Helper()
		items, _, listErr := publisher.ListAddressDimensionGCCandidates(ctx, now, AddressDimensionGCFilter{Limit: 10})
		if listErr != nil {
			t.Fatal(listErr)
		}
		return items
	}
	if items := list(); len(items) != 0 {
		t.Fatalf("future activation/reference/latest installed version must block GC: %#v", items)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM dimension_snapshot_activations WHERE id = '01JADDRESSGCACTIVATE00003'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE dimension_snapshot_references SET max_event_time = ?, retain_until = ? WHERE snapshot_id = ?`, now.Add(-2*time.Hour), now.Add(-time.Hour), oldID); err != nil {
		t.Fatal(err)
	}
	if items := list(); len(items) != 0 {
		t.Fatalf("latest installed version must still block GC: %#v", items)
	}
	current, err := publisher.GetAddressDimensionSnapshot(ctx, currentID)
	if err != nil {
		t.Fatal(err)
	}
	extendedRetention := now.Add(48 * time.Hour)
	current, err = publisher.ScheduleAddressDimensionObjectGC(ctx, userID, currentID, current.RowVersion, extendedRetention)
	if err != nil || current.RetentionUntil == nil || !current.RetentionUntil.Equal(extendedRetention) {
		t.Fatalf("extend retention = %#v err=%v", current, err)
	}
	if _, err := publisher.ScheduleAddressDimensionObjectGC(ctx, userID, currentID, current.RowVersion, now.Add(24*time.Hour)); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("retention shortening error = %v", err)
	}
	if _, err := publisher.ReportDimensionPublicationAcknowledgement(ctx, AddressDimensionAcknowledgement{
		SnapshotID: currentID, WorkerID: "worker-a", BootID: "boot-b",
		SoftwareVersion: "1.1.0", Checksum: current.Checksum, State: AddressDimensionAckInstalled,
	}); err != nil {
		t.Fatal(err)
	}
	candidates := list()
	if len(candidates) != 1 || candidates[0].SnapshotID != oldID {
		t.Fatalf("eligible candidates = %#v", candidates)
	}
	stalePayload, err := EncodeAddressDimensionObjectGCJobPayload(candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := publisher.GetAddressDimensionSnapshot(ctx, oldID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.ScheduleAddressDimensionObjectGC(ctx, userID, oldID, oldSnapshot.RowVersion, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	staleHandler := NewAddressDimensionObjectGCJobHandler(publisher, func() time.Time { return now })
	if _, err := staleHandler(ctx, opjob.Job{ID: "01JADDRESSGCSTALEJOB000001", CreatedBy: string(userID), CheckpointJSON: stalePayload}); !opjob.IsTerminalError(err) || !errors.Is(err, ErrAddressDimensionConflict) {
		t.Fatalf("old retention generation error = %v", err)
	}
	now = now.Add(2 * time.Hour)
	candidates = list()
	if len(candidates) != 1 || !candidates[0].RetentionUntil.Equal(now.Add(-time.Hour)) {
		t.Fatalf("rescheduled candidates = %#v", candidates)
	}
	if _, err := publisher.RollbackDimensionPublication(ctx, userID, AddressDimensionRollbackRequest{
		SnapshotID: oldID, EffectiveFrom: now.Add(2 * time.Hour), ExpectedRowVersion: oldSnapshot.RowVersion + 1,
	}); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("expired rollback error = %v", err)
	}

	jobID := ID("01JADDRESSGCJOB00000000001")
	if _, err := publisher.DeleteAddressDimensionObject(ctx, oldID, jobID, userID, oldObject.Ref, oldObject.Checksum, candidates[0].RetentionUntil, now); err == nil {
		t.Fatal("simulated post-delete crash was not returned")
	}
	if snapshot, err := publisher.GetAddressDimensionSnapshot(ctx, oldID); err != nil || snapshot.ObjectDeletedAt != nil {
		t.Fatalf("failed attempt must not mark DB deleted: %#v err=%v", snapshot, err)
	}
	payload, err := EncodeAddressDimensionObjectGCJobPayload(candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	handler := NewAddressDimensionObjectGCJobHandler(publisher, func() time.Time { return now })
	job := opjob.Job{ID: string(jobID), CreatedBy: string(userID), CheckpointJSON: payload}
	if _, err := handler(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := handler(ctx, job); err != nil {
		t.Fatalf("idempotent handler retry: %v", err)
	}
	if _, err := disk.ResolveDimensionObject(oldObject.Ref); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("object still resolves after GC: %v", err)
	}
	deleted, err := publisher.GetAddressDimensionSnapshot(ctx, oldID)
	if err != nil || deleted.ObjectDeletedAt == nil || !deleted.ObjectDeletedAt.Equal(now) {
		t.Fatalf("deleted snapshot = %#v err=%v", deleted, err)
	}
	var receipts int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs WHERE action = 'dimension_object.destroyed' AND resource_id = ?`, oldID).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("destruction receipts=%d err=%v", receipts, err)
	}
}
