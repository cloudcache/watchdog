package watchdog

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

type failAfterDimensionObjectRemove struct {
	DiskDimensionObjectStore
	fail bool
}

func (store *failAfterDimensionObjectRemove) RemoveDimensionObject(ref string) error {
	if err := store.DiskDimensionObjectStore.RemoveDimensionObject(ref); err != nil {
		return err
	}
	if store.fail {
		store.fail = false
		return errors.New("simulated crash after object deletion")
	}
	return nil
}

type blockingDimensionObjectRemove struct {
	DiskDimensionObjectStore
	started chan struct{}
	release chan struct{}
}

func (store *blockingDimensionObjectRemove) RemoveDimensionObject(ref string) error {
	close(store.started)
	<-store.release
	return store.DiskDimensionObjectStore.RemoveDimensionObject(ref)
}

func TestMySQLAddressDimensionObjectGCLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address dimension GC integration test")
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
	runEmbeddedMigrationAgain(t, store.db, "049")

	tenantID := ID("01JADDRESSGCTESTTENANT001")
	userID := ID("01JADDRESSGCTESTUSER0001")
	_, _ = store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address GC', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id) VALUES (?, ?, 'address-gc@test.invalid', 'Address GC', 'active', 'test', 'address-gc-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	disk := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	crashingStore := &failAfterDimensionObjectRemove{DiskDimensionObjectStore: disk, fail: true}
	publisher, err := NewMySQLAddressDimensionPublisher(store, crashingStore,
		WithAddressDimensionObjectRetention(24*time.Hour), withAddressDimensionClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}

	insertSnapshot := func(snapshotID ID, version uint64, effective, retention time.Time) DimensionObject {
		t.Helper()
		object, saveErr := disk.SaveDimensionObject(ctx, tenantID, snapshotID, []byte(`{"snapshot":"`+string(snapshotID)+`"}`))
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		_, insertErr := store.db.ExecContext(ctx, `
			INSERT INTO dimension_snapshots (
				id, tenant_id, module_key, dimension_key, version, effective_from,
				object_ref, checksum, draft_digest, source_manifest_version, source_manifest,
				bundle_schema_version, status, approval_state, retention_until,
				row_version, created_by, retired_by, created_at, retired_at
			) VALUES (?, ?, 'flow', 'address', ?, ?, ?, ?, ?, 0, JSON_ARRAY(), 1,
			          'retired', 'approved', ?, 1, ?, ?, ?, ?)
		`, snapshotID, tenantID, version, effective, object.Ref, object.Checksum,
			"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			retention, userID, userID, effective, effective.Add(time.Hour))
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		return object
	}

	oldID := ID("01JADDRESSGCOBJECTOLD0001")
	currentID := ID("01JADDRESSGCOBJECTNEW0001")
	oldObject := insertSnapshot(oldID, 1, now.Add(-72*time.Hour), now.Add(-time.Hour))
	_ = insertSnapshot(currentID, 2, now.Add(-48*time.Hour), now.Add(24*time.Hour))
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_activations (id, tenant_id, module_key, dimension_key, snapshot_id, effective_from, reason, rollback_of_snapshot_id, created_by)
		VALUES
		('01JADDRESSGCACTIVATE00001', ?, 'flow', 'address', ?, ?, 'publish', NULL, ?),
		('01JADDRESSGCACTIVATE00002', ?, 'flow', 'address', ?, ?, 'publish', NULL, ?),
		('01JADDRESSGCACTIVATE00003', ?, 'flow', 'address', ?, ?, 'rollback', ?, ?)
	`, tenantID, oldID, now.Add(-72*time.Hour), userID,
		tenantID, currentID, now.Add(-48*time.Hour), userID,
		tenantID, oldID, now.Add(time.Hour), currentID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_references (
			tenant_id, snapshot_id, consumer_kind, consumer_id, min_event_time, max_event_time, retain_until, last_observed_at
		) VALUES (?, ?, 'flow_fact', 'partition-0', ?, ?, ?, ?)
	`, tenantID, oldID, now.Add(-24*time.Hour), now.Add(-2*time.Hour), now.Add(time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_acks (
			tenant_id, snapshot_id, worker_id, boot_id, software_version, checksum, state, attempted_at, installed_at
		) VALUES (?, ?, 'worker-a', 'boot-a', '1.0.0', ?, 'installed', ?, ?)
	`, tenantID, oldID, oldObject.Checksum, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	list := func() []AddressDimensionGCCandidate {
		t.Helper()
		items, _, listErr := publisher.ListAddressDimensionGCCandidates(ctx, tenantID, now, AddressDimensionGCFilter{Limit: 10})
		if listErr != nil {
			t.Fatal(listErr)
		}
		return items
	}
	if items := list(); len(items) != 0 {
		t.Fatalf("future activation/reference/latest installed version must block GC: %#v", items)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM dimension_snapshot_activations WHERE id = '01JADDRESSGCACTIVATE00003'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE dimension_snapshot_references SET max_event_time = ?, retain_until = ? WHERE tenant_id = ? AND snapshot_id = ?`, now.Add(-2*time.Hour), now.Add(-time.Hour), tenantID, oldID); err != nil {
		t.Fatal(err)
	}
	if items := list(); len(items) != 0 {
		t.Fatalf("latest installed version must still block GC: %#v", items)
	}
	current, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, currentID)
	if err != nil {
		t.Fatal(err)
	}
	extendedRetention := now.Add(48 * time.Hour)
	current, err = publisher.ScheduleAddressDimensionObjectGC(ctx, tenantID, userID, currentID, current.RowVersion, extendedRetention)
	if err != nil || current.RetentionUntil == nil || !current.RetentionUntil.Equal(extendedRetention) {
		t.Fatalf("extend retention = %#v err=%v", current, err)
	}
	if _, err := publisher.ScheduleAddressDimensionObjectGC(ctx, tenantID, userID, currentID, current.RowVersion, now.Add(24*time.Hour)); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("retention shortening error = %v", err)
	}
	if _, err := publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: currentID, WorkerID: "worker-a", BootID: "boot-b",
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
	oldSnapshot, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, oldID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.ScheduleAddressDimensionObjectGC(ctx, tenantID, userID, oldID, oldSnapshot.RowVersion, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	staleHandler := NewAddressDimensionObjectGCJobHandler(publisher, func() time.Time { return now })
	if _, err := staleHandler(ctx, OperationJob{
		ID: "01JADDRESSGCSTALEJOB000001", TenantID: tenantID, CreatedBy: userID, CheckpointJSON: stalePayload,
	}); !IsTerminalJobError(err) || !errors.Is(err, ErrAddressDimensionConflict) {
		t.Fatalf("old retention generation error = %v", err)
	}
	now = now.Add(2 * time.Hour)
	candidates = list()
	if len(candidates) != 1 || !candidates[0].RetentionUntil.Equal(now.Add(-time.Hour)) {
		t.Fatalf("rescheduled candidates = %#v", candidates)
	}
	if _, err := publisher.RollbackAddressDimension(ctx, tenantID, userID, AddressDimensionRollbackRequest{
		SnapshotID: oldID, EffectiveFrom: now.Add(2 * time.Hour), ExpectedRowVersion: oldSnapshot.RowVersion + 1,
	}); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("expired rollback error = %v", err)
	}

	jobID := ID("01JADDRESSGCJOB00000000001")
	if _, err := publisher.DeleteAddressDimensionObject(ctx, tenantID, oldID, jobID, userID, oldObject.Ref, oldObject.Checksum, candidates[0].RetentionUntil, now); err == nil {
		t.Fatal("simulated post-delete crash was not returned")
	}
	if snapshot, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, oldID); err != nil || snapshot.ObjectDeletedAt != nil {
		t.Fatalf("failed attempt must not mark DB deleted: %#v err=%v", snapshot, err)
	}
	payload, err := EncodeAddressDimensionObjectGCJobPayload(candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	handler := NewAddressDimensionObjectGCJobHandler(publisher, func() time.Time { return now })
	job := OperationJob{ID: jobID, TenantID: tenantID, CreatedBy: userID, CheckpointJSON: payload}
	if _, err := handler(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := handler(ctx, job); err != nil {
		t.Fatalf("idempotent handler retry: %v", err)
	}
	if _, err := disk.ResolveDimensionObject(oldObject.Ref); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("object still resolves after GC: %v", err)
	}
	deleted, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, oldID)
	if err != nil || deleted.ObjectDeletedAt == nil || !deleted.ObjectDeletedAt.Equal(now) {
		t.Fatalf("deleted snapshot = %#v err=%v", deleted, err)
	}
	var receipts int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND action = 'dimension_object.destroyed' AND resource_id = ?`, tenantID, oldID).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("destruction receipts=%d err=%v", receipts, err)
	}
}

func TestMySQLAddressDimensionGCSerializesLateReference(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address dimension GC concurrency test")
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
	tenantID := ID("01JADDRESSGCRACETENANT001")
	userID := ID("01JADDRESSGCRACEUSER0001")
	snapshotID := ID("01JADDRESSGCRACESNAPSHOT01")
	_, _ = store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address GC Race', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id) VALUES (?, ?, 'address-gc-race@test.invalid', 'Address GC Race', 'active', 'test', 'address-gc-race-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	disk := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	object, err := disk.SaveDimensionObject(ctx, tenantID, snapshotID, []byte(`{"race":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, checksum, draft_digest, source_manifest_version, source_manifest,
			bundle_schema_version, status, approval_state, retention_until,
			row_version, created_by, retired_by, created_at, retired_at
		) VALUES (?, ?, 'flow', 'address', 1, ?, ?, ?, ?, 0, JSON_ARRAY(), 1,
		          'retired', 'approved', ?, 1, ?, ?, ?, ?)
	`, snapshotID, tenantID, now.Add(-48*time.Hour), object.Ref, object.Checksum,
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		now.Add(-time.Hour), userID, userID, now.Add(-48*time.Hour), now.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	blocking := &blockingDimensionObjectRemove{DiskDimensionObjectStore: disk, started: make(chan struct{}), release: make(chan struct{})}
	publisher, err := NewMySQLAddressDimensionPublisher(store, blocking, withAddressDimensionClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	deleteDone := make(chan error, 1)
	go func() {
		_, deleteErr := publisher.DeleteAddressDimensionObject(ctx, tenantID, snapshotID, "01JADDRESSGCRACEJOB0000001", userID, object.Ref, object.Checksum, now.Add(-time.Hour), now)
		deleteDone <- deleteErr
	}()
	<-blocking.started
	referenceDone := make(chan error, 1)
	go func() {
		_, referenceErr := publisher.ReportAddressDimensionReference(ctx, AddressDimensionReference{
			TenantID: tenantID, SnapshotID: snapshotID, ConsumerKind: "flow_fact", ConsumerID: "partition-late",
			MinEventTime: now.Add(-time.Hour), MaxEventTime: now, RetainUntil: now.Add(time.Hour),
		})
		referenceDone <- referenceErr
	}()
	select {
	case err := <-referenceDone:
		t.Fatalf("reference bypassed GC tenant lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(blocking.release)
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if err := <-referenceDone; !errors.Is(err, ErrAddressDimensionInvalid) {
		t.Fatalf("late reference error = %v", err)
	}
}
