package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

func TestMySQLAddressDraftRevisionPrepareCASApplyAndAudit(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address draft revision integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	const tenantID = ID("01JADDRDRAFTTENANT000001")
	const userID = ID("01JADDRDRAFTUSER00000001")
	_, _ = db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address draft revision', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'draft-revision@test.invalid', 'Draft revision', 'active', 'test', 'draft-revision-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	old, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000041", TenantID: tenantID,
		CIDR: "198.51.100.0/24", Labels: map[string]string{"state": "old"}, Source: "manual",
	})
	if err != nil {
		t.Fatal(err)
	}
	keep, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000042", TenantID: tenantID,
		CIDR: "192.0.2.0/24", Labels: map[string]string{"state": "keep"}, Source: "manual",
	})
	if err != nil {
		t.Fatal(err)
	}
	operations := []AddressPrefixBatchOperation{
		{Action: "create", CIDR: "203.0.113.7/24", Labels: map[string]string{"state": "new"}},
		{Action: "delete", PrefixID: old.ID, ExpectedVersion: old.RowVersion},
	}
	revision, err := store.PrepareAddressPrefixRevision(ctx, tenantID, userID, operations)
	if err != nil {
		t.Fatal(err)
	}
	if revision.Status != AddressDraftRevisionStatusPrepared || revision.RowVersion != 1 || revision.Preview.CreateCount != 1 || revision.Preview.DeleteCount != 1 {
		t.Fatalf("prepared revision = %#v", revision)
	}
	repeated, err := store.PrepareAddressPrefixRevision(ctx, tenantID, userID, []AddressPrefixBatchOperation{operations[1], operations[0]})
	if err != nil || repeated.ID != revision.ID {
		t.Fatalf("idempotent preview = %#v err=%v", repeated, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE address_draft_revisions
		SET created_at = UTC_TIMESTAMP(3) - INTERVAL 2 DAY, expires_at = UTC_TIMESTAMP(3) - INTERVAL 1 DAY
		WHERE tenant_id = ? AND id = ?`, tenantID, revision.ID); err != nil {
		t.Fatal(err)
	}
	refreshed, err := store.PrepareAddressPrefixRevision(ctx, tenantID, userID, operations)
	if err != nil || refreshed.ID != revision.ID || refreshed.RowVersion != 2 || !refreshed.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("refreshed preview = %#v err=%v", refreshed, err)
	}
	revision = refreshed
	items, cursor, _, err := store.ListAddressDraftRevisions(ctx, tenantID, AddressDraftRevisionListFilter{Status: "prepared", Limit: 10})
	if err != nil || cursor != "" || len(items) != 1 || items[0].ID != revision.ID {
		t.Fatalf("prepared list=%#v cursor=%q err=%v", items, cursor, err)
	}
	items, cursor, total, err := store.ListAddressDraftRevisions(ctx, tenantID, AddressDraftRevisionListFilter{
		Search: string(revision.ID), Status: "prepared", Sort: "operations", Desc: true, Limit: 25, TableMode: true,
	})
	if err != nil || cursor != "" || total != 1 || len(items) != 1 || items[0].ID != revision.ID {
		t.Fatalf("server list=%#v cursor=%q total=%d err=%v", items, cursor, total, err)
	}

	keep.Labels["state"] = "changed-after-preview"
	if _, err := store.UpdateAddressPrefix(ctx, keep, keep.RowVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyAddressDraftRevision(ctx, tenantID, userID, revision.ID, revision.RowVersion); !errors.Is(err, ErrAddressDraftRevisionChanged) {
		t.Fatalf("stale apply error = %v", err)
	}
	superseded, err := store.GetAddressDraftRevision(ctx, tenantID, revision.ID)
	if err != nil || superseded.Status != AddressDraftRevisionStatusSuperseded || superseded.RowVersion != 3 {
		t.Fatalf("superseded revision=%#v err=%v", superseded, err)
	}

	revision, err = store.PrepareAddressPrefixRevision(ctx, tenantID, userID, operations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyAddressDraftRevision(ctx, tenantID, userID, revision.ID, revision.RowVersion+1); !errors.Is(err, ErrAddressDraftRevisionConflict) {
		t.Fatalf("stale row version error = %v", err)
	}
	applied, err := store.ApplyAddressDraftRevision(ctx, tenantID, userID, revision.ID, revision.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Status != AddressDraftRevisionStatusApplied || applied.RowVersion != 2 || applied.ResultDigest != applied.Preview.ExpectedResultDigest || applied.AppliedAt == nil {
		t.Fatalf("applied revision = %#v", applied)
	}
	if _, err := store.GetAddressPrefix(ctx, tenantID, old.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted prefix error = %v", err)
	}
	createdID := ""
	for _, operation := range applied.Operations {
		if operation.Action == "create" {
			createdID = operation.PrefixID
		}
	}
	created, err := store.GetAddressPrefix(ctx, tenantID, createdID)
	if err != nil || created.CIDR != "203.0.113.0/24" || created.Labels["state"] != "new" {
		t.Fatalf("created prefix=%#v err=%v", created, err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs
		WHERE tenant_id = ? AND resource_type = 'address_revision' AND resource_id = ?`, tenantID, applied.ID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != len(applied.Operations) {
		t.Fatalf("audit count=%d want=%d", auditCount, len(applied.Operations))
	}
}

func TestMySQLAddressDraftRevisionRejectsDisabledReferences(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address draft revision integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	const tenantID = ID("01JADDRDRAFTTENANT000002")
	const userID = ID("01JADDRDRAFTUSER00000002")
	_, _ = db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Disabled address reference', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'disabled-reference@test.invalid', 'Disabled reference', 'active', 'test', 'disabled-reference-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	node, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{
		TenantID: tenantID, Kind: GeoKindCountry, Code: "ZZ", Name: "Disabled", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PrepareAddressPrefixRevision(ctx, tenantID, userID, []AddressPrefixBatchOperation{{
		Action: "create", CIDR: "203.0.113.0/24", GeoLeafID: node.ID,
	}})
	if !errors.Is(err, ErrAddressDraftRevisionInvalid) {
		t.Fatalf("disabled reference error = %v", err)
	}
}
