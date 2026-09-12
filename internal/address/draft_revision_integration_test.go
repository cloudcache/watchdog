package address

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// Integration port (de-tenanted) of the draft-revision lifecycle: prepare (with
// idempotent re-preview and expiry refresh), server-side listing, base-digest
// re-check on apply (supersede on drift), row-version CAS, applied result +
// per-operation audit rows. Tenant arg/column removed; a v2 user is seeded; the
// audit assertion uses the v2 `resource` column.
func TestStoreAddressDraftRevisionPrepareCASApplyAndAudit(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const userID = ID("01JADDRDRAFTUSER00000001")
	seedAddressTestUser(t, db, userID)

	old, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000041",
		CIDR: "198.51.100.0/24", Labels: map[string]string{"state": "old"}, Source: "manual",
	})
	if err != nil {
		t.Fatal(err)
	}
	keep, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000042",
		CIDR: "192.0.2.0/24", Labels: map[string]string{"state": "keep"}, Source: "manual",
	})
	if err != nil {
		t.Fatal(err)
	}
	operations := []AddressPrefixBatchOperation{
		{Action: "create", CIDR: "203.0.113.7/24", Labels: map[string]string{"state": "new"}},
		{Action: "delete", PrefixID: old.ID, ExpectedVersion: old.RowVersion},
	}
	revision, err := store.PrepareAddressPrefixRevision(ctx, userID, operations)
	if err != nil {
		t.Fatal(err)
	}
	if revision.Status != AddressDraftRevisionStatusPrepared || revision.RowVersion != 1 || revision.Preview.CreateCount != 1 || revision.Preview.DeleteCount != 1 {
		t.Fatalf("prepared revision = %#v", revision)
	}
	repeated, err := store.PrepareAddressPrefixRevision(ctx, userID, []AddressPrefixBatchOperation{operations[1], operations[0]})
	if err != nil || repeated.ID != revision.ID {
		t.Fatalf("idempotent preview = %#v err=%v", repeated, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE address_draft_revisions
		SET created_at = UTC_TIMESTAMP(3) - INTERVAL 2 DAY, expires_at = UTC_TIMESTAMP(3) - INTERVAL 1 DAY
		WHERE id = ?`, revision.ID); err != nil {
		t.Fatal(err)
	}
	refreshed, err := store.PrepareAddressPrefixRevision(ctx, userID, operations)
	if err != nil || refreshed.ID != revision.ID || refreshed.RowVersion != 2 || !refreshed.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("refreshed preview = %#v err=%v", refreshed, err)
	}
	revision = refreshed
	items, cursor, _, err := store.ListAddressDraftRevisions(ctx, AddressDraftRevisionListFilter{Status: "prepared", Limit: 10})
	if err != nil || cursor != "" || len(items) != 1 || items[0].ID != revision.ID {
		t.Fatalf("prepared list=%#v cursor=%q err=%v", items, cursor, err)
	}
	items, cursor, total, err := store.ListAddressDraftRevisions(ctx, AddressDraftRevisionListFilter{
		Search: string(revision.ID), Status: "prepared", Sort: "operations", Desc: true, Limit: 25, TableMode: true,
	})
	if err != nil || cursor != "" || total != 1 || len(items) != 1 || items[0].ID != revision.ID {
		t.Fatalf("server list=%#v cursor=%q total=%d err=%v", items, cursor, total, err)
	}

	keep.Labels["state"] = "changed-after-preview"
	if _, err := store.UpdateAddressPrefix(ctx, keep, keep.RowVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyAddressDraftRevision(ctx, userID, revision.ID, revision.RowVersion); !errors.Is(err, ErrAddressDraftRevisionChanged) {
		t.Fatalf("stale apply error = %v", err)
	}
	superseded, err := store.GetAddressDraftRevision(ctx, revision.ID)
	if err != nil || superseded.Status != AddressDraftRevisionStatusSuperseded || superseded.RowVersion != 3 {
		t.Fatalf("superseded revision=%#v err=%v", superseded, err)
	}

	revision, err = store.PrepareAddressPrefixRevision(ctx, userID, operations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyAddressDraftRevision(ctx, userID, revision.ID, revision.RowVersion+1); !errors.Is(err, ErrAddressDraftRevisionConflict) {
		t.Fatalf("stale row version error = %v", err)
	}
	applied, err := store.ApplyAddressDraftRevision(ctx, userID, revision.ID, revision.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Status != AddressDraftRevisionStatusApplied || applied.RowVersion != 2 || applied.ResultDigest != applied.Preview.ExpectedResultDigest || applied.AppliedAt == nil {
		t.Fatalf("applied revision = %#v", applied)
	}
	if _, err := store.GetAddressPrefix(ctx, old.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted prefix error = %v", err)
	}
	createdID := ""
	for _, operation := range applied.Operations {
		if operation.Action == "create" {
			createdID = operation.PrefixID
		}
	}
	created, err := store.GetAddressPrefix(ctx, createdID)
	if err != nil || created.CIDR != "203.0.113.0/24" || created.Labels["state"] != "new" {
		t.Fatalf("created prefix=%#v err=%v", created, err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs
		WHERE resource = 'address_revision' AND resource_id = ?`, applied.ID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != len(applied.Operations) {
		t.Fatalf("audit count=%d want=%d", auditCount, len(applied.Operations))
	}
}

func TestStoreAddressDraftRevisionRejectsDisabledReferences(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const userID = ID("01JADDRDRAFTUSER00000002")
	seedAddressTestUser(t, db, userID)

	node, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindCountry, Code: "ZZ", Name: "Disabled", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PrepareAddressPrefixRevision(ctx, userID, []AddressPrefixBatchOperation{{
		Action: "create", CIDR: "203.0.113.0/24", GeoLeafID: node.ID,
	}})
	if !errors.Is(err, ErrAddressDraftRevisionInvalid) {
		t.Fatalf("disabled reference error = %v", err)
	}
}
