package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func TestMySQLFlowSavedFilterLifecycleAndVisibility(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	for _, user := range []struct{ id, email string }{{"user_filter_a", "filter-a@test.local"}, {"user_filter_b", "filter-b@test.local"}} {
		if _, err := db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status)
			VALUES (?, ?, ?, ?, 'active')`, user.id, tenant, user.email, user.id); err != nil {
			t.Fatal(err)
		}
	}
	predicate := func(asn string) flowquery.FilterExpression {
		return flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "asn", Operator: flowquery.FilterEqual, Values: []string{asn}}
	}
	privateA, err := store.CreateFlowSavedFilter(ctx, FlowSavedFilter{
		TenantID: tenant, OwnerUserID: "user_filter_a", Name: "Private A", ShareScope: FlowSavedFilterPrivate, Filter: predicate("4134"),
	})
	if err != nil || privateA.RowVersion != 1 || privateA.FilterSchemaVersion != 1 {
		t.Fatalf("private A=%+v err=%v", privateA, err)
	}
	privateB, err := store.CreateFlowSavedFilter(ctx, FlowSavedFilter{
		TenantID: tenant, OwnerUserID: "user_filter_b", Name: "Private B", ShareScope: FlowSavedFilterPrivate, Filter: predicate("4837"),
	})
	if err != nil {
		t.Fatal(err)
	}
	sharedB, err := store.CreateFlowSavedFilter(ctx, FlowSavedFilter{
		TenantID: tenant, OwnerUserID: "user_filter_b", Name: "Shared B", Description: "backbone", ShareScope: FlowSavedFilterTenant, Filter: predicate("9929"),
	})
	if err != nil {
		t.Fatal(err)
	}

	visibleA, total, err := store.ListFlowSavedFilters(ctx, tenant, FlowSavedFilterListQuery{
		ViewerUserID: "user_filter_a", SortBy: "name", Limit: 10,
	})
	if err != nil || total != 2 || len(visibleA) != 2 || visibleA[0].ID != privateA.ID || visibleA[1].ID != sharedB.ID {
		t.Fatalf("visible A=%+v total=%d err=%v", visibleA, total, err)
	}
	if _, err := store.GetFlowSavedFilter(ctx, tenant, "user_filter_a", privateB.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("foreign private get err=%v", err)
	}
	sharedOnly, total, err := store.ListFlowSavedFilters(ctx, tenant, FlowSavedFilterListQuery{
		ViewerUserID: "user_filter_a", Search: "back", ShareScope: FlowSavedFilterTenant,
		OwnerUserID: "user_filter_b", SortBy: "updated_at", Descending: true, Limit: 1,
	})
	if err != nil || total != 1 || len(sharedOnly) != 1 || sharedOnly[0].ID != sharedB.ID {
		t.Fatalf("shared=%+v total=%d err=%v", sharedOnly, total, err)
	}
	owners, err := store.ListFlowSavedFilterOwners(ctx, tenant, "user_filter_a", "user_filter_b", 10)
	if err != nil || len(owners) != 1 || owners[0].OwnerUserID != "user_filter_b" || owners[0].Count != 1 {
		t.Fatalf("owners=%+v err=%v", owners, err)
	}

	privateA.Description = "updated"
	updated, err := store.UpdateFlowSavedFilter(ctx, privateA, privateA.RowVersion)
	if err != nil || updated.RowVersion != 2 || updated.Description != "updated" {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	if _, err := store.UpdateFlowSavedFilter(ctx, privateA, privateA.RowVersion); !errors.Is(err, ErrFlowSavedFilterVersionConflict) {
		t.Fatalf("stale update err=%v", err)
	}
	if err := store.DeleteFlowSavedFilter(ctx, tenant, privateA.ID, updated.RowVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetFlowSavedFilter(ctx, tenant, "user_filter_a", privateA.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("soft-deleted get err=%v", err)
	}
	var deletedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT deleted_at FROM flow_saved_filters WHERE id = ?`, privateA.ID).Scan(&deletedAt); err != nil || !deletedAt.Valid {
		t.Fatalf("deleted_at=%+v err=%v", deletedAt, err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM users WHERE id = 'user_filter_b'`); err != nil {
		t.Fatal(err)
	}
	orphanedShared, err := store.GetFlowSavedFilter(ctx, tenant, "user_filter_a", sharedB.ID)
	if err != nil || orphanedShared.OwnerUserID != "" || orphanedShared.ShareScope != FlowSavedFilterTenant {
		t.Fatalf("orphaned shared=%+v err=%v", orphanedShared, err)
	}
}
