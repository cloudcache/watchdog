package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
)

func TestMySQLDashboardLifecycle(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	// An owner user (owner_id FK -> users, ON DELETE SET NULL).
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_dash_owner', ?, 'dash-owner@test.local', 'Owner', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	graph, err := store.CreateAggregateGraph(ctx, AggregateGraph{
		ID: "graph_dash_01", TenantID: tenant, Name: "Dashboard Traffic",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceAggregateGraphItems(ctx, tenant, graph.ID, []AggregateGraphItem{{
		ID: "series_dash_01", Metric: "ifHCInOctets", Direction: "in",
	}}); err != nil {
		t.Fatal(err)
	}

	created, err := store.CreateDashboard(ctx, Dashboard{
		TenantID:    tenant,
		OwnerID:     "user_dash_owner",
		Name:        "Ops Overview",
		Description: "core links",
		Layout:      json.RawMessage(`{"panels":[{"graph_id":"graph_dash_01"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Version != 1 || created.OwnerID != "user_dash_owner" || created.CreatedAt.IsZero() {
		t.Fatalf("created = %+v", created)
	}

	got, err := store.GetDashboard(ctx, tenant, created.ID)
	if err != nil || got.Name != "Ops Overview" || string(got.Layout) == "" {
		t.Fatalf("get = %+v err=%v", got, err)
	}

	listed, total, err := store.ListDashboards(ctx, tenant, DashboardListFilter{Search: "core", OwnerID: "user_dash_owner", Limit: 1})
	if err != nil || total != 1 || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("list = %+v total=%d err=%v", listed, total, err)
	}
	references, err := store.ResolveDashboardGraphReferences(ctx, tenant, []ID{"graph_dash_01", "graph_missing"})
	if err != nil || len(references) != 2 || !references[0].Exists || references[0].Graph == nil ||
		len(references[0].Series) != 1 || references[1].Exists {
		t.Fatalf("references = %+v err=%v", references, err)
	}
	options, optionTotal, err := store.ListDashboardGraphOptions(ctx, tenant, DashboardGraphOptionListFilter{Search: "traffic", Limit: 1})
	if err != nil || optionTotal != 1 || len(options) != 1 || options[0].ID != graph.ID {
		t.Fatalf("graph options = %+v total=%d err=%v", options, optionTotal, err)
	}

	// Duplicate name in the same tenant is a conflict.
	if _, err := store.CreateDashboard(ctx, Dashboard{
		TenantID: tenant, Name: "Ops Overview", Layout: json.RawMessage(`{"panels":[]}`),
	}); !errors.Is(err, ErrDashboardNameConflict) {
		t.Fatalf("duplicate name = %v, want ErrDashboardNameConflict", err)
	}

	// Update bumps the version and replaces layout/name.
	updated, err := store.UpdateDashboard(ctx, Dashboard{
		TenantID: tenant, ID: created.ID, Name: "Ops v2",
		Layout: json.RawMessage(`{"panels":[{"graph_id":"g1"},{"graph_id":"g2"}]}`),
	}, created.Version)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Name != "Ops v2" {
		t.Fatalf("updated = %+v", updated)
	}

	// Updating a missing dashboard is a not-found.
	if _, err := store.UpdateDashboard(ctx, Dashboard{
		TenantID: tenant, ID: "dash_missing", Name: "x", Layout: json.RawMessage(`{"panels":[]}`),
	}, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("update missing = %v, want ErrNoRows", err)
	}
	if _, err := store.UpdateDashboard(ctx, Dashboard{
		TenantID: tenant, ID: created.ID, Name: "stale", Layout: json.RawMessage(`{"panels":[]}`),
	}, created.Version); !errors.Is(err, ErrDashboardVersionConflict) {
		t.Fatalf("stale update = %v, want ErrDashboardVersionConflict", err)
	}

	// A different tenant cannot see or fetch the dashboard.
	if _, err := store.GetDashboard(ctx, "tenant_other_dash", created.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-tenant get = %v, want ErrNoRows", err)
	}
	if other, total, err := store.ListDashboards(ctx, "tenant_other_dash", DashboardListFilter{}); err != nil || total != 0 || len(other) != 0 {
		t.Fatalf("cross-tenant list = %+v err=%v", other, err)
	}

	// Deleting the owner nulls owner_id (SET NULL), the dashboard survives.
	if _, err := db.ExecContext(ctx, "DELETE FROM users WHERE id = 'user_dash_owner'"); err != nil {
		t.Fatal(err)
	}
	afterOwnerDelete, err := store.GetDashboard(ctx, tenant, created.ID)
	if err != nil || afterOwnerDelete.OwnerID != "" {
		t.Fatalf("after owner delete = %+v err=%v", afterOwnerDelete, err)
	}

	// Delete is idempotent-ish: first removes, second is not-found.
	if err := store.DeleteDashboard(ctx, tenant, created.ID, updated.Version); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteDashboard(ctx, tenant, created.ID, updated.Version); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second delete = %v, want ErrNoRows", err)
	}
}
