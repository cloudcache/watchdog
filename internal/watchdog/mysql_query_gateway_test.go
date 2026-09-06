package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestMySQLQueryDatasetPolicyCRUDCASAndLifecycle(t *testing.T) {
	db, tenantID := operationJobTestDB(t)
	ctx := context.Background()
	store := NewMySQLStore(db)
	runEmbeddedMigrationAgain(t, db, "045")

	actorID := ID("user_query_policy_045")
	_, _ = db.ExecContext(ctx, "DELETE FROM users WHERE id = ?", actorID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES (?, ?, 'query-policy-045@watchdog.local', 'Query Policy', 'active')
	`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}
	policy := QueryDatasetPolicy{
		TenantID: tenantID, DatasetKey: "flow.traffic", Enabled: true,
		AllowRaw: true, AllowSupplier: true, AllowCustomer: true,
		MaxRangeSeconds: 604_800, MaxConcurrent: 8, MaxResultRows: 500_000,
		QueryTimeoutMS: 60_000, UpdatedBy: actorID,
	}
	created, err := store.PutQueryDatasetPolicy(ctx, policy, 0)
	if err != nil {
		t.Fatal(err)
	}
	if created.RowVersion != 1 || created.UpdatedBy != actorID {
		t.Fatalf("created policy = %#v", created)
	}
	if _, err := store.PutQueryDatasetPolicy(ctx, policy, 0); !errors.Is(err, ErrQueryDatasetPolicyConflict) {
		t.Fatalf("duplicate create error = %v", err)
	}

	policy.MaxConcurrent = 5
	updated, err := store.PutQueryDatasetPolicy(ctx, policy, created.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if updated.RowVersion != 2 || updated.MaxConcurrent != 5 {
		t.Fatalf("updated policy = %#v", updated)
	}
	if _, err := store.PutQueryDatasetPolicy(ctx, policy, created.RowVersion); !errors.Is(err, ErrQueryDatasetPolicyConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	items, err := store.ListQueryDatasetPolicies(ctx, tenantID)
	if err != nil || len(items) != 1 || items[0].DatasetKey != policy.DatasetKey {
		t.Fatalf("items=%#v err=%v", items, err)
	}

	if _, err := db.ExecContext(ctx, "DELETE FROM users WHERE id = ?", actorID); err != nil {
		t.Fatal(err)
	}
	afterActorDelete, err := store.GetQueryDatasetPolicy(ctx, tenantID, policy.DatasetKey)
	if err != nil || afterActorDelete.UpdatedBy != "" {
		t.Fatalf("after actor delete=%#v err=%v", afterActorDelete, err)
	}
	if err := store.DeleteQueryDatasetPolicy(ctx, tenantID, policy.DatasetKey, 1); !errors.Is(err, ErrQueryDatasetPolicyConflict) {
		t.Fatalf("stale delete error = %v", err)
	}
	if err := store.DeleteQueryDatasetPolicy(ctx, tenantID, policy.DatasetKey, updated.RowVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetQueryDatasetPolicy(ctx, tenantID, policy.DatasetKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted policy error = %v", err)
	}

	policy.UpdatedBy = ""
	if _, err := store.PutQueryDatasetPolicy(ctx, policy, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenantID); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM query_dataset_policies WHERE tenant_id = ?", tenantID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("retained policies=%d err=%v", retained, err)
	}
}
