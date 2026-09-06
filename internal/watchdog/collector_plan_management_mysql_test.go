package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestMySQLCollectorPlanManagementCreateCloneListActivate(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_PLAN_MANAGEMENT_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_PLAN_MANAGEMENT_MYSQL_TEST_DSN to an isolated database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(20)
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"collector_plan_signing_keys", "collector_plan_trust_state"} {
		var count int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s is not empty; plan management test requires an isolated database", table)
		}
	}

	ctx := context.Background()
	tenantID := ID("tenant_plan_manage_00001")
	userID := ID("user_plan_manage_0000001")
	collectorID := ID("collector_plan_manage_001")
	_, _ = db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenantID)
	defer func() { _, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenantID) }()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Plan Management Test', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'plan-management@watchdog.local', 'Plan Management', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, plan_schema_min, plan_schema_max,
			created_by, updated_by
		) VALUES (?, ?, 'flow', 'management-test', 'flow-collect', 'listen',
			'pending', 'unknown', 'token', 'test-token-hash', 1, 2, ?, ?)
	`, collectorID, tenantID, userID, userID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	signer := newCollectorPlanSignerForTest(t, store, "plan-management-key")
	if _, err := store.ActivateCollectorPlanSigningKey(ctx, signer.KeyID(), signer.PublicKey(), time.Now().UTC().Truncate(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	service, err := NewCollectorPlanManagementService(store, signer)
	if err != nil {
		t.Fatal(err)
	}

	expiresAt := time.Now().UTC().Add(6 * time.Hour).Truncate(time.Millisecond)
	request := CollectorPlanCreateRequest{
		TenantID: tenantID, CollectorID: collectorID, ActorID: userID,
		PlanSchemaVersion: 1, SpecJSON: []byte(` { "kafka": {"topic":"flows"}, "schema_version": 1 } `),
		ExpiresAt: expiresAt,
	}
	const concurrentCreates = 8
	versions := make(chan uint64, concurrentCreates)
	errorsSeen := make(chan error, concurrentCreates)
	var group sync.WaitGroup
	for range concurrentCreates {
		group.Add(1)
		go func() {
			defer group.Done()
			created, err := service.CreateCollectorPlanRevision(ctx, request)
			if err != nil {
				errorsSeen <- err
				return
			}
			versions <- created.ConfigVersion
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("concurrent create: %v", err)
	}
	close(versions)
	gotVersions := make([]int, 0, concurrentCreates)
	for version := range versions {
		gotVersions = append(gotVersions, int(version))
	}
	sort.Ints(gotVersions)
	for i, version := range gotVersions {
		if version != i+1 {
			t.Fatalf("versions=%v", gotVersions)
		}
	}

	first, cursor, err := service.ListCollectorPlanRevisions(ctx, tenantID, collectorID, CollectorPlanPageFilter{Limit: 3})
	if err != nil || len(first) != 3 || cursor == "" || first[0].ConfigVersion != 8 || first[2].ConfigVersion != 6 {
		t.Fatalf("first page=%+v cursor=%q err=%v", first, cursor, err)
	}
	before, err := decodeCollectorPlanCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := service.ListCollectorPlanRevisions(ctx, tenantID, collectorID, CollectorPlanPageFilter{Limit: 3, BeforeVersion: before})
	if err != nil || len(second) != 3 || second[0].ConfigVersion != 5 || second[2].ConfigVersion != 3 {
		t.Fatalf("second page=%+v err=%v", second, err)
	}

	clone, err := service.CreateCollectorPlanRevision(ctx, CollectorPlanCreateRequest{
		TenantID: tenantID, CollectorID: collectorID, ActorID: userID,
		FromConfigVersion: 1, ExpiresAt: expiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := store.GetCollectorPlanRevision(ctx, tenantID, collectorID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if clone.ConfigVersion != 9 || clone.SpecHash != source.SpecHash || clone.PlanSchemaVersion != source.PlanSchemaVersion || clone.ID == source.ID || clone.SigningKeyID != signer.KeyID() {
		t.Fatalf("source=%+v clone=%+v", source, clone)
	}
	active, err := service.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: clone.ConfigVersion,
		ExpectedCollectorRowVersion: 1, ExpectedPlanRowVersion: clone.RowVersion,
		ActorID: userID,
	})
	if err != nil || active.Status != CollectorPlanActive || active.RowVersion != 2 {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if _, err := service.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: clone.ConfigVersion,
		ExpectedCollectorRowVersion: 1, ExpectedPlanRowVersion: clone.RowVersion,
		ActorID: userID,
	}); !errors.Is(err, ErrCollectorPlanConflict) {
		t.Fatalf("stale activation error=%v", err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs
		WHERE tenant_id = ? AND resource_type = 'collector_plan'
	`, tenantID).Scan(&auditCount); err != nil || auditCount != concurrentCreates+2 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}
}
