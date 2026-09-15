package watchdog

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

// TestMySQLAuditLogWriteAndList proves the write fixes (auto id, non-user
// actor preserved in detail instead of vanishing on the FK) and keyset
// pagination with filters.
func TestMySQLAuditLogWriteAndList(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run audit log integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	tenant := ID("tenant_audit_test_001")
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenant); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenant)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Audit', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_audit_admin_01', ?, 'audit@test.local', 'Audit Admin', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	base := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	// No id, real user actor: previously rejected outright.
	if err := store.CreateAuditLog(ctx, AuditLog{
		TenantID: tenant, ActorID: "user_audit_admin_01", Action: "user.create",
		ResourceType: "user", ResourceID: "u-1", CreatedAt: base,
	}); err != nil {
		t.Fatalf("auto-id audit write: %v", err)
	}
	// System actor not in users: previously an FK violation dropped the row.
	if err := store.CreateAuditLog(ctx, AuditLog{
		TenantID: tenant, ActorID: "system:enrollment", Action: "collector.enroll",
		ResourceType: "collector", ResourceID: "c-1", CreatedAt: base.Add(time.Second),
	}); err != nil {
		t.Fatalf("system actor audit write: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := store.CreateAuditLog(ctx, AuditLog{
			TenantID: tenant, ActorID: "user_audit_admin_01", Action: "role.update",
			ResourceType: "role", ResourceID: ID("r-" + string(rune('1'+i))),
			CreatedAt: base.Add(time.Duration(2+i) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Newest first, keyset cursor walks the rest without overlap.
	page1, cursor, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{Limit: 4})
	if err != nil || len(page1) != 4 || cursor == "" {
		t.Fatalf("page1 = %d rows cursor=%q err=%v", len(page1), cursor, err)
	}
	if page1[0].Action != "role.update" || !page1[0].CreatedAt.After(page1[3].CreatedAt) {
		t.Fatalf("page1 order = %+v", page1[0])
	}
	page2, cursor2, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{Limit: 4, Cursor: cursor})
	if err != nil || len(page2) != 3 || cursor2 != "" {
		t.Fatalf("page2 = %d rows cursor=%q err=%v", len(page2), cursor2, err)
	}
	seen := map[ID]bool{}
	for _, log := range append(append([]AuditLog{}, page1...), page2...) {
		if seen[log.ID] {
			t.Fatalf("cursor pages overlap on %s", log.ID)
		}
		seen[log.ID] = true
	}

	// Filters: action prefix and the preserved system actor detail.
	enrolls, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{Action: "collector."})
	if err != nil || len(enrolls) != 1 {
		t.Fatalf("action filter = %d rows err=%v", len(enrolls), err)
	}
	if enrolls[0].ActorID != "" || enrolls[0].Detail["actor"] != "system:enrollment" {
		t.Fatalf("system actor not preserved: %+v", enrolls[0])
	}
	users, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{ResourceType: "user"})
	if err != nil || len(users) != 1 || users[0].ResourceID != "u-1" {
		t.Fatalf("resource filter = %+v err=%v", users, err)
	}
}
