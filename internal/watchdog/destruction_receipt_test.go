package watchdog

import (
	"context"
	"strings"
	"testing"
)

// TestMySQLDestructionReceiptIsIdempotentAndQueryable proves the target-delete
// handler records exactly one destruction receipt even across a retry/takeover
// (deterministic id), that the receipt is found through the audit-log read
// surface, and that a non-user actor is preserved in detail.actor.
func TestMySQLDestructionReceiptIsIdempotentAndQueryable(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	// A user actor so the FK path is exercised, plus a target the handler deletes.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_destroy_01', ?, 'd@test.local', 'D', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES ('target_destroy_01', ?, 'Destroy', 'network', '192.0.2.201', 'pending')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	handler := NewTargetDeleteJobHandler(store, nil, store)
	payload, err := EncodeTargetDeletePayload("target_destroy_01", map[string]int{"network_device": 2, "network_port": 40})
	if err != nil {
		t.Fatal(err)
	}
	job := OperationJob{
		ID: "job_destroy_01", TenantID: tenant, JobType: TargetDeleteJobType,
		CreatedBy: "user_destroy_01", CheckpointJSON: payload,
	}

	// Run the handler twice (simulating a retry/takeover): the target delete is
	// idempotent and the receipt must not duplicate.
	if _, err := handler(ctx, job); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if _, err := handler(ctx, job); err != nil {
		t.Fatalf("second run: %v", err)
	}

	var receiptCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND action = 'target.destroyed' AND resource_id = 'target_destroy_01'
	`, tenant).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("destruction receipt count = %d, err = %v (want exactly 1)", receiptCount, err)
	}

	// Queryable through the audit-log read surface with the impact and series
	// matcher preserved.
	logs, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{ResourceType: "target", Action: "target.destroyed"})
	if err != nil || len(logs) != 1 {
		t.Fatalf("audit list = %d err=%v", len(logs), err)
	}
	receipt := logs[0]
	if receipt.ActorID != "user_destroy_01" {
		t.Fatalf("receipt actor = %q", receipt.ActorID)
	}
	if match, _ := receipt.Detail["series_match"].(string); !strings.Contains(match, "target_destroy_01") {
		t.Fatalf("series match missing: %+v", receipt.Detail)
	}
	impact, ok := receipt.Detail["impact"].(map[string]any)
	if !ok || impact["network_port"] != float64(40) {
		t.Fatalf("impact not recorded: %+v", receipt.Detail)
	}

	// A system (non-user) actor is preserved in detail, not dropped by the FK.
	sysJob := OperationJob{
		ID: "job_destroy_sys", TenantID: tenant, JobType: TargetDeleteJobType,
		CreatedBy: "system:reaper", CheckpointJSON: payload,
	}
	if _, err := handler(ctx, sysJob); err != nil {
		t.Fatalf("system-actor run: %v", err)
	}
	sysLogs, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{ActorID: ""})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, log := range sysLogs {
		if log.ID == stableID("destroy", "job_destroy_sys") {
			if actor, _ := log.Detail["actor"].(string); actor == "system:reaper" && log.ActorID == "" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("system actor receipt not preserved in detail.actor")
	}
}
