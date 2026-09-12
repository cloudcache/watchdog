package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestMySQLCollectorDeletePreviewBlockingAndAsync proves the collector delete
// contract: a collector with ownership evidence is not deletable (preview +
// 409 + terminal job), while a clean collector deletes asynchronously with its
// bindings/plan revisions cascading and a receipt recorded.
func TestMySQLCollectorDeletePreviewBlockingAndAsync(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_admin', ?, 'admin@coldel.local', 'Admin', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	// The seeded principal's created_by -> users is RESTRICT; clear it before
	// the tenant-cascade cleanup (LIFO: this runs before operationJobTestDB's).
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM collector_service_principals WHERE tenant_id = ?", tenant)
	})

	// Blocked collector: has a service principal (RESTRICT evidence).
	blocked := ID("collector_del_blocked")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (id, tenant_id, module_key, name, agent_type, mode, status, observed_health, auth_type, token_hash, created_by, updated_by)
		VALUES (?, ?, 'flow', 'blocked', 'flow_collect', 'listen', 'active', 'unknown', 'token', 'tok', 'user_admin', 'user_admin')
	`, blocked, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_service_principals (
			id, tenant_id, collector_id, service_type, principal_ref, credential_secret_ref, provider,
			grant_operation_key, grant_request_hash, grant_receipt_ref, grant_receipt_sha256, acl_propagation_delay_ms, created_by, updated_by
		) VALUES ('sp_blk_1', ?, ?, 'kafka', 'User:blk', 'secret://blk', 'kafka-admin', ?, ?, 'receipt://blk', ?, 2000, 'user_admin', 'user_admin')
	`, tenant, blocked, strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}

	previewBlocked, err := store.PreviewCollectorDelete(ctx, tenant, blocked)
	if err != nil {
		t.Fatal(err)
	}
	if previewBlocked.Deletable {
		t.Fatalf("collector with a principal must not be deletable: %+v", previewBlocked)
	}
	if err := store.DeleteCollector(ctx, tenant, blocked); !errors.Is(err, ErrCollectorDeleteBlocked) {
		t.Fatalf("blocked delete error = %v", err)
	}

	// Clean collector: bindings + a plan revision cascade, no evidence.
	clean := ID("collector_del_clean")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (id, tenant_id, module_key, name, agent_type, mode, status, observed_health, auth_type, token_hash, created_by, updated_by)
		VALUES (?, ?, 'flow', 'clean', 'flow_collect', 'listen', 'active', 'unknown', 'token', 'tok', 'user_admin', 'user_admin')
	`, clean, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_bindings (collector_id, tenant_id, resource_type, resource_id, binding_role, created_by, updated_by)
		VALUES (?, ?, 'target', 'some_target', 'collect', 'user_admin', 'user_admin')
	`, clean, tenant); err != nil {
		t.Fatal(err)
	}

	previewClean, err := store.PreviewCollectorDelete(ctx, tenant, clean)
	if err != nil {
		t.Fatal(err)
	}
	if !previewClean.Deletable {
		t.Fatalf("clean collector must be deletable: %+v", previewClean)
	}
	bindingImpact := map[string]TargetDeleteImpact{}
	for _, impact := range previewClean.Impacts {
		bindingImpact[impact.ResourceType] = impact
	}
	if bindingImpact["collector_binding"].Count != 1 || bindingImpact["collector_binding"].Behavior != "deleted" {
		t.Fatalf("binding impact = %+v", bindingImpact["collector_binding"])
	}

	// Async delete through the API + worker.
	worker := &OperationJobWorker{
		Repo: store, JobType: CollectorDeleteJobType, Owner: "worker-coldel",
		PollInterval: 50 * time.Millisecond, LeaseFor: 3 * time.Second,
		Handler: NewCollectorDeleteJobHandler(store, store),
	}
	go worker.Run(ctx)

	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: tenant, UserID: "", IsAdmin: true}, nil
		},
		CollectorDeletePreview: store, OperationJobs: store,
	})

	// Blocked collector delete via API is 409.
	blockedRec := httptest.NewRecorder()
	router.ServeHTTP(blockedRec, httptest.NewRequest(http.MethodDelete, "/api/v1/collectors/"+string(blocked), nil))
	if blockedRec.Code != http.StatusConflict {
		t.Fatalf("blocked delete status = %d, body = %s", blockedRec.Code, blockedRec.Body.String())
	}

	// Clean collector delete is 202 + job.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/collectors/"+string(clean), nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("clean delete status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var accepted struct {
		JobID ID `json:"job_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil || accepted.JobID == "" {
		t.Fatalf("delete response = %s err=%v", rec.Body.String(), err)
	}
	waitForOperationJob(t, store, tenant, accepted.JobID, OperationJobStatusSucceeded)

	for query, want := range map[string]int{
		"SELECT COUNT(*) FROM collector_agents WHERE id = 'collector_del_clean'":             0,
		"SELECT COUNT(*) FROM collector_bindings WHERE collector_id = 'collector_del_clean'": 0,
		"SELECT COUNT(*) FROM collector_agents WHERE id = 'collector_del_blocked'":           1,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if count != want {
			t.Fatalf("%s = %d, want %d", query, count, want)
		}
	}

	logs, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{ResourceType: "collector", Action: "collector.destroyed"})
	if err != nil || len(logs) != 1 {
		t.Fatalf("collector destruction receipt = %d err=%v", len(logs), err)
	}
}
