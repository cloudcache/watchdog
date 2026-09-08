package watchdog

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestOperationJobScopeValidation covers the scope/tenant correspondence that
// EnqueueOperationJob enforces before touching the database.
func TestOperationJobScopeValidation(t *testing.T) {
	store := &MySQLStore{}
	ctx := context.Background()
	hash := strings.Repeat("a", 64)

	if _, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: OperationJobScopeSystem, TenantID: "tenant-x", JobType: "t", IdempotencyKey: "k", RequestHash: hash,
	}); err == nil {
		t.Fatal("system-scope job with a tenant must be rejected")
	}
	if _, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: OperationJobScopeTenant, JobType: "t", IdempotencyKey: "k", RequestHash: hash,
	}); err == nil {
		t.Fatal("tenant-scope job without a tenant must be rejected")
	}
	if _, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: "galaxy", JobType: "t", IdempotencyKey: "k", RequestHash: hash,
	}); err == nil {
		t.Fatal("unknown scope must be rejected")
	}
}

// TestMySQLOperationJobSystemScope proves a system-scope job has no tenant,
// dedupes in its own idempotency domain (never colliding with a tenant's same
// key), stays out of tenant listings, and runs through the shared lease/finish
// state machine unchanged.
func TestMySQLOperationJobSystemScope(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	hash := strings.Repeat("a", 64)

	// A system job carries no tenant.
	sys, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: OperationJobScopeSystem, JobType: "sys_scope_job", IdempotencyKey: "k1", RequestHash: hash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sys.ScopeType != OperationJobScopeSystem || sys.TenantID != "" {
		t.Fatalf("system job = %+v", sys)
	}

	// Re-enqueuing the same (type, key) dedupes to the same row.
	again, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: OperationJobScopeSystem, JobType: "sys_scope_job", IdempotencyKey: "k1", RequestHash: hash,
	})
	if err != nil || again.ID != sys.ID {
		t.Fatalf("system dedup: again=%+v err=%v", again, err)
	}

	// A tenant job with the SAME (type, key) is a distinct row: the generated
	// idempotency_domain separates the tenant namespace from '__system__'.
	tj, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenant, JobType: "sys_scope_job", IdempotencyKey: "k1", RequestHash: hash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tj.ID == sys.ID {
		t.Fatal("tenant and system jobs with the same key must not collide")
	}
	if tj.ScopeType != OperationJobScopeTenant || tj.TenantID != tenant {
		t.Fatalf("tenant job = %+v", tj)
	}

	// A different request hash on the same system key is a conflict — proving
	// the NULL-safe re-select found the existing system row.
	if _, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: OperationJobScopeSystem, JobType: "sys_scope_job", IdempotencyKey: "k1", RequestHash: strings.Repeat("b", 64),
	}); err != ErrOperationJobHashMismatch {
		t.Fatalf("system hash mismatch = %v, want ErrOperationJobHashMismatch", err)
	}

	// Tenant listing never leaks the system job.
	listed, _, err := store.ListOperationJobs(ctx, tenant, OperationJobFilter{JobType: "sys_scope_job"})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range listed {
		if j.ID == sys.ID {
			t.Fatal("tenant listing leaked a system job")
		}
	}
	if len(listed) != 1 || listed[0].ID != tj.ID {
		t.Fatalf("tenant listing = %+v", listed)
	}

	// A tenant admin must not reach a system job through the tenant-scoped API
	// surface either. Get-by-id and cancel are the only vectors the operation-job
	// HTTP handlers expose, and both filter on tenant_id, so a real tenant can
	// never resolve or cancel a system job (tenant_id=''). This is the guard that
	// lets PLAT-04F2 (system job management API) stay deferred until a platform
	// admin role exists: no tenant admin may impersonate a global admin meanwhile.
	if _, err := store.GetOperationJob(ctx, tenant, sys.ID); err == nil {
		t.Fatal("tenant-scoped get must not resolve a system job")
	}
	if err := store.RequestOperationJobCancel(ctx, tenant, sys.ID); err == nil {
		t.Fatal("tenant-scoped cancel must not reach a system job")
	}
	// The tenant cancel attempt left the system job untouched.
	if unchanged, err := store.GetSystemOperationJob(ctx, sys.ID); err != nil || unchanged.Status != OperationJobStatusQueued {
		t.Fatalf("system job after tenant cancel = %+v err=%v", unchanged, err)
	}

	// GetSystemOperationJob resolves system jobs only.
	got, err := store.GetSystemOperationJob(ctx, sys.ID)
	if err != nil || got.ID != sys.ID {
		t.Fatalf("get system = %+v err=%v", got, err)
	}
	if _, err := store.GetSystemOperationJob(ctx, tj.ID); err == nil {
		t.Fatal("GetSystemOperationJob must not resolve a tenant job")
	}

	// The shared state machine: a system job leases, heartbeats and finishes on
	// the same path as a tenant job. Use a dedicated type to lease it precisely.
	sys2, err := store.EnqueueOperationJob(ctx, OperationJob{
		ScopeType: OperationJobScopeSystem, JobType: "sys_only_job", IdempotencyKey: "k2", RequestHash: hash,
	})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := store.LeaseNextOperationJob(ctx, "sys_only_job", "worker-sys", time.Minute)
	if err != nil || leased.ID != sys2.ID || leased.ScopeType != OperationJobScopeSystem || leased.TenantID != "" {
		t.Fatalf("lease system = %+v err=%v", leased, err)
	}
	if _, err := store.HeartbeatOperationJob(ctx, leased.ID, leased.LeaseToken, time.Minute, 1, nil); err != nil {
		t.Fatalf("heartbeat system: %v", err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, leased.ID, leased.LeaseToken, "done"); err != nil {
		t.Fatalf("complete system: %v", err)
	}
	final, err := store.GetSystemOperationJob(ctx, sys2.ID)
	if err != nil || final.Status != OperationJobStatusSucceeded {
		t.Fatalf("final system job = %+v err=%v", final, err)
	}

	// Clean the system rows the shared test tenant's cleanup cannot cascade
	// (they have no tenant), so the reused 'OpJob' tenant name is freed.
	if _, err := db.ExecContext(ctx, "DELETE FROM operation_jobs WHERE scope_type = 'system' AND job_type IN ('sys_scope_job','sys_only_job')"); err != nil {
		t.Fatal(err)
	}
}
