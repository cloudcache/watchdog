package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNormalizeFlowStoragePolicy(t *testing.T) {
	valid := FlowStoragePolicy{
		TenantID: "tenant-a", BootstrapFrom: time.Date(2026, 9, 7, 12, 34, 0, 0, time.FixedZone("x", 8*3600)),
		RawRetentionSeconds: 7 * 86400, ArchiveRetentionSeconds: 365 * 86400,
		LateArrivalSeconds: 600, DeleteGraceSeconds: 86400, MaxPartitionsPerRun: 7,
	}
	got, err := normalizeFlowStoragePolicy(valid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != FlowStoragePolicyDraft || got.DownsampleResolutionSeconds != 3600 ||
		!got.BootstrapFrom.Equal(time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("normalized policy = %#v", got)
	}
	for name, mutate := range map[string]func(*FlowStoragePolicy){
		"raw too short":      func(item *FlowStoragePolicy) { item.RawRetentionSeconds = 3600 },
		"archive before raw": func(item *FlowStoragePolicy) { item.ArchiveRetentionSeconds = item.RawRetentionSeconds },
		"wrong resolution":   func(item *FlowStoragePolicy) { item.DownsampleResolutionSeconds = 60 },
		"unbounded scan":     func(item *FlowStoragePolicy) { item.MaxPartitionsPerRun = 0 },
		"published mutation": func(item *FlowStoragePolicy) { item.Status = FlowStoragePolicyPublished },
	} {
		t.Run(name, func(t *testing.T) {
			item := got
			mutate(&item)
			if _, err := normalizeFlowStoragePolicy(item); !errors.Is(err, ErrFlowStorageInvalid) {
				t.Fatalf("error = %v, want invalid", err)
			}
		})
	}
}

type fakeFlowStorageLifecycleRepository struct {
	items []FlowStoragePolicy
}

func (repo *fakeFlowStorageLifecycleRepository) ListFlowStoragePolicies(context.Context, ID) ([]FlowStoragePolicy, error) {
	return append([]FlowStoragePolicy(nil), repo.items...), nil
}
func (repo *fakeFlowStorageLifecycleRepository) GetFlowStoragePolicy(_ context.Context, tenantID, policyID ID) (FlowStoragePolicy, error) {
	for _, item := range repo.items {
		if item.TenantID == tenantID && item.ID == policyID {
			return item, nil
		}
	}
	return FlowStoragePolicy{}, sql.ErrNoRows
}
func (repo *fakeFlowStorageLifecycleRepository) CreateFlowStoragePolicyDraft(_ context.Context, item FlowStoragePolicy) (FlowStoragePolicy, error) {
	if item.RawDeleteEnabled {
		return FlowStoragePolicy{}, ErrFlowStorageRawDeleteLocked
	}
	item.ID, item.PolicyVersion, item.RowVersion = "policy-a", 1, 1
	repo.items = append(repo.items, item)
	return item, nil
}
func (repo *fakeFlowStorageLifecycleRepository) UpdateFlowStoragePolicyDraft(_ context.Context, item FlowStoragePolicy, expected uint64) (FlowStoragePolicy, error) {
	if expected != item.RowVersion {
		return FlowStoragePolicy{}, ErrFlowStorageVersionConflict
	}
	item.RowVersion++
	repo.items[0] = item
	return item, nil
}
func (repo *fakeFlowStorageLifecycleRepository) DeleteFlowStoragePolicyDraft(context.Context, ID, ID, uint64) error {
	return nil
}
func (repo *fakeFlowStorageLifecycleRepository) PublishFlowStoragePolicy(_ context.Context, tenantID, policyID, actor ID, expected uint64, now time.Time) (FlowStoragePolicy, error) {
	item, err := repo.GetFlowStoragePolicy(context.Background(), tenantID, policyID)
	if err != nil {
		return FlowStoragePolicy{}, err
	}
	if expected != item.RowVersion {
		return FlowStoragePolicy{}, ErrFlowStorageVersionConflict
	}
	item.Status, item.PublishedBy, item.PublishedAt, item.RowVersion = FlowStoragePolicyPublished, actor, now, item.RowVersion+1
	repo.items[0] = item
	return item, nil
}
func (repo *fakeFlowStorageLifecycleRepository) ListFlowStoragePartitions(context.Context, ID, FlowStoragePartitionFilter) ([]FlowStoragePartitionState, int, error) {
	return nil, 0, nil
}

func TestFlowStorageLifecycleAPI(t *testing.T) {
	repo := &fakeFlowStorageLifecycleRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressDimensionTestAuth, FlowStorage: repo})
	body := `{"bootstrap_from":"2026-09-01T00:00:00Z","raw_retention_seconds":604800,"downsample_resolution_seconds":3600,"archive_retention_seconds":31536000,"late_arrival_seconds":600,"delete_grace_seconds":86400,"max_partitions_per_run":7,"raw_delete_enabled":false}`
	create := httptest.NewRecorder()
	router.ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/api/v1/flow/storage/policies", strings.NewReader(body)))
	if create.Code != http.StatusCreated || create.Header().Get("ETag") != `"1"` {
		t.Fatalf("create = %d %s, ETag=%q", create.Code, create.Body.String(), create.Header().Get("ETag"))
	}
	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, httptest.NewRequest(http.MethodPatch, "/api/v1/flow/storage/policies/policy-a", strings.NewReader(`{"late_arrival_seconds":900}`)))
	if missing.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match = %d %s", missing.Code, missing.Body.String())
	}
	patch := httptest.NewRequest(http.MethodPatch, "/api/v1/flow/storage/policies/policy-a", strings.NewReader(`{"late_arrival_seconds":900}`))
	patch.Header.Set("If-Match", `"1"`)
	updated := httptest.NewRecorder()
	router.ServeHTTP(updated, patch)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("patch = %d %s, ETag=%q", updated.Code, updated.Body.String(), updated.Header().Get("ETag"))
	}
	publish := httptest.NewRequest(http.MethodPost, "/api/v1/flow/storage/policies/policy-a/actions/publish", nil)
	publish.Header.Set("If-Match", `"2"`)
	published := httptest.NewRecorder()
	router.ServeHTTP(published, publish)
	if published.Code != http.StatusOK || !strings.Contains(published.Body.String(), `"status":"published"`) {
		t.Fatalf("publish = %d %s", published.Code, published.Body.String())
	}
	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/flow/storage/partitions?limit=20&offset=0&state=reconciled", nil))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"total":0`) {
		t.Fatalf("partition list = %d %s", list.Code, list.Body.String())
	}
}

func TestMySQLFlowStoragePolicyLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}
	schema := "watchdog_flow_storage_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	const tenantID, actorID = ID("tenant_flow_storage"), ID("user_flow_storage")
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Flow Storage', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'flow-storage@watchdog.local', 'Flow Storage', 'active')", actorID, tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	base := FlowStoragePolicy{TenantID: tenantID, BootstrapFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), RawRetentionSeconds: 7 * 86400,
		ArchiveRetentionSeconds: 365 * 86400, LateArrivalSeconds: 600, DeleteGraceSeconds: 86400, MaxPartitionsPerRun: 7, CreatedBy: actorID}
	first, err := store.CreateFlowStoragePolicyDraft(ctx, base)
	if err != nil || first.PolicyVersion != 1 || first.RowVersion != 1 {
		t.Fatalf("create first = %#v, %v", first, err)
	}
	first.LateArrivalSeconds = 900
	first, err = store.UpdateFlowStoragePolicyDraft(ctx, first, 1)
	if err != nil || first.RowVersion != 2 {
		t.Fatalf("update = %#v, %v", first, err)
	}
	if _, err := store.UpdateFlowStoragePolicyDraft(ctx, first, 1); !errors.Is(err, ErrFlowStorageVersionConflict) {
		t.Fatalf("stale update = %v", err)
	}
	first, err = store.PublishFlowStoragePolicy(ctx, tenantID, first.ID, actorID, 2, time.Now())
	if err != nil || first.Status != FlowStoragePolicyPublished {
		t.Fatalf("publish first = %#v, %v", first, err)
	}
	second, err := store.CreateFlowStoragePolicyDraft(ctx, base)
	if err != nil || second.PolicyVersion != 2 {
		t.Fatalf("create second = %#v, %v", second, err)
	}
	second, err = store.PublishFlowStoragePolicy(ctx, tenantID, second.ID, actorID, 1, time.Now())
	if err != nil || second.Status != FlowStoragePolicyPublished {
		t.Fatalf("publish second = %#v, %v", second, err)
	}
	first, err = store.GetFlowStoragePolicy(ctx, tenantID, first.ID)
	if err != nil || first.Status != FlowStoragePolicyRetired || first.RetiredAt.IsZero() {
		t.Fatalf("retired first = %#v, %v", first, err)
	}
	job, err := store.EnqueueOperationJob(ctx, OperationJob{TenantID: tenantID, JobType: FlowStorageDownsampleJobType,
		IdempotencyKey: "state-test", RequestHash: strings.Repeat("a", 64), CreatedBy: actorID})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	generation, _ := flowStorageGeneration(second.PolicyVersion, 1)
	state, err := store.BeginFlowStoragePartition(ctx, second, day, generation, job.ID)
	if err != nil || state.State != FlowStoragePartitionSealed || state.DownsampleJobID != job.ID {
		t.Fatalf("begin partition = %#v, %v", state, err)
	}
	state, err = store.MarkFlowStoragePartitionDownsampleWritten(ctx, second, day, generation, job.ID, time.Now())
	if err != nil || state.State != FlowStoragePartitionDownsampleWritten || state.DownsampledAt.IsZero() {
		t.Fatalf("mark partition = %#v, %v", state, err)
	}
	counters := FlowStoragePartitionCounters{RecordCount: 2, RawBytes: 10, RawPackets: 2, EstimatedBytes: 100, EstimatedPackets: 20, EstimatedValidRecords: 2}
	state, err = store.CompleteFlowStoragePartition(ctx, second, day, generation, job.ID, counters, counters, time.Now())
	if err != nil || state.State != FlowStoragePartitionReconciled || state.Source == nil || *state.Source != counters || state.ReconciledAt.IsZero() {
		t.Fatalf("complete partition = %#v, %v", state, err)
	}
	items, total, err := store.ListFlowStoragePartitions(ctx, tenantID, FlowStoragePartitionFilter{State: FlowStoragePartitionReconciled, Limit: 10})
	if err != nil || total != 1 || len(items) != 1 || items[0].SourceDate != day {
		t.Fatalf("list partitions = %#v total=%d err=%v", items, total, err)
	}
	boundary, err := store.FlowStorageArchiveThrough(ctx, tenantID, day.Add(12*time.Hour), day.Add(48*time.Hour))
	if err != nil || !boundary.Equal(day.Add(24*time.Hour)) {
		t.Fatalf("archive boundary=%s err=%v", boundary, err)
	}
	orphanDay := day.Add(24 * time.Hour)
	if _, err := store.BeginFlowStoragePartition(ctx, second, orphanDay, generation, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM operation_jobs WHERE id = ?", job.ID); err != nil {
		t.Fatal(err)
	}
	repairs, err := store.ListFlowStorageRepairCandidates(ctx, second, 10)
	if err != nil || len(repairs) != 1 || !repairs[0].SourceDate.Equal(orphanDay) {
		t.Fatalf("orphan repair candidates=%#v err=%v", repairs, err)
	}
}
