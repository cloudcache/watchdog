package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeTargetRepository struct {
	targets []Target
	created Target
	updated Target
	deleted ID
	// captured by ListTargetsPage for assertions
	pagedAll     bool
	pagedAllowed []ID
	pagedFilter  TargetPageFilter
	pagedCalled  bool
	pageNext     string
	tableFilter  TargetTableQuery
	tableTotal   int
}

func (r *fakeTargetRepository) ListTargets(context.Context, ID) ([]Target, error) {
	return r.targets, nil
}

func (r *fakeTargetRepository) ListTargetsPage(_ context.Context, _ ID, all bool, allowedIDs []ID, filter TargetPageFilter) ([]Target, string, error) {
	r.pagedCalled = true
	r.pagedAll = all
	r.pagedAllowed = allowedIDs
	r.pagedFilter = filter
	return r.targets, r.pageNext, nil
}

func (r *fakeTargetRepository) ListTargetsTablePage(_ context.Context, _ ID, all bool, allowedIDs []ID, filter TargetTableQuery) ([]Target, int, error) {
	r.pagedCalled = true
	r.pagedAll = all
	r.pagedAllowed = allowedIDs
	r.tableFilter = filter
	total := r.tableTotal
	if total == 0 {
		total = len(r.targets)
	}
	return r.targets, total, nil
}

func (r *fakeTargetRepository) GetTargetsByIDs(_ context.Context, _ ID, ids []ID) (map[ID]Target, error) {
	out := make(map[ID]Target, len(ids))
	for _, target := range r.targets {
		for _, id := range ids {
			if target.ID == id {
				out[id] = target
			}
		}
	}
	return out, nil
}

func (r *fakeTargetRepository) GetTarget(_ context.Context, _ ID, targetID ID) (Target, error) {
	for _, target := range r.targets {
		if target.ID == targetID {
			return target, nil
		}
	}
	return Target{}, errNotFoundForTest{}
}

func (r *fakeTargetRepository) CreateTarget(_ context.Context, target Target) (Target, error) {
	r.created = target
	return target, nil
}

func (r *fakeTargetRepository) UpdateTarget(_ context.Context, target Target) (Target, error) {
	r.updated = target
	return target, nil
}

func (r *fakeTargetRepository) DeleteTarget(_ context.Context, _ ID, targetID ID) error {
	r.deleted = targetID
	return nil
}

type errNotFoundForTest struct{}

func (errNotFoundForTest) Error() string { return "not found" }

func TestAPITargetsList(t *testing.T) {
	repo := &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    targetTestAuth,
		Targets: repo,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "target-a") {
		t.Fatalf("body missing target: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"kind":"network"`) || strings.Contains(rec.Body.String(), `"Type"`) || strings.Contains(rec.Body.String(), `"target_type"`) {
		t.Fatalf("target response does not use canonical snake_case fields: %s", rec.Body.String())
	}
	// No limit/cursor => legacy full-list path, the paged repo method is untouched.
	if repo.pagedCalled {
		t.Fatal("full-list request must not hit the paged path")
	}
}

func adminTargetAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-a", UserID: "admin", IsAdmin: true}, nil
}

func TestAPITargetsListPagedOptIn(t *testing.T) {
	// limit triggers the paged path; grant scope is pushed down (non-admin sees
	// only its granted target), and next_cursor surfaces only when non-empty.
	repo := &fakeTargetRepository{
		targets:  []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}},
		pageNext: "CURSOR2",
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: targetTestAuth, Targets: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets?limit=2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !repo.pagedCalled || repo.pagedFilter.Limit != 2 {
		t.Fatalf("paged not called with limit 2: called=%v filter=%+v", repo.pagedCalled, repo.pagedFilter)
	}
	if repo.pagedAll || len(repo.pagedAllowed) != 1 || repo.pagedAllowed[0] != "target-a" {
		t.Fatalf("grant pushdown wrong: all=%v allowed=%v", repo.pagedAll, repo.pagedAllowed)
	}
	if !strings.Contains(rec.Body.String(), `"next_cursor":"CURSOR2"`) {
		t.Fatalf("expected next_cursor in body: %s", rec.Body.String())
	}

	// A cursor alone (no limit) also opts in and threads through.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets?cursor=abc", nil))
	if !repo.pagedCalled || repo.pagedFilter.Cursor != "abc" {
		t.Fatalf("cursor not threaded: %+v", repo.pagedFilter)
	}

	// exclude_kind threads through (the Hosts view excludes network targets).
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets?limit=2&exclude_kind=network", nil))
	if repo.pagedFilter.ExcludeKind != "network" {
		t.Fatalf("exclude_kind not threaded: %+v", repo.pagedFilter)
	}

	// An admin pushes down as "whole tenant" (all=true, no id filter).
	adminRepo := &fakeTargetRepository{}
	adminRouter := NewAPIV1Router(APIV1RouterConfig{Auth: adminTargetAuth, Targets: adminRepo})
	rec = httptest.NewRecorder()
	adminRouter.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets?limit=5", nil))
	if !adminRepo.pagedAll || adminRepo.pagedAllowed != nil {
		t.Fatalf("admin scope wrong: all=%v allowed=%v", adminRepo.pagedAll, adminRepo.pagedAllowed)
	}
	// Empty page with no next cursor omits next_cursor and returns [] not null.
	if strings.Contains(rec.Body.String(), "next_cursor") || !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Fatalf("empty admin page body = %s", rec.Body.String())
	}
}

func TestAPITargetsListPagedRejectsBadLimit(t *testing.T) {
	repo := &fakeTargetRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: targetTestAuth, Targets: repo})
	for _, bad := range []string{"0", "-3", "abc"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets?limit="+bad, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("limit=%q status = %d, want 400", bad, rec.Code)
		}
		if repo.pagedCalled {
			t.Fatalf("limit=%q must be rejected before the repo", bad)
		}
	}
}

func TestAPITargetsListServerTable(t *testing.T) {
	repo := &fakeTargetRepository{
		targets:    []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Host A", Kind: TargetKindSystem, Host: "10.0.0.1", Status: "up"}},
		tableTotal: 7,
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: targetTestAuth, Targets: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/targets?q=host&status=up&exclude_kind=network&sort=updated_at&order=desc&limit=25&offset=50", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if repo.tableFilter.Search != "host" || repo.tableFilter.Status != "up" ||
		repo.tableFilter.ExcludeKind != "network" || repo.tableFilter.Sort != "updated_at" ||
		!repo.tableFilter.Desc || repo.tableFilter.Limit != 25 || repo.tableFilter.Offset != 50 {
		t.Fatalf("table filter=%+v", repo.tableFilter)
	}
	if repo.pagedAll || len(repo.pagedAllowed) != 1 || repo.pagedAllowed[0] != "target-a" {
		t.Fatalf("scope all=%v allowed=%v", repo.pagedAll, repo.pagedAllowed)
	}
	if !strings.Contains(rec.Body.String(), `"total":7`) || !strings.Contains(rec.Body.String(), `"limit":25`) ||
		!strings.Contains(rec.Body.String(), `"offset":50`) {
		t.Fatalf("body=%s", rec.Body.String())
	}

	for _, query := range []string{
		"q=x&cursor=bad", "kind=broken", "exclude_kind=broken", "status=broken", "sort=raw_sql", "order=sideways",
		"limit=501&sort=name", "offset=-1", "unknown=1&q=x",
	} {
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query=%q status=%d body=%s", query, rec.Code, rec.Body.String())
		}
	}
}

func TestAPITargetsListFiltersByTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: targetTestAuth,
		Targets: &fakeTargetRepository{targets: []Target{
			{ID: "target-a", TenantID: "tenant-a", Name: "Core A", Kind: TargetKindNetwork, Host: "10.0.0.1"},
			{ID: "target-b", TenantID: "tenant-a", Name: "Core B", Kind: TargetKindNetwork, Host: "10.0.0.2"},
		}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "target-a") || strings.Contains(body, "target-b") {
		t.Fatalf("unexpected body = %s", body)
	}
}

func TestAPITargetsCreate(t *testing.T) {
	repo := &fakeTargetRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    targetTestAuth,
		Targets: repo,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/targets", strings.NewReader(`{"id":"target-a","name":"Core","kind":"network","host":"10.0.0.1"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.created.TenantID != "tenant-a" {
		t.Fatalf("TenantID = %s, want tenant-a", repo.created.TenantID)
	}
	if repo.created.Kind != TargetKindNetwork {
		t.Fatalf("Kind = %s, want network", repo.created.Kind)
	}
}

func TestAPITargetsCreateNetworkFromHostOnly(t *testing.T) {
	repo := &fakeTargetRepository{}
	network := &fakeNetworkRepository{}
	jobs := &fakeDiscoveryJobRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          targetTestAuth,
		Targets:       repo,
		Network:       network,
		SNMP:          &fakeSNMPRepository{profiles: []SNMPProfile{{ID: "profile-a", TenantID: "tenant-a", Name: "default", Version: SNMPVersion2c}}},
		DiscoveryJobs: jobs,
	})
	rec := httptest.NewRecorder()
	body := `{"id":"22e97b7d-6b32-433f-b44e-936c205661b7","kind":"network","host":" 10.0.0.2 ","status":"up","snmp_security":{"community":"private-a"}}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/targets", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.created.Name != "10.0.0.2" || repo.created.Host != "10.0.0.2" {
		t.Fatalf("created target = %#v", repo.created)
	}
	if repo.created.Status != "pending" {
		t.Fatalf("created target status = %q, want pending", repo.created.Status)
	}
	if repo.created.ID == "" || len(repo.created.ID) > 26 || repo.created.ID == "22e97b7d-6b32-433f-b44e-936c205661b7" {
		t.Fatalf("server-generated target ID = %q", repo.created.ID)
	}
	if len(network.devices) != 1 {
		t.Fatalf("network devices = %#v", network.devices)
	}
	device := network.devices[0]
	if device.TargetID != repo.created.ID || device.SNMPProfileID != "profile-a" || device.SNMPPort != 161 {
		t.Fatalf("provisioned device = %#v", device)
	}
	if device.SNMPSecurity["community"] != "private-a" {
		t.Fatalf("provisioned device SNMP security = %#v", device.SNMPSecurity)
	}
	if len(jobs.enqueued) != 1 || jobs.enqueued[0].DeviceID != device.ID {
		t.Fatalf("discovery jobs = %#v", jobs.enqueued)
	}
}

func TestAPITargetsCreateRejectsDuplicateHostNotDuplicateName(t *testing.T) {
	repo := &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Any name", Kind: TargetKindNetwork, Host: "10.0.0.1"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: targetTestAuth, Targets: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/targets", strings.NewReader(`{"name":"Another name","kind":"network","host":"10.0.0.1"}`)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestPromoteDiscoveredTargetNamePreservesManualName(t *testing.T) {
	automatic := &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "10.0.0.1", Kind: TargetKindNetwork, Host: "10.0.0.1"}}}
	if err := promoteDiscoveredTargetName(context.Background(), automatic, "tenant-a", "target-a", "edge-switch-01"); err != nil {
		t.Fatal(err)
	}
	if automatic.updated.Name != "edge-switch-01" {
		t.Fatalf("automatic name = %q", automatic.updated.Name)
	}

	manual := &fakeTargetRepository{targets: []Target{{ID: "target-b", TenantID: "tenant-a", Name: "西溪谷接入交换机", Kind: TargetKindNetwork, Host: "10.0.0.2"}}}
	if err := promoteDiscoveredTargetName(context.Background(), manual, "tenant-a", "target-b", "edge-switch-02"); err != nil {
		t.Fatal(err)
	}
	if manual.updated.ID != "" {
		t.Fatalf("manual target was overwritten: %#v", manual.updated)
	}
}

func TestNormalizeTargetHost(t *testing.T) {
	for input, want := range map[string]string{
		" Core-01.EXAMPLE.COM. ": "core-01.example.com",
		"[2001:0db8::1]":         "2001:db8::1",
		" 10.0.0.1 ":             "10.0.0.1",
	} {
		if got := normalizeTargetHost(input); got != want {
			t.Errorf("normalizeTargetHost(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestAPITargetsGetRequiresTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a"}, nil
		},
		Targets: &fakeTargetRepository{},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets/target-a", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestAPITargetsDelete(t *testing.T) {
	repo := &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork, Host: "10.0.0.1"}}}
	cleaner := &fakeSeriesCleaner{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:          targetTestAuth,
		Targets:       repo,
		SeriesCleaner: cleaner,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/targets/target-a", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.deleted != "target-a" {
		t.Fatalf("deleted = %s", repo.deleted)
	}
	if len(cleaner.calls) != 1 {
		t.Fatalf("series cleaner calls = %d, want 1", len(cleaner.calls))
	}
	if !strings.Contains(cleaner.calls[0], `target_id="target-a"`) {
		t.Fatalf("cleaner matcher = %q", cleaner.calls[0])
	}
}

func targetTestAuth(*http.Request) (AuthContext, error) {
	return AuthContext{
		TenantID: "tenant-a",
		UserID:   "user-a",
		Grants: []Permission{
			{TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: []Action{ActionView, ActionConfigure}},
			{TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTarget, ResourceID: "target-a", Actions: []Action{ActionView, ActionConfigure}},
		},
	}, nil
}

func TestAPITargetsIfMatchOptimisticLocking(t *testing.T) {
	updatedAt := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	repo := &fakeTargetRepository{targets: []Target{{
		ID: "target-a", TenantID: "tenant-a", Name: "Core", Kind: TargetKindNetwork,
		Host: "10.0.0.1", UpdatedAt: updatedAt,
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: targetTestAuth, Targets: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets/target-a", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" || !strings.HasPrefix(etag, `W/"`) {
		t.Fatalf("expected weak ETag, got %q", etag)
	}

	stale := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/targets/target-a", strings.NewReader(`{"name":"Renamed","host":"10.0.0.1","kind":"network"}`))
	req.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(stale, req)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale patch status = %d, body = %s", stale.Code, stale.Body.String())
	}
	if repo.updated.ID != "" {
		t.Fatalf("stale patch must not update, got %+v", repo.updated)
	}

	fresh := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPatch, "/api/v1/targets/target-a", strings.NewReader(`{"name":"Renamed","host":"10.0.0.1","kind":"network"}`))
	req.Header.Set("If-Match", etag)
	router.ServeHTTP(fresh, req)
	if fresh.Code != http.StatusOK {
		t.Fatalf("fresh patch status = %d, body = %s", fresh.Code, fresh.Body.String())
	}

	staleDelete := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/targets/target-a", nil)
	req.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(staleDelete, req)
	if staleDelete.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale delete status = %d", staleDelete.Code)
	}
	if repo.deleted != "" {
		t.Fatalf("stale delete must not delete, got %q", repo.deleted)
	}
}
