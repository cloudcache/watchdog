package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

type fakeDashboardRepo struct {
	items      map[ID]Dashboard
	graphs     map[ID]AggregateGraph
	graphItems map[ID][]AggregateGraphItem
	clock      time.Time
}

func newFakeDashboardRepo() *fakeDashboardRepo {
	return &fakeDashboardRepo{
		items: map[ID]Dashboard{}, graphs: map[ID]AggregateGraph{}, graphItems: map[ID][]AggregateGraphItem{},
		clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (f *fakeDashboardRepo) tick() time.Time {
	f.clock = f.clock.Add(time.Second)
	return f.clock
}

func (f *fakeDashboardRepo) ListDashboards(_ context.Context, tenantID ID, filter DashboardListFilter) ([]Dashboard, int64, error) {
	filter, err := normalizeDashboardListFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	out := []Dashboard{}
	for _, d := range f.items {
		if d.TenantID == tenantID && (filter.OwnerID == "" || d.OwnerID == filter.OwnerID) &&
			(filter.Search == "" || strings.Contains(strings.ToLower(d.Name+" "+d.Description), strings.ToLower(filter.Search))) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if filter.Desc {
			return out[i].Name > out[j].Name
		}
		return out[i].Name < out[j].Name
	})
	total := int64(len(out))
	if filter.Offset >= len(out) {
		return []Dashboard{}, total, nil
	}
	out = out[filter.Offset:]
	if len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, total, nil
}

func (f *fakeDashboardRepo) GetDashboard(_ context.Context, tenantID, id ID) (Dashboard, error) {
	d, ok := f.items[id]
	if !ok || d.TenantID != tenantID {
		return Dashboard{}, sql.ErrNoRows
	}
	return d, nil
}

func (f *fakeDashboardRepo) CreateDashboard(_ context.Context, d Dashboard) (Dashboard, error) {
	for _, existing := range f.items {
		if existing.TenantID == d.TenantID && existing.Name == d.Name {
			return Dashboard{}, ErrDashboardNameConflict
		}
	}
	d.ID = stableID("dashboard", string(d.TenantID), d.Name)
	d.Version = 1
	now := f.tick()
	d.CreatedAt, d.UpdatedAt = now, now
	f.items[d.ID] = d
	return d, nil
}

func (f *fakeDashboardRepo) UpdateDashboard(_ context.Context, d Dashboard, expectedVersion uint32) (Dashboard, error) {
	existing, ok := f.items[d.ID]
	if !ok || existing.TenantID != d.TenantID {
		return Dashboard{}, sql.ErrNoRows
	}
	if existing.Version != expectedVersion {
		return Dashboard{}, ErrDashboardVersionConflict
	}
	existing.Name = d.Name
	existing.Description = d.Description
	existing.Layout = d.Layout
	existing.Version++
	existing.UpdatedAt = f.tick()
	f.items[d.ID] = existing
	return existing, nil
}

func (f *fakeDashboardRepo) DeleteDashboard(_ context.Context, tenantID, id ID, expectedVersion uint32) error {
	d, ok := f.items[id]
	if !ok || d.TenantID != tenantID {
		return sql.ErrNoRows
	}
	if d.Version != expectedVersion {
		return ErrDashboardVersionConflict
	}
	delete(f.items, id)
	return nil
}

func (f *fakeDashboardRepo) ResolveDashboardGraphReferences(_ context.Context, tenantID ID, graphIDs []ID) ([]DashboardGraphReference, error) {
	refs := make([]DashboardGraphReference, 0, len(graphIDs))
	for _, id := range graphIDs {
		ref := DashboardGraphReference{GraphID: id, Series: []AggregateGraphItem{}}
		if graph, ok := f.graphs[id]; ok && graph.TenantID == tenantID {
			graphCopy := graph
			ref.Exists = true
			ref.Graph = &graphCopy
			ref.Series = f.graphItems[id]
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func (f *fakeDashboardRepo) ListDashboardGraphOptions(_ context.Context, tenantID ID, filter DashboardGraphOptionListFilter) ([]AggregateGraph, int64, error) {
	filter, err := normalizeDashboardGraphOptionListFilter(filter)
	if err != nil {
		return nil, 0, err
	}
	graphs := make([]AggregateGraph, 0)
	for _, graph := range f.graphs {
		if graph.TenantID != tenantID || (filter.Search != "" && !strings.Contains(strings.ToLower(graph.Name+" "+graph.Description), strings.ToLower(filter.Search))) {
			continue
		}
		graphs = append(graphs, graph)
	}
	sort.Slice(graphs, func(i, j int) bool { return graphs[i].Name < graphs[j].Name })
	total := int64(len(graphs))
	if filter.Offset >= len(graphs) {
		return []AggregateGraph{}, total, nil
	}
	graphs = graphs[filter.Offset:]
	if len(graphs) > filter.Limit {
		graphs = graphs[:filter.Limit]
	}
	return graphs, total, nil
}

func dashboardTestRouter(repo DashboardRepository) http.Handler {
	return dashboardTestRouterWithAudit(repo, nil)
}

func dashboardTestRouterWithAudit(repo DashboardRepository, audit AuditRepository) http.Handler {
	return NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: true}, nil
		},
		Dashboards: repo,
		Audit:      audit,
	})
}

func TestDashboardAPICRUDAndETag(t *testing.T) {
	repo := newFakeDashboardRepo()
	repo.graphs["g1"] = AggregateGraph{ID: "g1", TenantID: "tenant-a", Name: "Traffic"}
	repo.graphItems["g1"] = []AggregateGraphItem{{ID: "s1", GraphID: "g1", Metric: "ifHCInOctets"}}
	audit := &recordingAuditRepository{}
	router := dashboardTestRouterWithAudit(repo, audit)

	// Create.
	body := `{"name":"Ops","description":"d","layout":{"panels":[{"graph_id":"g1"}]}}`
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/dashboards", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var created Dashboard
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Version != 1 || created.OwnerID != "user-a" {
		t.Fatalf("created = %+v", created)
	}

	// Get returns an ETag.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/dashboards/"+string(created.ID), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d", rec.Code)
	}
	etag := rec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	// Patch with a stale If-Match is refused with 412.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/dashboards/"+string(created.ID), strings.NewReader(body))
	req.Header.Set("If-Match", `"99"`)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale patch status = %d, want 412", rec.Code)
	}

	// Patch with the current ETag succeeds and bumps the version.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPatch, "/api/v1/dashboards/"+string(created.ID),
		strings.NewReader(`{"name":"Ops v2","layout":{"panels":[{"graph_id":"g1"},{"graph_id":"g2"}]}}`))
	req.Header.Set("If-Match", etag)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var updated Dashboard
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if updated.Version != 2 || updated.Name != "Ops v2" {
		t.Fatalf("updated = %+v", updated)
	}

	// Preview resolves existing graph series and reports missing references.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/dashboards/"+string(created.ID)+"/preview", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"graph_id":"g1"`) ||
		!strings.Contains(rec.Body.String(), `"graph_id":"g2"`) || !strings.Contains(rec.Body.String(), `"missing_graph_ids":["g2"]`) {
		t.Fatalf("preview status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Delete with the current version ETag.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/dashboards/"+string(created.ID), nil)
	req.Header.Set("If-Match", `"2"`)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/dashboards/"+string(created.ID), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get-after-delete status = %d, want 404", rec.Code)
	}
	if len(audit.logs) != 3 || audit.logs[0].Action != "dashboard.created" ||
		audit.logs[1].Action != "dashboard.updated" || audit.logs[2].Action != "dashboard.deleted" {
		t.Fatalf("audit logs = %+v", audit.logs)
	}
}

func TestDashboardAPIListParsesServerFiltersAndPagination(t *testing.T) {
	repo := newFakeDashboardRepo()
	for _, dashboard := range []Dashboard{
		{ID: "d1", TenantID: "tenant-a", OwnerID: "user-a", Name: "Core links", Description: "edge", Layout: json.RawMessage(`{"panels":[]}`), Version: 1},
		{ID: "d2", TenantID: "tenant-a", OwnerID: "user-b", Name: "Access", Description: "core links", Layout: json.RawMessage(`{"panels":[]}`), Version: 1},
		{ID: "d3", TenantID: "tenant-b", OwnerID: "user-a", Name: "Foreign", Layout: json.RawMessage(`{"panels":[]}`), Version: 1},
	} {
		repo.items[dashboard.ID] = dashboard
	}
	rec := httptest.NewRecorder()
	dashboardTestRouter(repo).ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/dashboards?q=core&owner_id=user-a&limit=1&offset=0&sort=name&order=desc", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"total":1`) ||
		!strings.Contains(rec.Body.String(), `"name":"Core links"`) || strings.Contains(rec.Body.String(), "Foreign") {
		t.Fatalf("list status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDashboardAPIGraphOptionsAreTenantScopedAndPaged(t *testing.T) {
	repo := newFakeDashboardRepo()
	repo.graphs["g2"] = AggregateGraph{ID: "g2", TenantID: "tenant-a", Name: "Core Out", Description: "uplink"}
	repo.graphs["g1"] = AggregateGraph{ID: "g1", TenantID: "tenant-a", Name: "Core In", Description: "downlink"}
	repo.graphs["foreign"] = AggregateGraph{ID: "foreign", TenantID: "tenant-b", Name: "Core Foreign"}
	rec := httptest.NewRecorder()
	dashboardTestRouter(repo).ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/api/v1/dashboards/graph-options?q=core&limit=1&offset=1", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"total":2`) ||
		!strings.Contains(rec.Body.String(), `"ID":"g2"`) || strings.Contains(rec.Body.String(), "foreign") {
		t.Fatalf("graph options status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDashboardAPIRejectsBadLayoutAndConflict(t *testing.T) {
	repo := newFakeDashboardRepo()
	router := dashboardTestRouter(repo)

	// Invalid layout -> 400.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/dashboards",
		strings.NewReader(`{"name":"Bad","layout":[1,2,3]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad-layout status = %d, want 400", rec.Code)
	}

	// Duplicate name -> 409.
	good := `{"name":"Dup","layout":{"panels":[]}}`
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/dashboards", strings.NewReader(good)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("first create status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/dashboards", strings.NewReader(good)))
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate create status = %d, want 409", rec.Code)
	}
}
