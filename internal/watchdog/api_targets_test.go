package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeTargetRepository struct {
	targets []Target
	created Target
	updated Target
	deleted ID
}

func (r *fakeTargetRepository) ListTargets(context.Context, ID) ([]Target, error) {
	return r.targets, nil
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
	repo := &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Type: TargetTypeNetwork, Host: "10.0.0.1"}}}
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
}

func TestAPITargetsListFiltersByTargetPermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: targetTestAuth,
		Targets: &fakeTargetRepository{targets: []Target{
			{ID: "target-a", TenantID: "tenant-a", Name: "Core A", Type: TargetTypeNetwork, Host: "10.0.0.1"},
			{ID: "target-b", TenantID: "tenant-a", Name: "Core B", Type: TargetTypeNetwork, Host: "10.0.0.2"},
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
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/targets", strings.NewReader(`{"ID":"target-a","Name":"Core","Type":"network","Host":"10.0.0.1"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.created.TenantID != "tenant-a" {
		t.Fatalf("TenantID = %s, want tenant-a", repo.created.TenantID)
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
	repo := &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a", Name: "Core", Type: TargetTypeNetwork, Host: "10.0.0.1"}}}
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
