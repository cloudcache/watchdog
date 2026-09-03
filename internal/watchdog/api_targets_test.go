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
	body := `{"id":"22e97b7d-6b32-433f-b44e-936c205661b7","kind":"network","host":" 10.0.0.2 "}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/targets", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.created.Name != "10.0.0.2" || repo.created.Host != "10.0.0.2" {
		t.Fatalf("created target = %#v", repo.created)
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
