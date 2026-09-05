package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeSNMPRepository struct {
	profiles []SNMPProfile
	modules  []MIBModule
}

func (r *fakeSNMPRepository) ListSNMPProfiles(context.Context, ID) ([]SNMPProfile, error) {
	return r.profiles, nil
}

func (r *fakeSNMPRepository) GetSNMPProfile(_ context.Context, _ ID, profileID ID) (SNMPProfile, error) {
	for _, profile := range r.profiles {
		if profile.ID == profileID {
			return profile, nil
		}
	}
	return SNMPProfile{}, errNotFoundForTest{}
}

func (r *fakeSNMPRepository) UpsertSNMPProfile(_ context.Context, profile SNMPProfile) (SNMPProfile, error) {
	r.profiles = append(r.profiles, profile)
	return profile, nil
}

func (r *fakeSNMPRepository) DeleteSNMPProfile(_ context.Context, _ ID, profileID ID) error {
	for index, profile := range r.profiles {
		if profile.ID == profileID {
			r.profiles = append(r.profiles[:index], r.profiles[index+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeSNMPRepository) ListMIBModules(context.Context) ([]MIBModule, error) {
	return r.modules, nil
}

func (r *fakeSNMPRepository) UpsertMIBModule(_ context.Context, module MIBModule) (MIBModule, error) {
	if module.ID == "" {
		module.ID = stableID("mib", module.Source, module.Name)
	}
	r.modules = append(r.modules, module)
	return module, nil
}

func (r *fakeSNMPRepository) DeleteMIBModule(context.Context, ID) error {
	r.modules = nil
	return nil
}

func TestAPISNMPProfilesListRequiresConfigurePermission(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: billingTestAuth(false),
		SNMP: &fakeSNMPRepository{profiles: []SNMPProfile{{
			ID:      "profile-a",
			Name:    "Core v2c",
			Version: SNMPVersion2c,
			Timeout: 5 * time.Second,
			Retries: 2,
		}}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/snmp/profiles", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "profile-a") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPISNMPProfileGetNotFound(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), SNMP: &fakeSNMPRepository{}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/snmp/profiles/missing", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestAPISNMPProfileIfMatchOptimisticLocking(t *testing.T) {
	updatedAt := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	repo := &fakeSNMPRepository{profiles: []SNMPProfile{{
		ID: "profile-a", Name: "Core v2c", Version: SNMPVersion2c, Timeout: 5 * time.Second, Retries: 2, UpdatedAt: updatedAt,
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), SNMP: repo})

	// GET exposes a weak ETag derived from updated_at.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/snmp/profiles/profile-a", nil))
	etag := rec.Header().Get("ETag")
	if rec.Code != http.StatusOK || etag != WeakETagFromTime(updatedAt) {
		t.Fatalf("get status=%d etag=%q", rec.Code, etag)
	}

	body := `{"ID":"profile-a","Name":"Core v3","Version":"2c","Security":{"community":"public"},"Retries":2}`

	// A stale If-Match is rejected with 412 and the write does not happen.
	stale := httptest.NewRecorder()
	staleReq := httptest.NewRequest(http.MethodPatch, "/api/v1/snmp/profiles/profile-a", strings.NewReader(body))
	staleReq.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(stale, staleReq)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match status = %d, want 412", stale.Code)
	}

	// The current ETag is accepted.
	ok := httptest.NewRecorder()
	okReq := httptest.NewRequest(http.MethodPatch, "/api/v1/snmp/profiles/profile-a", strings.NewReader(body))
	okReq.Header.Set("If-Match", etag)
	router.ServeHTTP(ok, okReq)
	if ok.Code != http.StatusOK {
		t.Fatalf("matched If-Match status = %d body = %s", ok.Code, ok.Body.String())
	}

	// DELETE also honors a stale If-Match.
	del := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/snmp/profiles/profile-a", nil)
	delReq.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(del, delReq)
	if del.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale delete If-Match status = %d, want 412", del.Code)
	}
}

func TestAPISNMPProfileCreateStoresTenantScopedProfile(t *testing.T) {
	repo := &fakeSNMPRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), SNMP: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/snmp/profiles", strings.NewReader(`{"ID":"profile-a","Name":"Core","Version":"2c","Security":{"community":"public"},"Retries":2}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.profiles) != 1 || repo.profiles[0].TenantID != "tenant-a" || repo.profiles[0].Timeout != 5*time.Second {
		t.Fatalf("profiles = %#v", repo.profiles)
	}
}

func TestAPISNMPMIBModulesList(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: billingTestAuth(false),
		SNMP: &fakeSNMPRepository{modules: []MIBModule{{
			ID:       "mib-a",
			Name:     "IF-MIB",
			Source:   "librenms",
			Checksum: "sha256:a",
			Enabled:  true,
		}}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/snmp/mib-modules", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "IF-MIB") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPISNMPMIBModulePut(t *testing.T) {
	repo := &fakeSNMPRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), SNMP: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/snmp/mib-modules", strings.NewReader(`{"Name":"IF-MIB","Source":"librenms","Version":"2026.06","Checksum":"sha256:a","Enabled":true}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.modules) != 1 || repo.modules[0].Name != "IF-MIB" || !repo.modules[0].Enabled {
		t.Fatalf("modules = %#v", repo.modules)
	}
}
