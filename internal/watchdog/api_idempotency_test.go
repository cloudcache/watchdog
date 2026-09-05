package watchdog

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeIdempotencyRepository struct {
	records map[string]IdempotencyRecord
}

func newFakeIdempotencyRepository() *fakeIdempotencyRepository {
	return &fakeIdempotencyRepository{records: map[string]IdempotencyRecord{}}
}

func (f *fakeIdempotencyRepository) GetIdempotencyRecord(_ context.Context, tenantID ID, key string) (IdempotencyRecord, error) {
	record, ok := f.records[string(tenantID)+"/"+key]
	if !ok {
		return IdempotencyRecord{}, sql.ErrNoRows
	}
	return record, nil
}

func (f *fakeIdempotencyRepository) SaveIdempotencyRecord(_ context.Context, record IdempotencyRecord) error {
	f.records[string(record.TenantID)+"/"+record.Key] = record
	return nil
}

func newIdempotencyTestRouter(repo *fakeIdentityAdminRepository, idem IdempotencyRepository) http.Handler {
	return NewAPIV1Router(APIV1RouterConfig{
		Auth:          identityAdminTestAuth,
		IdentityAdmin: repo,
		Audit:         &recordingAuditRepository{},
		Idempotency:   idem,
	})
}

func TestIdempotencyReplaysStoredResponse(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	idem := newFakeIdempotencyRepository()
	router := newIdempotencyTestRouter(repo, idem)
	body := `{"email":"ops@example.com","name":"Ops","auth_provider":"pocketbase","external_subject_id":"pb_1"}`

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "create-ops-1")
	router.ServeHTTP(first, req)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "create-ops-1")
	router.ServeHTTP(second, req)
	if second.Code != http.StatusCreated {
		t.Fatalf("replay status = %d, body = %s", second.Code, second.Body.String())
	}
	if second.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay missing Idempotency-Replayed header")
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("replay body differs: %s vs %s", second.Body.String(), first.Body.String())
	}
	if len(repo.users) != 1 {
		t.Fatalf("expected one user after replay, got %d", len(repo.users))
	}
}

func TestIdempotencyKeyReuseWithDifferentBodyConflicts(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	router := newIdempotencyTestRouter(repo, newFakeIdempotencyRepository())

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users",
		strings.NewReader(`{"email":"a@example.com","auth_provider":"pocketbase","external_subject_id":"pb_a"}`))
	req.Header.Set("Idempotency-Key", "shared-key")
	router.ServeHTTP(first, req)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/users",
		strings.NewReader(`{"email":"b@example.com","auth_provider":"pocketbase","external_subject_id":"pb_b"}`))
	req.Header.Set("Idempotency-Key", "shared-key")
	router.ServeHTTP(second, req)
	if second.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, body = %s", second.Code, second.Body.String())
	}
	if len(repo.users) != 1 {
		t.Fatalf("conflicting request must not create a user, got %d", len(repo.users))
	}
}

func TestIdempotencyWithoutKeyOrRepoPassesThrough(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	router := newIdempotencyTestRouter(repo, nil) // no repository configured

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/users",
		strings.NewReader(`{"email":"ops@example.com","auth_provider":"pocketbase","external_subject_id":"pb_1"}`))
	req.Header.Set("Idempotency-Key", "ignored-without-repo")
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestIfMatchRejectsStaleUpdate(t *testing.T) {
	repo := newFakeIdentityAdminRepository()
	updatedAt := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	repo.users["user-1"] = User{ID: "user-1", TenantID: "tenant-a", Email: "ops@example.com", Status: "active",
		AuthProvider: "pocketbase", ExternalSubjectID: "pb_1", UpdatedAt: updatedAt}
	router := newIdempotencyTestRouter(repo, nil)

	get := httptest.NewRecorder()
	router.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/users/user-1", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d", get.Code)
	}
	etag := get.Header().Get("ETag")
	if etag == "" || !strings.HasPrefix(etag, `W/"`) {
		t.Fatalf("expected weak ETag, got %q", etag)
	}

	stale := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/users/user-1", strings.NewReader(`{"name":"New"}`))
	req.Header.Set("If-Match", WeakETagFromTime(updatedAt.Add(-time.Hour)))
	router.ServeHTTP(stale, req)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale status = %d, body = %s", stale.Code, stale.Body.String())
	}
	if !strings.Contains(stale.Body.String(), "version_conflict") {
		t.Fatalf("expected version_conflict code, body = %s", stale.Body.String())
	}

	fresh := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPatch, "/api/v1/users/user-1", strings.NewReader(`{"name":"New"}`))
	req.Header.Set("If-Match", etag)
	router.ServeHTTP(fresh, req)
	if fresh.Code != http.StatusOK {
		t.Fatalf("fresh status = %d, body = %s", fresh.Code, fresh.Body.String())
	}
	if repo.users["user-1"].Name != "New" {
		t.Fatalf("update did not apply: %+v", repo.users["user-1"])
	}
}
