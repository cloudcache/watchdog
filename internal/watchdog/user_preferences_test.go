package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeUserPreferencesRepo struct {
	prefs    UserPreferences
	getErr   error
	putErr   error
	lastPut  UserPreferences
	lastExpV uint64
}

func (f *fakeUserPreferencesRepo) GetUserPreferences(context.Context, ID, ID) (UserPreferences, error) {
	return f.prefs, f.getErr
}
func (f *fakeUserPreferencesRepo) UpsertUserPreferences(_ context.Context, prefs UserPreferences, expectedRowVersion uint64) (UserPreferences, error) {
	f.lastPut = prefs
	f.lastExpV = expectedRowVersion
	if f.putErr != nil {
		return UserPreferences{}, f.putErr
	}
	prefs.RowVersion = expectedRowVersion + 1
	return prefs, nil
}

func TestUserPreferencesGetReturnsDefaultAndETag(t *testing.T) {
	repo := &fakeUserPreferencesRepo{prefs: UserPreferences{Settings: json.RawMessage("{}"), RowVersion: 0}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), UserPreferences: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me/preferences", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("ETag") != `"0"` {
		t.Fatalf("etag = %q, want \"0\"", rec.Header().Get("ETag"))
	}
	var body struct {
		Settings   map[string]any `json:"settings"`
		RowVersion uint64         `json:"row_version"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Settings) != 0 || body.RowVersion != 0 {
		t.Fatalf("default body = %+v", body)
	}
}

func TestUserPreferencesPutValidatesObjectAndVersion(t *testing.T) {
	repo := &fakeUserPreferencesRepo{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), UserPreferences: repo})

	// A non-object body is rejected.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/me/preferences", strings.NewReader(`[1,2,3]`))
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("array body status = %d", rec.Code)
	}

	// A valid object with If-Match threads the expected version through.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/api/v1/me/preferences", strings.NewReader(`{"chartTime":"1h","unitTemp":"c"}`))
	req.Header.Set("If-Match", `"3"`)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.lastExpV != 3 {
		t.Fatalf("expected version passed = %d, want 3", repo.lastExpV)
	}
	if rec.Header().Get("ETag") != `"4"` {
		t.Fatalf("etag after put = %q, want \"4\"", rec.Header().Get("ETag"))
	}

	// A stale version surfaces as 412.
	repo.putErr = ErrUserPreferencesConflict
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPut, "/api/v1/me/preferences", strings.NewReader(`{"chartTime":"1h"}`))
	req.Header.Set("If-Match", `"1"`)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed || !strings.Contains(rec.Body.String(), "version_conflict") {
		t.Fatalf("conflict status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

// TestMySQLUserPreferencesLifecycle proves the read-default-no-write and
// optimistic-concurrency contract against real MySQL.
func TestMySQLUserPreferencesLifecycle(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_prefs_1', ?, 'prefs@test.local', 'Prefs', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	// First read returns an empty default and does NOT create a row.
	prefs, err := store.GetUserPreferences(ctx, tenant, "user_prefs_1")
	if err != nil || prefs.RowVersion != 0 || string(prefs.Settings) != "{}" {
		t.Fatalf("default prefs = %+v err=%v", prefs, err)
	}
	var rowCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_preferences WHERE user_id = 'user_prefs_1'").Scan(&rowCount); err != nil || rowCount != 0 {
		t.Fatalf("read must not create a row, count=%d err=%v", rowCount, err)
	}

	// First write (expected version 0) creates the row.
	created, err := store.UpsertUserPreferences(ctx, UserPreferences{
		UserID: "user_prefs_1", TenantID: tenant, Settings: json.RawMessage(`{"chartTime":"1h"}`),
	}, 0)
	if err != nil || created.RowVersion != 1 {
		t.Fatalf("create = %+v err=%v", created, err)
	}

	// A stale write (wrong expected version) conflicts.
	if _, err := store.UpsertUserPreferences(ctx, UserPreferences{
		UserID: "user_prefs_1", TenantID: tenant, Settings: json.RawMessage(`{"chartTime":"24h"}`),
	}, 0); err != ErrUserPreferencesConflict {
		t.Fatalf("stale create conflict error = %v", err)
	}

	// A fresh write advances the version and persists.
	updated, err := store.UpsertUserPreferences(ctx, UserPreferences{
		UserID: "user_prefs_1", TenantID: tenant, Settings: json.RawMessage(`{"chartTime":"24h","unitNet":"bytes"}`),
	}, 1)
	if err != nil || updated.RowVersion != 2 {
		t.Fatalf("update = %+v err=%v", updated, err)
	}
	var storedChartTime string
	if err := db.QueryRowContext(ctx, `SELECT settings_json->>'$.chartTime' FROM user_preferences WHERE user_id = 'user_prefs_1'`).Scan(&storedChartTime); err != nil || storedChartTime != "24h" {
		t.Fatalf("stored chartTime = %q err=%v", storedChartTime, err)
	}
}
