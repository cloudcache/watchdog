package watchdog

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeQuietHoursRepo struct {
	windows    []QuietHourWindow
	lastCreate QuietHourWindow
	lastUpdate QuietHourWindow
	lastDelete ID
}

func (f *fakeQuietHoursRepo) ListQuietHours(context.Context, ID, ID) ([]QuietHourWindow, error) {
	return f.windows, nil
}
func (f *fakeQuietHoursRepo) CreateQuietHour(_ context.Context, _, _ ID, w QuietHourWindow) (QuietHourWindow, error) {
	w.ID = "qh_new"
	f.lastCreate = w
	return w, nil
}
func (f *fakeQuietHoursRepo) UpdateQuietHour(_ context.Context, _, _ ID, w QuietHourWindow) (QuietHourWindow, error) {
	f.lastUpdate = w
	return w, nil
}
func (f *fakeQuietHoursRepo) DeleteQuietHour(_ context.Context, _, _ ID, windowID ID) error {
	f.lastDelete = windowID
	return nil
}
func (f *fakeQuietHoursRepo) QuietHoursForExternalSubject(context.Context, string, string, string) ([]QuietHourWindow, error) {
	return f.windows, nil
}

func TestQuietHourValidation(t *testing.T) {
	start := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	if err := ValidateQuietHourWindow(QuietHourWindow{Type: "one-time", Start: start, End: end}); err != nil {
		t.Fatalf("valid one-time rejected: %v", err)
	}
	// Daily windows compare clock time, so equal start/end is fine.
	if err := ValidateQuietHourWindow(QuietHourWindow{Type: "daily", Start: start, End: start}); err != nil {
		t.Fatalf("valid daily rejected: %v", err)
	}
	if err := ValidateQuietHourWindow(QuietHourWindow{Type: "weekly", Start: start, End: end}); err == nil {
		t.Fatal("unknown type must be rejected")
	}
	if err := ValidateQuietHourWindow(QuietHourWindow{Type: "one-time", Start: start}); err == nil {
		t.Fatal("zero end must be rejected")
	}
	if err := ValidateQuietHourWindow(QuietHourWindow{Type: "one-time", Start: end, End: start}); err == nil {
		t.Fatal("one-time end before start must be rejected")
	}
}

func TestQuietHoursAPI(t *testing.T) {
	repo := &fakeQuietHoursRepo{windows: []QuietHourWindow{
		{ID: "qh_1", SystemID: "sys_a", Type: "daily", Start: time.Unix(0, 0).UTC(), End: time.Unix(3600, 0).UTC()},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), QuietHours: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me/quiet-hours", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "sys_a") {
		t.Fatalf("get status = %d body = %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	body := `{"system_id":"sys_b","type":"one-time","start":"2026-01-02T09:00:00Z","end":"2026-01-02T10:00:00Z"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/me/quiet-hours", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("post status = %d body = %s", rec.Code, rec.Body.String())
	}
	if repo.lastCreate.SystemID != "sys_b" || repo.lastCreate.Type != "one-time" {
		t.Fatalf("created = %+v", repo.lastCreate)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/me/quiet-hours/qh_1", strings.NewReader(body)))
	if rec.Code != http.StatusOK || repo.lastUpdate.ID != "qh_1" {
		t.Fatalf("patch status = %d update = %+v", rec.Code, repo.lastUpdate)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/me/quiet-hours/qh_1", nil))
	if rec.Code != http.StatusNoContent || repo.lastDelete != "qh_1" {
		t.Fatalf("delete status = %d deleted = %q", rec.Code, repo.lastDelete)
	}

	// A bad window type is rejected before it reaches the repo.
	rec = httptest.NewRecorder()
	bad := `{"type":"weekly","start":"2026-01-02T09:00:00Z","end":"2026-01-02T10:00:00Z"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/me/quiet-hours", strings.NewReader(bad)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid type status = %d", rec.Code)
	}
}

// TestMySQLQuietHoursLifecycle proves per-user CRUD and the alert silencing read
// path that resolves a PocketBase user id to the MySQL user and filters by system.
func TestMySQLQuietHoursLifecycle(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES ('user_qh_1', ?, 'qh@test.local', 'QH', 'active', 'pocketbase', 'pb_qh_1')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	start := time.Now().UTC().Truncate(time.Millisecond)
	end := start.Add(time.Hour)
	// A global window (all systems) and a system-scoped one.
	global, err := store.CreateQuietHour(ctx, tenant, "user_qh_1", QuietHourWindow{SystemID: "", Type: "daily", Start: start, End: end})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateQuietHour(ctx, tenant, "user_qh_1", QuietHourWindow{SystemID: "sys_a", Type: "one-time", Start: start, End: end}); err != nil {
		t.Fatal(err)
	}

	list, err := store.ListQuietHours(ctx, tenant, "user_qh_1")
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %+v err=%v", list, err)
	}

	// The alert path resolves by PocketBase id; sys_a sees global + its own window.
	forA, err := store.QuietHoursForExternalSubject(ctx, "pocketbase", "pb_qh_1", "sys_a")
	if err != nil || len(forA) != 2 {
		t.Fatalf("for sys_a = %+v err=%v", forA, err)
	}
	// A different system sees only the global window.
	forB, err := store.QuietHoursForExternalSubject(ctx, "pocketbase", "pb_qh_1", "sys_b")
	if err != nil || len(forB) != 1 || forB[0].SystemID != "" {
		t.Fatalf("for sys_b = %+v err=%v", forB, err)
	}

	// Update the global window's schedule.
	global.End = end.Add(time.Hour)
	updated, err := store.UpdateQuietHour(ctx, tenant, "user_qh_1", global)
	if err != nil || !updated.End.Equal(global.End) {
		t.Fatalf("update = %+v err=%v", updated, err)
	}

	// Delete one; the list shrinks to one.
	if err := store.DeleteQuietHour(ctx, tenant, "user_qh_1", global.ID); err != nil {
		t.Fatal(err)
	}
	after, _ := store.ListQuietHours(ctx, tenant, "user_qh_1")
	if len(after) != 1 {
		t.Fatalf("after delete = %+v", after)
	}

	// Deleting a stranger's window (wrong user) is a no-op miss.
	if err := store.DeleteQuietHour(ctx, tenant, "user_qh_1", ID(fmt.Sprintf("%s_missing", global.ID))); err == nil {
		t.Fatal("deleting unknown window must report no rows")
	}

	// An unknown PocketBase subject resolves to empty (not an error).
	empty, err := store.QuietHoursForExternalSubject(ctx, "pocketbase", "pb_unknown", "sys_a")
	if err != nil || len(empty) != 0 {
		t.Fatalf("unknown subject = %+v err=%v", empty, err)
	}
}
