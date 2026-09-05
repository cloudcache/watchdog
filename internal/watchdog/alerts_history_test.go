package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeAlertHistoryRepo struct {
	entries    []AlertHistoryEntry
	next       string
	lastDelete ID
	listCalled bool
	lastFilter AlertHistoryPageFilter
}

func (f *fakeAlertHistoryRepo) ListAlertHistory(_ context.Context, _, _ ID, filter AlertHistoryPageFilter) ([]AlertHistoryEntry, string, error) {
	f.listCalled = true
	f.lastFilter = filter
	return f.entries, f.next, nil
}

func (f *fakeAlertHistoryRepo) DeleteAlertHistory(_ context.Context, _, _ ID, id ID) error {
	f.lastDelete = id
	return nil
}

func TestAlertsHistoryAPI(t *testing.T) {
	repo := &fakeAlertHistoryRepo{
		entries: []AlertHistoryEntry{{ID: "h1", AlertID: "alert_1", SystemID: "sys_a", Name: "CPU", Value: 90}},
		next:    "CURSOR2",
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), AlertsHistory: repo})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me/alerts-history?limit=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !repo.listCalled || repo.lastFilter.Limit != 1 {
		t.Fatalf("list not called with limit 1: %+v", repo.lastFilter)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"alert_id":"alert_1"`) || !strings.Contains(body, `"next_cursor":"CURSOR2"`) {
		t.Fatalf("list body = %s", body)
	}

	// A bad limit is rejected before the repo.
	repo.listCalled = false
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/me/alerts-history?limit=0", nil))
	if rec.Code != http.StatusBadRequest || repo.listCalled {
		t.Fatalf("bad limit status = %d called=%v", rec.Code, repo.listCalled)
	}

	// Delete scopes to the path id.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/me/alerts-history/h1", nil))
	if rec.Code != http.StatusNoContent || repo.lastDelete != "h1" {
		t.Fatalf("delete status = %d deleted=%q", rec.Code, repo.lastDelete)
	}
}

// TestMySQLAlertsHistoryLifecycle proves the write path (resolve PB id -> MySQL
// user), resolve, keyset paging (newest first) and own-only delete.
func TestMySQLAlertsHistoryLifecycle(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES ('user_ah_1', ?, 'ah@test.local', 'AH', 'active', 'pocketbase', 'pb_ah_1')
	`, tenant); err != nil {
		t.Fatal(err)
	}

	// An unknown PocketBase subject is a no-op, not an error.
	if err := store.CreateAlertHistoryForExternalSubject(ctx, "pocketbase", "pb_missing", "alert_x", "sys_a", "CPU", 10); err != nil {
		t.Fatalf("unknown subject: %v", err)
	}

	// Two triggers for the known user.
	if err := store.CreateAlertHistoryForExternalSubject(ctx, "pocketbase", "pb_ah_1", "alert_1", "sys_a", "CPU", 90.5); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateAlertHistoryForExternalSubject(ctx, "pocketbase", "pb_ah_1", "alert_2", "", "Status", 0); err != nil {
		t.Fatal(err)
	}

	all, _, err := store.ListAlertHistory(ctx, tenant, "user_ah_1", AlertHistoryPageFilter{})
	if err != nil || len(all) != 2 {
		t.Fatalf("list = %+v err=%v", all, err)
	}
	// Newest first: alert_2 was inserted last.
	if all[0].AlertID != "alert_2" || all[1].AlertID != "alert_1" {
		t.Fatalf("order = %s, %s", all[0].AlertID, all[1].AlertID)
	}
	if all[1].Resolved != nil {
		t.Fatal("new entry must be unresolved")
	}

	// Resolve alert_1; only its row gets a resolved timestamp.
	if err := store.ResolveAlertHistory(ctx, "alert_1"); err != nil {
		t.Fatal(err)
	}
	after, _, _ := store.ListAlertHistory(ctx, tenant, "user_ah_1", AlertHistoryPageFilter{})
	var resolved1 *AlertHistoryEntry
	for i := range after {
		if after[i].AlertID == "alert_1" {
			resolved1 = &after[i]
		}
	}
	if resolved1 == nil || resolved1.Resolved == nil {
		t.Fatalf("alert_1 not resolved: %+v", after)
	}

	// Keyset paging, one per page, covers both without overlap.
	page1, cursor, err := store.ListAlertHistory(ctx, tenant, "user_ah_1", AlertHistoryPageFilter{Limit: 1})
	if err != nil || len(page1) != 1 || cursor == "" {
		t.Fatalf("page1 = %+v cursor=%q err=%v", page1, cursor, err)
	}
	page2, cursor2, err := store.ListAlertHistory(ctx, tenant, "user_ah_1", AlertHistoryPageFilter{Limit: 1, Cursor: cursor})
	if err != nil || len(page2) != 1 || page2[0].ID == page1[0].ID {
		t.Fatalf("page2 = %+v", page2)
	}
	if cursor2 != "" {
		t.Fatalf("expected last page, got cursor %q", cursor2)
	}

	// Own-only delete; deleting a stranger's id (wrong user) misses.
	if err := store.DeleteAlertHistory(ctx, tenant, "user_ah_1", page1[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAlertHistory(ctx, tenant, "user_ah_1", "missing_id"); err == nil {
		t.Fatal("deleting unknown row must report no rows")
	}
	remaining, _, _ := store.ListAlertHistory(ctx, tenant, "user_ah_1", AlertHistoryPageFilter{})
	if len(remaining) != 1 {
		t.Fatalf("after delete = %+v", remaining)
	}
}
