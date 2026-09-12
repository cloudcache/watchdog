// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestFlowSavedFiltersAPI drives the KISS-06 saved-filter CRUD surface against a
// real, empty MySQL through the gin router + RBAC: create/list/get with ETag,
// If-Match/412 optimistic concurrency on update, shared-scope creation, owner
// facets, and soft delete followed by 404. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestFlowSavedFiltersAPI(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_saved_filters_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	cfg := Config{
		MySQL:   MySQLConfig{DSN: parsed.FormatDSN()},
		Admin:   AdminConfig{Username: "sf-admin", Password: "sf-password"},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "sf-admin", "password": "sf-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: %d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	if csrf == "" {
		t.Fatal("login did not issue a CSRF token")
	}
	post := map[string]string{"X-CSRF-Token": csrf}
	field := func(resp *httptest.ResponseRecorder, key string) any {
		t.Helper()
		var body map[string]any
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode %q: %v (%s)", key, err, resp.Body.String())
		}
		return body[key]
	}
	predicate := map[string]any{"op": "predicate", "field": "src_ip", "operator": "in", "values": []string{"203.0.113.0/24"}}

	// --- create private -> 201 + ETag, can_edit true ---
	created := requestJSON(t, s, http.MethodPost, "/api/v1/flow/filters", map[string]any{
		"name": "Overseas egress", "description": "customer egress abroad", "filter": predicate,
	}, post, cookies...)
	if created.Code != http.StatusCreated || created.Header().Get("ETag") == "" ||
		field(created, "share_scope") != "private" || field(created, "can_edit") != true {
		t.Fatalf("create: %d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	filterID, _ := field(created, "id").(string)
	firstETag := created.Header().Get("ETag")

	// --- get echoes the ETag ---
	got := requestJSON(t, s, http.MethodGet, "/api/v1/flow/filters/"+filterID, nil, nil, cookies...)
	if got.Code != http.StatusOK || got.Header().Get("ETag") != firstETag {
		t.Fatalf("get: %d etag=%q want %q", got.Code, got.Header().Get("ETag"), firstETag)
	}

	// --- list includes it and reports can_share for the admin ---
	list := requestJSON(t, s, http.MethodGet, "/api/v1/flow/filters?scope=private", nil, nil, cookies...)
	if list.Code != http.StatusOK {
		t.Fatalf("list: %d %s", list.Code, list.Body.String())
	}
	if meta, _ := field(list, "meta").(map[string]any); meta == nil || meta["can_share"] != true {
		t.Fatalf("list meta = %v", field(list, "meta"))
	}

	// --- update with the correct If-Match rolls the row version ---
	patch := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/filters/"+filterID, map[string]any{
		"name": "Overseas egress v2", "filter": predicate,
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": firstETag}, cookies...)
	if patch.Code != http.StatusOK || patch.Header().Get("ETag") == firstETag || field(patch, "name") != "Overseas egress v2" {
		t.Fatalf("patch: %d etag=%q body=%s", patch.Code, patch.Header().Get("ETag"), patch.Body.String())
	}

	// --- a stale If-Match is rejected 412 ---
	stale := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/filters/"+filterID, map[string]any{
		"name": "should fail", "filter": predicate,
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": firstETag}, cookies...)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale patch: %d %s", stale.Code, stale.Body.String())
	}

	// --- admin may create a shared filter ---
	shared := requestJSON(t, s, http.MethodPost, "/api/v1/flow/filters", map[string]any{
		"name": "Team shared", "share_scope": "shared", "filter": predicate,
	}, post, cookies...)
	if shared.Code != http.StatusCreated || field(shared, "share_scope") != "shared" {
		t.Fatalf("create shared: %d %s", shared.Code, shared.Body.String())
	}

	// --- owner facet lists the admin owner ---
	owners := requestJSON(t, s, http.MethodGet, "/api/v1/flow/filters/facets/owners", nil, nil, cookies...)
	if owners.Code != http.StatusOK {
		t.Fatalf("owners: %d %s", owners.Code, owners.Body.String())
	}
	if items, _ := field(owners, "items").([]any); len(items) == 0 {
		t.Fatalf("owners items empty: %s", owners.Body.String())
	}

	// --- delete with If-Match, then 404 ---
	latestETag := patch.Header().Get("ETag")
	del := requestJSON(t, s, http.MethodDelete, "/api/v1/flow/filters/"+filterID, nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": latestETag}, cookies...)
	if del.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", del.Code, del.Body.String())
	}
	after := requestJSON(t, s, http.MethodGet, "/api/v1/flow/filters/"+filterID, nil, nil, cookies...)
	if after.Code != http.StatusNotFound {
		t.Fatalf("get after delete: %d %s", after.Code, after.Body.String())
	}
}
