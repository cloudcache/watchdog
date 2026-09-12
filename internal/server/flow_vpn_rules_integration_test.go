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

// TestFlowVPNRulesAPI drives the KISS-06 VPN rule CRUD surface against a real,
// empty MySQL through the gin router + RBAC: create/list/get with ETag, If-Match
// optimistic concurrency, unique-name 409, and soft delete. Opt-in via
// WATCHDOG_TEST_MYSQL_DSN.
func TestFlowVPNRulesAPI(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_vpn_rules_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	cfg := Config{
		MySQL:   MySQLConfig{DSN: parsed.FormatDSN()},
		Admin:   AdminConfig{Username: "vpn-admin", Password: "vpn-password"},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "vpn-admin", "password": "vpn-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: %d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	post := map[string]string{"X-CSRF-Token": csrf}
	field := func(resp *httptest.ResponseRecorder, key string) any {
		t.Helper()
		var body map[string]any
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode %q: %v (%s)", key, err, resp.Body.String())
		}
		return body[key]
	}
	ruleBody := map[string]any{
		"name": "Risk HTTPS", "kind": "passive", "effect": "score", "weight": 25,
		"match": map[string]any{"remote_ports": []int{443}},
	}

	// --- create -> 201 + ETag, defaults applied ---
	created := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rules", ruleBody, post, cookies...)
	if created.Code != http.StatusCreated || created.Header().Get("ETag") == "" ||
		field(created, "effect") != "score" || field(created, "status") != "draft" {
		t.Fatalf("create: %d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	ruleID, _ := field(created, "id").(string)
	firstETag := created.Header().Get("ETag")

	// --- duplicate name -> 409 ---
	dup := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rules", ruleBody, post, cookies...)
	if dup.Code != http.StatusConflict {
		t.Fatalf("duplicate name: %d %s", dup.Code, dup.Body.String())
	}

	// --- get echoes the ETag ---
	got := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/rules/"+ruleID, nil, nil, cookies...)
	if got.Code != http.StatusOK || got.Header().Get("ETag") != firstETag {
		t.Fatalf("get: %d etag=%q want %q", got.Code, got.Header().Get("ETag"), firstETag)
	}

	// --- list includes it ---
	list := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/rules?status=draft", nil, nil, cookies...)
	if list.Code != http.StatusOK || field(list, "total").(float64) < 1 {
		t.Fatalf("list: %d %s", list.Code, list.Body.String())
	}

	// --- update with matching If-Match rolls the row version ---
	patch := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/vpn/rules/"+ruleID, map[string]any{
		"name": "Risk HTTPS", "kind": "passive", "effect": "score", "weight": 40, "status": "active",
		"match": map[string]any{"remote_ports": []int{443}},
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": firstETag}, cookies...)
	if patch.Code != http.StatusOK || field(patch, "status") != "active" || patch.Header().Get("ETag") == firstETag {
		t.Fatalf("patch: %d etag=%q body=%s", patch.Code, patch.Header().Get("ETag"), patch.Body.String())
	}

	// --- stale If-Match -> 412 ---
	stale := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/vpn/rules/"+ruleID, ruleBody,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": firstETag}, cookies...)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale patch: %d %s", stale.Code, stale.Body.String())
	}

	// --- delete then 404 ---
	del := requestJSON(t, s, http.MethodDelete, "/api/v1/flow/vpn/rules/"+ruleID, nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": patch.Header().Get("ETag")}, cookies...)
	if del.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", del.Code, del.Body.String())
	}
	after := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/rules/"+ruleID, nil, nil, cookies...)
	if after.Code != http.StatusNotFound {
		t.Fatalf("get after delete: %d %s", after.Code, after.Body.String())
	}
}
