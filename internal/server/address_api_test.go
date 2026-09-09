package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestAddressLibraryAPI drives the KISS-05 address HTTP surface against a real,
// empty MySQL through the gin router + RBAC: editable CRUD with server-side
// pagination and If-Match/428/412/ETag CAS, previews, draft revisions, and the
// corrected import/publish contracts. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestAddressLibraryAPI(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_addr_api_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	cfg := Config{
		MySQL:   MySQLConfig{DSN: parsed.FormatDSN()},
		Admin:   AdminConfig{Username: "addr-api-admin", Password: "addr-api-password"},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "addr-api-admin", "password": "addr-api-password",
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

	// --- address prefix: create -> 201 + ETag, get echoes it ---
	created := requestJSON(t, s, http.MethodPost, "/api/v1/address-prefixes", map[string]any{
		"cidr": "192.0.2.0/24", "source": "manual", "labels": map[string]string{"tier": "gold"},
	}, post, cookies...)
	if created.Code != http.StatusCreated || created.Header().Get("ETag") == "" || field(created, "cidr") != "192.0.2.0/24" {
		t.Fatalf("create prefix: %d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	prefixID, _ := field(created, "id").(string)
	firstETag := created.Header().Get("ETag")

	got := requestJSON(t, s, http.MethodGet, "/api/v1/address-prefixes/"+prefixID, nil, nil, cookies...)
	if got.Code != http.StatusOK || got.Header().Get("ETag") != firstETag {
		t.Fatalf("get prefix: %d etag=%q want %q", got.Code, got.Header().Get("ETag"), firstETag)
	}

	// server-side list contract (table mode)
	list := requestJSON(t, s, http.MethodGet, "/api/v1/address-prefixes?limit=10&sort=cidr", nil, nil, cookies...)
	if list.Code != http.StatusOK || field(list, "total") == nil || field(list, "limit") == nil {
		t.Fatalf("list prefixes: %d %s", list.Code, list.Body.String())
	}

	// strict param whitelist -> 400
	if bad := requestJSON(t, s, http.MethodGet, "/api/v1/address-prefixes?bogus=1", nil, nil, cookies...); bad.Code != http.StatusBadRequest {
		t.Fatalf("unknown query param should be 400, got %d", bad.Code)
	}

	// update without If-Match -> 428
	if r := requestJSON(t, s, http.MethodPatch, "/api/v1/address-prefixes/"+prefixID, map[string]any{"source": "vendor"}, post, cookies...); r.Code != http.StatusPreconditionRequired {
		t.Fatalf("update without If-Match should be 428, got %d %s", r.Code, r.Body.String())
	}
	// update with stale If-Match -> 412
	if r := requestJSON(t, s, http.MethodPatch, "/api/v1/address-prefixes/"+prefixID, map[string]any{"source": "vendor"},
		map[string]string{"X-CSRF-Token": csrf, "If-Match": `"999"`}, cookies...); r.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match should be 412, got %d", r.Code)
	}
	// update with correct If-Match -> 200 + bumped ETag
	updated := requestJSON(t, s, http.MethodPatch, "/api/v1/address-prefixes/"+prefixID, map[string]any{"source": "vendor"},
		map[string]string{"X-CSRF-Token": csrf, "If-Match": firstETag}, cookies...)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") == firstETag || field(updated, "source") != "vendor" {
		t.Fatalf("update prefix: %d etag=%q body=%s", updated.Code, updated.Header().Get("ETag"), updated.Body.String())
	}

	// merge-preview + set-op preview reuse the internal/address engine
	if r := requestJSON(t, s, http.MethodPost, "/api/v1/address-prefixes/actions/merge-preview", map[string]any{
		"prefixes": []map[string]any{{"cidr": "192.0.2.0/25"}, {"cidr": "192.0.2.128/25"}},
	}, post, cookies...); r.Code != http.StatusOK {
		t.Fatalf("merge-preview: %d %s", r.Code, r.Body.String())
	}
	if r := requestJSON(t, s, http.MethodPost, "/api/v1/address-sets/actions/preview", map[string]any{
		"operation": "normalize", "left": []string{"10.0.0.0/24", "10.0.1.0/24"},
	}, post, cookies...); r.Code != http.StatusOK {
		t.Fatalf("set preview: %d %s", r.Code, r.Body.String())
	}

	// --- draft revision: preview -> apply (If-Match) ---
	preview := requestJSON(t, s, http.MethodPost, "/api/v1/address-draft-revisions/preview", map[string]any{
		"operations": []map[string]any{{"action": "create", "cidr": "203.0.113.0/24"}},
	}, post, cookies...)
	if preview.Code != http.StatusCreated {
		t.Fatalf("draft preview: %d %s", preview.Code, preview.Body.String())
	}
	revisionID, _ := field(preview, "id").(string)
	apply := requestJSON(t, s, http.MethodPost, "/api/v1/address-draft-revisions/"+revisionID+"/apply", nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": preview.Header().Get("ETag")}, cookies...)
	if apply.Code != http.StatusOK || field(apply, "status") != "applied" {
		t.Fatalf("draft apply: %d %s", apply.Code, apply.Body.String())
	}

	// --- the invented import retry endpoint is gone (404) ---
	if r := requestJSON(t, s, http.MethodPost, "/api/v1/address-imports/anything/actions/retry", nil, post, cookies...); r.Code != http.StatusNotFound {
		t.Fatalf("retry endpoint should be gone (404), got %d", r.Code)
	}

	// --- publish validates effective_from (bad -> 400), not a random-id path ---
	if r := requestJSON(t, s, http.MethodPost, "/api/v1/dimensions/address/publish", map[string]any{
		"effective_from": "not-a-time", "preview_digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	}, post, cookies...); r.Code != http.StatusBadRequest {
		t.Fatalf("publish with bad effective_from should be 400, got %d %s", r.Code, r.Body.String())
	}

	// consumers status with no activation -> 404 (not a 500)
	if r := requestJSON(t, s, http.MethodGet, "/api/v1/dimensions/address/status", nil, nil, cookies...); r.Code != http.StatusNotFound {
		t.Fatalf("consumer status with no activation should be 404, got %d %s", r.Code, r.Body.String())
	}

	// delete with If-Match -> 204
	if r := requestJSON(t, s, http.MethodDelete, "/api/v1/address-prefixes/"+prefixID, nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": updated.Header().Get("ETag")}, cookies...); r.Code != http.StatusNoContent {
		t.Fatalf("delete prefix: %d %s", r.Code, r.Body.String())
	}
}
