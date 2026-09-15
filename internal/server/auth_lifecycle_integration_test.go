package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestAuthSessionLifecycle freezes the local MySQL authentication boundary on
// a disposable database. It deliberately exercises the public router instead
// of calling handlers directly so cookie, CSRF and RBAC middleware remain part
// of the contract.
func TestAuthSessionLifecycle(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_auth_lifecycle_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	s, err := New(Config{MySQL: MySQLConfig{DSN: dsn}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	install := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "auth-admin", "password": "original-password", "email": "admin@example.test",
	}, nil)
	if install.Code != http.StatusCreated {
		t.Fatalf("install: status=%d body=%s", install.Code, install.Body.String())
	}

	invalid := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "auth-admin", "password": "wrong-password",
	}, nil)
	if invalid.Code != http.StatusUnauthorized || !strings.Contains(invalid.Body.String(), `"code":"invalid_credentials"`) ||
		strings.Contains(invalid.Body.String(), "auth-admin") || strings.Contains(invalid.Body.String(), "wrong-password") {
		t.Fatalf("invalid login leaked credential detail: status=%d body=%s", invalid.Code, invalid.Body.String())
	}

	first := loginForAuthLifecycle(t, s, "original-password")
	firstSession := cookieValue(first.Result().Cookies(), sessionCookie)
	firstCSRF := cookieValue(first.Result().Cookies(), csrfCookie)
	second := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "auth-admin", "password": "original-password",
	}, nil, first.Result().Cookies()...)
	if second.Code != http.StatusOK {
		t.Fatalf("second login: status=%d body=%s", second.Code, second.Body.String())
	}
	secondCookies := second.Result().Cookies()
	secondSession := cookieValue(secondCookies, sessionCookie)
	if firstSession == "" || secondSession == "" || firstSession == secondSession {
		t.Fatalf("login did not rotate the browser session token: first=%q second=%q", firstSession, secondSession)
	}
	var rawTokens, hashedTokens int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE token_sha256 IN (?,?)`, firstSession, secondSession).Scan(&rawTokens); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE token_sha256 IN (?,?)`, sha256hex(firstSession), sha256hex(secondSession)).Scan(&hashedTokens); err != nil {
		t.Fatal(err)
	}
	if rawTokens != 0 || hashedTokens != 2 {
		t.Fatalf("session persistence boundary: raw=%d hashed=%d", rawTokens, hashedTokens)
	}
	if containsSensitiveAuthValue(second.Body.String(), "original-password", firstSession, secondSession, firstCSRF, cookieValue(secondCookies, csrfCookie)) {
		t.Fatalf("login response exposed an authentication secret: %s", second.Body.String())
	}

	if _, err := s.db.Exec(`UPDATE sessions SET expires_at=DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 SECOND) WHERE token_sha256=?`, sha256hex(secondSession)); err != nil {
		t.Fatal(err)
	}
	expired := requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, secondCookies...)
	assertHTTPErrorCode(t, expired, http.StatusUnauthorized, "unauthorized")
	firstCurrent := requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, first.Result().Cookies()...)
	if firstCurrent.Code != http.StatusOK {
		t.Fatalf("independent session was expired: status=%d body=%s", firstCurrent.Code, firstCurrent.Body.String())
	}

	missingCSRF := requestJSON(t, s, http.MethodPost, "/api/v1/me/password", map[string]string{
		"current_password": "original-password", "new_password": "replacement-password",
	}, nil, first.Result().Cookies()...)
	assertHTTPErrorCode(t, missingCSRF, http.StatusForbidden, "csrf")
	wrongCSRF := requestJSON(t, s, http.MethodPost, "/api/v1/me/password", map[string]string{
		"current_password": "original-password", "new_password": "replacement-password",
	}, map[string]string{"X-CSRF-Token": "wrong-token"}, first.Result().Cookies()...)
	assertHTTPErrorCode(t, wrongCSRF, http.StatusForbidden, "csrf")
	changed := requestJSON(t, s, http.MethodPost, "/api/v1/me/password", map[string]string{
		"current_password": "original-password", "new_password": "replacement-password",
	}, map[string]string{"X-CSRF-Token": firstCSRF}, first.Result().Cookies()...)
	if changed.Code != http.StatusNoContent {
		t.Fatalf("change password: status=%d body=%s", changed.Code, changed.Body.String())
	}
	assertHTTPErrorCode(t,
		requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, first.Result().Cookies()...),
		http.StatusUnauthorized, "unauthorized")
	if oldPassword := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "auth-admin", "password": "original-password",
	}, nil); oldPassword.Code != http.StatusUnauthorized {
		t.Fatalf("old password remained valid: status=%d body=%s", oldPassword.Code, oldPassword.Body.String())
	}

	admin := loginForAuthLifecycle(t, s, "replacement-password")
	adminCookies := admin.Result().Cookies()
	adminCSRF := cookieValue(adminCookies, csrfCookie)
	created := requestJSON(t, s, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "auth-viewer", "password": "viewer-password", "roles": []string{"viewer"},
	}, map[string]string{"X-CSRF-Token": adminCSRF}, adminCookies...)
	if created.Code != http.StatusCreated {
		t.Fatalf("create viewer: status=%d body=%s", created.Code, created.Body.String())
	}
	var viewer struct {
		ID string `json:"id"`
	}
	decodeJSON(t, created, &viewer)
	users := requestJSON(t, s, http.MethodGet, "/api/v1/users", nil, nil, adminCookies...)
	if users.Code != http.StatusOK || containsSensitiveAuthValue(users.Body.String(), "replacement-password", "viewer-password", "password_hash", "token_sha256") {
		t.Fatalf("user list exposed a secret: status=%d body=%s", users.Code, users.Body.String())
	}
	viewerLogin := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "auth-viewer", "password": "viewer-password",
	}, nil)
	if viewerLogin.Code != http.StatusOK {
		t.Fatalf("viewer login: status=%d body=%s", viewerLogin.Code, viewerLogin.Body.String())
	}
	viewerCookies := viewerLogin.Result().Cookies()
	assertHTTPErrorCode(t, requestJSON(t, s, http.MethodGet, "/api/v1/users", nil, nil, viewerCookies...), http.StatusForbidden, "forbidden")

	disabled := requestJSON(t, s, http.MethodPatch, "/api/v1/users/"+viewer.ID, map[string]string{"status": "disabled"},
		map[string]string{"X-CSRF-Token": adminCSRF}, adminCookies...)
	if disabled.Code != http.StatusOK {
		t.Fatalf("disable viewer: status=%d body=%s", disabled.Code, disabled.Body.String())
	}
	assertHTTPErrorCode(t, requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, viewerCookies...), http.StatusUnauthorized, "unauthorized")

	logout := requestJSON(t, s, http.MethodPost, "/api/v1/session/logout", nil,
		map[string]string{"X-CSRF-Token": adminCSRF}, adminCookies...)
	if logout.Code != http.StatusNoContent {
		t.Fatalf("logout: status=%d body=%s", logout.Code, logout.Body.String())
	}
	assertHTTPErrorCode(t, requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, adminCookies...), http.StatusUnauthorized, "unauthorized")
}

func loginForAuthLifecycle(t *testing.T, s *Server, password string) *httptest.ResponseRecorder {
	t.Helper()
	response := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "auth-admin", "password": password,
	}, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("login: status=%d body=%s", response.Code, response.Body.String())
	}
	return response
}

func containsSensitiveAuthValue(body string, values ...string) bool {
	for _, value := range values {
		if value != "" && strings.Contains(body, value) {
			return true
		}
	}
	return false
}

func assertHTTPErrorCode(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status || !strings.Contains(response.Body.String(), `"code":"`+code+`"`) ||
		!strings.Contains(response.Body.String(), `"retryable":false`) {
		t.Fatalf("error response: status=%d body=%s", response.Code, response.Body.String())
	}
}
