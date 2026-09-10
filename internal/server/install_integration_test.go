package server

import (
	"net/http"
	"os"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestInstallRequestValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		request installRequest
		valid   bool
	}{
		{name: "valid", request: installRequest{Username: " admin ", Password: "password-123"}, valid: true},
		{name: "missing username", request: installRequest{Password: "password-123"}},
		{name: "short password", request: installRequest{Username: "admin", Password: "short"}},
		{name: "long password", request: installRequest{Username: "admin", Password: strings.Repeat("x", 73)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.request.normalizeAndValidate()
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t error=%v", test.valid, err)
			}
		})
	}
}

// TestFreshInstallLifecycle proves the interactive install contract against a
// disposable real MySQL database. ClickHouse migrations are exercised by the
// deployment smoke test because the production database name is intentionally
// fixed to watchdog_flow.
func TestFreshInstallLifecycle(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_install_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	s, err := New(Config{MySQL: MySQLConfig{DSN: dsn}})
	if err != nil {
		t.Fatalf("start fresh server: %v", err)
	}
	defer s.Close()

	var tableCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 {
		t.Fatalf("startup mutated empty database: tables=%d", tableCount)
	}

	status := requestJSON(t, s, http.MethodGet, "/api/v1/install-status", nil, nil)
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"requires_install":true`) {
		t.Fatalf("fresh status: code=%d body=%s", status.Code, status.Body.String())
	}
	health := requestJSON(t, s, http.MethodGet, "/api/v1/health", nil, nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"install_required"`) {
		t.Fatalf("fresh health: code=%d body=%s", health.Code, health.Body.String())
	}
	blockedLogin := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if blockedLogin.Code != http.StatusPreconditionRequired || !strings.Contains(blockedLogin.Body.String(), "install_required") {
		t.Fatalf("login before install: code=%d body=%s", blockedLogin.Code, blockedLogin.Body.String())
	}

	installed := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "admin", "password": "install-password", "email": "admin@example.test", "display_name": "Administrator",
	}, nil)
	if installed.Code != http.StatusCreated || !strings.Contains(installed.Body.String(), `"installed":true`) || !strings.Contains(installed.Body.String(), `"runtime_ready":true`) {
		t.Fatalf("install: code=%d body=%s", installed.Code, installed.Body.String())
	}

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if login.Code != http.StatusOK || cookieValue(login.Result().Cookies(), sessionCookie) == "" {
		t.Fatalf("login after install: code=%d body=%s", login.Code, login.Body.String())
	}
	repeat := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "other", "password": "other-password",
	}, nil)
	if repeat.Code != http.StatusConflict || !strings.Contains(repeat.Body.String(), "already_installed") {
		t.Fatalf("repeat install: code=%d body=%s", repeat.Code, repeat.Body.String())
	}
	var users int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&users); err != nil || users != 1 {
		t.Fatalf("administrator cardinality: users=%d error=%v", users, err)
	}
}
