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

// TestInstallAndManagementPlaneSurviveUnavailableClickHouse freezes the
// deployment boundary: MySQL installation, authentication and device APIs do
// not depend on ClickHouse availability. Telemetry health/query surfaces stay
// explicit and fail closed until the dependency is fixed and the server is
// restarted. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestInstallAndManagementPlaneSurviveUnavailableClickHouse(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_install_ch_degraded_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	cfg := Config{
		MySQL: MySQLConfig{DSN: dsn},
		ClickHouse: ClickHouseConfig{
			Address: "127.0.0.1:1", Database: "watchdog_flow", Username: "default",
		},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("start fresh server: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()
	installed := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if installed.Code != http.StatusCreated || !strings.Contains(installed.Body.String(), `"runtime_ready":true`) {
		t.Fatalf("install with unavailable ClickHouse: code=%d body=%s", installed.Code, installed.Body.String())
	}

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login with unavailable ClickHouse: code=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	devices := requestJSON(t, s, http.MethodGet, "/api/v1/devices?limit=1", nil, nil, cookies...)
	if devices.Code != http.StatusOK {
		t.Fatalf("management API with unavailable ClickHouse: code=%d body=%s", devices.Code, devices.Body.String())
	}
	health := requestJSON(t, s, http.MethodGet, "/api/v1/health", nil, nil)
	if health.Code != http.StatusServiceUnavailable ||
		!strings.Contains(health.Body.String(), `"clickhouse":false`) ||
		!strings.Contains(health.Body.String(), `"runtime_ready":true`) ||
		!strings.Contains(health.Body.String(), `"clickhouse_recovery"`) {
		t.Fatalf("degraded health: code=%d body=%s", health.Code, health.Body.String())
	}
	csrf := cookieValue(cookies, csrfCookie)
	flow := requestJSON(t, s, http.MethodPost, "/api/v1/flow/query", nil,
		map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if flow.Code != http.StatusServiceUnavailable || !strings.Contains(flow.Body.String(), "flow_query_unavailable") {
		t.Fatalf("Flow query without ClickHouse: code=%d body=%s", flow.Code, flow.Body.String())
	}
	if s.jobs == nil {
		t.Fatal("management operation job store was not initialized")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	restarted, err := New(cfg)
	if err != nil {
		t.Fatalf("restart installed server with unavailable ClickHouse: %v", err)
	}
	defer restarted.Close()
	relogin := requestJSON(t, restarted, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if relogin.Code != http.StatusOK {
		t.Fatalf("login after degraded restart: code=%d body=%s", relogin.Code, relogin.Body.String())
	}
}

// TestInstalledRuntimeStartsClickHouseWorkers proves the positive half of the
// same boundary against real MySQL and ClickHouse: once ClickHouse is ready,
// every query/service worker that depends on it is wired to the single shared
// operation-job store. Opt-in to avoid requiring ClickHouse for unit runs.
func TestInstalledRuntimeStartsClickHouseWorkers(t *testing.T) {
	if os.Getenv("WATCHDOG_SERVER_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("WATCHDOG_SERVER_CLICKHOUSE_INTEGRATION is not set")
	}
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Fatal("WATCHDOG_TEST_MYSQL_DSN is required")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_install_ch_ready_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	root := t.TempDir()
	secret := root + "/clickhouse-password"
	if err := os.WriteFile(secret, []byte(os.Getenv("WATCHDOG_SNMP_CLICKHOUSE_PASSWORD")), 0o600); err != nil {
		t.Fatal(err)
	}
	clickHouseAddress := strings.TrimSpace(os.Getenv("WATCHDOG_TEST_CLICKHOUSE_ADDRESS"))
	if clickHouseAddress == "" {
		clickHouseAddress = "127.0.0.1:9000"
	}
	cfg := Config{
		MySQL: MySQLConfig{DSN: dsn},
		ClickHouse: ClickHouseConfig{
			Address: clickHouseAddress, Database: "watchdog_flow", Username: "default", PasswordFile: secret,
		},
		Address: AddressConfig{ArtifactDir: root + "/address-imports", SnapshotDir: root + "/address-snapshots"},
		Flow:    FlowConfig{Export: FlowExportConfig{Dir: root + "/flow-exports"}},
		SNMP:    SNMPConfig{ExportDir: root + "/snmp-exports"},
		Billing: BillingConfig{ExportDir: root + "/billing-exports"},
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("start fresh server: %v", err)
	}
	defer s.Close()
	installed := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if installed.Code != http.StatusCreated {
		t.Fatalf("install with ClickHouse: code=%d body=%s", installed.Code, installed.Body.String())
	}
	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login with ClickHouse: code=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	enrollment := requestJSON(t, s, http.MethodPost, "/api/v1/agents/enrollment-tokens", map[string]any{
		"kind": "snmp", "expires_in_seconds": 300,
	}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	var enrollmentBody struct {
		Token string `json:"token"`
	}
	decodeJSON(t, enrollment, &enrollmentBody)
	if enrollment.Code != http.StatusCreated || enrollmentBody.Token == "" {
		t.Fatalf("clean-stack enrollment: code=%d body=%s", enrollment.Code, enrollment.Body.String())
	}
	registered := requestJSON(t, s, http.MethodPost, "/api/v1/agents/register", map[string]any{
		"id": "clean_stack_snmp", "enrollment_token": enrollmentBody.Token, "name": "Clean stack SNMP", "kind": "snmp",
		"capabilities": []string{"snmp.poll/v2"},
	}, nil)
	var registeredBody struct {
		Agent struct {
			ID string `json:"id"`
		} `json:"agent"`
		Credential struct {
			Token string `json:"token"`
		} `json:"credential"`
	}
	decodeJSON(t, registered, &registeredBody)
	if registered.Code != http.StatusCreated || registeredBody.Agent.ID != "clean_stack_snmp" || registeredBody.Credential.Token == "" {
		t.Fatalf("clean-stack register: code=%d body=%s", registered.Code, registered.Body.String())
	}
	heartbeat := requestJSON(t, s, http.MethodPost, "/api/v1/agents/clean_stack_snmp/heartbeat", map[string]any{
		"software_version": "clean-stack", "capabilities": []string{"snmp.poll/v2"},
	}, map[string]string{"X-Watchdog-Agent-Token": registeredBody.Credential.Token})
	if heartbeat.Code != http.StatusAccepted {
		t.Fatalf("clean-stack heartbeat: code=%d body=%s", heartbeat.Code, heartbeat.Body.String())
	}
	if s.jobs == nil || s.clickHouse == nil || s.snmpMetrics == nil || s.flowQuery == nil ||
		s.flowExportCancel == nil || s.snmpExportCancel == nil || s.billingService == nil {
		t.Fatalf("incomplete ClickHouse runtime: jobs=%t ch=%t snmp=%t flow=%t flow_export=%t snmp_export=%t billing=%t",
			s.jobs != nil, s.clickHouse != nil, s.snmpMetrics != nil, s.flowQuery != nil,
			s.flowExportCancel != nil, s.snmpExportCancel != nil, s.billingService != nil)
	}
	health := requestJSON(t, s, http.MethodGet, "/api/v1/health", nil, nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"clickhouse":true`) ||
		!strings.Contains(health.Body.String(), `"runtime_ready":true`) {
		t.Fatalf("healthy runtime: code=%d body=%s", health.Code, health.Body.String())
	}
}
