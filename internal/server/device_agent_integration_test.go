package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// TestDeviceAndAgentAPI exercises the complete management path against a real,
// empty MySQL schema. It is opt-in so ordinary unit tests do not require MySQL.
func TestDeviceAndAgentAPI(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	cfg := Config{
		MySQL: MySQLConfig{DSN: dsn},
		Admin: AdminConfig{Username: "api-test-admin", Password: "api-test-password"},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "api-test-admin", "password": "api-test-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: status=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	if csrf == "" || cookieValue(cookies, sessionCookie) == "" {
		t.Fatalf("login did not issue auth cookies: %v", cookies)
	}
	authHeaders := map[string]string{"X-CSRF-Token": csrf}
	profileCreated := requestJSON(t, s, http.MethodPost, "/api/v1/snmp/profiles", map[string]any{
		"ID": "snmp_api_test", "Name": "API v2c", "Version": "2c",
		"Security": map[string]string{"community": "private-test"}, "Timeout": 5_000_000_000, "Retries": 2,
	}, authHeaders, cookies...)
	if profileCreated.Code != http.StatusCreated || !strings.Contains(profileCreated.Body.String(), `"timeout":5000000000`) {
		t.Fatalf("create SNMP profile: status=%d body=%s", profileCreated.Code, profileCreated.Body.String())
	}
	profiles := requestJSON(t, s, http.MethodGet, "/api/v1/snmp/profiles", nil, nil, cookies...)
	if profiles.Code != http.StatusOK || !strings.Contains(profiles.Body.String(), "snmp_api_test") || strings.Contains(profiles.Body.String(), "private-test") {
		t.Fatalf("list SNMP profiles: status=%d body=%s", profiles.Code, profiles.Body.String())
	}
	profileLoaded := requestJSON(t, s, http.MethodGet, "/api/v1/snmp/profiles/snmp_api_test", nil, nil, cookies...)
	profileUpdated := requestJSON(t, s, http.MethodPatch, "/api/v1/snmp/profiles/snmp_api_test", map[string]any{
		"Retries": 3,
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": profileLoaded.Header().Get("ETag")}, cookies...)
	if profileLoaded.Code != http.StatusOK || profileUpdated.Code != http.StatusOK || !strings.Contains(profileUpdated.Body.String(), `"retries":3`) {
		t.Fatalf("get/patch SNMP profile: get=%d patch=%d body=%s", profileLoaded.Code, profileUpdated.Code, profileUpdated.Body.String())
	}

	created := requestJSON(t, s, http.MethodPost, "/api/v1/devices", map[string]any{
		"host": "Router.Example.COM.", "display_name": "Core router", "kind": "network",
		"labels": map[string]string{"site": "dc-a"}, "snmp_profile_id": "snmp_api_test",
		"snmp_security": map[string]string{"community": "device-private"},
	}, authHeaders, cookies...)
	if created.Code != http.StatusCreated {
		t.Fatalf("create device: status=%d body=%s", created.Code, created.Body.String())
	}
	var device deviceDTO
	decodeJSON(t, created, &device)
	if device.ID == "" || device.Host != "router.example.com" || device.TargetID != device.ID || device.Labels["site"] != "dc-a" || strings.Contains(created.Body.String(), "device-private") {
		t.Fatalf("unexpected device: %+v", device)
	}
	var storedCommunity string
	if err := s.db.QueryRow(`SELECT JSON_UNQUOTE(JSON_EXTRACT(snmp_security_json,'$.community')) FROM devices WHERE id=?`, device.ID).Scan(&storedCommunity); err != nil || storedCommunity != "device-private" {
		t.Fatalf("device SNMP override was not persisted: value=%q err=%v", storedCommunity, err)
	}
	duplicate := requestJSON(t, s, http.MethodPost, "/api/v1/devices", map[string]any{
		"host": "router.example.com",
	}, authHeaders, cookies...)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate host: status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}

	patched := requestJSON(t, s, http.MethodPatch, "/api/v1/targets/"+device.ID, map[string]any{
		"name": "Core-1", "status": "up",
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": created.Header().Get("ETag")}, cookies...)
	if patched.Code != http.StatusOK {
		t.Fatalf("patch device through target alias: status=%d body=%s", patched.Code, patched.Body.String())
	}
	stale := requestJSON(t, s, http.MethodPatch, "/api/v1/devices/"+device.ID, map[string]any{
		"display_name": "stale overwrite",
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": created.Header().Get("ETag")}, cookies...)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale device update: status=%d body=%s", stale.Code, stale.Body.String())
	}

	summary := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/summary?q=core-1", nil, nil, cookies...)
	if summary.Code != http.StatusOK || !strings.Contains(summary.Body.String(), device.ID) {
		t.Fatalf("device summary: status=%d body=%s", summary.Code, summary.Body.String())
	}
	assertDeviceScope(t, s, device.ID)
	systemCreated := requestJSON(t, s, http.MethodPost, "/api/v1/targets", map[string]any{
		"host": "host.example.com", "name": "System host", "kind": "system",
	}, authHeaders, cookies...)
	if systemCreated.Code != http.StatusCreated {
		t.Fatalf("create system target alias: status=%d body=%s", systemCreated.Code, systemCreated.Body.String())
	}
	var systemDevice deviceDTO
	decodeJSON(t, systemCreated, &systemDevice)
	loadedSystem := requestJSON(t, s, http.MethodGet, "/api/v1/targets/"+systemDevice.ID, nil, nil, cookies...)
	if loadedSystem.Code != http.StatusOK || !strings.Contains(loadedSystem.Body.String(), `"kind":"system"`) {
		t.Fatalf("system target kind mapping: status=%d body=%s", loadedSystem.Code, loadedSystem.Body.String())
	}
	hosts := requestJSON(t, s, http.MethodGet, "/api/v1/targets?exclude_kind=network", nil, nil, cookies...)
	if hosts.Code != http.StatusOK || !strings.Contains(hosts.Body.String(), systemDevice.ID) || strings.Contains(hosts.Body.String(), device.ID) {
		t.Fatalf("target kind exclusion: status=%d body=%s", hosts.Code, hosts.Body.String())
	}

	agentToken := "direct-agent-secret"
	createdAgent := requestJSON(t, s, http.MethodPost, "/api/v1/agent-registry", map[string]any{
		"ID": "agent_api_test", "TargetID": device.ID, "AgentType": "snmp", "Mode": "push",
		"Endpoint": "udp://127.0.0.1:161", "Token": agentToken,
	}, authHeaders, cookies...)
	if createdAgent.Code != http.StatusCreated || strings.Contains(createdAgent.Body.String(), agentToken) {
		t.Fatalf("create agent: status=%d body=%s", createdAgent.Code, createdAgent.Body.String())
	}
	loadedAgent := requestJSON(t, s, http.MethodGet, "/api/v1/agents/agent_api_test", nil, nil, cookies...)
	if loadedAgent.Code != http.StatusOK || loadedAgent.Header().Get("ETag") == "" {
		t.Fatalf("get agent: status=%d body=%s", loadedAgent.Code, loadedAgent.Body.String())
	}
	updatedAgent := requestJSON(t, s, http.MethodPatch, "/api/v1/agents/agent_api_test", map[string]any{
		"endpoint": "udp://127.0.0.1:1161",
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": loadedAgent.Header().Get("ETag")}, cookies...)
	if updatedAgent.Code != http.StatusOK || !strings.Contains(updatedAgent.Body.String(), "1161") {
		t.Fatalf("patch agent: status=%d body=%s", updatedAgent.Code, updatedAgent.Body.String())
	}

	heartbeat := requestJSON(t, s, http.MethodPost, "/api/v1/agents/agent_api_test/heartbeat", map[string]any{
		"software_version": "1.2.3", "capabilities": []string{"snmp.poll/v2"},
	}, map[string]string{"X-Watchdog-Agent-Token": agentToken})
	if heartbeat.Code != http.StatusAccepted {
		t.Fatalf("heartbeat: status=%d body=%s", heartbeat.Code, heartbeat.Body.String())
	}
	badRun := requestJSON(t, s, http.MethodPost, "/api/v1/agents/agent_api_test/status", map[string]any{
		"status": "arbitrary",
	}, map[string]string{"X-Watchdog-Agent-Token": agentToken})
	if badRun.Code != http.StatusBadRequest {
		t.Fatalf("invalid run status: status=%d body=%s", badRun.Code, badRun.Body.String())
	}

	run := requestJSON(t, s, http.MethodPost, "/api/v1/agents/agent_api_test/status", map[string]any{
		"status": "success", "duration_ms": 42,
	}, map[string]string{"X-Watchdog-Agent-Token": agentToken})
	if run.Code != http.StatusAccepted {
		t.Fatalf("run: status=%d body=%s", run.Code, run.Body.String())
	}
	runs := requestJSON(t, s, http.MethodGet, "/api/v1/agent-registry/agent_api_test/runs", nil, nil, cookies...)
	if runs.Code != http.StatusOK || !strings.Contains(runs.Body.String(), `"duration_ms":42`) {
		t.Fatalf("runs: status=%d body=%s", runs.Code, runs.Body.String())
	}

	enrollment := requestJSON(t, s, http.MethodPost, "/api/v1/agents/enrollment-tokens", map[string]any{
		"kind": "system", "expires_in_seconds": 300,
	}, authHeaders, cookies...)
	if enrollment.Code != http.StatusCreated {
		t.Fatalf("enrollment token: status=%d body=%s", enrollment.Code, enrollment.Body.String())
	}
	var enrollmentBody struct {
		Token string `json:"token"`
	}
	decodeJSON(t, enrollment, &enrollmentBody)
	mismatched := requestJSON(t, s, http.MethodPost, "/api/v1/agents/register", map[string]any{
		"enrollment_token": enrollmentBody.Token, "name": "wrong-kind", "kind": "snmp",
	}, nil)
	if mismatched.Code != http.StatusBadRequest {
		t.Fatalf("enrollment kind mismatch: status=%d body=%s", mismatched.Code, mismatched.Body.String())
	}
	enrolled := requestJSON(t, s, http.MethodPost, "/api/v1/agents/register", map[string]any{
		"enrollment_token": enrollmentBody.Token, "name": "system-agent", "kind": "system",
		"device_id": systemDevice.ID, "capabilities": []string{"system.samples/v1"},
	}, nil)
	if enrolled.Code != http.StatusCreated || !strings.Contains(enrolled.Body.String(), `"credential"`) {
		t.Fatalf("register: status=%d body=%s", enrolled.Code, enrolled.Body.String())
	}
	reused := requestJSON(t, s, http.MethodPost, "/api/v1/agents/register", map[string]any{
		"enrollment_token": enrollmentBody.Token, "name": "replay", "kind": "system",
	}, nil)
	if reused.Code != http.StatusUnauthorized {
		t.Fatalf("enrollment replay: status=%d body=%s", reused.Code, reused.Body.String())
	}

	badHeartbeat := requestJSON(t, s, http.MethodPost, "/api/v1/agents/agent_api_test/heartbeat", nil,
		map[string]string{"X-Watchdog-Agent-Token": "wrong"})
	if badHeartbeat.Code != http.StatusUnauthorized {
		t.Fatalf("bad heartbeat: status=%d body=%s", badHeartbeat.Code, badHeartbeat.Body.String())
	}
	deletedAgent := requestJSON(t, s, http.MethodDelete, "/api/v1/agents/agent_api_test", nil, authHeaders, cookies...)
	if deletedAgent.Code != http.StatusNoContent {
		t.Fatalf("delete agent: status=%d body=%s", deletedAgent.Code, deletedAgent.Body.String())
	}
	deletedDevice := requestJSON(t, s, http.MethodDelete, "/api/v1/devices/"+device.ID, nil, authHeaders, cookies...)
	if deletedDevice.Code != http.StatusNoContent {
		t.Fatalf("delete device: status=%d body=%s", deletedDevice.Code, deletedDevice.Body.String())
	}
	deletedSystem := requestJSON(t, s, http.MethodDelete, "/api/v1/devices/"+systemDevice.ID, nil, authHeaders, cookies...)
	if deletedSystem.Code != http.StatusNoContent {
		t.Fatalf("delete system device: status=%d body=%s", deletedSystem.Code, deletedSystem.Body.String())
	}
	deletedProfile := requestJSON(t, s, http.MethodDelete, "/api/v1/snmp/profiles/snmp_api_test", nil, authHeaders, cookies...)
	if deletedProfile.Code != http.StatusNoContent {
		t.Fatalf("delete SNMP profile: status=%d body=%s", deletedProfile.Code, deletedProfile.Body.String())
	}

	// A second startup must skip already-applied migrations and preserve the
	// single bootstrap administrator instead of creating duplicates.
	second, err := New(cfg)
	if err != nil {
		t.Fatalf("second startup: %v", err)
	}
	_ = second.Close()
}

func assertDeviceScope(t *testing.T, s *Server, deviceID string) {
	t.Helper()
	userID := newID()
	hash, err := hashPassword("viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO users (id,username,email,display_name,password_hash,status) VALUES (?,?,?,? ,?,'active')`, userID, "scope-viewer", "scope-viewer@example.test", "Scope viewer", hash); err != nil {
		t.Fatal(err)
	}
	roleID := newID()
	if _, err := s.db.Exec(`INSERT INTO roles (id,name,title) VALUES (?,'scope-test','Scope test')`, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO role_permissions (role_id,permission_id) SELECT ?,id FROM permissions WHERE ability='device.view'`, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO user_roles (user_id,role_id) VALUES (?,?)`, userID, roleID); err != nil {
		t.Fatal(err)
	}
	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "scope-viewer", "password": "viewer-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("viewer login: status=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	before := requestJSON(t, s, http.MethodGet, "/api/v1/devices", nil, nil, cookies...)
	if before.Code != http.StatusOK || !strings.Contains(before.Body.String(), `"total":0`) {
		t.Fatalf("ungranted device scope: status=%d body=%s", before.Code, before.Body.String())
	}
	if _, err := s.db.Exec(`INSERT INTO user_device_permissions (user_id,device_id) VALUES (?,?)`, userID, deviceID); err != nil {
		t.Fatal(err)
	}
	after := requestJSON(t, s, http.MethodGet, "/api/v1/devices", nil, nil, cookies...)
	if after.Code != http.StatusOK || !strings.Contains(after.Body.String(), deviceID) {
		t.Fatalf("granted device scope: status=%d body=%s", after.Code, after.Body.String())
	}
	csrf := cookieValue(cookies, csrfCookie)
	forbidden := requestJSON(t, s, http.MethodPatch, "/api/v1/devices/"+deviceID, map[string]any{
		"display_name": "forbidden",
	}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("viewer mutation: status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
}

func requestJSON(t *testing.T, s *Server, method, path string, body any, headers map[string]string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	s.engine.ServeHTTP(rec, req)
	return rec
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder, out any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatal(err)
	}
}

func cookieValue(cookies []*http.Cookie, name string) string {
	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie.Value
		}
	}
	return ""
}
