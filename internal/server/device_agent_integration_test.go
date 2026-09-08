package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
	exerciseDeviceOrganizationAPI(t, s, device.ID, patched.Header().Get("ETag"), authHeaders, cookies)

	summary := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/summary?q=core-1", nil, nil, cookies...)
	if summary.Code != http.StatusOK || !strings.Contains(summary.Body.String(), device.ID) {
		t.Fatalf("device summary: status=%d body=%s", summary.Code, summary.Body.String())
	}
	assertDeviceScope(t, s, device.ID)
	exerciseDeviceInventoryAPI(t, s, device.ID, authHeaders, cookies)

	// A disabled account immediately loses access even when it still has an
	// unexpired session cookie.
	createdUser := requestJSON(t, s, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "disabled-session-user", "email": "disabled-session@example.test",
		"display_name": "Disabled session", "password": "disabled-password", "roles": []string{"viewer"},
	}, authHeaders, cookies...)
	if createdUser.Code != http.StatusCreated {
		t.Fatalf("create session test user: status=%d body=%s", createdUser.Code, createdUser.Body.String())
	}
	var createdUserBody struct {
		ID string `json:"id"`
	}
	decodeJSON(t, createdUser, &createdUserBody)
	userLogin := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "disabled-session-user", "password": "disabled-password",
	}, nil)
	if userLogin.Code != http.StatusOK {
		t.Fatalf("session test login: status=%d body=%s", userLogin.Code, userLogin.Body.String())
	}
	userCookies := userLogin.Result().Cookies()
	disabledUser := requestJSON(t, s, http.MethodPatch, "/api/v1/users/"+createdUserBody.ID,
		map[string]any{"status": "disabled"}, authHeaders, cookies...)
	if disabledUser.Code != http.StatusOK {
		t.Fatalf("disable user: status=%d body=%s", disabledUser.Code, disabledUser.Body.String())
	}
	disabledCurrent := requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, userCookies...)
	if disabledCurrent.Code != http.StatusUnauthorized {
		t.Fatalf("disabled session: status=%d body=%s", disabledCurrent.Code, disabledCurrent.Body.String())
	}
	deletedUser := requestJSON(t, s, http.MethodDelete, "/api/v1/users/"+createdUserBody.ID, nil, authHeaders, cookies...)
	if deletedUser.Code != http.StatusNoContent {
		t.Fatalf("delete session test user: status=%d body=%s", deletedUser.Code, deletedUser.Body.String())
	}

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
	if systemDevice.Kind != "system" {
		t.Fatalf("target create returned canonical device kind %q instead of target kind system", systemDevice.Kind)
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

	flowAgent := requestJSON(t, s, http.MethodPost, "/api/v1/agents", map[string]any{
		"id": "flow_collect_api_test", "name": "Flow collector", "kind": "flow_collect", "mode": "push",
		"token": "flow-collector-secret",
	}, authHeaders, cookies...)
	if flowAgent.Code != http.StatusCreated {
		t.Fatalf("create flow collector: status=%d body=%s", flowAgent.Code, flowAgent.Body.String())
	}
	badFlowBinding := requestJSON(t, s, http.MethodPost, "/api/v1/flow/devices", map[string]any{
		"device_id": device.ID, "collector_agent_id": "agent_api_test", "source_prefix": "192.0.2.10", "protocol": "sflow5",
	}, authHeaders, cookies...)
	if badFlowBinding.Code != http.StatusBadRequest {
		t.Fatalf("non-flow collector accepted: status=%d body=%s", badFlowBinding.Code, badFlowBinding.Body.String())
	}
	flowCreated := requestJSON(t, s, http.MethodPost, "/api/v1/flow/devices", map[string]any{
		"device_id": device.ID, "collector_agent_id": "flow_collect_api_test", "source_prefix": "192.0.2.10",
		"protocol": "sflow5", "sampling_mode": "sampled", "default_sampling_rate": 1000,
		"observation_domain_id": 7, "observations": map[string]any{"1": map[string]any{"direction": 1}},
	}, authHeaders, cookies...)
	if flowCreated.Code != http.StatusCreated {
		t.Fatalf("create flow binding: status=%d body=%s", flowCreated.Code, flowCreated.Body.String())
	}
	var flowBinding flowExporterDTO
	decodeJSON(t, flowCreated, &flowBinding)
	if flowBinding.DeviceID != device.ID || flowBinding.SourcePrefix != "192.0.2.10/32" || flowBinding.DeploymentState != "unpublished" || flowBinding.ObservationDomainID == nil || *flowBinding.ObservationDomainID != 7 {
		t.Fatalf("unexpected flow binding: %+v", flowBinding)
	}
	flowList := requestJSON(t, s, http.MethodGet, "/api/v1/flow/exporter-bindings?protocol=sflow5&q=core-1&enabled=true", nil, nil, cookies...)
	if flowList.Code != http.StatusOK || !strings.Contains(flowList.Body.String(), flowBinding.ID) || !strings.Contains(flowList.Body.String(), `"total":1`) {
		t.Fatalf("list flow bindings: status=%d body=%s", flowList.Code, flowList.Body.String())
	}
	invalidFlowFilter := requestJSON(t, s, http.MethodGet, "/api/v1/flow/devices?protocol=guess", nil, nil, cookies...)
	if invalidFlowFilter.Code != http.StatusBadRequest {
		t.Fatalf("invalid flow filter: status=%d body=%s", invalidFlowFilter.Code, invalidFlowFilter.Body.String())
	}
	duplicateFlow := requestJSON(t, s, http.MethodPost, "/api/v1/flow/devices", map[string]any{
		"device_id": device.ID, "source_prefix": "192.0.2.10/32", "protocol": "sflow5", "observation_domain_id": 7,
	}, authHeaders, cookies...)
	if duplicateFlow.Code != http.StatusConflict {
		t.Fatalf("duplicate flow selector: status=%d body=%s", duplicateFlow.Code, duplicateFlow.Body.String())
	}
	flowLoaded := requestJSON(t, s, http.MethodGet, "/api/v1/flow/devices/"+flowBinding.ID, nil, nil, cookies...)
	flowPatched := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/devices/"+flowBinding.ID, map[string]any{
		"observation_domain_id": nil, "default_sampling_rate": 2000,
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": flowLoaded.Header().Get("ETag")}, cookies...)
	if flowLoaded.Code != http.StatusOK || flowPatched.Code != http.StatusOK || !strings.Contains(flowPatched.Body.String(), `"observation_domain_id":null`) {
		t.Fatalf("get/patch flow binding: get=%d patch=%d body=%s", flowLoaded.Code, flowPatched.Code, flowPatched.Body.String())
	}
	staleFlow := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/devices/"+flowBinding.ID, map[string]any{
		"default_sampling_rate": 3000,
	}, map[string]string{"X-CSRF-Token": csrf, "If-Match": flowLoaded.Header().Get("ETag")}, cookies...)
	if staleFlow.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale flow binding update: status=%d body=%s", staleFlow.Code, staleFlow.Body.String())
	}
	preview := requestJSON(t, s, http.MethodGet, "/api/v1/targets/"+device.ID+"/delete-preview", nil, nil, cookies...)
	if preview.Code != http.StatusOK || !strings.Contains(preview.Body.String(), `"resource_type":"flow_exporter_binding"`) || !strings.Contains(preview.Body.String(), `"behavior":"blocked"`) {
		t.Fatalf("device delete preview omitted flow binding: status=%d body=%s", preview.Code, preview.Body.String())
	}
	blockedDeviceDelete := requestJSON(t, s, http.MethodDelete, "/api/v1/devices/"+device.ID, nil, authHeaders, cookies...)
	if blockedDeviceDelete.Code != http.StatusConflict {
		t.Fatalf("device with flow binding was deleted: status=%d body=%s", blockedDeviceDelete.Code, blockedDeviceDelete.Body.String())
	}

	var bindingVersion uint64
	if err := s.db.QueryRow(`SELECT row_version FROM flow_exporter_bindings WHERE id=?`, flowBinding.ID).Scan(&bindingVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE flow_exporter_bindings SET published_row_version=row_version,published_plan_version=1 WHERE id=?`, flowBinding.ID); err != nil {
		t.Fatal(err)
	}
	publishedDelete := requestJSON(t, s, http.MethodDelete, "/api/v1/flow/devices/"+flowBinding.ID, nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": `"` + strconv.FormatUint(bindingVersion, 10) + `"`}, cookies...)
	if publishedDelete.Code != http.StatusConflict {
		t.Fatalf("published enabled binding was deleted: status=%d body=%s", publishedDelete.Code, publishedDelete.Body.String())
	}
	disableFlow := requestJSON(t, s, http.MethodPatch, "/api/v1/flow/devices/"+flowBinding.ID, map[string]any{"enabled": false},
		map[string]string{"X-CSRF-Token": csrf, "If-Match": `"` + strconv.FormatUint(bindingVersion, 10) + `"`}, cookies...)
	if disableFlow.Code != http.StatusOK {
		t.Fatalf("disable flow binding: status=%d body=%s", disableFlow.Code, disableFlow.Body.String())
	}
	var disabled flowExporterDTO
	decodeJSON(t, disableFlow, &disabled)
	if _, err := s.db.Exec(`UPDATE flow_exporter_bindings SET published_row_version=row_version,published_plan_version=2 WHERE id=?`, flowBinding.ID); err != nil {
		t.Fatal(err)
	}
	flowDeleted := requestJSON(t, s, http.MethodDelete, "/api/v1/flow/exporter-bindings/"+flowBinding.ID, nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": `"` + strconv.FormatUint(disabled.RowVersion, 10) + `"`}, cookies...)
	if flowDeleted.Code != http.StatusNoContent {
		t.Fatalf("delete withdrawn flow binding: status=%d body=%s", flowDeleted.Code, flowDeleted.Body.String())
	}
	flowAgentDeleted := requestJSON(t, s, http.MethodDelete, "/api/v1/agents/flow_collect_api_test", nil, authHeaders, cookies...)
	if flowAgentDeleted.Code != http.StatusNoContent {
		t.Fatalf("delete flow collector: status=%d body=%s", flowAgentDeleted.Code, flowAgentDeleted.Body.String())
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
	logout := requestJSON(t, s, http.MethodPost, "/api/v1/session/logout", nil, authHeaders, cookies...)
	if logout.Code != http.StatusNoContent {
		t.Fatalf("logout: status=%d body=%s", logout.Code, logout.Body.String())
	}
	loggedOutCurrent := requestJSON(t, s, http.MethodGet, "/api/v1/session/current", nil, nil, cookies...)
	if loggedOutCurrent.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session: status=%d body=%s", loggedOutCurrent.Code, loggedOutCurrent.Body.String())
	}

	// A second startup must skip already-applied migrations and preserve the
	// single bootstrap administrator instead of creating duplicates.
	second, err := New(cfg)
	if err != nil {
		t.Fatalf("second startup: %v", err)
	}
	_ = second.Close()
	var bootstrapUserCount, administratorCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM users WHERE username = 'api-test-admin'`).Scan(&bootstrapUserCount); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`
		SELECT COUNT(*) FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE r.name = 'administrator'
	`).Scan(&administratorCount); err != nil {
		t.Fatal(err)
	}
	if bootstrapUserCount != 1 || administratorCount != 1 {
		t.Fatalf("second startup bootstrap users=%d administrators=%d, want 1/1", bootstrapUserCount, administratorCount)
	}

	// An applied migration is immutable: startup must fail before serving when
	// the recorded checksum no longer matches the embedded schema.
	var originalChecksum string
	if err := s.db.QueryRow(`SELECT checksum FROM schema_migrations WHERE store='mysql' AND version='0001_baseline'`).Scan(&originalChecksum); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE schema_migrations SET checksum=REPEAT('0',64) WHERE store='mysql' AND version='0001_baseline'`); err != nil {
		t.Fatal(err)
	}
	if tampered, err := New(cfg); err == nil {
		_ = tampered.Close()
		t.Fatal("startup accepted a tampered schema migration checksum")
	} else if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("unexpected tampered schema error: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE schema_migrations SET checksum=? WHERE store='mysql' AND version='0001_baseline'`, originalChecksum); err != nil {
		t.Fatal(err)
	}
}

func exerciseDeviceOrganizationAPI(t *testing.T, s *Server, deviceID, deviceETag string, authHeaders map[string]string, cookies []*http.Cookie) {
	t.Helper()
	locationCreated := requestJSON(t, s, http.MethodPost, "/api/v1/locations", map[string]any{
		"name": "Singapore POP", "latitude": 1.3521, "longitude": 103.8198, "address": "Singapore",
	}, authHeaders, cookies...)
	if locationCreated.Code != http.StatusCreated {
		t.Fatalf("create location: status=%d body=%s", locationCreated.Code, locationCreated.Body.String())
	}
	var location locationDTO
	decodeJSON(t, locationCreated, &location)
	devicePatched := requestJSON(t, s, http.MethodPatch, "/api/v1/devices/"+deviceID,
		map[string]any{"location_id": location.ID},
		map[string]string{"X-CSRF-Token": authHeaders["X-CSRF-Token"], "If-Match": deviceETag}, cookies...)
	if devicePatched.Code != http.StatusOK {
		t.Fatalf("attach location: status=%d body=%s", devicePatched.Code, devicePatched.Body.String())
	}
	locations := requestJSON(t, s, http.MethodGet, "/api/v1/locations?q=singapore&sort=device_count&order=desc", nil, nil, cookies...)
	if locations.Code != http.StatusOK || !strings.Contains(locations.Body.String(), `"device_count":1`) {
		t.Fatalf("list locations: status=%d body=%s", locations.Code, locations.Body.String())
	}

	staticCreated := requestJSON(t, s, http.MethodPost, "/api/v1/device-groups", map[string]any{
		"name": "Core routers", "kind": "static", "description": "Explicit core set",
	}, authHeaders, cookies...)
	if staticCreated.Code != http.StatusCreated {
		t.Fatalf("create static group: status=%d body=%s", staticCreated.Code, staticCreated.Body.String())
	}
	var staticGroup deviceGroupDTO
	decodeJSON(t, staticCreated, &staticGroup)
	replaced := requestJSON(t, s, http.MethodPut, "/api/v1/device-groups/"+staticGroup.ID+"/members",
		map[string]any{"device_ids": []string{deviceID, deviceID}}, authHeaders, cookies...)
	if replaced.Code != http.StatusOK || !strings.Contains(replaced.Body.String(), `"total":1`) {
		t.Fatalf("replace static members: status=%d body=%s", replaced.Code, replaced.Body.String())
	}

	dynamicCreated := requestJSON(t, s, http.MethodPost, "/api/v1/device-groups", map[string]any{
		"name": "Singapore network", "kind": "dynamic",
		"rule": map[string]any{"kind": []string{"network"}, "location_id": []string{location.ID}, "labels": map[string][]string{"site": {"dc-a"}}},
	}, authHeaders, cookies...)
	if dynamicCreated.Code != http.StatusCreated {
		t.Fatalf("create dynamic group: status=%d body=%s", dynamicCreated.Code, dynamicCreated.Body.String())
	}
	var dynamicGroup deviceGroupDTO
	decodeJSON(t, dynamicCreated, &dynamicGroup)
	if dynamicGroup.MemberCount != 1 {
		t.Fatalf("dynamic group member_count=%d, want 1", dynamicGroup.MemberCount)
	}
	manualDynamic := requestJSON(t, s, http.MethodPut, "/api/v1/device-groups/"+dynamicGroup.ID+"/members/"+deviceID, nil, authHeaders, cookies...)
	if manualDynamic.Code != http.StatusConflict {
		t.Fatalf("manual dynamic member accepted: status=%d body=%s", manualDynamic.Code, manualDynamic.Body.String())
	}

	changedLabels := requestJSON(t, s, http.MethodPatch, "/api/v1/devices/"+deviceID,
		map[string]any{"labels": map[string]string{"site": "dc-b"}},
		map[string]string{"X-CSRF-Token": authHeaders["X-CSRF-Token"], "If-Match": devicePatched.Header().Get("ETag")}, cookies...)
	if changedLabels.Code != http.StatusOK {
		t.Fatalf("change dynamic selector field: status=%d body=%s", changedLabels.Code, changedLabels.Body.String())
	}
	dynamicMembers := requestJSON(t, s, http.MethodGet, "/api/v1/device-groups/"+dynamicGroup.ID+"/members", nil, nil, cookies...)
	if dynamicMembers.Code != http.StatusOK || !strings.Contains(dynamicMembers.Body.String(), `"total":0`) {
		t.Fatalf("dynamic membership did not refresh: status=%d body=%s", dynamicMembers.Code, dynamicMembers.Body.String())
	}
	restoredLabels := requestJSON(t, s, http.MethodPatch, "/api/v1/devices/"+deviceID,
		map[string]any{"labels": map[string]string{"site": "dc-a"}},
		map[string]string{"X-CSRF-Token": authHeaders["X-CSRF-Token"], "If-Match": changedLabels.Header().Get("ETag")}, cookies...)
	if restoredLabels.Code != http.StatusOK {
		t.Fatalf("restore dynamic selector field: status=%d body=%s", restoredLabels.Code, restoredLabels.Body.String())
	}

	roleCreated := requestJSON(t, s, http.MethodPost, "/api/v1/roles", map[string]any{
		"name": "scoped-device-view", "title": "Scoped device view", "permissions": []string{"device.view", "port.view", "flow.device.view"},
	}, authHeaders, cookies...)
	if roleCreated.Code != http.StatusCreated {
		t.Fatalf("create scoped role: status=%d body=%s", roleCreated.Code, roleCreated.Body.String())
	}
	var role struct {
		ID string `json:"id"`
	}
	decodeJSON(t, roleCreated, &role)
	userCreated := requestJSON(t, s, http.MethodPost, "/api/v1/users", map[string]any{
		"username": "organization-viewer", "email": "organization-viewer@example.test", "password": "organization-password", "roles": []string{"scoped-device-view"},
	}, authHeaders, cookies...)
	if userCreated.Code != http.StatusCreated {
		t.Fatalf("create scoped user: status=%d body=%s", userCreated.Code, userCreated.Body.String())
	}
	var user struct {
		ID string `json:"id"`
	}
	decodeJSON(t, userCreated, &user)
	access := requestJSON(t, s, http.MethodPut, "/api/v1/users/"+user.ID+"/access", map[string]any{
		"device_group_ids": []string{staticGroup.ID, staticGroup.ID},
	}, authHeaders, cookies...)
	if access.Code != http.StatusOK || strings.Count(access.Body.String(), staticGroup.ID) != 1 {
		t.Fatalf("replace user access: status=%d body=%s", access.Code, access.Body.String())
	}
	badAccess := requestJSON(t, s, http.MethodPut, "/api/v1/users/"+user.ID+"/access", map[string]any{
		"device_ids": []string{"missing-device"},
	}, authHeaders, cookies...)
	if badAccess.Code != http.StatusBadRequest {
		t.Fatalf("unknown grant accepted: status=%d body=%s", badAccess.Code, badAccess.Body.String())
	}
	afterBadAccess := requestJSON(t, s, http.MethodGet, "/api/v1/users/"+user.ID+"/access", nil, nil, cookies...)
	if afterBadAccess.Code != http.StatusOK || !strings.Contains(afterBadAccess.Body.String(), staticGroup.ID) {
		t.Fatalf("failed access replacement was not atomic: status=%d body=%s", afterBadAccess.Code, afterBadAccess.Body.String())
	}
	viewerLogin := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "organization-viewer", "password": "organization-password",
	}, nil)
	viewerCookies := viewerLogin.Result().Cookies()
	viewerDevices := requestJSON(t, s, http.MethodGet, "/api/v1/devices", nil, nil, viewerCookies...)
	viewerGroups := requestJSON(t, s, http.MethodGet, "/api/v1/device-groups", nil, nil, viewerCookies...)
	viewerLocations := requestJSON(t, s, http.MethodGet, "/api/v1/locations", nil, nil, viewerCookies...)
	if viewerLogin.Code != http.StatusOK || !strings.Contains(viewerDevices.Body.String(), deviceID) || !strings.Contains(viewerGroups.Body.String(), staticGroup.ID) || !strings.Contains(viewerLocations.Body.String(), location.ID) {
		t.Fatalf("group inherited scope failed: login=%d devices=%s groups=%s locations=%s", viewerLogin.Code, viewerDevices.Body.String(), viewerGroups.Body.String(), viewerLocations.Body.String())
	}

	for path, expected := range map[string]int{
		"/api/v1/users/" + user.ID:                 http.StatusNoContent,
		"/api/v1/roles/" + role.ID:                 http.StatusNoContent,
		"/api/v1/device-groups/" + dynamicGroup.ID: http.StatusNoContent,
		"/api/v1/device-groups/" + staticGroup.ID:  http.StatusNoContent,
		"/api/v1/locations/" + location.ID:         http.StatusNoContent,
	} {
		response := requestJSON(t, s, http.MethodDelete, path, nil, authHeaders, cookies...)
		if response.Code != expected {
			t.Fatalf("cleanup %s: status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
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

func exerciseDeviceInventoryAPI(t *testing.T, s *Server, deviceID string, authHeaders map[string]string, cookies []*http.Cookie) {
	t.Helper()
	for _, row := range []struct {
		id      string
		ifIndex int
		name    string
		oper    string
	}{
		{"port_api_test", 10, "xe-0/0/0", "up"},
		{"port_scope_hidden", 11, "xe-0/0/1", "down"},
		{"port_delete_test", 12, "xe-0/0/2", "down"},
	} {
		if _, err := s.db.Exec(`INSERT INTO ports
			(id,device_id,if_index,if_name,if_descr,if_alias,if_speed,if_oper_status,if_admin_status,metadata_json)
			VALUES (?,?,?,?,?,'',100000000000,?,'up',JSON_OBJECT('source','IF-MIB'))`, row.id, deviceID, row.ifIndex, row.name, "uplink "+row.name, row.oper); err != nil {
			t.Fatal(err)
		}
	}
	for _, address := range []struct {
		id, port, ip, context, origin string
		family, prefix                int
	}{
		{"addr_v4_test", "port_api_test", "192.0.2.1", "default", "manual", 4, 31},
		{"addr_v6_test", "port_api_test", "2001:db8::1", "default", "manual", 6, 127},
		{"addr_hidden_test", "port_scope_hidden", "203.0.113.2", "default", "manual", 4, 31},
		{"addr_delete_test", "port_delete_test", "198.51.100.1", "default", "manual", 4, 31},
	} {
		if _, err := s.db.Exec(`INSERT INTO interface_addresses
			(id,device_id,port_id,if_index,family,address,prefix_len,context,origin)
			VALUES (?,?,?,?,?,INET6_ATON(?),?,?,?)`, address.id, deviceID, address.port,
			map[string]int{"port_api_test": 10, "port_scope_hidden": 11, "port_delete_test": 12}[address.port], address.family,
			address.ip, address.prefix, address.context, address.origin); err != nil {
			t.Fatal(err)
		}
	}
	for _, session := range []struct {
		id, peer, afi string
	}{
		{"bgp_v4_test", "203.0.113.1", "ipv4"},
		{"bgp_v6_test", "2001:db8:ffff::1", "ipv6"},
	} {
		if _, err := s.db.Exec(`INSERT INTO bgp_sessions
			(id,device_id,peer_address,peer_as,local_as,afi,safi,state,prefixes,denied_prefixes,advertised_prefixes,uptime_seconds,metadata_json)
			VALUES (?,?,?,64501,64500,?,'unicast','established',42,1,40,3600,JSON_OBJECT('source','BGP4-MIB'))`,
			session.id, deviceID, session.peer, session.afi); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`INSERT INTO sensors
		(id,device_id,port_id,sensor_index,class,label,oid_index,oid,unit,value_num,warn_limit,crit_limit,status,metadata_json)
		VALUES ('sensor_test',?,'port_api_test',1,'temperature','FPC temperature','1','.1.3.6.1.2.1','C',72,65,80,'warning',JSON_OBJECT('mib','ENTITY-SENSOR-MIB'))`, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO physical_entities
		(id,device_id,entity_index,name,description,class,vendor_type,contained_in,parent_rel_pos,hardware_revision,
		 firmware_revision,software_revision,serial,manufacturer_name,model_name,alias,asset_id,is_fru)
		VALUES ('entity_test',?,'1001','Power supply','PSU 0','powerSupply','.1.3.6',0,1,'A','1.0','1.0','PSU123','Juniper','JPSU','PSU0','asset-1',1)`, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO vlans (id,device_id,vlan_id,name,status) VALUES ('vlan_test',?,100,'users','active')`, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO lag_groups (id,device_id,lag_if_index,name,mac_address,mode) VALUES ('lag_test',?,500,'ae0','00:11:22:33:44:55','lacp')`, deviceID); err != nil {
		t.Fatal(err)
	}

	ports := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/ports?address_family=ipv6&q=2001:db8&sort=speed&order=desc&limit=10&offset=0", nil, nil, cookies...)
	if ports.Code != http.StatusOK || !strings.Contains(ports.Body.String(), `"ID":"port_api_test"`) ||
		!strings.Contains(ports.Body.String(), `"Family":"ipv6"`) || !strings.Contains(ports.Body.String(), `"counts":{"down":2,"total":3,"up":1}`) {
		t.Fatalf("paged ports with IPv6: status=%d body=%s", ports.Code, ports.Body.String())
	}
	addresses := requestJSON(t, s, http.MethodGet, "/api/v1/devices/"+deviceID+"/addresses?family=ipv6&q=2001:db8", nil, nil, cookies...)
	if addresses.Code != http.StatusOK || !strings.Contains(addresses.Body.String(), "2001:db8::1") || strings.Contains(addresses.Body.String(), "192.0.2.1") {
		t.Fatalf("IPv6 address list: status=%d body=%s", addresses.Code, addresses.Body.String())
	}
	loadedPort := requestJSON(t, s, http.MethodGet, "/api/v1/network/ports/port_api_test", nil, nil, cookies...)
	if loadedPort.Code != http.StatusOK || loadedPort.Header().Get("ETag") == "" || !strings.Contains(loadedPort.Body.String(), `"source":"IF-MIB"`) {
		t.Fatalf("get port: status=%d body=%s", loadedPort.Code, loadedPort.Body.String())
	}
	badPort := requestJSON(t, s, http.MethodPatch, "/api/v1/network/ports/port_api_test", map[string]any{"AdminStatus": "fabricated"},
		authHeaders, cookies...)
	if badPort.Code != http.StatusBadRequest {
		t.Fatalf("invalid port state accepted: status=%d body=%s", badPort.Code, badPort.Body.String())
	}
	updatedPort := requestJSON(t, s, http.MethodPatch, "/api/v1/network/ports/port_api_test", map[string]any{"IfAlias": "customer-a"},
		map[string]string{"X-CSRF-Token": authHeaders["X-CSRF-Token"], "If-Match": loadedPort.Header().Get("ETag")}, cookies...)
	if updatedPort.Code != http.StatusOK || !strings.Contains(updatedPort.Body.String(), `"IfAlias":"customer-a"`) {
		t.Fatalf("patch port: status=%d body=%s", updatedPort.Code, updatedPort.Body.String())
	}
	stalePort := requestJSON(t, s, http.MethodPatch, "/api/v1/network/ports/port_api_test", map[string]any{"IfAlias": "stale"},
		map[string]string{"X-CSRF-Token": authHeaders["X-CSRF-Token"], "If-Match": loadedPort.Header().Get("ETag")}, cookies...)
	if stalePort.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale port update: status=%d body=%s", stalePort.Code, stalePort.Body.String())
	}

	bgp6 := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/bgp?afi=ipv6&q=2001:db8&limit=10", nil, nil, cookies...)
	if bgp6.Code != http.StatusOK || !strings.Contains(bgp6.Body.String(), "2001:db8:ffff::1") || strings.Contains(bgp6.Body.String(), "203.0.113.1") {
		t.Fatalf("IPv6 BGP list: status=%d body=%s", bgp6.Code, bgp6.Body.String())
	}
	allBGP := requestJSON(t, s, http.MethodGet, "/api/v1/bgp?sort=peer_as&order=desc&limit=10", nil, nil, cookies...)
	if allBGP.Code != http.StatusOK || !strings.Contains(allBGP.Body.String(), "bgp_v4_test") || !strings.Contains(allBGP.Body.String(), "bgp_v6_test") {
		t.Fatalf("global BGP list: status=%d body=%s", allBGP.Code, allBGP.Body.String())
	}
	sensors := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/sensors?health=problem&q=temp&sort=value&order=desc", nil, nil, cookies...)
	if sensors.Code != http.StatusOK || !strings.Contains(sensors.Body.String(), `"Status":"warning"`) || !strings.Contains(sensors.Body.String(), `"problems":1`) {
		t.Fatalf("sensor list: status=%d body=%s", sensors.Code, sensors.Body.String())
	}
	inventory := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/inventory?class=powerSupply&fru=true&q=PSU", nil, nil, cookies...)
	if inventory.Code != http.StatusOK || !strings.Contains(inventory.Body.String(), `"SerialNumber":"PSU123"`) || !strings.Contains(inventory.Body.String(), `"IsFRU":true`) {
		t.Fatalf("inventory list: status=%d body=%s", inventory.Code, inventory.Body.String())
	}
	vlans := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/vlans?status=active&q=user", nil, nil, cookies...)
	if vlans.Code != http.StatusOK || !strings.Contains(vlans.Body.String(), `"VLANID":100`) {
		t.Fatalf("VLAN list: status=%d body=%s", vlans.Code, vlans.Body.String())
	}
	lags := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/lags?mode=lacp&q=00:11", nil, nil, cookies...)
	if lags.Code != http.StatusOK || !strings.Contains(lags.Body.String(), `"AggregateIndex":500`) {
		t.Fatalf("LAG list: status=%d body=%s", lags.Code, lags.Body.String())
	}

	assertExplicitPortScope(t, s, deviceID, "port_api_test", "port_scope_hidden")

	if _, err := s.db.Exec(`INSERT INTO billing_accounts (id,name) VALUES ('bill_port_delete','Port delete guard')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO billing_account_ports (account_id,port_id) VALUES ('bill_port_delete','port_delete_test')`); err != nil {
		t.Fatal(err)
	}
	deletePreview := requestJSON(t, s, http.MethodGet, "/api/v1/network/ports/port_delete_test/delete-preview", nil, nil, cookies...)
	if deletePreview.Code != http.StatusOK || !strings.Contains(deletePreview.Body.String(), `"resource_type":"billing_account"`) || !strings.Contains(deletePreview.Body.String(), `"behavior":"blocked"`) {
		t.Fatalf("port delete preview: status=%d body=%s", deletePreview.Code, deletePreview.Body.String())
	}
	blocked := requestJSON(t, s, http.MethodDelete, "/api/v1/network/ports/port_delete_test", nil, authHeaders, cookies...)
	if blocked.Code != http.StatusConflict {
		t.Fatalf("billing port deletion was not blocked: status=%d body=%s", blocked.Code, blocked.Body.String())
	}
	if _, err := s.db.Exec(`DELETE FROM billing_accounts WHERE id='bill_port_delete'`); err != nil {
		t.Fatal(err)
	}
	deleted := requestJSON(t, s, http.MethodDelete, "/api/v1/network/ports/port_delete_test", nil, authHeaders, cookies...)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete detached port: status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	var addressCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM interface_addresses WHERE id='addr_delete_test'`).Scan(&addressCount); err != nil || addressCount != 0 {
		t.Fatalf("deleted port address count=%d err=%v", addressCount, err)
	}
}

func assertExplicitPortScope(t *testing.T, s *Server, deviceID, grantedPortID, hiddenPortID string) {
	t.Helper()
	userID, roleID := newID(), newID()
	hash, err := hashPassword("port-viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO users (id,username,email,display_name,password_hash,status) VALUES (?, 'port-viewer','port-viewer@example.test','Port viewer',?,'active')`, userID, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO roles (id,name,title) VALUES (?,'port-scope-test','Port scope test')`, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO role_permissions (role_id,permission_id) SELECT ?,id FROM permissions WHERE ability='port.view'`, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO user_roles (user_id,role_id) VALUES (?,?)`, userID, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO user_port_permissions (user_id,port_id) VALUES (?,?)`, userID, grantedPortID); err != nil {
		t.Fatal(err)
	}
	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{"username": "port-viewer", "password": "port-viewer-password"}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("port viewer login: status=%d body=%s", login.Code, login.Body.String())
	}
	portCookies := login.Result().Cookies()
	list := requestJSON(t, s, http.MethodGet, "/api/v1/network/devices/"+deviceID+"/ports?limit=100", nil, nil, portCookies...)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), grantedPortID) || strings.Contains(list.Body.String(), hiddenPortID) || !strings.Contains(list.Body.String(), `"total":1`) {
		t.Fatalf("explicit port list scope: status=%d body=%s", list.Code, list.Body.String())
	}
	granted := requestJSON(t, s, http.MethodGet, "/api/v1/network/ports/"+grantedPortID, nil, nil, portCookies...)
	if granted.Code != http.StatusOK {
		t.Fatalf("explicit port grant: status=%d body=%s", granted.Code, granted.Body.String())
	}
	hidden := requestJSON(t, s, http.MethodGet, "/api/v1/network/ports/"+hiddenPortID, nil, nil, portCookies...)
	if hidden.Code != http.StatusForbidden {
		t.Fatalf("ungranted port: status=%d body=%s", hidden.Code, hidden.Body.String())
	}
	addresses := requestJSON(t, s, http.MethodGet, "/api/v1/devices/"+deviceID+"/addresses?limit=100", nil, nil, portCookies...)
	if addresses.Code != http.StatusOK || !strings.Contains(addresses.Body.String(), "192.0.2.1") || strings.Contains(addresses.Body.String(), "203.0.113.2") {
		t.Fatalf("explicit port address scope: status=%d body=%s", addresses.Code, addresses.Body.String())
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
