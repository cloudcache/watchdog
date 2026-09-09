package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/opjob"
)

// TestAgentPlanAPI is deliberately isolated from the device lifecycle suite:
// KISS-04 can evolve and run without repeatedly editing or rerunning that broad
// integration fixture. The DSN must point at a disposable empty database.
func TestAgentPlanAPI(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_AGENT_PLAN_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_AGENT_PLAN_TEST_MYSQL_DSN is not set")
	}
	s, err := New(Config{
		MySQL: MySQLConfig{DSN: dsn},
		Admin: AdminConfig{Username: "plan-test-admin", Password: "plan-test-password"},
		AgentPlans: AgentPlansConfig{
			SigningKeyID: "plan-test-key", SigningPrivateKey: filepath.Join(t.TempDir(), "agent-plan.pem"),
			DefaultTTL: 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "plan-test-admin", "password": "plan-test-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: status=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	auth := map[string]string{"X-CSRF-Token": cookieValue(cookies, csrfCookie)}
	const agentToken = "agent-plan-test-secret"
	createdAgent := requestJSON(t, s, http.MethodPost, "/api/v1/agents", map[string]any{
		"id": "agent_plan_test", "name": "Plan test", "kind": "snmp", "mode": "push",
		"token": agentToken, "api_version": "v1", "capabilities": []string{"snmp.poll/v2"},
	}, auth, cookies...)
	if createdAgent.Code != http.StatusCreated || strings.Contains(createdAgent.Body.String(), agentToken) {
		t.Fatalf("create agent: status=%d body=%s", createdAgent.Code, createdAgent.Body.String())
	}

	badPlan := createPlanRequest(t, s, cookies, auth, "agent_plan_test", []string{"flow.write.clickhouse/v1"}, 0, 60)
	if badPlan.Code != http.StatusUnprocessableEntity {
		t.Fatalf("incompatible plan accepted: status=%d body=%s", badPlan.Code, badPlan.Body.String())
	}
	created := createPlanRequest(t, s, cookies, auth, "agent_plan_test", []string{"snmp.poll/v2"}, 0, 60)
	if created.Code != http.StatusCreated {
		t.Fatalf("create plan: status=%d body=%s", created.Code, created.Body.String())
	}
	fetched := requestJSON(t, s, http.MethodGet, "/api/v1/agents/agent_plan_test/plan", nil, map[string]string{"Authorization": "Bearer " + agentToken})
	envelope, spec, err := agentplan.Verify(fetched.Body.Bytes(), s.agentPlanPublic, time.Now().UTC(), true)
	if fetched.Code != http.StatusOK || err != nil || envelope.Metadata.PlanVersion != 1 || spec.Kind != "snmp" {
		t.Fatalf("fetch/verify plan: status=%d metadata=%+v spec=%+v err=%v body=%s", fetched.Code, envelope.Metadata, spec, err, fetched.Body.String())
	}
	notModified := requestJSON(t, s, http.MethodGet, "/api/v1/agents/agent_plan_test/plan", nil, map[string]string{
		"Authorization": "Bearer " + agentToken, "If-None-Match": fetched.Header().Get("ETag"),
	})
	if notModified.Code != http.StatusNotModified {
		t.Fatalf("plan ETag: status=%d", notModified.Code)
	}
	ackBody := map[string]any{
		"plan_version": 1, "payload_sha256": envelope.Metadata.PayloadSHA256, "status": "applied",
		"boot_id": "boot-1", "software_version": "1.0.0",
	}
	ack := machineRequest(t, s, http.MethodPost, "/api/v1/agents/agent_plan_test/plan-acks", agentToken, ackBody)
	replay := machineRequest(t, s, http.MethodPost, "/api/v1/agents/agent_plan_test/plan-acks", agentToken, ackBody)
	if ack.Code != http.StatusAccepted || replay.Code != http.StatusAccepted || !strings.Contains(replay.Body.String(), `"created":false`) {
		t.Fatalf("ACK idempotency: first=%d/%s replay=%d/%s", ack.Code, ack.Body.String(), replay.Code, replay.Body.String())
	}
	conflictBody := cloneAnyMap(ackBody)
	conflictBody["software_version"] = "changed"
	if conflict := machineRequest(t, s, http.MethodPost, "/api/v1/agents/agent_plan_test/plan-acks", agentToken, conflictBody); conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting ACK accepted: status=%d body=%s", conflict.Code, conflict.Body.String())
	}

	second := createPlanRequest(t, s, cookies, auth, "agent_plan_test", []string{"snmp.poll/v2"}, 1, 30)
	var secondPlan agentPlanDTO
	decodeJSON(t, second, &secondPlan)
	if second.Code != http.StatusCreated || secondPlan.PlanVersion != 2 || secondPlan.SupersedesPlanVersion != 1 {
		t.Fatalf("second plan: status=%d body=%s", second.Code, second.Body.String())
	}
	secondACK := machineRequest(t, s, http.MethodPost, "/api/v1/agents/agent_plan_test/plan-acks", agentToken, map[string]any{
		"plan_version": 2, "payload_sha256": secondPlan.PayloadSHA256, "status": "applied", "boot_id": "boot-2", "software_version": "1.0.0",
	})
	if secondACK.Code != http.StatusAccepted {
		t.Fatalf("second ACK: status=%d body=%s", secondACK.Code, secondACK.Body.String())
	}
	staleBody := cloneAnyMap(ackBody)
	staleBody["boot_id"] = "boot-stale"
	if stale := machineRequest(t, s, http.MethodPost, "/api/v1/agents/agent_plan_test/plan-acks", agentToken, staleBody); stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "plan_downgrade") {
		t.Fatalf("plan downgrade accepted: status=%d body=%s", stale.Code, stale.Body.String())
	}
	plans := requestJSON(t, s, http.MethodGet, "/api/v1/agents/agent_plan_test/plans?q=plan-test-key&sort=plan_version&order=desc", nil, nil, cookies...)
	if plans.Code != http.StatusOK || !strings.Contains(plans.Body.String(), `"total":2`) {
		t.Fatalf("plan VTable query: status=%d body=%s", plans.Code, plans.Body.String())
	}

	createRolloutAgent(t, s, cookies, auth)
	rollout := requestJSON(t, s, http.MethodPost, "/api/v1/agents/plan-rollouts", map[string]any{
		"agent_ids": []string{"agent_rollout_test"}, "required_capabilities": []string{"snmp.poll/v2"},
		"config": map[string]any{"interval_seconds": 45},
	}, map[string]string{"X-CSRF-Token": auth["X-CSRF-Token"], "Idempotency-Key": "agent-rollout-test"}, cookies...)
	var job opjob.Job
	decodeJSON(t, rollout, &job)
	if rollout.Code != http.StatusAccepted || job.ID == "" {
		t.Fatalf("enqueue rollout: status=%d body=%s", rollout.Code, rollout.Body.String())
	}
	waitForRollout(t, s, cookies, &job)
	var desired uint64
	if err := s.db.QueryRow(`SELECT desired_plan_version FROM agents WHERE id='agent_rollout_test'`).Scan(&desired); err != nil || desired != 1 {
		t.Fatalf("rollout desired=%d err=%v", desired, err)
	}

	current := requestJSON(t, s, http.MethodGet, "/api/v1/agents/agent_plan_test", nil, nil, cookies...)
	revoked := requestJSON(t, s, http.MethodPost, "/api/v1/agents/agent_plan_test/revoke", nil, map[string]string{
		"X-CSRF-Token": auth["X-CSRF-Token"], "If-Match": current.Header().Get("ETag"),
	}, cookies...)
	denied := requestJSON(t, s, http.MethodGet, "/api/v1/agents/agent_plan_test/plan", nil, map[string]string{"Authorization": "Bearer " + agentToken})
	if revoked.Code != http.StatusOK || denied.Code != http.StatusUnauthorized {
		t.Fatalf("revocation: revoke=%d/%s fetch=%d/%s", revoked.Code, revoked.Body.String(), denied.Code, denied.Body.String())
	}
}

func createPlanRequest(t *testing.T, s *Server, cookies []*http.Cookie, auth map[string]string, agentID string, capabilities []string, expected uint64, interval int) *httptest.ResponseRecorder {
	t.Helper()
	return requestJSON(t, s, http.MethodPost, "/api/v1/agents/"+agentID+"/plans", map[string]any{
		"required_capabilities": capabilities, "config": map[string]any{"interval_seconds": interval},
		"expected_desired_plan_version": expected,
	}, auth, cookies...)
}

func machineRequest(t *testing.T, s *Server, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	return requestJSON(t, s, method, path, body, map[string]string{"Authorization": "Bearer " + token})
}

func createRolloutAgent(t *testing.T, s *Server, cookies []*http.Cookie, auth map[string]string) {
	t.Helper()
	response := requestJSON(t, s, http.MethodPost, "/api/v1/agents", map[string]any{
		"id": "agent_rollout_test", "name": "Rollout test", "kind": "snmp", "mode": "push",
		"token": "rollout-secret", "api_version": "v1", "capabilities": []string{"snmp.poll/v2"},
	}, auth, cookies...)
	if response.Code != http.StatusCreated {
		t.Fatalf("create rollout agent: status=%d body=%s", response.Code, response.Body.String())
	}
}

func waitForRollout(t *testing.T, s *Server, cookies []*http.Cookie, job *opjob.Job) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		response := requestJSON(t, s, http.MethodGet, "/api/v1/agents/plan-rollouts/"+job.ID, nil, nil, cookies...)
		if response.Code != http.StatusOK {
			t.Fatalf("get rollout: status=%d body=%s", response.Code, response.Body.String())
		}
		decodeJSON(t, response, job)
		if job.Status == opjob.StatusSucceeded {
			return
		}
		if job.Status == opjob.StatusFailed || time.Now().After(deadline) {
			t.Fatalf("rollout did not converge: %+v", *job)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func cloneAnyMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
