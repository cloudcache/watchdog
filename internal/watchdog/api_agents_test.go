package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeAgentRepository struct {
	agent   SNMPAgentConfig
	deleted ID
	run     AgentRunReport
	runs    []AgentRunHistory
}

func (r *fakeAgentRepository) GetAgent(context.Context, ID) (SNMPAgentConfig, error) {
	return r.agent, nil
}

func (r *fakeAgentRepository) ListAgents(context.Context, ID) ([]SNMPAgentConfig, error) {
	return []SNMPAgentConfig{r.agent}, nil
}

func (r *fakeAgentRepository) UpsertAgent(_ context.Context, agent SNMPAgentConfig) (SNMPAgentConfig, error) {
	r.agent = agent
	return agent, nil
}

func (r *fakeAgentRepository) DeleteAgent(_ context.Context, _ ID, agentID ID) error {
	r.deleted = agentID
	return nil
}

func (r *fakeAgentRepository) MarkAgentSeen(context.Context, ID) error {
	return nil
}

func (r *fakeAgentRepository) RecordAgentRun(_ context.Context, report AgentRunReport) error {
	r.run = report
	return nil
}

func (r *fakeAgentRepository) ListAgentRuns(context.Context, ID, ID, int) ([]AgentRunHistory, error) {
	return r.runs, nil
}

func TestAPIAgentRegistryListsRuns(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: billingTestAuth(true),
		Agents: &fakeAgentRepository{
			agent: SNMPAgentConfig{ID: "agent-a", TenantID: "tenant-a", TargetID: "target-a"},
			runs: []AgentRunHistory{{
				ID:       "run-a",
				TenantID: "tenant-a",
				AgentID:  "agent-a",
				TargetID: "target-a",
				Status:   AgentRunFailure,
				Error:    "snmp timeout",
			}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/agent-registry/agent-a/runs", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "run-a") || !strings.Contains(body, "snmp timeout") {
		t.Fatalf("body = %s", body)
	}
}

func TestAPIAgentRegistryCreatesSystemAgent(t *testing.T) {
	repo := &fakeAgentRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:   billingTestAuth(true),
		Agents: repo,
	})
	body := `{"ID":"agent-system-a","TargetID":"target-a","AgentType":"system","Mode":"push","Endpoint":"http://127.0.0.1:45876","Token":"secret-a"}`
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/agent-registry", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.agent.AgentType != AgentTypeSystem {
		t.Fatalf("agent type = %q", repo.agent.AgentType)
	}
	if !strings.Contains(rec.Body.String(), `"AgentType":"system"`) {
		t.Fatalf("body missing agent type: %s", rec.Body.String())
	}
}

func TestAPIAgentRegistryListAllowsViewOnlyTenant(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: permissionTestAuth(false),
		Agents: &fakeAgentRepository{
			agent: SNMPAgentConfig{ID: "agent-a", TenantID: "tenant-a", TargetID: "target-a"},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/agent-registry", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (view-only user should reach list), body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "agent-a") {
		t.Fatalf("body missing agent-a: %s", rec.Body.String())
	}
}

func TestAPIAgentRegistryGetAllowsViewOnlyTenant(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: permissionTestAuth(false),
		Agents: &fakeAgentRepository{
			agent: SNMPAgentConfig{ID: "agent-a", TenantID: "tenant-a", TargetID: "target-a"},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/agent-registry/agent-a", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (view-only user should reach get), body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "agent-a") {
		t.Fatalf("body missing agent-a: %s", rec.Body.String())
	}
}

func TestAPIAgentRegistryCreateStillRequiresConfigure(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:   permissionTestAuth(false),
		Agents: &fakeAgentRepository{},
	})
	body := `{"ID":"agent-x","TargetID":"target-a","AgentType":"snmp","Mode":"pull","Endpoint":"127.0.0.1:161","Token":"secret-x"}`
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/agent-registry", strings.NewReader(body)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (view-only user cannot create)", rec.Code)
	}
}

func TestAPISystemAgentPlanBuildsTargetPlan(t *testing.T) {
	repo := &fakeHeartbeatAgentRepository{fakeAgentRepository: fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-system-a",
		TenantID:  "tenant-a",
		TargetID:  "target-system-a",
		AgentType: AgentTypeSystem,
		Mode:      AgentModePush,
		TokenHash: NewAgentTokenHash("secret-a"),
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Agents: repo,
		Targets: &fakeTargetRepository{targets: []Target{{
			ID:       "target-system-a",
			TenantID: "tenant-a",
			Type:     TargetTypeSystem,
			Host:     "192.0.2.10",
		}}},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system-agents/agent-system-a/plan", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{`"Agent":{"ID":"agent-system-a"`, `"Interval":60000000000`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, "TokenHash") {
		t.Fatalf("body leaked token hash: %s", body)
	}
}

func TestAPISystemAgentPlanRejectsNetworkTarget(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Agents: &fakeAgentRepository{agent: SNMPAgentConfig{
			ID:        "agent-system-a",
			TenantID:  "tenant-a",
			TargetID:  "target-network-a",
			AgentType: AgentTypeSystem,
			Mode:      AgentModePush,
			TokenHash: NewAgentTokenHash("secret-a"),
		}},
		Targets: &fakeTargetRepository{targets: []Target{{
			ID:       "target-network-a",
			TenantID: "tenant-a",
			Type:     TargetTypeNetwork,
		}}},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system-agents/agent-system-a/plan", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "not a system target") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPIAgentRegistryCreateDoesNotLeakTokenHash(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:   billingTestAuth(false),
		Agents: &fakeAgentRepository{},
	})
	rec := httptest.NewRecorder()
	body := `{"ID":"agent-a","TargetID":"target-a","Mode":"push","Endpoint":"http://127.0.0.1:9273","Token":"secret-a"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/agent-registry", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret-a") || strings.Contains(rec.Body.String(), "TokenHash") {
		t.Fatalf("response leaked token: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"TenantID":"tenant-a"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAPIAgentRegistryDelete(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: billingTestAuth(false),
		Agents: &fakeAgentRepository{agent: SNMPAgentConfig{
			ID:       "agent-a",
			TenantID: "tenant-a",
			TargetID: "target-a",
		}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/agent-registry/agent-a", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

type fakeMetricsImporter struct {
	payload string
}

type fakeHeartbeatAgentRepository struct {
	fakeAgentRepository
	seen ID
}

func (r *fakeHeartbeatAgentRepository) MarkAgentSeen(_ context.Context, agentID ID) error {
	r.seen = agentID
	return nil
}

func (r *fakeHeartbeatAgentRepository) RecordAgentRun(_ context.Context, report AgentRunReport) error {
	r.run = report
	return nil
}

func TestAPIAgentHeartbeatMarksAgentSeen(t *testing.T) {
	repo := &fakeHeartbeatAgentRepository{fakeAgentRepository: fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-a",
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		TokenHash: NewAgentTokenHash("secret-a"),
	}}}
	router := NewAPIV1Router(APIV1RouterConfig{Agents: repo})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/agent-a/heartbeat", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.seen != "agent-a" {
		t.Fatalf("seen = %s", repo.seen)
	}
}

func TestAPIAgentHeartbeatRejectsInvalidToken(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Agents: &fakeAgentRepository{agent: SNMPAgentConfig{
			ID:        "agent-a",
			TokenHash: NewAgentTokenHash("secret-a"),
		}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/agent-a/heartbeat", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "wrong")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAPIAgentStatusRecordsFailure(t *testing.T) {
	repo := &fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-a",
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		TokenHash: NewAgentTokenHash("secret-a"),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Agents: repo})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/agent-a/status", strings.NewReader(`{"Status":"failure","Error":"snmp timeout"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.run.Status != AgentRunFailure || repo.run.Error != "snmp timeout" || !repo.run.Seen {
		t.Fatalf("run = %#v", repo.run)
	}
}

func TestAPIAgentErrorsRecordsFailure(t *testing.T) {
	repo := &fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-a",
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		TokenHash: NewAgentTokenHash("secret-a"),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Agents: repo})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/agent-a/errors", strings.NewReader(`{"Error":"oid missing"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.run.Status != AgentRunFailure || repo.run.Error != "oid missing" || !repo.run.Seen {
		t.Fatalf("run = %#v", repo.run)
	}
}

func (i *fakeMetricsImporter) ImportPrometheus(_ context.Context, payload []byte) error {
	i.payload = string(payload)
	return nil
}

func TestAPISystemAgentPushImportsPrometheusPayload(t *testing.T) {
	importer := &fakeMetricsImporter{}
	agents := &fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-system-a",
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		AgentType: AgentTypeSystem,
		Mode:      AgentModePush,
		TokenHash: NewAgentTokenHash("secret-a"),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Agents:  agents,
		Metrics: MetricsService{Importer: importer},
	})
	batch := SystemSampleBatch{
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		SampledAt: time.Now().UTC(),
		System: SystemResourceSample{
			CPUPercent:    11,
			MemoryPercent: 22,
			DiskPercent:   33,
			NetInBps:      44,
			NetOutBps:     55,
		},
		Containers: []ContainerSample{{
			Name:        "nginx",
			CPUPercent:  6,
			MemoryBytes: 1024,
			NetTxBps:    7,
			NetRxBps:    8,
		}},
		GPUs: []GPUSample{{
			Index:              "0",
			Name:               "A100",
			Up:                 true,
			UtilizationPercent: 66,
		}},
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/system-agents/agent-system-a/samples", strings.NewReader(string(body)))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{MetricSystemCPUPercent, MetricContainerCPUPercent, `container_name="nginx"`, MetricGPUUtilPercent, `gpu_name="A100"`} {
		if !strings.Contains(importer.payload, want) {
			t.Fatalf("payload missing %s:\n%s", want, importer.payload)
		}
	}
	if agents.run.Status != AgentRunSuccess || !agents.run.Seen {
		t.Fatalf("run = %#v", agents.run)
	}
}

func TestAPISystemAgentPushRejectsSNMPAgent(t *testing.T) {
	importer := &fakeMetricsImporter{}
	agents := &fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-snmp-a",
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		AgentType: AgentTypeSNMP,
		Mode:      AgentModePush,
		TokenHash: NewAgentTokenHash("secret-a"),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Agents:  agents,
		Metrics: MetricsService{Importer: importer},
	})
	body := `{"TenantID":"tenant-a","TargetID":"target-a","SampledAt":"2026-06-19T00:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/system-agents/agent-snmp-a/samples", strings.NewReader(body))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if importer.payload != "" {
		t.Fatalf("unexpected payload:\n%s", importer.payload)
	}
}
