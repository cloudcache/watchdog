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

// fakeAgentRepository remains intentionally compatible with the historical
// repository contract while the system plan/sample path is migrated to Gin.
// Registry CRUD, heartbeat and run lifecycle coverage now lives in
// internal/server/device_agent_integration_test.go against the MySQL schema.
type fakeAgentRepository struct {
	agent SNMPAgentConfig
	run   AgentRunReport
	runs  []AgentRunHistory
}

func (r *fakeAgentRepository) ListAgentRunsPage(context.Context, ID, ID, AgentRunPageFilter) ([]AgentRunHistory, string, int, error) {
	return r.runs, "", len(r.runs), nil
}

func (r *fakeAgentRepository) GetAgent(context.Context, ID) (SNMPAgentConfig, error) {
	return r.agent, nil
}

func (r *fakeAgentRepository) ListAgents(context.Context, ID) ([]SNMPAgentConfig, error) {
	return []SNMPAgentConfig{r.agent}, nil
}

func (r *fakeAgentRepository) ListAgentsPage(context.Context, ID, bool, []ID, AgentPageFilter) ([]SNMPAgentConfig, int, error) {
	return []SNMPAgentConfig{r.agent}, 1, nil
}

func (r *fakeAgentRepository) UpsertAgent(_ context.Context, agent SNMPAgentConfig) (SNMPAgentConfig, error) {
	r.agent = agent
	return agent, nil
}

func (r *fakeAgentRepository) DeleteAgent(context.Context, ID, ID) error { return nil }
func (r *fakeAgentRepository) MarkAgentSeen(context.Context, ID) error   { return nil }

func (r *fakeAgentRepository) RecordAgentRun(_ context.Context, report AgentRunReport) error {
	r.run = report
	return nil
}

func (r *fakeAgentRepository) ListAgentRuns(context.Context, ID, ID, int) ([]AgentRunHistory, error) {
	return r.runs, nil
}

func TestAPISystemAgentPlanBuildsTargetPlan(t *testing.T) {
	repo := &fakeAgentRepository{agent: SNMPAgentConfig{
		ID:        "agent-system-a",
		TenantID:  "tenant-a",
		TargetID:  "target-system-a",
		AgentType: AgentTypeSystem,
		Mode:      AgentModePush,
		TokenHash: NewAgentTokenHash("secret-a"),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Agents: repo,
		Targets: &fakeTargetRepository{targets: []Target{{
			ID:       "target-system-a",
			TenantID: "tenant-a",
			Kind:     TargetKindSystem,
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
			Kind:     TargetKindNetwork,
		}}},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system-agents/agent-system-a/plan", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret-a")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "not a system target") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

type fakeMetricsImporter struct {
	payload string
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
			Name: "nginx", CPUPercent: 6, MemoryBytes: 1024, NetTxBps: 7, NetRxBps: 8,
		}},
		GPUs: []GPUSample{{
			Index: "0", Name: "A100", Up: true, UtilizationPercent: 66,
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
	router := NewAPIV1Router(APIV1RouterConfig{Agents: agents, Metrics: MetricsService{Importer: importer}})
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
