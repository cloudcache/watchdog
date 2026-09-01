package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// AgentPlanService handles agent plan building, heartbeat/status reporting,
// and push-mode sample/discovery import. SNMP pull-mode collection is handled
// by the SNMP collector engine (snmp_poller.go + snmp_collection_recipes).
type AgentPlanService struct {
	Agents  AgentRepository
	Targets TargetRepository
	Network NetworkRepository
	SNMP    SNMPRepository
	Metrics MetricsService
}

func (s AgentPlanService) BuildSystemAgentPlan(ctx context.Context, agentID ID, token string) (SystemAgentPlan, error) {
	if s.Agents == nil {
		return SystemAgentPlan{}, errors.New("agent plan service is not configured")
	}
	agent, err := s.Agents.GetAgent(ctx, agentID)
	if err != nil {
		return SystemAgentPlan{}, err
	}
	if !AgentTokenMatches(token, agent.TokenHash) {
		return SystemAgentPlan{}, errors.New("invalid agent token")
	}
	if !isSystemAgent(agent) {
		return SystemAgentPlan{}, errors.New("agent is not configured for system collection")
	}
	if s.Targets != nil {
		target, err := s.Targets.GetTarget(ctx, agent.TenantID, agent.TargetID)
		if err != nil {
			return SystemAgentPlan{}, err
		}
		if target.Type != TargetTypeSystem {
			return SystemAgentPlan{}, errors.New("target is not a system target")
		}
	}
	plan := SystemAgentPlan{Agent: agent, HubURL: s.systemHubURL(), Interval: time.Minute}
	return plan, nil
}

func (s AgentPlanService) MarkHeartbeat(ctx context.Context, agentID ID, token string) error {
	if s.Agents == nil {
		return errors.New("agent plan service is not configured")
	}
	agent, err := s.Agents.GetAgent(ctx, agentID)
	if err != nil {
		return err
	}
	if !AgentTokenMatches(token, agent.TokenHash) {
		return errors.New("invalid agent token")
	}
	return s.Agents.MarkAgentSeen(ctx, agentID)
}

func (s AgentPlanService) ReportStatus(ctx context.Context, agentID ID, token string, report AgentRunReport) error {
	if s.Agents == nil {
		return errors.New("agent plan service is not configured")
	}
	agent, err := s.Agents.GetAgent(ctx, agentID)
	if err != nil {
		return err
	}
	if !AgentTokenMatches(token, agent.TokenHash) {
		return errors.New("invalid agent token")
	}
	report.AgentID = agentID
	report.Seen = true
	return s.Agents.RecordAgentRun(ctx, report)
}

func (s AgentPlanService) ImportSystemPush(ctx context.Context, agentID ID, token string, batch SystemSampleBatch) error {
	startedAt := time.Now().UTC()
	if s.Agents == nil || s.Metrics.Importer == nil {
		return errors.New("system agent ingestion service is not configured")
	}
	agent, err := s.Agents.GetAgent(ctx, agentID)
	if err != nil {
		return err
	}
	if !isSystemAgent(agent) {
		return errors.New("agent is not configured for system collection")
	}
	if err := ValidateSystemPush(SystemPushRequest{Agent: agent, Token: token, Batch: batch}); err != nil {
		_ = s.Agents.RecordAgentRun(ctx, AgentRunReport{AgentID: agentID, Status: AgentRunFailure, Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return err
	}
	if err := s.Metrics.Importer.ImportPrometheus(ctx, RenderSystemBatchPrometheus(batch)); err != nil {
		_ = s.Agents.RecordAgentRun(ctx, AgentRunReport{AgentID: agentID, Status: AgentRunFailure, Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return err
	}
	_ = s.Agents.RecordAgentRun(ctx, AgentRunReport{AgentID: agentID, Status: AgentRunSuccess, Seen: true, StartedAt: startedAt, EndedAt: time.Now().UTC()})
	return nil
}

func (s AgentPlanService) systemHubURL() string {
	return ""
}

func isSNMPAgent(agent SNMPAgentConfig) bool {
	return normalizeAgentType(agent.AgentType) == AgentTypeSNMP
}

func isSystemAgent(agent SNMPAgentConfig) bool {
	return normalizeAgentType(agent.AgentType) == AgentTypeSystem
}

func (s AgentPlanService) deviceForTarget(ctx context.Context, tenantID, targetID ID) (NetworkDevice, error) {
	devices, err := s.Network.ListDevices(ctx, tenantID)
	if err != nil {
		return NetworkDevice{}, err
	}
	for _, device := range devices {
		if device.TargetID == targetID {
			return device, nil
		}
	}
	return NetworkDevice{}, sql.ErrNoRows
}

var _ = context.Background
