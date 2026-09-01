package watchdog

import (
	"errors"
	"math"
	"time"
)

type SystemSampleBatch struct {
	TenantID   ID
	TargetID   ID
	SampledAt  time.Time
	System     SystemResourceSample
	Containers []ContainerSample
	GPUs       []GPUSample
}

type SystemAgentPlan struct {
	Agent    SNMPAgentConfig
	HubURL   string
	Interval time.Duration
}

type SystemResourceSample struct {
	CPUPercent    float64
	MemoryPercent float64
	DiskPercent   float64
	NetInBps      float64
	NetOutBps     float64
}

type ContainerSample struct {
	Name        string
	CPUPercent  float64
	MemoryBytes uint64
	NetTxBps    float64
	NetRxBps    float64
}

type GPUSample struct {
	Index              string
	Name               string
	Up                 bool
	UtilizationPercent float64
}

type SystemPushRequest struct {
	Agent SNMPAgentConfig
	Token string
	Batch SystemSampleBatch
}

func ValidateSystemPush(req SystemPushRequest) error {
	if normalizeAgentType(req.Agent.AgentType) != AgentTypeSystem {
		return errors.New("agent is not configured for system collection")
	}
	if req.Agent.Mode != AgentModePush {
		return errors.New("agent is not configured for push mode")
	}
	if req.Agent.TokenHash == "" || !AgentTokenMatches(req.Token, req.Agent.TokenHash) {
		return errors.New("invalid agent token")
	}
	if req.Batch.TenantID != req.Agent.TenantID || req.Batch.TargetID != req.Agent.TargetID {
		return errors.New("sample batch does not match agent scope")
	}
	if req.Batch.SampledAt.IsZero() || req.Batch.SampledAt.After(time.Now().Add(5*time.Minute)) {
		return errors.New("invalid sample timestamp")
	}
	for _, value := range []float64{
		req.Batch.System.CPUPercent,
		req.Batch.System.MemoryPercent,
		req.Batch.System.DiskPercent,
		req.Batch.System.NetInBps,
		req.Batch.System.NetOutBps,
	} {
		if invalidMetricValue(value) {
			return errors.New("sample contains invalid numeric value")
		}
	}
	for _, sample := range req.Batch.Containers {
		if sample.Name == "" {
			return errors.New("container sample name is required")
		}
		if invalidMetricValue(sample.CPUPercent) || invalidMetricValue(sample.NetTxBps) || invalidMetricValue(sample.NetRxBps) {
			return errors.New("sample contains invalid numeric value")
		}
	}
	for _, sample := range req.Batch.GPUs {
		if sample.Index == "" && sample.Name == "" {
			return errors.New("gpu sample index or name is required")
		}
		if invalidMetricValue(sample.UtilizationPercent) {
			return errors.New("sample contains invalid numeric value")
		}
	}
	return nil
}

func invalidMetricValue(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0)
}
