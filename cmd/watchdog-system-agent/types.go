package main

import "time"

type systemAgentScope struct {
	TenantID string
	TargetID string
}

type systemAgentPlan struct {
	Agent    systemAgentScope
	HubURL   string
	Interval time.Duration
}

type systemSampleBatch struct {
	TenantID   string
	TargetID   string
	SampledAt  time.Time
	System     systemResourceSample
	Containers []containerSample
	GPUs       []gpuSample
}

type systemResourceSample struct {
	CPUPercent    float64
	MemoryPercent float64
	DiskPercent   float64
	NetInBps      float64
	NetOutBps     float64
}

type containerSample struct {
	Name        string
	CPUPercent  float64
	MemoryBytes uint64
	NetTxBps    float64
	NetRxBps    float64
}

type gpuSample struct {
	Index              string
	Name               string
	Up                 bool
	UtilizationPercent float64
}
