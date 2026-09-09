package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
)

func TestApplySNMPAgentPlan(t *testing.T) {
	interval, limit := time.Minute, 100
	spec := agentplan.Spec{Config: json.RawMessage(`{"interval_seconds":30,"poll_limit":250}`)}
	if err := applySNMPAgentPlan(&interval, &limit, spec); err != nil || interval != 30*time.Second || limit != 250 {
		t.Fatalf("apply: interval=%s limit=%d err=%v", interval, limit, err)
	}
	spec.Config = json.RawMessage(`{"poll_limit":0}`)
	if err := applySNMPAgentPlan(&interval, &limit, spec); err == nil {
		t.Fatal("invalid poll limit accepted")
	}
}
