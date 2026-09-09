package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
)

func TestApplySystemAgentPlan(t *testing.T) {
	interval, root := time.Minute, "/"
	spec := agentplan.Spec{Config: json.RawMessage(`{"interval_seconds":15,"root_path":"/host"}`)}
	if err := applySystemAgentPlan(&interval, &root, spec); err != nil || interval != 15*time.Second || root != "/host" {
		t.Fatalf("apply: interval=%s root=%q err=%v", interval, root, err)
	}
	spec.Config = json.RawMessage(`{"interval_seconds":1}`)
	if err := applySystemAgentPlan(&interval, &root, spec); err == nil {
		t.Fatal("invalid interval accepted")
	}
}
