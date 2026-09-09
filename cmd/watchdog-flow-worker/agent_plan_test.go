package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
)

func TestApplyFlowWorkerAgentPlan(t *testing.T) {
	opt := options{fetchMinBytes: 1, fetchMaxWait: time.Second, blockMaxRows: 100, blockMaxBytes: 1 << 20}
	spec := agentplan.Spec{Config: json.RawMessage(`{"kafka_fetch_min_bytes":1048576,"kafka_fetch_max_wait_ms":250,"clickhouse_block_max_rows":50000,"clickhouse_block_max_bytes":67108864}`)}
	if err := applyFlowWorkerAgentPlan(&opt, spec); err != nil || opt.fetchMaxWait != 250*time.Millisecond || opt.blockMaxRows != 50000 {
		t.Fatalf("apply: options=%+v err=%v", opt, err)
	}
	spec.Config = json.RawMessage(`{"clickhouse_block_max_rows":0}`)
	if err := applyFlowWorkerAgentPlan(&opt, spec); err == nil {
		t.Fatal("invalid block limit accepted")
	}
}
