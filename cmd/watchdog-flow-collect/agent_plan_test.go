package main

import (
	"encoding/json"
	"testing"

	"github.com/cloudcache/watchdog/internal/agentplan"
)

func TestApplyFlowCollectAgentPlan(t *testing.T) {
	opt := options{sflowListen: ":6343", netflowListen: ":2055", sockets: 1, receiveBuffer: 1 << 20, maxUDP: 65535}
	spec := agentplan.Spec{Config: json.RawMessage(`{"sockets":8,"receive_buffer_bytes":33554432,"max_datagram_bytes":9000}`)}
	if err := applyFlowCollectAgentPlan(&opt, spec); err != nil || opt.sockets != 8 || opt.maxUDP != 9000 {
		t.Fatalf("apply: options=%+v err=%v", opt, err)
	}
	spec.Config = json.RawMessage(`{"sflow_listen":"","netflow_listen":""}`)
	if err := applyFlowCollectAgentPlan(&opt, spec); err == nil {
		t.Fatal("plan disabled every listener")
	}
}
