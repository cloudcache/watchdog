package server

import (
	"strings"
	"testing"
)

func TestAgentPlanACKValidation(t *testing.T) {
	valid := agentPlanAckRequest{
		PlanVersion: 1, PayloadSHA256: strings.Repeat("a", 64), Status: "applied",
		BootID: "boot-1", Software: "1.2.3",
	}
	if err := validateAgentPlanAck(valid); err != nil {
		t.Fatal(err)
	}
	rejected := valid
	rejected.Status = "rejected"
	rejected.ErrorCode = "ACTIVATE_FAILED"
	rejected.Error = "invalid local path"
	if err := validateAgentPlanAck(rejected); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string]func(*agentPlanAckRequest){
		"missing boot":            func(v *agentPlanAckRequest) { v.BootID = "" },
		"invalid digest":          func(v *agentPlanAckRequest) { v.PayloadSHA256 = "no" },
		"applied error":           func(v *agentPlanAckRequest) { v.ErrorCode, v.Error = "FAILED", "bad" },
		"rejected without detail": func(v *agentPlanAckRequest) { v.Status, v.ErrorCode = "rejected", "FAILED" },
	} {
		candidate := valid
		mutation(&candidate)
		if err := validateAgentPlanAck(candidate); err == nil {
			t.Errorf("%s ACK accepted: %+v", name, candidate)
		}
	}
}
