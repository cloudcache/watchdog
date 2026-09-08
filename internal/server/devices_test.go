package server

import (
	"encoding/json"
	"testing"
)

func TestNormalizeDeviceHost(t *testing.T) {
	for input, want := range map[string]string{
		" Router.Example.COM. ": "router.example.com",
		"010.000.000.001":       "010.000.000.001",
		"[2001:0db8::1]":        "2001:db8::1",
		"192.0.2.10":            "192.0.2.10",
	} {
		if got := normalizeDeviceHost(input); got != want {
			t.Errorf("normalizeDeviceHost(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestNormalizeCapabilities(t *testing.T) {
	got, err := normalizeCapabilities(json.RawMessage(`["snmp.poll/v2"]`), false)
	if err != nil || got != `["snmp.poll/v2"]` {
		t.Fatalf("valid capability rejected: got=%q err=%v", got, err)
	}
	for _, input := range []string{`{"snmp":true}`, `["unversioned"]`} {
		if _, err := normalizeCapabilities(json.RawMessage(input), false); err == nil {
			t.Errorf("invalid capability accepted: %s", input)
		}
	}
}

func TestAgentAndDeviceEnums(t *testing.T) {
	if !validDeviceKind("network") || !validDeviceKind(canonicalDeviceKind("system")) || validDeviceKind("target") {
		t.Fatal("device kind validation is not fail-closed")
	}
	if !validAgentKind("flow_collect") || validAgentKind("arbitrary") {
		t.Fatal("agent kind validation is not fail-closed")
	}
	if !validAgentStatus("disabled") || validAgentStatus("deleted") {
		t.Fatal("agent status validation is not fail-closed")
	}
}
