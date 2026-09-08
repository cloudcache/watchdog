package server

import (
	"encoding/json"
	"strings"
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

func TestCanonicalFlowSourcePrefix(t *testing.T) {
	for input, want := range map[string]string{
		"192.0.2.10":      "192.0.2.10/32",
		"192.0.2.99/24":   "192.0.2.0/24",
		"[2001:db8::1]":   "2001:db8::1/128",
		"2001:db8::42/64": "2001:db8::/64",
	} {
		got, err := canonicalFlowSourcePrefix(input)
		if err != nil || got != want {
			t.Errorf("canonicalFlowSourcePrefix(%q)=(%q,%v), want %q", input, got, err, want)
		}
	}
	if _, err := canonicalFlowSourcePrefix("not-an-address"); err == nil {
		t.Fatal("invalid flow source was accepted")
	}
}

func TestValidateFlowExporterValues(t *testing.T) {
	valid := flowExporterValues{
		DeviceID: "device-test", SourcePrefix: "192.0.2.10", Protocol: "sflow5",
		SamplingMode: "sampled", SamplingRules: `[]`, Observations: `{}`, Enabled: true, OwnershipEpoch: 1,
	}
	got, err := validateFlowExporterValues(valid)
	if err != nil {
		t.Fatalf("valid exporter rejected: %v", err)
	}
	if got.SourcePrefix != "192.0.2.10/32" || got.SamplingRules != `[]` || got.Observations != `{}` {
		t.Fatalf("exporter was not canonicalized: %+v", got)
	}

	preScaled := valid
	preScaled.SamplingMode = "pre_scaled"
	preScaled.DefaultSamplingRate = 1000
	if _, err := validateFlowExporterValues(preScaled); err == nil || !strings.Contains(err.Error(), "default_sampling_rate") {
		t.Fatalf("pre-scaled exporter accepted a multiplier: %v", err)
	}

	badRule := valid
	badRule.SamplingRules = `[{"mode":1,"rate":1000}]`
	if _, err := validateFlowExporterValues(badRule); err == nil || !strings.Contains(err.Error(), "source_id") {
		t.Fatalf("ambiguous sFlow rule accepted: %v", err)
	}
}

func TestFlowExporterDeploymentState(t *testing.T) {
	for name, record := range map[string]flowExporterRecord{
		"unpublished":     {RowVersion: 1},
		"pending_publish": {RowVersion: 2, PublishedRowVersion: 1, PublishedPlanVersion: 7},
		"active":          {RowVersion: 2, PublishedRowVersion: 2, PublishedPlanVersion: 8, Enabled: true},
		"inactive":        {RowVersion: 2, PublishedRowVersion: 2, PublishedPlanVersion: 9},
	} {
		if got := record.deploymentState(); got != name {
			t.Errorf("deploymentState(%s)=%q", name, got)
		}
	}
}
