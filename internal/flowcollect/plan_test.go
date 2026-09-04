package flowcollect

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/netip"
	"testing"
	"time"
)

func TestRegistryUsesLongestPrefixThenExactDomain(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{
		{Protocol: ProtocolIPFIX, SourcePrefix: "10.0.0.0/8", TenantID: "broad", ExporterID: "broad", TargetID: "target", DeviceID: "device", SamplingMode: SamplingModeSampled, Enabled: true},
		{Protocol: ProtocolIPFIX, SourcePrefix: "10.1.0.0/16", TenantID: "wildcard", ExporterID: "wildcard", TargetID: "target", DeviceID: "device", SamplingMode: SamplingModeSampled, Enabled: true},
		{Protocol: ProtocolIPFIX, SourcePrefix: "10.1.0.0/16", ObservationDomainID: &domain, TenantID: "exact", ExporterID: "exact", TargetID: "target", DeviceID: "device", SamplingMode: SamplingModeSampled, Enabled: true},
	}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	binding, ok := registry.Admit(ProtocolIPFIX, netip.MustParseAddr("10.1.2.3"), 42)
	if !ok || binding.TenantID != "exact" {
		t.Fatalf("unexpected binding: %+v %v", binding, ok)
	}
	binding, ok = registry.Admit(ProtocolIPFIX, netip.MustParseAddr("10.1.2.3"), 7)
	if !ok || binding.TenantID != "wildcard" {
		t.Fatalf("unexpected wildcard binding: %+v %v", binding, ok)
	}
}

func TestSignedPlanRejectsTampering(t *testing.T) {
	now := time.Now()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(validPlan(now))
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(privateKey, payload)
	envelope, _ := json.Marshal(signedPlanEnvelope{SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(signature)})
	if _, err := VerifySignedPlan(envelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-2] ^= 1
	tampered, _ := json.Marshal(signedPlanEnvelope{SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(signature)})
	if _, err := VerifySignedPlan(tampered, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err == nil {
		t.Fatal("tampered plan was accepted")
	}
}

func TestPlanRejectsAmbiguousSamplingRules(t *testing.T) {
	now := time.Now()
	ifIndex, subAgent, sourceType, sourceID := uint32(10), uint32(1), uint32(0), uint32(20)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{Protocol: ProtocolSFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant", ExporterID: "exporter", TargetID: "target", DeviceID: "device", SamplingMode: SamplingModeSampled, Enabled: true, SamplingRules: []SamplingRule{{SubAgentID: &subAgent, SourceIDType: &sourceType, SourceIDValue: &sourceID, Mode: SamplingModeSampled, Rate: 1000}, {SourceIDType: &sourceType, SourceIDValue: &sourceID, IfIndex: &ifIndex, Mode: SamplingModeSampled, Rate: 10000}}}}
	if _, err := CompilePlan(plan, now); err == nil {
		t.Fatal("ambiguous equal-specificity sampling rules were accepted")
	}
}

func TestPlanV2RequiresOwnershipEpoch(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.Sources = []SourceBinding{{Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", TenantID: "tenant", ExporterID: "exporter", TargetID: "target", SamplingMode: SamplingModeSampled, Enabled: true}}
	if _, err := CompilePlan(plan, now); err == nil {
		t.Fatal("schema-v2 plan without ownership_epoch was accepted")
	}
	plan.Sources[0].OwnershipEpoch = 2
	if _, err := CompilePlan(plan, now); err != nil {
		t.Fatalf("schema-v2 plan with ownership_epoch was rejected: %v", err)
	}
}

func validPlan(now time.Time) Plan {
	partitionMap := make([]uint32, VirtualShardCount)
	for index := range partitionMap {
		partitionMap[index] = uint32(index % 8)
	}
	return Plan{SchemaVersion: 1, Revision: 1, CollectorID: "collector-test", NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), PartitionMapVersion: 1, PartitionMap: partitionMap}
}
