package flowcollect

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	originalPayload := append([]byte(nil), payload...)
	envelope, _ := json.Marshal(signedPlanEnvelope{SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(signature)})
	if _, err := VerifySignedPlan(envelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-2] ^= 1
	tampered, _ := json.Marshal(signedPlanEnvelope{SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(signature)})
	if _, err := VerifySignedPlan(tampered, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err == nil {
		t.Fatal("tampered plan was accepted")
	}
	confused, _ := json.Marshal(signedPlanEnvelope{SchemaVersion: 1, Payload: base64.StdEncoding.EncodeToString(originalPayload), Signature: base64.StdEncoding.EncodeToString(signature), TenantID: "unsigned-tenant"})
	if _, err := VerifySignedPlan(confused, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err == nil {
		t.Fatal("legacy envelope with unsigned control-plane metadata was accepted")
	}
}

func TestControlPlaneSignedPlanMatchesRepositoryEnvelopeContract(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	plan := validPlan(now)
	plan.NotBefore = plan.NotBefore.Truncate(time.Millisecond)
	plan.ExpiresAt = plan.ExpiresAt.Truncate(time.Millisecond)
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	metadata := PlanSignatureMetadata{
		PlanID: "plan-test", TenantID: "tenant-test", CollectorID: plan.CollectorID,
		ConfigVersion: plan.Revision, PlanSchemaVersion: uint16(plan.SchemaVersion),
		SpecHash: hex.EncodeToString(digest[:]), SigningKeyID: "key-test",
		NotBeforeUnixMilli: plan.NotBefore.UnixMilli(), ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(),
	}
	signingPayload, err := BuildPlanSignaturePayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := MarshalControlPlaneSignedPlan(metadata, payload, ed25519.Sign(privateKey, signingPayload))
	if err != nil {
		t.Fatal(err)
	}
	registry, err := VerifySignedPlan(envelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now)
	if err != nil {
		t.Fatal(err)
	}
	if registry.plan.Revision != metadata.ConfigVersion || registry.plan.CollectorID != metadata.CollectorID {
		t.Fatalf("unexpected verified plan: %+v", registry.plan)
	}

	var tampered signedPlanEnvelope
	if err := json.Unmarshal(envelope, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.SigningKeyID = "other-key"
	tamperedEnvelope, err := json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySignedPlan(tamperedEnvelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err == nil {
		t.Fatal("tampered control-plane metadata was accepted")
	}

	tampered = signedPlanEnvelope{}
	if err := json.Unmarshal(envelope, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.Payload = base64.StdEncoding.EncodeToString(append(payload, ' '))
	tamperedEnvelope, _ = json.Marshal(tampered)
	if _, err := VerifySignedPlan(tamperedEnvelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now); err == nil {
		t.Fatal("tampered control-plane payload was accepted")
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

func TestCompiledRegistryOwnsAnImmutablePlanSnapshot(t *testing.T) {
	now := time.Now()
	domain := uint64(42)
	ifIndex := uint32(7)
	plan := validPlan(now)
	plan.Sources = []SourceBinding{{
		Protocol: ProtocolNetFlow9, SourcePrefix: "192.0.2.1/32", ObservationDomainID: &domain,
		TenantID: "tenant", ExporterID: "exporter", TargetID: "target", SamplingMode: SamplingModeSampled,
		SamplingRules: []SamplingRule{{ObservationDomainID: &domain, IfIndex: &ifIndex, Mode: SamplingModeSampled, Rate: 100}},
		Observations:  map[uint32]Observation{7: {Direction: 1}}, Enabled: true,
	}}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	plan.PartitionMap[0] = 99
	plan.Sources[0].TenantID = "mutated-input"
	plan.Sources[0].SamplingRules[0].Rate = 999
	plan.Sources[0].Observations[7] = Observation{Direction: 2}
	returned := registry.Plan()
	returned.PartitionMap[0] = 88
	returned.Sources[0].TenantID = "mutated-output"
	returned.Sources[0].SamplingRules[0].Rate = 888
	returned.Sources[0].Observations[7] = Observation{Direction: 2}

	snapshot := registry.Plan()
	if snapshot.PartitionMap[0] == 99 || snapshot.PartitionMap[0] == 88 || snapshot.Sources[0].TenantID != "tenant" || snapshot.Sources[0].SamplingRules[0].Rate != 100 || snapshot.Sources[0].Observations[7].Direction != 1 {
		t.Fatalf("compiled registry plan was mutated: %+v", snapshot)
	}
}

func validPlan(now time.Time) Plan {
	partitionMap := make([]uint32, VirtualShardCount)
	for index := range partitionMap {
		partitionMap[index] = uint32(index % 8)
	}
	return Plan{SchemaVersion: 1, Revision: 1, CollectorID: "collector-test", NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), PartitionMapVersion: 1, PartitionMap: partitionMap}
}
