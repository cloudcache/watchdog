package flowplan

import (
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestCompilePlanRejectsSampledFallbackWithoutRate(t *testing.T) {
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	domain := uint64(42)
	_, err := CompilePlan(Plan{
		SchemaVersion: 2, Revision: 1, CollectorID: "collector_test", NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		Sources: []SourceBinding{{
			Protocol: ProtocolIPFIX, SourcePrefix: "192.0.2.0/24", TenantID: "tenant", ExporterID: "exporter", TargetID: "target",
			OwnershipEpoch: 1, SamplingMode: SamplingModeSampled, Enabled: true,
			SamplingRules: []SamplingRule{{ObservationDomainID: &domain, Mode: SamplingModeSampled}},
		}},
	}, now)
	if err == nil || !strings.Contains(err.Error(), "rate is required") {
		t.Fatalf("error=%v", err)
	}
}

func TestCompilePlanRejectsDefaultRateForPreScaledCounters(t *testing.T) {
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	_, err := CompilePlan(Plan{
		SchemaVersion: 2, Revision: 1, CollectorID: "collector_test", NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		Sources: []SourceBinding{{
			Protocol: ProtocolNetFlow5, SourcePrefix: "192.0.2.0/24", TenantID: "tenant", ExporterID: "exporter", TargetID: "target",
			OwnershipEpoch: 1, SamplingMode: SamplingModePreScaled, DefaultSamplingRate: 1000, Enabled: true,
		}},
	}, now)
	if err == nil || !strings.Contains(err.Error(), "invalid for pre-scaled") {
		t.Fatalf("error=%v", err)
	}
}

func TestAdmitSourceFamily(t *testing.T) {
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	registry, err := CompilePlan(Plan{
		SchemaVersion: 2,
		Revision:      1,
		CollectorID:   "collector_test",
		NotBefore:     now.Add(-time.Minute),
		ExpiresAt:     now.Add(time.Hour),
		Sources: []SourceBinding{
			{Protocol: ProtocolSFlow5, SourcePrefix: "192.0.2.0/24", TenantID: "tenant", ExporterID: "sflow", TargetID: "target", OwnershipEpoch: 1, SamplingMode: SamplingModeSampled, Enabled: true},
			{Protocol: ProtocolIPFIX, SourcePrefix: "2001:db8::/32", TenantID: "tenant", ExporterID: "ipfix", TargetID: "target", OwnershipEpoch: 1, SamplingMode: SamplingModeSampled, Enabled: true},
		},
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	netflowFamily := []Protocol{ProtocolNetFlow5, ProtocolNetFlow9, ProtocolIPFIX}
	if !registry.AdmitSourceFamily(netflowFamily, netip.MustParseAddr("2001:db8::1")) {
		t.Fatal("IPFIX source was rejected by the shared NetFlow/IPFIX listener")
	}
	if registry.AdmitSourceFamily(netflowFamily, netip.MustParseAddr("192.0.2.1")) {
		t.Fatal("sFlow-only source was admitted by the NetFlow/IPFIX listener")
	}
	if !registry.AdmitSourceFamily([]Protocol{ProtocolSFlow5}, netip.MustParseAddr("192.0.2.1")) {
		t.Fatal("sFlow source was rejected")
	}
	if registry.AdmitSourceFamily(netflowFamily, netip.MustParseAddr("198.51.100.1")) {
		t.Fatal("unknown source was admitted")
	}
}
